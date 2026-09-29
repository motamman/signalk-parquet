/**
 * The plugin's side of the Track API worker (track-worker.ts).
 *
 * `TrackWorkerClient` owns the forked process: start, one promise per request,
 * close. `WorkerTrackApi` is what the server's Track API registry is given: it
 * sends each call to the worker when one is up and answers in-process when not,
 * so a worker that has not started yet, failed to start or died costs the old
 * behaviour, never an error.
 *
 * Failure rules, as the buffer worker had them: a worker that goes away
 * rejects every request still waiting on it and is never restarted, and a
 * request whose outcome is unknown is never replayed.
 */

import { fork, ChildProcess } from 'child_process';
import * as path from 'path';
import { Context, ServerAPI } from '@signalk/server-api';
import {
  TrackApi,
  TrackProvider,
  TracksRequest,
  TracksResponse,
  contextNameFor,
  durationToMillis,
  instantToMillis,
} from '../track-provider';
import { isAngularPath } from './angular-paths';
import {
  TrackWorkerInbound,
  TrackWorkerOutbound,
} from './track-worker-protocol';

/**
 * How long close() waits for the worker to exit before killing it. Closing a
 * read-only connection and exiting takes milliseconds; the bound exists so a
 * worker stuck inside a query cannot hold up plugin.stop().
 */
const EXIT_GRACE_MS = 5000;

/**
 * How long a request may wait for its answer before the worker is taken as
 * hung, failed and killed, so later calls answer in-process. A bound on a
 * stuck process, not a performance target: the worker is never restarted, so
 * it sits well above any real query.
 */
const REQUEST_TIMEOUT_MS = 60000;

/** Thrown when the worker goes away; never retried, by design. */
export class TrackWorkerGoneError extends Error {}

export interface TrackWorkerClientOptions {
  dataDir: string;
  dbPath?: string;
  selfId: string;
  log?: (level: 'debug' | 'error', msg: string) => void;
  /** Overridden by the tests to fork the TypeScript source; dist/track-worker.js by default. */
  workerPath?: string;
  /** Extra node arguments for the fork (a TypeScript loader in tests). */
  workerExecArgv?: string[];
  /** Overridden by the tests; REQUEST_TIMEOUT_MS by default. */
  requestTimeoutMs?: number;
}

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timer: NodeJS.Timeout;
}

export class TrackWorkerClient {
  private child?: ChildProcess;
  private readonly pending = new Map<number, PendingRequest>();
  private nextId = 1;
  private ready = false;
  private gone?: Error;
  private readyPromise?: Promise<void>;
  private exited?: Promise<void>;

  constructor(private readonly options: TrackWorkerClientOptions) {}

  /** Fork the worker and wait for it to report ready. */
  start(): Promise<void> {
    if (this.readyPromise) return this.readyPromise;
    const workerPath =
      this.options.workerPath ?? path.join(__dirname, '..', 'track-worker.js');

    this.readyPromise = new Promise<void>((resolve, reject) => {
      let child: ChildProcess;
      try {
        child = fork(workerPath, [], {
          execArgv: this.options.workerExecArgv ?? [],
          // The V8 serializer: the answer is arrays of numbers and strings,
          // and it is the plugin's thread that decodes it.
          serialization: 'advanced',
        });
      } catch (err) {
        this.fail(new TrackWorkerGoneError((err as Error).message));
        reject(err as Error);
        return;
      }
      this.child = child;
      this.exited = new Promise<void>(done => child.once('exit', () => done()));

      let settled = false;
      const settle = (error?: Error): void => {
        if (settled) return;
        settled = true;
        if (error) reject(error);
        else resolve();
      };

      child.on('message', (msg: TrackWorkerOutbound) => {
        if (!msg || typeof msg !== 'object') return;
        switch (msg.type) {
          case 'ready':
            this.ready = true;
            settle();
            return;
          case 'fatal':
            settle(new Error(msg.message));
            this.fail(new TrackWorkerGoneError(msg.message));
            return;
          case 'log':
            this.options.log?.(msg.level, msg.msg);
            return;
          case 'reply':
          case 'error': {
            const p = this.pending.get(msg.id);
            if (!p) return;
            this.pending.delete(msg.id);
            clearTimeout(p.timer);
            if (msg.type === 'reply') p.resolve(msg.value);
            else p.reject(new Error(msg.message));
            return;
          }
        }
      });
      child.on('error', err => {
        settle(err);
        this.fail(new TrackWorkerGoneError(err.message));
      });
      child.on('exit', (code, signal) => {
        const why = `track worker exited (${signal ?? `code ${code}`})`;
        settle(new Error(why));
        this.fail(new TrackWorkerGoneError(why));
      });

      this.post({
        type: 'init',
        dataDir: this.options.dataDir,
        dbPath: this.options.dbPath,
        selfId: this.options.selfId,
      });
    });
    return this.readyPromise;
  }

  /** Up and answering: reported ready, and not gone since. */
  isAlive(): boolean {
    return this.ready && this.gone === undefined;
  }

  request(
    method: 'getTracks' | 'getTrackContexts',
    query: TracksRequest,
    angular: Record<string, boolean>
  ): Promise<unknown> {
    if (this.gone) return Promise.reject(this.gone);
    if (!this.child || !this.ready) {
      return Promise.reject(new Error('track worker is not ready'));
    }
    const id = this.nextId++;
    const timeoutMs = this.options.requestTimeoutMs ?? REQUEST_TIMEOUT_MS;
    return new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        // A worker that does not answer is gone: fail() rejects this and
        // every other waiting request, and the kill stops the stuck query.
        const child = this.child;
        this.fail(
          new TrackWorkerGoneError(
            `track worker request ${id} timed out after ${timeoutMs} ms`
          )
        );
        try {
          child?.kill('SIGKILL');
        } catch {
          // already gone
        }
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      this.post({ type: 'request', id, method, query, angular });
    });
  }

  /**
   * Stop the worker, whether it is up, still starting or already gone. Waits
   * for it to exit, and kills it if it has not within EXIT_GRACE_MS.
   */
  async close(): Promise<void> {
    const child = this.child;
    this.fail(new TrackWorkerGoneError('track worker closed'));
    if (!child || child.exitCode !== null || child.signalCode !== null) return;
    this.post({ type: 'shutdown' }, child);
    let timer: NodeJS.Timeout | undefined;
    const killed = new Promise<void>(resolve => {
      timer = setTimeout(() => {
        try {
          child.kill('SIGKILL');
        } catch {
          // already gone
        }
        resolve();
      }, EXIT_GRACE_MS);
    });
    await Promise.race([this.exited, killed]);
    clearTimeout(timer);
  }

  private post(msg: TrackWorkerInbound, child = this.child): void {
    if (!child || !child.connected) return;
    try {
      child.send(msg);
    } catch {
      // The channel closed under us; the 'exit' handler reports it.
    }
  }

  /** The worker is gone: reject everything outstanding and refuse new work. */
  private fail(error: Error): void {
    if (this.gone) return;
    this.gone = error;
    for (const [, p] of this.pending) {
      clearTimeout(p.timer);
      p.reject(error);
    }
    this.pending.clear();
  }
}

/**
 * The request as it crosses to the worker. Instants become epoch milliseconds
 * and durations `{ milliseconds }`, both of which the provider accepts as
 * they are; the server passes Temporal objects, whose fields do not survive a
 * structured clone. Converting here also means an invalid instant or duration
 * is refused on this side with the provider's own error.
 */
export function toWireRequest(query: TracksRequest): TracksRequest {
  const wire: TracksRequest = { ...query };
  if (query.from !== undefined) wire.from = instantToMillis(query.from);
  if (query.to !== undefined) wire.to = instantToMillis(query.to);
  if (query.duration !== undefined) {
    wire.duration = { milliseconds: durationToMillis(query.duration) };
  }
  if (query.resolution !== undefined) {
    wire.resolution = { milliseconds: durationToMillis(query.resolution) };
  }
  return wire;
}

/**
 * The Track API as registered with the server: through the worker when one
 * is up, in-process otherwise.
 */
export class WorkerTrackApi implements TrackApi {
  private readonly selfContext: Context;

  constructor(
    private readonly inProcess: TrackProvider,
    private readonly worker: () => TrackWorkerClient | undefined,
    private readonly app: ServerAPI,
    selfId: string
  ) {
    this.selfContext = `vessels.${selfId}` as Context;
  }

  async getTracks(query: TracksRequest): Promise<TracksResponse> {
    const client = this.live();
    if (!client) return this.inProcess.getTracks(query);
    const response = (await client.request(
      'getTracks',
      toWireRequest(query),
      this.angularFlags(query.properties)
    )) as TracksResponse;
    // The worker has no server to ask for names.
    for (const feature of response.features) {
      feature.properties.contextName = contextNameFor(
        this.app,
        this.selfContext,
        feature.properties.context
      );
    }
    return response;
  }

  async getTrackContexts(query: TracksRequest): Promise<Context[]> {
    const client = this.live();
    if (!client) return this.inProcess.getTrackContexts(query);
    return (await client.request(
      'getTrackContexts',
      toWireRequest(query),
      {}
    )) as Context[];
  }

  private live(): TrackWorkerClient | undefined {
    const client = this.worker();
    return client?.isAlive() ? client : undefined;
  }

  /**
   * Whether each requested property holds angles, asked of the server's
   * metadata. Asked under the own vessel's context, as the daily aggregation
   * asks: the schema matches every vessel context with a wildcard, so the
   * answer does not depend on which vessel the property is read for.
   */
  private angularFlags(properties: TracksRequest['properties']) {
    const flags: Record<string, boolean> = {};
    for (const p of properties ?? []) {
      const signalkPath = String(p);
      flags[signalkPath] = isAngularPath(
        signalkPath,
        this.app,
        this.selfContext
      );
    }
    return flags;
  }
}
