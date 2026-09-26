/**
 * The main thread's side of the buffer worker.
 *
 * Every method is a message and a promise. The only row-shaped thing that
 * crosses the boundary is a columnar page whose buffers are transferred, so
 * receiving one costs this thread a fraction of a millisecond regardless of how
 * many rows it holds; see `buffer-columnar.ts` for the measurements that forced
 * that shape.
 *
 * `scan()` is an async iterable of pages. Each page is acknowledged when the
 * consumer comes back for the next one, which is what bounds how far ahead the
 * worker may read: a consumer that stops consuming stops the worker reading.
 *
 * Failure rules, deliberately blunt because a buffer that quietly answers short
 * is worse than one that fails: a scan that errors part-way throws into the
 * consumer, and a worker that dies rejects every pending request and every open
 * scan rather than retrying anything.
 */

import { Worker } from 'node:worker_threads';
import * as path from 'path';
import { ColumnReader, ColumnarPage, readPage } from './buffer-columnar';
import {
  BufferWorkerMessage,
  BufferWorkerRequest,
} from './buffer-worker-protocol';

/** One page as the consumer sees it: readers in the table's column order. */
export interface ScanPage {
  rowCount: number;
  columns: ColumnReader[];
  /** Time the worker spent reading this page. For measurement, not for logic. */
  readMs: number;
}

export interface BufferWorkerClientOptions {
  dbPath: string;
  retentionHours?: number;
  /** Overridden by the tests to point at the built worker; defaults beside this file. */
  workerPath?: string;
  /** Worker `execArgv`, so a test runner's loader flags do not leak in. */
  workerExecArgv?: string[];
}

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
}

interface OpenScan {
  /** Pages received and not yet handed to the consumer. */
  queued: ScanPage[];
  /** Waiting consumer, if any. */
  waiting?: {
    resolve: (page: ScanPage | null) => void;
    reject: (error: Error) => void;
  };
  ended: boolean;
  failure?: Error;
}

/** Thrown when the worker goes away; never retried, by design. */
export class BufferWorkerGoneError extends Error {}

export class BufferWorkerClient {
  private worker?: Worker;
  private readonly pending = new Map<number, PendingRequest>();
  private readonly scans = new Map<number, OpenScan>();
  private nextId = 1;
  private gone?: Error;
  private readyPromise?: Promise<void>;

  constructor(private readonly options: BufferWorkerClientOptions) {}

  /** Start the worker and wait for it to report the database open. */
  start(): Promise<void> {
    if (this.readyPromise) return this.readyPromise;
    const workerPath =
      this.options.workerPath ?? path.join(__dirname, '..', 'buffer-worker.js');

    this.readyPromise = new Promise<void>((resolve, reject) => {
      const worker = new Worker(workerPath, {
        workerData: {
          dbPath: this.options.dbPath,
          retentionHours: this.options.retentionHours,
        },
        execArgv: this.options.workerExecArgv,
      });
      this.worker = worker;

      let settled = false;
      worker.on('message', (msg: BufferWorkerMessage) => {
        if (msg.type === 'ready') {
          if (!settled) {
            settled = true;
            resolve();
          }
          return;
        }
        if (msg.type === 'fatal') {
          if (!settled) {
            settled = true;
            reject(new Error(msg.message));
          }
          this.fail(new BufferWorkerGoneError(msg.message));
          return;
        }
        this.receive(msg);
      });
      worker.on('error', (error: Error) => {
        if (!settled) {
          settled = true;
          reject(error);
        }
        this.fail(new BufferWorkerGoneError(error.message));
      });
      worker.on('exit', code => {
        if (!settled) {
          settled = true;
          reject(new Error(`buffer worker exited with code ${code}`));
        }
        this.fail(
          new BufferWorkerGoneError(`buffer worker exited with code ${code}`)
        );
      });
    });
    return this.readyPromise;
  }

  /** The declared column schema of a path's table, or null when there is none. */
  async getTableSchema(
    signalkPath: string
  ): Promise<Array<{ name: string; type: string }> | null> {
    return (await this.request({ op: 'schema', id: 0, signalkPath })) as Array<{
      name: string;
      type: string;
    }> | null;
  }

  /** A round trip that does nothing, for liveness and latency. */
  async ping(): Promise<void> {
    await this.request({ op: 'ping', id: 0 });
  }

  /**
   * Pages of one path, context and window, in `(signalk_timestamp, id)` order.
   * Yields nothing at all when the path has no table.
   *
   * Abandoning the iteration early (a `break`, or a throw downstream) closes
   * the scan in the worker, so a consumer that gives up does not leave it
   * reading.
   */
  async *scan(args: {
    signalkPath: string;
    context: string;
    fromIso: string;
    toIso: string;
    pageRows: number;
  }): AsyncGenerator<ScanPage, void, void> {
    const scanId = (await this.request({
      op: 'openScan',
      id: 0,
      ...args,
    })) as number | null;
    if (scanId === null) return;

    const state: OpenScan = { queued: [], ended: false };
    this.scans.set(scanId, state);
    try {
      for (;;) {
        const page = await this.nextPage(scanId, state);
        if (page === null) return;
        yield page;
        // The consumer is done with that page and has come back for another:
        // release the worker to read one more.
        this.post({ op: 'ackScan', scanId });
      }
    } finally {
      if (this.scans.delete(scanId) && !state.ended && !this.gone) {
        this.post({ op: 'closeScan', scanId });
      }
    }
  }

  /** Close the database in the worker and stop the thread. */
  async close(): Promise<void> {
    if (!this.worker || this.gone) return;
    try {
      await this.request({ op: 'close', id: 0 });
    } catch {
      // Already gone; the terminate below is what matters.
    }
    const worker = this.worker;
    this.worker = undefined;
    await worker.terminate();
    this.fail(new BufferWorkerGoneError('buffer worker closed'));
  }

  // -- internals ------------------------------------------------------------

  private nextPage(scanId: number, state: OpenScan): Promise<ScanPage | null> {
    if (state.failure) return Promise.reject(state.failure);
    if (state.queued.length > 0) {
      return Promise.resolve(state.queued.shift() as ScanPage);
    }
    if (state.ended) return Promise.resolve(null);
    if (this.gone) return Promise.reject(this.gone);
    return new Promise<ScanPage | null>((resolve, reject) => {
      state.waiting = { resolve, reject };
    });
  }

  private receive(msg: BufferWorkerMessage): void {
    switch (msg.type) {
      case 'reply': {
        const p = this.pending.get(msg.id);
        if (!p) return;
        this.pending.delete(msg.id);
        p.resolve(msg.value);
        return;
      }
      case 'error': {
        const p = this.pending.get(msg.id);
        if (!p) return;
        this.pending.delete(msg.id);
        p.reject(new Error(msg.message));
        return;
      }
      case 'page': {
        const state = this.scans.get(msg.scanId);
        if (!state) return; // Page for a scan the consumer abandoned.
        const page = this.toScanPage(msg.page, msg.readMs);
        const waiting = state.waiting;
        if (waiting) {
          state.waiting = undefined;
          waiting.resolve(page);
        } else {
          state.queued.push(page);
        }
        return;
      }
      case 'end': {
        const state = this.scans.get(msg.scanId);
        if (!state) return;
        state.ended = true;
        const waiting = state.waiting;
        if (waiting && state.queued.length === 0) {
          state.waiting = undefined;
          waiting.resolve(null);
        }
        return;
      }
      case 'scanError': {
        const state = this.scans.get(msg.scanId);
        if (!state) return;
        const error = new Error(msg.message);
        state.failure = error;
        const waiting = state.waiting;
        if (waiting) {
          state.waiting = undefined;
          waiting.reject(error);
        }
        return;
      }
      default:
        return;
    }
  }

  private toScanPage(page: ColumnarPage, readMs: number): ScanPage {
    return { rowCount: page.rowCount, columns: readPage(page), readMs };
  }

  private request(msg: BufferWorkerRequest): Promise<unknown> {
    if (this.gone) return Promise.reject(this.gone);
    if (!this.worker) {
      return Promise.reject(new Error('buffer worker has not been started'));
    }
    const id = this.nextId++;
    return new Promise<unknown>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      (this.worker as Worker).postMessage({ ...msg, id });
    });
  }

  /** Fire and forget: acks and scan closes carry no reply. */
  private post(msg: BufferWorkerRequest): void {
    if (this.gone || !this.worker) return;
    this.worker.postMessage(msg);
  }

  /**
   * The worker is gone. Reject everything outstanding and refuse new work:
   * a request whose outcome is unknown is never replayed.
   */
  private fail(error: Error): void {
    if (this.gone) return;
    this.gone = error;
    for (const [, p] of this.pending) p.reject(error);
    this.pending.clear();
    for (const [, state] of this.scans) {
      state.failure = error;
      const waiting = state.waiting;
      if (waiting) {
        state.waiting = undefined;
        waiting.reject(error);
      }
    }
  }
}
