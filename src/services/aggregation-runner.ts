/**
 * Run one date's aggregation in a short-lived forked process.
 *
 * The aggregation makes tens of thousands of DuckDB `read_parquet` calls whose
 * memory the allocator never returns to the OS in-process, so doing it in the
 * long-lived server ratchets ~¾ GB that sticks until restart (the midnight
 * OOM). A forked worker (aggregation-worker.ts) does the identical work and
 * EXITS, so the OS reclaims all of it. The worker's results are returned
 * unchanged; a crash/timeout is surfaced as a failed result so the caller
 * skips retention cleanup (as it already does when in-process aggregation
 * fails).
 *
 * Used by the daily export and by the startup catch-up, which re-aggregates
 * each day it exported late (#142).
 */
import { fork, ChildProcess } from 'child_process';
import * as path from 'path';
import { ServerAPI } from '@signalk/server-api';
import { AggregationConfig, AggregationResult } from './aggregation-service';

export interface AggregationWorkerInput {
  config: AggregationConfig;
  dataDir: string;
  dateISO: string;
  /** Path names the live server reports as angular (units === 'rad'). */
  angularPathNames: string[];
}

export interface AggregationWorkerOptions {
  /** Log prefix, naming the caller. */
  label?: string;
  /** The worker script; the built aggregation-worker.js when omitted. */
  workerPath?: string;
  /**
   * Node flags for the worker, e.g. a loader so a test can fork the .ts.
   * Omitted, the worker inherits the server's.
   */
  execArgv?: string[];
}

export function runAggregationInWorker(
  input: AggregationWorkerInput,
  app: Pick<ServerAPI, 'debug' | 'error'>,
  activeWorkers?: Set<ChildProcess>,
  options: AggregationWorkerOptions = {}
): Promise<AggregationResult[]> {
  const label = options.label ?? '[DailyExport]';
  return new Promise(resolve => {
    const workerPath =
      options.workerPath ?? path.join(__dirname, '..', 'aggregation-worker.js');
    // Without execArgv the worker inherits the server's own Node flags, as it
    // always has.
    const child = options.execArgv
      ? fork(workerPath, [], { execArgv: options.execArgv })
      : fork(workerPath);
    activeWorkers?.add(child);
    let settled = false;
    const TIMEOUT_MS = 30 * 60 * 1000;

    const finishFailure = (reason: string): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      app.error(`${label} Aggregation worker ${reason}`);
      try {
        child.kill('SIGKILL');
      } catch {
        // already gone
      }
      resolve([
        {
          sourceTier: 'raw',
          targetTier: '5s',
          filesProcessed: 0,
          recordsAggregated: 0,
          filesCreated: 0,
          duration: 0,
          errors: [`aggregation worker ${reason}`],
        },
      ]);
    };

    const timer = setTimeout(() => finishFailure('timed out'), TIMEOUT_MS);

    child.on(
      'message',
      (msg: {
        type?: string;
        level?: string;
        msg?: string;
        message?: string;
        results?: AggregationResult[];
      }) => {
        if (!msg || typeof msg !== 'object') return;
        if (msg.type === 'log') {
          if (msg.level === 'error') app.error(msg.msg || '');
          else app.debug(msg.msg || '');
        } else if (msg.type === 'result') {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          resolve(msg.results || []);
        } else if (msg.type === 'error') {
          finishFailure(`errored: ${msg.message}`);
        }
      }
    );

    child.on('exit', code => {
      activeWorkers?.delete(child);
      if (!settled) finishFailure(`exited early (code ${code})`);
    });
    child.on('error', err => {
      activeWorkers?.delete(child);
      finishFailure(`spawn error: ${err.message}`);
    });

    child.send(input);
  });
}
