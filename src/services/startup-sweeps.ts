/**
 * The crash-recovery sweeps that run once at plugin start: remove stranded
 * compaction and aggregation temp files, restore or discard compaction
 * trash, and quarantine 0-byte parquet stubs. Each walks the store, which
 * on a large one (thousands of vessels, hundreds of thousands of
 * directories) takes tens of seconds and, run in the server process, holds
 * its main thread so the server answers nothing meanwhile.
 *
 * So they run in a short-lived forked worker (startup-sweep-worker.ts), the
 * same arrangement the daily aggregation uses. The caller awaits the result
 * as before, so the ordering guarantee stands: the sweeps finish before
 * DuckDB opens and before any new data subscription can write a file a
 * trash restore might clobber. If the worker cannot be forked the sweeps
 * run in-process, as they did before; if it dies or times out the plugin
 * starts without them and says so.
 */

import { fork, ChildProcess } from 'child_process';
import * as fs from 'fs-extra';
import * as path from 'path';
import { ServerAPI } from '@signalk/server-api';
import { quarantineEmptyParquetFiles } from '../parquet-writer';
import {
  cleanupStrandedCompactionTempFiles,
  recoverStrandedCompactionTrash,
} from './compaction-service';

export interface StartupSweepResult {
  /** Stranded temp files removed. */
  removed: number;
  /** Pre-publish compaction trash directories restored. */
  restored: number;
  /** Post-publish compaction trash directories removed. */
  cleaned: number;
  /** Trash directories that could not be handled. */
  failed: number;
  /** Undersized parquet files moved to quarantine. */
  quarantined: number;
  /** Undersized parquet files that could not be moved. */
  quarantineFailed: number;
  /** DuckDB spill files removed from the store's `.duckdb/tmp`. */
  duckdbTempRemoved: number;
  /** Whether every sweep ran to completion. */
  complete: boolean;
  durationMs: number;
}

/** The slice of the server API the sweeps use. */
export interface SweepLog {
  debug(msg: string): void;
  error(msg: string): void;
}

/** A result for a run that did nothing. */
export function emptySweepResult(): StartupSweepResult {
  return {
    removed: 0,
    restored: 0,
    cleaned: 0,
    failed: 0,
    quarantined: 0,
    quarantineFailed: 0,
    duckdbTempRemoved: 0,
    complete: false,
    durationMs: 0,
  };
}

/**
 * Remove whatever DuckDB left in the store's spill directory. Every DuckDB
 * instance this plugin opens (the server's pool, the aggregation and
 * compaction workers) is configured with `<dataDir>/.duckdb/tmp` as its
 * temp_directory; a worker killed at a timeout, or a crash, skips DuckDB's
 * own cleanup and its spill files stay behind. Discovery never looks in
 * `.duckdb`, so they cost disk, not answers. Runs before any pool opens, so
 * nothing here is in use.
 */
export async function cleanupDuckDbTempFiles(
  dataDir: string
): Promise<{ removed: number }> {
  const tmpDir = path.join(dataDir, '.duckdb', 'tmp');
  let entries: string[];
  try {
    entries = await fs.readdir(tmpDir);
  } catch {
    return { removed: 0 };
  }
  let removed = 0;
  for (const name of entries) {
    await fs.remove(path.join(tmpDir, name));
    removed += 1;
  }
  return { removed };
}

/**
 * Run the four sweeps in this process, in order. Each is isolated: one
 * failing is logged and the next still runs.
 */
export async function runStartupSweeps(
  dataDir: string,
  log: SweepLog
): Promise<StartupSweepResult> {
  const app = log as unknown as ServerAPI;
  const started = Date.now();
  const result = emptySweepResult();
  let complete = true;
  try {
    result.removed = (
      await cleanupStrandedCompactionTempFiles(app, dataDir)
    ).removed;
  } catch (err) {
    complete = false;
    log.error(`Compaction startup cleanup failed: ${(err as Error).message}`);
  }
  try {
    const trash = await recoverStrandedCompactionTrash(app, dataDir);
    result.restored = trash.restored;
    result.cleaned = trash.cleaned;
    result.failed = trash.failed;
  } catch (err) {
    complete = false;
    log.error(`Compaction trash recovery failed: ${(err as Error).message}`);
  }
  try {
    const sweep = await quarantineEmptyParquetFiles(app, dataDir);
    result.quarantined = sweep.quarantined;
    result.quarantineFailed = sweep.failed;
  } catch (err) {
    complete = false;
    log.error(`Empty parquet startup sweep failed: ${(err as Error).message}`);
  }
  try {
    result.duckdbTempRemoved = (await cleanupDuckDbTempFiles(dataDir)).removed;
  } catch (err) {
    complete = false;
    log.error(`DuckDB temp cleanup failed: ${(err as Error).message}`);
  }
  result.complete = complete;
  result.durationMs = Date.now() - started;
  return result;
}

/** Messages the worker sends back. */
export type SweepWorkerMessage =
  | { type: 'log'; level: 'debug' | 'error'; msg: string }
  | { type: 'result'; result: StartupSweepResult }
  | { type: 'error'; message: string };

/** How long a sweep worker may run before it is killed and start goes on. */
const WORKER_TIMEOUT_MS = 10 * 60 * 1000;

/**
 * Run the sweeps in a forked worker and resolve with their result. Never
 * rejects: a worker that cannot be forked falls back to running in-process,
 * one that dies or times out resolves with an empty, incomplete result.
 */
export function runStartupSweepsInWorker(
  app: SweepLog,
  dataDir: string,
  activeWorkers?: Set<ChildProcess>
): Promise<StartupSweepResult> {
  return new Promise(resolve => {
    const workerPath = path.join(__dirname, '..', 'startup-sweep-worker.js');
    let child: ChildProcess;
    try {
      child = fork(workerPath);
    } catch (err) {
      app.error(
        `[StartupSweep] Could not fork the sweep worker (${(err as Error).message}); running the sweeps in-process`
      );
      resolve(runStartupSweeps(dataDir, app));
      return;
    }
    activeWorkers?.add(child);
    let settled = false;
    let heard = false;

    const finish = (result: StartupSweepResult): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      activeWorkers?.delete(child);
      resolve(result);
    };
    const abandon = (reason: string): void => {
      if (settled) return;
      app.error(
        `[StartupSweep] Sweep worker ${reason}; starting without the crash-recovery sweeps`
      );
      try {
        child.kill('SIGKILL');
      } catch {
        // already gone
      }
      finish(emptySweepResult());
    };
    const timer = setTimeout(() => abandon('timed out'), WORKER_TIMEOUT_MS);

    child.on('message', (msg: SweepWorkerMessage) => {
      if (!msg || typeof msg !== 'object') return;
      heard = true;
      if (msg.type === 'log') {
        if (msg.level === 'error') app.error(msg.msg);
        else app.debug(msg.msg);
      } else if (msg.type === 'result') {
        finish(msg.result);
      } else if (msg.type === 'error') {
        abandon(`failed: ${msg.message}`);
      }
    });
    child.on('exit', code => {
      activeWorkers?.delete(child);
      if (!settled) abandon(`exited early (code ${code})`);
    });
    child.on('error', err => {
      activeWorkers?.delete(child);
      if (settled) return;
      if (!heard) {
        // Spawn failure: nothing ran, so run it here.
        settled = true;
        clearTimeout(timer);
        app.error(
          `[StartupSweep] Sweep worker could not start (${err.message}); running the sweeps in-process`
        );
        resolve(runStartupSweeps(dataDir, app));
        return;
      }
      abandon(`errored: ${err.message}`);
    });

    child.send({ dataDir });
  });
}
