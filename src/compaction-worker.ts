/**
 * Compaction worker.
 *
 * A compaction is one DuckDB COPY per (tier, context, path, year) group over
 * every day file of that year. Measured on a test server, 2026-09-17: 22
 * groups, 62 files, 477 MB merged in 1.5 s, and the process that ran it grew
 * from 138 MB to 1,133 MB and kept 1,061 MB of that until its DuckDB
 * instance was closed. The server's pooled instance is never closed, so in
 * the server that memory stays until restart, exactly as the daily
 * aggregation's did before it moved into aggregation-worker.ts.
 *
 * Running here, in a short-lived forked process that EXITS when done, gives
 * the memory back by process exit. The merge logic, outputs and safety
 * (temp file, trash, publish rename) are the in-process CompactionService's,
 * unchanged; only the host process differs.
 *
 * Protocol (parent ⇄ this worker, over child_process IPC):
 *   parent → worker: one WorkerInput with the config and data directory,
 *                    optionally followed by {type:'shutdown'} (cancel at the
 *                    next group boundary; the in-flight COPY completes).
 *   worker → parent: {type:'log'} lines, {type:'progress'} snapshots of the
 *                    job's CompactionProgress, then exactly one
 *                    {type:'result', progress} or {type:'error'}.
 *   parent → worker: {type:'ack'} once that terminal message has settled the
 *                    parent's job table; the worker exits on it (or after
 *                    ACK_TIMEOUT_MS, or on disconnect, if no ack comes), so
 *                    the parent never sees the exit before the result.
 */
import { ServerAPI } from '@signalk/server-api';
import { DuckDBPool } from './utils/duckdb-pool';
import {
  CompactionConfig,
  CompactionProgress,
  CompactionService,
} from './services/compaction-service';

export interface CompactionWorkerInput {
  config: CompactionConfig;
  dataDir: string;
}

export type CompactionWorkerMessage =
  | { type: 'log'; level: 'debug' | 'error'; msg: string }
  | { type: 'progress'; progress: CompactionProgress }
  | { type: 'result'; progress: CompactionProgress }
  | { type: 'error'; message: string };

const PROGRESS_INTERVAL_MS = 250;
const ACK_TIMEOUT_MS = 5000;

function send(msg: CompactionWorkerMessage): void {
  process.send?.(msg);
}

function exit(): void {
  // Exit is what returns the memory, so DuckDB is not shut down first.
  process.exit(process.exitCode ?? 0);
}

/**
 * Send the terminal message and exit once the parent has acknowledged it.
 * process.send's callback only says the message was written to the channel,
 * not that the parent has handled it, so without the ack the parent's
 * 'exit' handler could run first and record a settled job as exited early.
 */
function exitAfter(msg: CompactionWorkerMessage): void {
  if (!process.send || !process.connected) return exit();
  const fallback = setTimeout(exit, ACK_TIMEOUT_MS);
  process.once('disconnect', exit);
  process.on('message', (reply: { type?: string }) => {
    if (reply && typeof reply === 'object' && reply.type === 'ack') {
      clearTimeout(fallback);
      exit();
    }
  });
  try {
    process.send(msg, (err: Error | null) => {
      if (err) exit();
    });
  } catch {
    exit();
  }
}

let service: CompactionService | undefined;
let jobId: string | undefined;
let shutdownRequested = false;

async function run(input: CompactionWorkerInput): Promise<void> {
  const shimApp = {
    debug: (m: string) => send({ type: 'log', level: 'debug', msg: m }),
    error: (m: string) => send({ type: 'log', level: 'error', msg: m }),
  } as unknown as ServerAPI;

  try {
    // Fresh DuckDB instance under the writable data directory; everything it
    // allocates dies with this process.
    await DuckDBPool.initialize(input.dataDir);
    service = new CompactionService(shimApp, { inProcess: true });
    jobId = await service.compact(input.config);
    if (shutdownRequested) service.cancel(jobId);

    let progress = service.getProgress(jobId);
    while (
      progress &&
      (progress.status === 'scanning' || progress.status === 'running')
    ) {
      send({ type: 'progress', progress });
      await new Promise(resolve => setTimeout(resolve, PROGRESS_INTERVAL_MS));
      progress = service.getProgress(jobId);
    }
    if (!progress) {
      process.exitCode = 1;
      exitAfter({ type: 'error', message: 'compaction job vanished' });
    } else {
      if (progress.status === 'error') process.exitCode = 1;
      exitAfter({ type: 'result', progress });
    }
  } catch (err) {
    process.exitCode = 1;
    exitAfter({ type: 'error', message: (err as Error).message });
  }
}

let started = false;
process.on('message', (msg: CompactionWorkerInput | { type: string }) => {
  if (msg && typeof msg === 'object' && 'type' in msg) {
    if (msg.type === 'shutdown') {
      shutdownRequested = true;
      if (service && jobId) service.cancel(jobId);
    }
    return;
  }
  if (started) return;
  started = true;
  void run(msg);
});
