/**
 * Daily aggregation worker.
 *
 * The daily aggregation runs tens of thousands of DuckDB `read_parquet` calls
 * over yesterday's tree. DuckDB's allocator retains that memory and never
 * returns it to the OS within the process (proven: closeSync + GC reclaims
 * nothing), so running it inside the long-lived SignalK process ratchets ~¾ GB
 * that sticks until the server is restarted — the midnight OOM.
 *
 * Running it here, in a short-lived forked process that EXITS when done, is the
 * only reliable way to give that memory back: process exit returns everything
 * to the OS regardless of allocator behaviour. The aggregation logic, tiers,
 * and output are byte-for-byte identical to running it in-process — only the
 * host process changes.
 *
 * Protocol (parent ⇄ this worker, over child_process IPC):
 *   parent → worker: one WorkerInput message with config + date + angular paths.
 *   worker → parent: {type:'log'} lines (relayed to app.debug/error),
 *                    then exactly one {type:'result'} or {type:'error'}, then exit.
 */
import { ServerAPI } from '@signalk/server-api';
import { DuckDBPool } from './utils/duckdb-pool';
import {
  AggregationService,
  AggregationConfig,
  AggregationResult,
} from './services/aggregation-service';

interface WorkerInput {
  config: AggregationConfig;
  dataDir: string;
  dateISO: string;
  // Path names the live server reports as angular (units === 'rad'). Computed
  // in the parent because the worker has no live SignalK metadata. Used to
  // reproduce isAngularPath() exactly via the shim app below.
  angularPathNames: string[];
}

function send(msg: unknown): void {
  process.send?.(msg);
}

async function run(input: WorkerInput): Promise<void> {
  const angular = new Set(input.angularPathNames);

  // Minimal ServerAPI shim: AggregationService only uses debug/error and (via
  // isAngularPath) getMetadata. getMetadata is answered from the parent-computed
  // angular set — isAngularPath calls getMetadata(`${context}.${path}`) and
  // checks units==='rad', so returning that shape for angular paths reproduces
  // the live server's decision exactly.
  const shimApp = {
    debug: (m: string) => send({ type: 'log', level: 'debug', msg: m }),
    error: (m: string) => send({ type: 'log', level: 'error', msg: m }),
    getMetadata: (fullPath: string) => {
      for (const p of angular) {
        if (fullPath === p || fullPath.endsWith(`.${p}`)) {
          return { units: 'rad' };
        }
      }
      return undefined;
    },
  } as unknown as ServerAPI;

  try {
    // Fresh DuckDB instance, home dir under the writable data directory. All of
    // its accumulated catalog/allocator state dies with this process on exit.
    await DuckDBPool.initialize(input.dataDir);

    const service = new AggregationService(input.config, shimApp);
    const results: AggregationResult[] = await service.aggregateDate(
      new Date(input.dateISO)
    );

    send({ type: 'result', results });
  } catch (err) {
    send({ type: 'error', message: (err as Error).message });
    process.exitCode = 1;
  } finally {
    // Give the IPC message a tick to flush, then exit so the OS reclaims the
    // aggregation's memory. Do not await DuckDB shutdown — exit frees it all.
    setTimeout(() => process.exit(process.exitCode ?? 0), 50);
  }
}

process.once('message', (input: WorkerInput) => {
  void run(input);
});
