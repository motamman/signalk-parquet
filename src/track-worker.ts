/**
 * Track API worker.
 *
 * Answers `getTracks` and `getTrackContexts` in a forked process so that none
 * of a track query runs on the server's event loop: not the SQLite read of the
 * buffer, not the append of those rows into DuckDB, not the parquet walk. The
 * plugin sends a request and receives the finished answer.
 *
 * It runs the plugin's own TrackProvider, unchanged, against its own DuckDB
 * instance and its own read-only connection to buffer.db. Two node:sqlite
 * connections to one database are safe (one library, coherent locks; see
 * sqlite-buffer.ts), and a read-only one never contends for the write lock the
 * ingestion connection holds. What it gives up is the live server: vessel names
 * and property units come from the plugin (see track-worker-client.ts).
 *
 * A separate process rather than a worker thread because of what 1.0.0
 * measured: DuckDB's native allocations stay with the process that made them,
 * and the aggregation and compaction workers exist so that memory is not the
 * server's. A thread would put this instance's memory back in the server.
 *
 * Protocol: utils/track-worker-protocol.ts.
 */

import { Context, ServerAPI } from '@signalk/server-api';
import { DuckDBPool } from './utils/duckdb-pool';
import { SQLiteBuffer } from './utils/sqlite-buffer';
import { TrackBufferSource, TrackProvider } from './track-provider';
import {
  TrackWorkerInbound,
  TrackWorkerInit,
  TrackWorkerOutbound,
  TrackWorkerRequest,
} from './utils/track-worker-protocol';

function send(msg: TrackWorkerOutbound): void {
  process.send?.(msg);
}

function debug(msg: string): void {
  send({ type: 'log', level: 'debug', msg });
}

let provider: TrackProvider | undefined;
let buffer: SQLiteBuffer | undefined;
let stopping = false;

/**
 * Units per property path as the plugin last reported them. A path's units
 * come from the Signal K schema, so concurrent requests report the same
 * answer for the same path and a later report only ever confirms an earlier
 * one.
 */
const angular = new Map<string, boolean>();

/**
 * The buffer as TrackProvider reads it, through this worker's read-only
 * connection. That connection loads its table map once, at open, so a path
 * first recorded since then is reloaded before the provider asks about it;
 * without that the provider would answer "no buffer table" for rows that are
 * there.
 */
function readOnlySource(b: SQLiteBuffer): TrackBufferSource {
  return {
    isOpen: () => b.isOpen(),
    hasTable: signalkPath => {
      b.loadTableIfMissing(signalkPath);
      return b.hasTable(signalkPath);
    },
    getTableColumns: signalkPath => {
      b.loadTableIfMissing(signalkPath);
      return b.getTableColumns(signalkPath);
    },
    getTableSchema: signalkPath => {
      b.loadTableIfMissing(signalkPath);
      return b.getTableSchema(signalkPath);
    },
    getRowsForFederation: (
      signalkPath,
      context,
      fromIso,
      toIso,
      after,
      limit
    ) =>
      b.getRowsForFederation(
        signalkPath,
        context,
        fromIso,
        toIso,
        after,
        limit
      ),
  };
}

async function init(msg: TrackWorkerInit): Promise<void> {
  try {
    // Same directory as the server's pool, so the extensions it cached are
    // found here and nothing is downloaded.
    await DuckDBPool.initialize(msg.dataDir, m =>
      send({ type: 'log', level: 'error', msg: m })
    );
    if (msg.dbPath) {
      buffer = new SQLiteBuffer({ dbPath: msg.dbPath, readOnly: true });
    }
    const shimApp = {
      debug,
      error: (m: string) => send({ type: 'log', level: 'error', msg: m }),
    } as unknown as ServerAPI;
    provider = new TrackProvider(
      msg.selfId,
      msg.dataDir,
      shimApp,
      debug,
      buffer ? readOnlySource(buffer) : undefined,
      {
        isAngular: (signalkPath: string, _context: Context) =>
          angular.get(signalkPath) ?? false,
      }
    );
    send({ type: 'ready' });
  } catch (err) {
    send({ type: 'fatal', message: (err as Error).message });
    exit(1);
  }
}

async function handle(msg: TrackWorkerRequest): Promise<void> {
  if (!provider) {
    send({ type: 'error', id: msg.id, message: 'track worker is not ready' });
    return;
  }
  for (const [signalkPath, isAngular] of Object.entries(msg.angular)) {
    angular.set(signalkPath, isAngular);
  }
  try {
    const value =
      msg.method === 'getTracks'
        ? await provider.getTracks(msg.query)
        : await provider.getTrackContexts(msg.query);
    send({ type: 'reply', id: msg.id, value });
  } catch (err) {
    send({ type: 'error', id: msg.id, message: (err as Error).message });
  }
}

/** Exit is what returns DuckDB's memory, so DuckDB is not shut down first. */
function exit(code = 0): void {
  if (buffer?.isOpen()) {
    try {
      buffer.close();
    } catch {
      // Closing a read-only connection that is already gone is not worth
      // failing the exit on.
    }
  }
  process.exit(code);
}

process.on('message', (msg: TrackWorkerInbound) => {
  if (!msg || typeof msg !== 'object') return;
  switch (msg.type) {
    case 'init':
      if (!provider && !stopping) void init(msg);
      return;
    case 'request':
      if (!stopping) void handle(msg);
      return;
    case 'shutdown':
      stopping = true;
      exit(0);
      return;
  }
});

// A plugin that went away without saying so (a crashed server) must not leave
// this process holding a DuckDB instance and a database handle.
process.on('disconnect', () => exit(0));
