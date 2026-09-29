/**
 * The messages between the plugin and the Track API worker (track-worker.ts),
 * over child_process IPC with `serialization: 'advanced'`.
 *
 * The worker answers a whole Track API call — the parquet walk, the buffer
 * staging and the DuckDB query — and only the finished answer crosses back,
 * once. That is the difference from the buffer worker removed in 1.0.1-beta.1
 * (devdocs/BUFFER_WORKER_REMOVED.md), which read buffer pages off the main
 * thread but sent each one back for the main thread to append into DuckDB.
 *
 * This file is the only vocabulary the two sides share.
 */

import type { TracksRequest } from '../track-provider';

/** What the worker needs before it can answer anything. */
export interface TrackWorkerInit {
  type: 'init';
  /** The plugin's output directory: parquet store and DuckDB home alike. */
  dataDir: string;
  /** buffer.db, opened read-only in the worker; absent when there is none. */
  dbPath?: string;
  selfId: string;
}

/**
 * One Track API call. `query` carries its instants as epoch milliseconds and
 * its durations as `{ milliseconds }` (see toWireRequest): a Temporal object
 * does not survive the structured clone, its fields live behind the prototype.
 *
 * `angular` answers, for each requested property path, whether it holds
 * angles. The worker has no server to ask; the plugin asks for it.
 */
export interface TrackWorkerRequest {
  type: 'request';
  id: number;
  method: 'getTracks' | 'getTrackContexts';
  query: TracksRequest;
  angular: Record<string, boolean>;
}

/** Close the buffer and exit. */
export interface TrackWorkerShutdown {
  type: 'shutdown';
}

export type TrackWorkerInbound =
  TrackWorkerInit | TrackWorkerRequest | TrackWorkerShutdown;

export type TrackWorkerOutbound =
  /** Sent once, when the worker can take requests. */
  | { type: 'ready' }
  /** The worker could not start; nothing else follows. */
  | { type: 'fatal'; message: string }
  | { type: 'log'; level: 'debug' | 'error'; msg: string }
  | { type: 'reply'; id: number; value: unknown }
  /** The request failed; the failure is that request's only. */
  | { type: 'error'; id: number; message: string };
