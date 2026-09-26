/**
 * The messages between the main thread and the buffer worker.
 *
 * One worker owns the only `node:sqlite` connection to `buffer.db`, and every
 * buffer operation is a message on one FIFO queue processed in arrival order.
 * That ordering is the correctness property the synchronous code gets for free
 * and the easiest one to lose: **a read issued after a write observes that
 * write.** No priority lane, no second connection, no parallel read path.
 *
 * Reads stream rather than answering one page per request. The main thread
 * opens a scan; the worker pushes pages and will not run more than
 * `SCAN_PAGES_IN_FLIGHT` ahead of the acks coming back, so a slow consumer
 * cannot make the worker hold the whole window in memory, and the worker can
 * read the next page while the main thread is still appending the last one.
 *
 * This file is the only shared vocabulary between the two sides; neither
 * imports the other.
 */

import { ColumnarPage } from './buffer-columnar';

/**
 * Pages the worker may send beyond the one being consumed. One is enough to
 * overlap a read with an append — measured on a Raspberry Pi 5, about 40 ms of
 * read against 35 ms of append for 5,000 rows — and more only buys memory.
 */
export const SCAN_PAGES_IN_FLIGHT = 1;

/** What the worker needs before it can answer anything. */
export interface BufferWorkerInit {
  dbPath: string;
  retentionHours?: number;
}

/**
 * The worker opens the database read-only while it serves reads only, so it
 * never contends for the write lock with the connection that owns ingestion.
 */

// ---------------------------------------------------------------------------
// Main thread to worker
// ---------------------------------------------------------------------------

/** Open a keyset scan of one path and context over a time window. */
export interface OpenScanRequest {
  op: 'openScan';
  id: number;
  signalkPath: string;
  context: string;
  fromIso: string;
  toIso: string;
  pageRows: number;
}

/** One consumed page acknowledged, which releases the next. */
export interface AckScanRequest {
  op: 'ackScan';
  scanId: number;
}

/** Abandon a scan before its end; the worker forgets it and sends no more. */
export interface CloseScanRequest {
  op: 'closeScan';
  scanId: number;
}

/** The declared column schema of a path's table, or null when there is none. */
export interface SchemaRequest {
  op: 'schema';
  id: number;
  signalkPath: string;
}

/** A liveness and latency probe, used by the tests and the measurements. */
export interface PingRequest {
  op: 'ping';
  id: number;
}

/** Close the database and exit. */
export interface CloseRequest {
  op: 'close';
  id: number;
}

export type BufferWorkerRequest =
  | OpenScanRequest
  | AckScanRequest
  | CloseScanRequest
  | SchemaRequest
  | PingRequest
  | CloseRequest;

// ---------------------------------------------------------------------------
// Worker to main thread
// ---------------------------------------------------------------------------

/** Sent once, when the database is open and the worker can take requests. */
export interface ReadyMessage {
  type: 'ready';
}

/** The worker could not start at all; nothing else will follow. */
export interface FatalMessage {
  type: 'fatal';
  message: string;
}

/** A reply to a request that carries a value. */
export interface ReplyMessage {
  type: 'reply';
  id: number;
  value: unknown;
}

/** A request failed. The failure belongs to that request only. */
export interface ErrorMessage {
  type: 'error';
  id: number;
  message: string;
}

/** One page of a scan. `page`'s buffers are transferred, not copied. */
export interface ScanPageMessage {
  type: 'page';
  scanId: number;
  page: ColumnarPage;
  /** Time the worker spent reading this page, for measurement. */
  readMs: number;
}

/** The scan reached the end of its window. No further pages. */
export interface ScanEndMessage {
  type: 'end';
  scanId: number;
  totalRows: number;
}

/** The scan failed part-way. The consumer must not answer with what it has. */
export interface ScanErrorMessage {
  type: 'scanError';
  scanId: number;
  message: string;
}

export type BufferWorkerMessage =
  | ReadyMessage
  | FatalMessage
  | ReplyMessage
  | ErrorMessage
  | ScanPageMessage
  | ScanEndMessage
  | ScanErrorMessage;
