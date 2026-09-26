/**
 * The buffer worker: one thread that owns the connection to `buffer.db`.
 *
 * It holds a real `SQLiteBuffer`, not a reimplementation of one — the table
 * mapping, the schema migration, the indexes and the queries are the same code
 * that runs in-process today, so this thread changes *where* the synchronous
 * SQLite calls happen and nothing about what they do.
 *
 * This is the read half. Writes, cleanup and the rest of the interface follow;
 * until they do, nothing in the plugin talks to this file, and the in-process
 * buffer remains the only one the plugin uses.
 *
 * Messages are handled in arrival order and never concurrently: every handler
 * here is synchronous, so a request cannot interleave with another. A scan
 * sends at most `1 + SCAN_PAGES_IN_FLIGHT` pages before it waits for an ack,
 * which is what stops a slow consumer from making this thread read the whole
 * window into memory, and what lets the read of one page overlap the consumer's
 * work on the last.
 */

import { parentPort, workerData } from 'node:worker_threads';
import { SQLiteBuffer, federationCursor } from './utils/sqlite-buffer';
import { FederationCursor } from './types';
import { encodePage } from './utils/buffer-columnar';
import {
  BufferWorkerInit,
  BufferWorkerRequest,
  BufferWorkerMessage,
  SCAN_PAGES_IN_FLIGHT,
} from './utils/buffer-worker-protocol';

if (!parentPort) {
  throw new Error('buffer-worker must be started as a worker thread');
}
const port = parentPort;

const init = workerData as BufferWorkerInit;

interface ScanState {
  signalkPath: string;
  context: string;
  fromIso: string;
  toIso: string;
  pageRows: number;
  schema: Array<{ name: string; type: string }>;
  cursor: FederationCursor | null;
  /** Pages this scan may still send before an ack comes back. */
  credits: number;
  totalRows: number;
  /** True once `end` has been sent; the scan is kept only to swallow late acks. */
  finished: boolean;
}

const scans = new Map<number, ScanState>();
let nextScanId = 1;
let buffer: SQLiteBuffer | undefined;

const send = (msg: BufferWorkerMessage, transfer?: ArrayBuffer[]): void => {
  if (transfer) {
    port.postMessage(msg, transfer);
  } else {
    port.postMessage(msg);
  }
};

/**
 * Send pages while this scan has credit and rows remain. Each page is read and
 * packed synchronously, then the loop returns to the message queue so acks and
 * other requests are seen.
 */
function pump(scanId: number): void {
  const scan = scans.get(scanId);
  if (!scan || scan.finished || !buffer) return;

  while (scan.credits > 0) {
    const t0 = process.hrtime.bigint();
    const rows = buffer.getRowsForFederation(
      scan.signalkPath,
      scan.context,
      scan.fromIso,
      scan.toIso,
      scan.cursor,
      scan.pageRows
    );
    const readMs = Number(process.hrtime.bigint() - t0) / 1e6;

    if (rows.length === 0) {
      scan.finished = true;
      send({ type: 'end', scanId, totalRows: scan.totalRows });
      scans.delete(scanId);
      return;
    }

    const { page, transfer } = encodePage(rows, scan.schema);
    scan.totalRows += rows.length;
    scan.cursor = federationCursor(rows[rows.length - 1]);
    scan.credits -= 1;
    send({ type: 'page', scanId, page, readMs }, transfer);

    // A short page is the end of the window. Say so now rather than spending
    // another query to learn it, which is what the in-process loop does too.
    if (rows.length < scan.pageRows) {
      scan.finished = true;
      send({ type: 'end', scanId, totalRows: scan.totalRows });
      scans.delete(scanId);
      return;
    }
  }
}

function failScan(scanId: number, error: unknown): void {
  scans.delete(scanId);
  send({
    type: 'scanError',
    scanId,
    message: error instanceof Error ? error.message : String(error),
  });
}

function handle(msg: BufferWorkerRequest): void {
  if (!buffer) {
    if (msg.op !== 'ackScan' && msg.op !== 'closeScan') {
      send({ type: 'error', id: msg.id, message: 'buffer worker is not open' });
    }
    return;
  }

  switch (msg.op) {
    case 'ping':
      send({ type: 'reply', id: msg.id, value: null });
      return;

    case 'schema':
      try {
        send({
          type: 'reply',
          id: msg.id,
          value: buffer.getTableSchema(msg.signalkPath) ?? null,
        });
      } catch (error) {
        send({
          type: 'error',
          id: msg.id,
          message: error instanceof Error ? error.message : String(error),
        });
      }
      return;

    case 'openScan': {
      const schema = buffer.getTableSchema(msg.signalkPath);
      if (!schema || schema.length === 0) {
        // No table for this path: an empty scan, not an error. The consumer
        // then skips the buffer side exactly as it does in-process.
        send({ type: 'reply', id: msg.id, value: null });
        return;
      }
      const scanId = nextScanId++;
      scans.set(scanId, {
        signalkPath: msg.signalkPath,
        context: msg.context,
        fromIso: msg.fromIso,
        toIso: msg.toIso,
        pageRows: msg.pageRows,
        schema,
        cursor: null,
        credits: 1 + SCAN_PAGES_IN_FLIGHT,
        totalRows: 0,
        finished: false,
      });
      send({ type: 'reply', id: msg.id, value: scanId });
      try {
        pump(scanId);
      } catch (error) {
        failScan(scanId, error);
      }
      return;
    }

    case 'ackScan': {
      const scan = scans.get(msg.scanId);
      if (!scan) return; // Ack for a scan already ended or closed.
      scan.credits += 1;
      try {
        pump(msg.scanId);
      } catch (error) {
        failScan(msg.scanId, error);
      }
      return;
    }

    case 'closeScan':
      scans.delete(msg.scanId);
      return;

    case 'close':
      scans.clear();
      try {
        buffer.close();
      } catch {
        // Closing twice, or a database already gone, is not worth failing on.
      }
      buffer = undefined;
      send({ type: 'reply', id: msg.id, value: null });
      return;
  }
}

port.on('message', (msg: BufferWorkerRequest) => {
  try {
    handle(msg);
  } catch (error) {
    // A handler that throws must not take the thread down: report it against
    // the request and keep serving.
    const id = (msg as { id?: number }).id;
    if (typeof id === 'number') {
      send({
        type: 'error',
        id,
        message: error instanceof Error ? error.message : String(error),
      });
    }
  }
});

try {
  buffer = new SQLiteBuffer({
    dbPath: init.dbPath,
    retentionHours: init.retentionHours,
    // Reads only, for now. A second writing connection would make the
    // ingestion connection contend for the write lock, and it sets no
    // busy_timeout because waiting for a lock on the event loop is what this
    // worker exists to avoid. Writes move here as their own step.
    readOnly: true,
  });
  send({ type: 'ready' });
} catch (error) {
  send({
    type: 'fatal',
    message: error instanceof Error ? error.message : String(error),
  });
}
