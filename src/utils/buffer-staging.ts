/**
 * Stage SQLite buffer rows into a DuckDB TEMP table for federated queries.
 *
 * The buffer must never be opened by DuckDB's bundled SQLite (ATTACH ... TYPE SQLITE):
 * two SQLite libraries in one process cannot see each other's POSIX advisory locks,
 * so DuckDB's copy would treat the live database as unused, run WAL recovery, and
 * truncate the -shm file under node:sqlite's active mmap — crashing the server with
 * SIGBUS on the next large write transaction. Instead, rows are read through a
 * node:sqlite connection and copied into a per-connection DuckDB temp table, which
 * the federated SQL unions with parquet exactly as before.
 *
 * The read is a synchronous `.all()` on the main thread, and the loop yields
 * between pages so one query cannot hold the event loop for its whole scan.
 * (A worker-thread read path existed briefly; see
 * devdocs/BUFFER_WORKER_REMOVED.md.)
 */

import { DuckDBConnection, DuckDBAppender } from '@duckdb/node-api';
import { federationCursor, pathToTableName } from './sqlite-buffer';
import { yieldToEventLoop } from './hive-walk';
import { FederationCursor } from '../types';

/** The minimal slice of SQLiteBuffer that in-process staging needs. */
export interface BufferStagingSource {
  isOpen(): boolean;
  getTableSchema(
    signalkPath: string
  ): Array<{ name: string; type: string }> | undefined;
  getRowsForFederation(
    signalkPath: string,
    context: string,
    fromIso: string,
    toIso: string,
    after: FederationCursor | null,
    limit: number
  ): Array<Record<string, unknown>>;
}

/**
 * How long one page of staging may hold the event loop.
 *
 * Anchored on the server's own jitter rather than picked: with no history load
 * at all, an unrelated client's request takes 2 ms at p50 and 31 ms at worst
 * (measured on brain, 2026-09-26). A block below 10 ms therefore stays inside
 * noise the server already makes, which is the point at which making it smaller
 * stops buying anything a client can feel.
 */
const PAGE_BUDGET_MS = 10;

/**
 * What a row costs the main thread, per path, measured on a Raspberry Pi 5
 * (2026-09-26): reading out of SQLite is about 8 µs and appending into the
 * DuckDB temp table about 7 µs, and the main thread pays both.
 */
const IN_PROCESS_US_PER_ROW = 15;

/**
 * Rows per page, from the budget and the per-row cost: the page is whatever
 * keeps one block inside PAGE_BUDGET_MS.
 *
 * This also bounds JS heap regardless of window size, and the keyset pagination
 * in `getRowsForFederation` is what lets a page be small without the read
 * costing more per row.
 */
const IN_PROCESS_BATCH_SIZE = Math.round(
  (PAGE_BUDGET_MS * 1000) / IN_PROCESS_US_PER_ROW
);

/** Hard cap on staged rows per query, far above any real buffer window. */
const MAX_STAGED_ROWS = 1_000_000;

/** A DuckDB column type, from the buffer column's declared SQLite type. */
type DuckType = 'BIGINT' | 'DOUBLE' | 'VARCHAR';

/** Map a declared SQLite column type to the DuckDB column type for staging. */
function duckdbTypeFor(sqliteType: string): DuckType {
  const t = sqliteType.toUpperCase();
  if (t.includes('INT')) return 'BIGINT';
  if (t.includes('REAL') || t.includes('FLOA') || t.includes('DOUB')) {
    return 'DOUBLE';
  }
  return 'VARCHAR';
}

/**
 * One page of rows to append, whatever produced it: `rowCount` rows, and the
 * value at a (row, column) pair where the column index is the schema's.
 */
interface StagedPage {
  rowCount: number;
  valueAt(row: number, column: number): unknown;
}

/**
 * Where a staged scan's pages come from. The in-process buffer is the only
 * implementation; the interface is where another source would plug in.
 */
interface StagingSource {
  /** Declared column schema, or null when the path has no buffer table. */
  schema(
    signalkPath: string
  ): Promise<Array<{ name: string; type: string }> | null>;
  /** Pages of the window in `(signalk_timestamp, id)` order. */
  pages(args: {
    signalkPath: string;
    context: string;
    fromIso: string;
    toIso: string;
    schema: Array<{ name: string; type: string }>;
  }): AsyncGenerator<StagedPage, void, void>;
}

/** Rows read synchronously on this thread, yielding between pages. */
function inProcessSource(buffer: BufferStagingSource): StagingSource {
  return {
    async schema(signalkPath) {
      return buffer.getTableSchema(signalkPath) ?? null;
    },
    async *pages({ signalkPath, context, fromIso, toIso, schema }) {
      let after: FederationCursor | null = null;
      for (;;) {
        const rows = buffer.getRowsForFederation(
          signalkPath,
          context,
          fromIso,
          toIso,
          after,
          IN_PROCESS_BATCH_SIZE
        );
        if (rows.length === 0) return;

        yield {
          rowCount: rows.length,
          valueAt: (row, column) => rows[row][schema[column].name],
        };

        after = federationCursor(rows[rows.length - 1]);
        if (rows.length < IN_PROCESS_BATCH_SIZE) return;

        // Give the event loop a turn between pages. The read above and the
        // append the consumer does are both synchronous, so without this one
        // query holds the server's main thread for its whole scan: measured on
        // a Raspberry Pi 5 (2026-09-26), 30,577 rows blocked it for 440 ms.
        // The page size sets how long a block is; this keeps it to one page.
        await yieldToEventLoop();

        // A close between pages (plugin stop, reconfigure) makes
        // getRowsForFederation return an empty array, which the loop cannot
        // tell from "no more rows": the query would answer with a silently
        // truncated buffer side. Refuse, as the row cap does.
        if (!buffer.isOpen()) {
          throw new Error(
            `[buffer-staging] ${signalkPath}: buffer closed mid-scan; ` +
              `refusing to answer with incomplete data`
          );
        }
      }
    },
  };
}

/** Append one page's rows to the appender, converting per the column's type. */
function appendPage(
  appender: DuckDBAppender,
  columnTypes: DuckType[],
  page: StagedPage
): void {
  for (let row = 0; row < page.rowCount; row++) {
    for (let column = 0; column < columnTypes.length; column++) {
      const value = page.valueAt(row, column);
      if (value === null || value === undefined) {
        appender.appendNull();
      } else if (columnTypes[column] === 'BIGINT') {
        // SQLite columns are dynamically typed — tolerate stray non-integer content
        const num = typeof value === 'bigint' ? value : Number(value);
        if (typeof num === 'bigint') {
          appender.appendBigInt(num);
        } else if (Number.isFinite(num)) {
          appender.appendBigInt(BigInt(Math.trunc(num)));
        } else {
          appender.appendNull();
        }
      } else if (columnTypes[column] === 'DOUBLE') {
        const num = Number(value);
        if (Number.isFinite(num)) {
          appender.appendDouble(num);
        } else {
          appender.appendNull();
        }
      } else {
        appender.appendVarchar(String(value));
      }
    }
    appender.endRow();
  }
}

/**
 * Copy the buffer rows a query needs (one path, one context, time-windowed,
 * unexported only) into a TEMP table on the given DuckDB connection.
 *
 * Returns the qualified temp table name to use in FROM clauses, or null when
 * the path has no buffer table or no rows match (callers skip the UNION ALL,
 * as they did when ATTACH-era subqueries returned null).
 *
 * The temp table carries all buffer columns, so the existing subquery WHERE
 * clauses (context/time/exported/filters) remain valid against it.
 */
export async function stageBufferTable(
  connection: DuckDBConnection,
  buffer: BufferStagingSource,
  context: string,
  signalkPath: string,
  fromIso: string,
  toIso: string,
  warn?: (msg: string) => void
): Promise<string | null> {
  const source = inProcessSource(buffer);
  const schema = await source.schema(signalkPath);
  if (!schema || schema.length === 0) {
    return null;
  }

  const tableName = `stage_${pathToTableName(signalkPath)}`;
  const qualifiedName = `temp.main.${tableName}`;
  const columnTypes = schema.map(col => duckdbTypeFor(col.type));
  const columnDefs = schema
    .map((col, i) => `"${col.name}" ${columnTypes[i]}`)
    .join(', ');

  await connection.run(
    `CREATE OR REPLACE TEMP TABLE ${tableName} (${columnDefs})`
  );

  const appender = await connection.createAppender(tableName, 'main', 'temp');
  let totalRows = 0;
  try {
    for await (const page of source.pages({
      signalkPath,
      context,
      fromIso,
      toIso,
      schema,
    })) {
      appendPage(appender, columnTypes, page);
      totalRows += page.rowCount;

      // Landing exactly on the cap is fine; a page that carries us past it
      // means there is more buffer than a query may silently leave out.
      if (totalRows > MAX_STAGED_ROWS) {
        warn?.(
          `[buffer-staging] ${signalkPath}: staged row cap reached (${MAX_STAGED_ROWS}); ` +
            `buffer data would be omitted from this query`
        );
        throw new Error(
          `[buffer-staging] ${signalkPath}: buffer rows exceed the staged row cap ` +
            `(${MAX_STAGED_ROWS}); refusing to answer with incomplete data`
        );
      }
    }
    appender.flushSync();
  } finally {
    appender.closeSync();
  }

  return totalRows > 0 ? qualifiedName : null;
}
