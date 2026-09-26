/**
 * Stage SQLite buffer rows into a DuckDB TEMP table for federated queries.
 *
 * The buffer must never be opened by DuckDB's bundled SQLite (ATTACH ... TYPE SQLITE):
 * two SQLite libraries in one process cannot see each other's POSIX advisory locks,
 * so DuckDB's copy would treat the live database as unused, run WAL recovery, and
 * truncate the -shm file under node:sqlite's active mmap — crashing the server with
 * SIGBUS on the next large write transaction. Instead, rows are read through the
 * buffer's own node:sqlite connection and copied into a per-connection DuckDB temp
 * table, which the federated SQL unions with parquet exactly as before.
 */

import { DuckDBConnection } from '@duckdb/node-api';
import { federationCursor, pathToTableName } from './sqlite-buffer';
import { yieldToEventLoop } from './hive-walk';
import { FederationCursor } from '../types';

/** The minimal slice of SQLiteBuffer that staging needs. */
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
 * Rows copied per read/append cycle. Bounds JS heap regardless of window size
 * and, because the loop yields between pages, how long any one query holds the
 * server's main thread. A row costs about 15 µs on a Raspberry Pi 5 (8 µs to
 * read, 7 µs to append), so a page is roughly a 15 ms block; measured on brain
 * 2026-09-26, 5,000-row pages held the loop for up to 100 ms. The keyset
 * pagination below is what lets the page shrink without the read costing more.
 */
const STAGING_BATCH_SIZE = 1000;

/** Hard cap on staged rows per query, far above any real buffer window. */
const MAX_STAGED_ROWS = 1_000_000;

/** Map a declared SQLite column type to the DuckDB column type for staging. */
function duckdbTypeFor(sqliteType: string): 'BIGINT' | 'DOUBLE' | 'VARCHAR' {
  const t = sqliteType.toUpperCase();
  if (t.includes('INT')) return 'BIGINT';
  if (t.includes('REAL') || t.includes('FLOA') || t.includes('DOUB')) {
    return 'DOUBLE';
  }
  return 'VARCHAR';
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
  const schema = buffer.getTableSchema(signalkPath);
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
  let after: FederationCursor | null = null;
  try {
    for (;;) {
      const rows = buffer.getRowsForFederation(
        signalkPath,
        context,
        fromIso,
        toIso,
        after,
        STAGING_BATCH_SIZE
      );
      if (rows.length === 0) break;

      for (const row of rows) {
        for (let i = 0; i < schema.length; i++) {
          const value = row[schema[i].name];
          if (value === null || value === undefined) {
            appender.appendNull();
          } else if (columnTypes[i] === 'BIGINT') {
            // SQLite columns are dynamically typed — tolerate stray non-integer content
            const num = typeof value === 'bigint' ? value : Number(value);
            if (typeof num === 'bigint') {
              appender.appendBigInt(num);
            } else if (Number.isFinite(num)) {
              appender.appendBigInt(BigInt(Math.trunc(num)));
            } else {
              appender.appendNull();
            }
          } else if (columnTypes[i] === 'DOUBLE') {
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

      totalRows += rows.length;
      after = federationCursor(rows[rows.length - 1]);

      if (totalRows >= MAX_STAGED_ROWS) {
        // Only fail on genuine overflow — landing exactly on the cap is fine
        const overflow = buffer.getRowsForFederation(
          signalkPath,
          context,
          fromIso,
          toIso,
          after,
          1
        );
        if (overflow.length === 0) break;
        warn?.(
          `[buffer-staging] ${signalkPath}: staged row cap reached (${MAX_STAGED_ROWS}); ` +
            `buffer data after ${after.signalkTimestamp} (id ${after.id}) ` +
            `would be omitted from this query`
        );
        throw new Error(
          `[buffer-staging] ${signalkPath}: buffer rows exceed the staged row cap ` +
            `(${MAX_STAGED_ROWS}); refusing to answer with incomplete data`
        );
      }
      if (rows.length < STAGING_BATCH_SIZE) break;

      // Give the event loop a turn between pages. The read above and the
      // append below are both synchronous, so without this one query holds
      // the server's main thread for its whole scan: measured on a Raspberry
      // Pi 5 (2026-09-26), 30,577 rows blocked it for 440 ms. The page size
      // sets how long a block is; this is what keeps it to one page.
      await yieldToEventLoop();

      // A close between pages (plugin stop, reconfigure) makes
      // getRowsForFederation return an empty array, which the loop cannot
      // tell from "no more rows": the query would answer with a silently
      // truncated buffer side. Refuse, as the row cap above does.
      if (!buffer.isOpen()) {
        throw new Error(
          `[buffer-staging] ${signalkPath}: buffer closed mid-scan after ` +
            `${totalRows} row(s); refusing to answer with incomplete data`
        );
      }
    }
    appender.flushSync();
  } finally {
    appender.closeSync();
  }

  return totalRows > 0 ? qualifiedName : null;
}
