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
import { pathToTableName } from './sqlite-buffer';

/** The minimal slice of SQLiteBuffer that staging needs. */
export interface BufferStagingSource {
  getTableSchema(
    signalkPath: string
  ): Array<{ name: string; type: string }> | undefined;
  getRowsForFederation(
    signalkPath: string,
    context: string,
    fromIso: string,
    toIso: string,
    afterId: number,
    limit: number
  ): Array<Record<string, unknown>>;
}

/** Rows copied per read/append cycle — bounds JS heap regardless of window size. */
const STAGING_BATCH_SIZE = 5000;

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
  let afterId = 0;
  try {
    for (;;) {
      const rows = buffer.getRowsForFederation(
        signalkPath,
        context,
        fromIso,
        toIso,
        afterId,
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
      afterId = Number(rows[rows.length - 1].id);

      if (totalRows >= MAX_STAGED_ROWS) {
        warn?.(
          `[buffer-staging] ${signalkPath}: staged row cap reached (${MAX_STAGED_ROWS}); ` +
            `buffer data after id ${afterId} omitted from this query`
        );
        break;
      }
      if (rows.length < STAGING_BATCH_SIZE) break;
    }
    appender.flushSync();
  } finally {
    appender.closeSync();
  }

  return totalRows > 0 ? qualifiedName : null;
}
