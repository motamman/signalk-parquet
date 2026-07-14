/**
 * SQL fragment builders for federating the SQLite buffer into DuckDB queries.
 *
 * Per-path table architecture: each SignalK path has its own table in buffer.db.
 * Scalar tables have a `value` column; object tables have flattened `value_*` columns.
 *
 * Buffer rows reach DuckDB via a staged TEMP table (see buffer-staging.ts) rather
 * than an ATTACH of the live buffer.db — callers stage first and pass the staged
 * table name in. The WHERE clauses here re-apply the staging filters harmlessly
 * and add the per-request value/filter conditions.
 */

import { Context, Path } from '@signalk/server-api';
import { ComponentInfo } from './schema-cache';
import { escapeSqlString } from './sql-escape';
import { PathFilter, buildBufferFilterClause } from './path-filters';

/**
 * Build a buffer subquery for a scalar (numeric) path.
 *
 * Output columns: signalk_timestamp, value
 * Matches raw-tier parquet schema so it can be UNION ALL'd directly.
 *
 * When filters are provided, rows are restricted to matching column values.
 */
export function buildBufferScalarSubquery(
  stagedTable: string,
  context: Context | string,
  signalkPath: Path | string,
  fromIso: string,
  toIso: string,
  filters?: PathFilter[]
): string {
  const pathStr = String(signalkPath);

  // Root-level paths without dots are string properties (name, mmsi, uuid, etc.)
  const isStringPath = !pathStr.includes('.');
  const valueExpr = isStringPath ? 'value' : 'TRY_CAST(value AS DOUBLE)';

  return `(SELECT
    signalk_timestamp,
    ${valueExpr} AS value,
    NULL::VARCHAR AS value_json
  FROM ${stagedTable}
  WHERE context = '${escapeSqlString(String(context))}'
    AND received_timestamp >= '${escapeSqlString(fromIso)}'
    AND received_timestamp < '${escapeSqlString(toIso)}'
    AND exported = 0
    AND value IS NOT NULL${buildBufferFilterClause(filters)})`;
}

/**
 * Build a buffer subquery for an object path (e.g. navigation.position).
 *
 * Per-path tables already have flattened value_* columns, so no json_extract needed.
 *
 * When filters are provided, rows are restricted to matching column values.
 */
export function buildBufferObjectSubquery(
  stagedTable: string,
  context: Context | string,
  fromIso: string,
  toIso: string,
  components: Map<string, ComponentInfo>,
  bufferTableColumns?: Set<string>,
  filters?: PathFilter[]
): string {
  const componentSelects = Array.from(components.entries())
    .map(([_name, comp]) => {
      // If we know the buffer table's columns, output NULL for missing ones
      if (bufferTableColumns && !bufferTableColumns.has(comp.columnName)) {
        return `NULL::DOUBLE AS ${comp.columnName}`;
      }
      // Columns are already flattened in per-path tables — just SELECT them directly
      if (comp.dataType === 'numeric') {
        return `TRY_CAST(${comp.columnName} AS DOUBLE) AS ${comp.columnName}`;
      }
      return `CAST(${comp.columnName} AS VARCHAR) AS ${comp.columnName}`;
    })
    .join(',\n    ');

  return `(SELECT
    signalk_timestamp,
    ${componentSelects}
  FROM ${stagedTable}
  WHERE context = '${escapeSqlString(String(context))}'
    AND received_timestamp >= '${escapeSqlString(fromIso)}'
    AND received_timestamp < '${escapeSqlString(toIso)}'
    AND exported = 0
    AND value_json IS NOT NULL${buildBufferFilterClause(filters)})`;
}
