/**
 * SQL fragment builders for federating the SQLite buffer into DuckDB queries.
 *
 * Per-path table architecture: each SignalK path has its own table in buffer.db.
 * Table name: buffer_{path_with_dots_as_underscores}
 * Scalar tables have a `value` column; object tables have flattened `value_*` columns.
 */

import { Context, Path } from '@signalk/server-api';
import { ComponentInfo } from './schema-cache';
import { pathToTableName } from './sqlite-buffer';

/**
 * Escape a string value for safe inclusion in SQL literals.
 * Prevents SQL injection by doubling single quotes.
 */
function escapeSql(value: string): string {
  return value.replace(/'/g, "''");
}

/**
 * Build a buffer subquery for a scalar (numeric) path.
 *
 * Output columns: signalk_timestamp, value
 * Matches raw-tier parquet schema so it can be UNION ALL'd directly.
 *
 * Returns null if no buffer table exists for this path (caller should skip UNION ALL).
 */
export function buildBufferScalarSubquery(
  context: Context | string,
  signalkPath: Path | string,
  fromIso: string,
  toIso: string,
  knownBufferPaths?: Set<string>
): string | null {
  const pathStr = String(signalkPath);

  // If we have the known paths set, check existence
  if (knownBufferPaths && !knownBufferPaths.has(pathStr)) {
    return null;
  }

  const tableName = pathToTableName(pathStr);

  // Root-level paths without dots are string properties (name, mmsi, uuid, etc.)
  const isStringPath = !pathStr.includes('.');
  const valueExpr = isStringPath ? 'value' : 'TRY_CAST(value AS DOUBLE)';

  return `(SELECT
    signalk_timestamp,
    ${valueExpr} AS value,
    NULL::VARCHAR AS value_json
  FROM buffer.${tableName}
  WHERE context = '${escapeSql(String(context))}'
    AND received_timestamp >= '${escapeSql(fromIso)}'
    AND received_timestamp < '${escapeSql(toIso)}'
    AND exported = 0
    AND value IS NOT NULL)`;
}

/**
 * Build a buffer subquery for an object path (e.g. navigation.position).
 *
 * Per-path tables already have flattened value_* columns, so no json_extract needed.
 *
 * Returns null if no buffer table exists for this path (caller should skip UNION ALL).
 */
export function buildBufferObjectSubquery(
  context: Context | string,
  signalkPath: Path | string,
  fromIso: string,
  toIso: string,
  components: Map<string, ComponentInfo>,
  knownBufferPaths?: Set<string>,
  bufferTableColumns?: Set<string>
): string | null {
  const pathStr = String(signalkPath);

  // If we have the known paths set, check existence
  if (knownBufferPaths && !knownBufferPaths.has(pathStr)) {
    return null;
  }

  const tableName = pathToTableName(pathStr);

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
  FROM buffer.${tableName}
  WHERE context = '${escapeSql(String(context))}'
    AND received_timestamp >= '${escapeSql(fromIso)}'
    AND received_timestamp < '${escapeSql(toIso)}'
    AND exported = 0
    AND value_json IS NOT NULL)`;
}
