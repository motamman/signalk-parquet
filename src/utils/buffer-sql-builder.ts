/**
 * SQL fragment builders for federating the SQLite buffer into DuckDB queries.
 *
 * These functions return SQL subquery strings whose output columns match
 * the parquet column schema, so they can be UNION ALL'd with read_parquet().
 *
 * The SQLite buffer stores context/path in dot notation (e.g. "vessels.urn:...",
 * "navigation.speedOverGround") — same as parquet, so no translation is needed.
 */

import { Context, Path } from '@signalk/server-api';
import { ComponentInfo } from './schema-cache';

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
 */
export function buildBufferScalarSubquery(
  context: Context | string,
  signalkPath: Path | string,
  fromIso: string,
  toIso: string
): string {
  return `(SELECT
    signalk_timestamp,
    TRY_CAST(value AS DOUBLE) AS value,
    NULL::VARCHAR AS value_json
  FROM buffer.buffer_records
  WHERE context = '${escapeSql(String(context))}'
    AND path = '${escapeSql(String(signalkPath))}'
    AND received_timestamp >= '${escapeSql(fromIso)}'
    AND received_timestamp < '${escapeSql(toIso)}'
    AND value IS NOT NULL)`;
}

/**
 * Build a buffer subquery for an object path (e.g. navigation.position).
 *
 * Extracts each component from the value_json column using json_extract_string,
 * producing output columns that match the parquet component schema
 * (e.g. value_latitude, value_longitude).
 */
export function buildBufferObjectSubquery(
  context: Context | string,
  signalkPath: Path | string,
  fromIso: string,
  toIso: string,
  components: Map<string, ComponentInfo>
): string {
  const componentSelects = Array.from(components.entries())
    .map(([name, comp]) => {
      if (comp.dataType === 'numeric') {
        return `TRY_CAST(json_extract_string(value_json, '$.${escapeSql(name)}') AS DOUBLE) AS ${comp.columnName}`;
      }
      return `json_extract_string(value_json, '$.${escapeSql(name)}') AS ${comp.columnName}`;
    })
    .join(',\n    ');

  return `(SELECT
    signalk_timestamp,
    ${componentSelects}
  FROM buffer.buffer_records
  WHERE context = '${escapeSql(String(context))}'
    AND path = '${escapeSql(String(signalkPath))}'
    AND received_timestamp >= '${escapeSql(fromIso)}'
    AND received_timestamp < '${escapeSql(toIso)}'
    AND value_json IS NOT NULL)`;
}
