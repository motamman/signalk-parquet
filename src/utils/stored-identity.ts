/**
 * A vessel's identity as last exported to parquet: the newest `identity` row
 * of its raw-tier files at or before an instant.
 *
 * Read by playback, to announce a vessel before its first delta, and by the
 * identity service, to tell whether a vessel it no longer remembers already
 * has its identity on file. Identity is written only when it changes, so a
 * vessel has a handful of these files.
 *
 * A file holds the identity as `value_json` text, as its flattened `value_*`
 * components, or both, depending on what wrote it; whichever it has is read.
 * Values come back as the file typed them (a numeric-looking text column is
 * DOUBLE, see the parquet writer), so callers normalise what they need.
 */
import { DuckDBPool } from './duckdb-pool';
import { filesFor, readParquetSql } from './parquet-files';
import { readFooters } from './parquet-footer';
import { escapeSqlString } from './sql-escape';
import { IDENTITY_PATH } from './vessel-identity';

export async function latestStoredIdentity(
  dataDir: string,
  context: string,
  atIso: string
): Promise<Record<string, unknown> | undefined> {
  const files = await filesFor({
    dataDir,
    contexts: [context],
    paths: [IDENTITY_PATH],
    fromIso: '1970-01-01T00:00:00.000Z',
    toIso: new Date(Date.parse(atIso) + 1).toISOString(),
  });
  if (files.length === 0) return undefined;
  // The footers drop the files that start after the instant asked about.
  const candidates = (await readFooters(files)).filter(
    f =>
      f.timestampSpans === null || f.timestampSpans.some(([lo]) => lo <= atIso)
  );
  if (candidates.length === 0) return undefined;

  const components = [
    ...new Set(
      candidates.flatMap(f =>
        [...f.columns.keys()]
          .filter(c => c.startsWith('value_') && c !== 'value_json')
          .map(c => c.slice('value_'.length))
      )
    ),
  ].sort();
  const valueJson = candidates.some(f => f.columns.has('value_json'))
    ? 'value_json AS v'
    : 'NULL AS v';
  // Legacy files have no context column, and naming an absent column fails
  // the whole query. The files were already chosen by context above.
  const contextFilter = candidates.some(f => f.columns.has('context'))
    ? `context = '${escapeSqlString(context)}' AND `
    : '';

  const connection = await DuckDBPool.getConnection();
  try {
    const result = await connection.runAndReadAll(
      `SELECT ${[valueJson, ...components.map(c => `"value_${c}"`)].join(', ')}
       FROM ${readParquetSql(candidates.map(f => f.file))}
       WHERE ${contextFilter}signalk_timestamp <= '${escapeSqlString(atIso)}'
       ORDER BY signalk_timestamp DESC LIMIT 1`
    );
    const rows = result.getRowObjects() as Array<Record<string, unknown>>;
    if (rows.length === 0) return undefined;
    const row = rows[0];
    if (row.v != null) {
      try {
        const parsed = JSON.parse(String(row.v));
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
          return parsed as Record<string, unknown>;
        }
      } catch {
        // Not JSON: fall back to the components.
      }
    }
    const out: Record<string, unknown> = {};
    for (const c of components) {
      const v = row[`value_${c}`];
      if (v === null || v === undefined) continue;
      out[c] = typeof v === 'bigint' ? Number(v) : v;
    }
    return Object.keys(out).length > 0 ? out : undefined;
  } finally {
    connection.disconnectSync();
  }
}
