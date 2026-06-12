/**
 * Inline per-path query filters for the History API.
 *
 * A request can narrow a single path to rows whose stored column matches a
 * value. The first such filter is `sourceRef`:
 *
 *   navigation.headingMagnetic:average|n2k-on-ve.can0.115
 *
 * filters to the source whose `$source` (stored in the `source_label` column)
 * is `n2k-on-ve.can0.115`.
 *
 * ── Adding another filter ────────────────────────────────────────────────
 * Add one entry to PATH_FILTER_DEFS below. Everything else in the read path is
 * generic over the filter list: raw-tier forcing, parquet/buffer SQL, parquet
 * schema probing, result keying, and the response echo all iterate the filters
 * and need no per-filter changes.
 *
 * Requirements for a new filter:
 *   - `column` must be written into raw-tier parquet AND the SQLite buffer.
 *     The `source_*` columns already are (see data-handler.ts / sqlite-buffer.ts).
 *   - `delimiter` must be a single character that cannot appear in a SignalK
 *     path or aggregate token (the path sanitiser in HistoryAPI must also allow
 *     it, alongside `|`).
 *   - `field` is the property name echoed back in each response `values` entry.
 */

import { escapeSqlString } from './sql-escape';

/** A resolved filter: match `column` = `value`, echoed back under `field`. */
export interface PathFilter {
  field: string;
  column: string;
  value: string;
}

interface PathFilterDef {
  /** Single character that introduces the filter's value in a path expression. */
  delimiter: string;
  /** Response property name echoed in each `values` entry. */
  field: string;
  /** Stored parquet/buffer column the value is matched against. */
  column: string;
}

/**
 * The registry of supported inline path filters — the single place to add one.
 */
export const PATH_FILTER_DEFS: readonly PathFilterDef[] = [
  // `path:aggregate|sourceRef` — restrict to one SignalK source.
  { delimiter: '|', field: 'sourceRef', column: 'source_label' },
];

const DEF_BY_DELIMITER = new Map(
  PATH_FILTER_DEFS.map(def => [def.delimiter, def])
);

/** The set of delimiter characters, for the path sanitiser to preserve. */
export const FILTER_DELIMITERS = PATH_FILTER_DEFS.map(d => d.delimiter).join(
  ''
);

/**
 * Split inline filters off a path expression, returning the base expression
 * (path + aggregate + smoothing) and the parsed filters.
 *
 * Input:  'navigation.position:first|gps-1'
 * Output: { base: 'navigation.position:first',
 *           filters: [{ field: 'sourceRef', column: 'source_label', value: 'gps-1' }] }
 */
export function parsePathFilters(expr: string): {
  base: string;
  filters: PathFilter[];
} {
  // The base expression ends at the first registered delimiter.
  let firstDelim = -1;
  for (let i = 0; i < expr.length; i++) {
    if (DEF_BY_DELIMITER.has(expr[i])) {
      firstDelim = i;
      break;
    }
  }
  if (firstDelim < 0) {
    return { base: expr, filters: [] };
  }

  const base = expr.substring(0, firstDelim);
  const filters: PathFilter[] = [];
  let i = firstDelim;
  while (i < expr.length) {
    const def = DEF_BY_DELIMITER.get(expr[i])!;
    let end = i + 1;
    while (end < expr.length && !DEF_BY_DELIMITER.has(expr[end])) {
      end++;
    }
    const value = expr.substring(i + 1, end);
    if (value.length > 0) {
      filters.push({ field: def.field, column: def.column, value });
    }
    i = end;
  }
  return { base, filters };
}

/**
 * Build filters from an already-parsed spec object (e.g. a server-provided
 * History API PathSpec) by reading each registered field off the object. Used
 * by the v2 provider, where the server — not this plugin — parses the request.
 */
export function filtersFromFields(spec: Record<string, unknown>): PathFilter[] {
  const filters: PathFilter[] = [];
  for (const def of PATH_FILTER_DEFS) {
    const value = spec[def.field];
    if (typeof value === 'string' && value.length > 0) {
      filters.push({ field: def.field, column: def.column, value });
    }
  }
  return filters;
}

/** Distinct stored columns referenced by the given filters. */
export function filterColumns(filters: PathFilter[]): string[] {
  return [...new Set(filters.map(f => f.column))];
}

/**
 * Build the parquet-side filter fragment to append to a WHERE clause.
 *
 * For each filter: if its column exists in `availableColumns`, append
 * `AND column = 'value'`; otherwise append `AND 1=0` so the parquet side
 * contributes nothing — legacy/imported files without the column cannot match,
 * and the always-tagged SQLite buffer answers through its own filtered subquery.
 */
export function buildParquetFilterClause(
  filters: PathFilter[],
  availableColumns: Set<string>
): string {
  return filters
    .map(f =>
      availableColumns.has(f.column)
        ? ` AND ${f.column} = '${escapeSqlString(f.value)}'`
        : ' AND 1=0'
    )
    .join('');
}

/**
 * Build the buffer-side filter fragment. The buffer schema always carries the
 * registered filter columns, so no existence check is needed.
 */
export function buildBufferFilterClause(filters?: PathFilter[]): string {
  if (!filters || filters.length === 0) {
    return '';
  }
  return filters
    .map(f => `\n    AND ${f.column} = '${escapeSqlString(f.value)}'`)
    .join('');
}

/** Response-echo properties for a set of filters, e.g. { sourceRef: '...' }. */
export function filterEcho(filters: PathFilter[]): Record<string, string> {
  return Object.fromEntries(filters.map(f => [f.field, f.value]));
}

/** Minimal view of a DuckDB connection needed to probe a parquet schema. */
export interface SchemaProbeConnection {
  runAndReadAll: (sql: string) => Promise<{ getRowObjects: () => unknown[] }>;
}

/**
 * Returns true if any of the given parquet globs exposes `column`. Null/empty
 * paths, globs that match no files, and probe errors are treated as "absent",
 * so a query that reads from S3 isn't penalised by a local glob that matches no
 * files (and vice versa).
 */
export async function parquetHasColumn(
  connection: SchemaProbeConnection,
  filePaths: Array<string | null | undefined>,
  column: string
): Promise<boolean> {
  for (const filePath of filePaths) {
    if (!filePath) {
      continue;
    }
    try {
      const result = await connection.runAndReadAll(
        `SELECT 1 FROM parquet_schema('${filePath}') WHERE name = '${column}' LIMIT 1`
      );
      if (result.getRowObjects().length > 0) {
        return true;
      }
    } catch {
      // Path may match no files; treat as absent and try the next.
    }
  }
  return false;
}

/**
 * The subset of the filters' columns that actually exist in the given parquet
 * globs. Pass the result to buildParquetFilterClause so absent columns become
 * `AND 1=0` rather than a "column not found" error.
 */
export async function availableFilterColumns(
  connection: SchemaProbeConnection,
  filePaths: Array<string | null | undefined>,
  filters: PathFilter[]
): Promise<Set<string>> {
  const available = new Set<string>();
  for (const column of filterColumns(filters)) {
    if (await parquetHasColumn(connection, filePaths, column)) {
      available.add(column);
    }
  }
  return available;
}
