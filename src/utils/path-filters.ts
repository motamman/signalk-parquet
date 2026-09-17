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

/**
 * A resolved filter: match `column` = `value`, echoed back under `field`.
 *
 * A `null` value matches rows whose column is NULL, or that predate the
 * column entirely: the "unattributed" rows a `sourcePolicy=all` expansion
 * returns as their own column. It is never parsed from a request and is not
 * echoed.
 */
export interface PathFilter {
  field: string;
  column: string;
  value: string | null;
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
 *
 * A null-valued filter selects the unattributed rows: `AND column IS NULL`
 * when the column exists (files that lack it read as NULL through
 * union_by_name), and no clause at all when no file has it, since every row
 * is then unattributed.
 */
export function buildParquetFilterClause(
  filters: PathFilter[],
  availableColumns: Set<string>
): string {
  return filters
    .map(f => {
      if (f.value === null) {
        return availableColumns.has(f.column) ? ` AND ${f.column} IS NULL` : '';
      }
      return availableColumns.has(f.column)
        ? ` AND ${f.column} = '${escapeSqlString(f.value)}'`
        : ' AND 1=0';
    })
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
    .map(f =>
      f.value === null
        ? `\n    AND ${f.column} IS NULL`
        : `\n    AND ${f.column} = '${escapeSqlString(f.value)}'`
    )
    .join('');
}

/**
 * Response-echo properties for a set of filters, e.g. { sourceRef: '...' }.
 * Unattributed (null) filters are not echoed: the column carries no claim.
 */
export function filterEcho(filters: PathFilter[]): Record<string, string> {
  return Object.fromEntries(
    filters.filter(f => f.value !== null).map(f => [f.field, f.value as string])
  );
}
