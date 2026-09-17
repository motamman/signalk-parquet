/**
 * One footer reader: every metadata question about a parquet file is
 * answered here, from the file's footer, in JavaScript, with parquetjs.
 *
 * The questions are: which context a file holds (the hive layout writes one
 * context per file, so the `context` column's min and max statistics are
 * that context), what span of `signalk_timestamp` it covers, and which
 * columns it has with what types (a scalar `value`, or the `value_*`
 * components of an object path).
 *
 * They used to be asked through DuckDB — `parquet_schema`,
 * `parquet_metadata`, `DESCRIBE SELECT *`, `SELECT 1 … LIMIT 1` probes — and
 * the memory DuckDB allocated for that work stayed in native allocator
 * arenas the process never returns. Measured on a shore station, 2026-09-17:
 * the same 528 footers cost 969 MB retained through DuckDB and 3 MB through
 * parquetjs, and the 3 MB came back at the next garbage collection because
 * a JavaScript reader's allocations live in V8's heap.
 *
 * Only a chunk whose min equals max is trusted for the context: parquet lets
 * a writer truncate string statistics, which would round min down and max
 * up and yield a prefix rather than the context. A footer that cannot say
 * says null, and the caller decides (usually: read the data through DuckDB
 * for that file, which is the one thing DuckDB is kept for).
 */

import { debugLogger } from './debug-logger';

/** A top-level column as the footer describes it. */
export interface FooterColumn {
  name: string;
  /** Parquet physical type: BOOLEAN, INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY… */
  physicalType: string | null;
  /** Parquet logical (original) type when one is set: UTF8, DECIMAL, JSON… */
  logicalType: string | null;
}

export interface ParquetFooter {
  file: string;
  /** Top-level columns by name. */
  columns: Map<string, FooterColumn>;
  /**
   * The one context the file holds, when every row group's `context`
   * statistics say so exactly; null when any row group lacks the statistic,
   * has a truncated one (min ≠ max), or disagrees with another.
   */
  context: string | null;
  /**
   * `[min, max]` of `signalk_timestamp` per row group, tested per group so
   * the envelope of two groups cannot bridge a gap between them; null when
   * any row group lacks the statistic.
   */
  timestampSpans: Array<[string, string]> | null;
}

/** An object path's component, as schema-cache and the providers use it. */
export interface FooterComponent {
  name: string;
  columnName: string;
  dataType: 'numeric' | 'string' | 'boolean' | 'unknown';
}

/** `value_*` columns that are metadata rather than object components. */
export const NON_COMPONENT_COLUMNS: ReadonlySet<string> = new Set([
  'value_json',
  'value_units',
  'value_description',
  'value_age',
]);

/** The slice of parquetjs this module uses. */
interface Statistics {
  min_value?: unknown;
  max_value?: unknown;
}
interface ColumnChunk {
  meta_data?: { path_in_schema?: string[]; statistics?: Statistics };
}
export interface RowGroup {
  columns: ColumnChunk[];
}
interface Reader {
  metadata: { row_groups: RowGroup[] } | null;
  schema: {
    fieldList: Array<{
      name: string;
      path: string[];
      primitiveType?: string;
      originalType?: string;
      isNested?: boolean;
    }>;
  };
  close(): Promise<void>;
}
interface ReaderLib {
  ParquetReader: { openFile(file: string): Promise<Reader> };
}

// Same guarded load as parquet-writer.ts: a missing library must not fail
// module load; callers see footerReaderAvailable() false and fall back.
let parquetjs: ReaderLib | null = null;
try {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  parquetjs = require('@dsnp/parquetjs') as ReaderLib;
} catch {
  parquetjs = null;
}

export function footerReaderAvailable(): boolean {
  return parquetjs !== null;
}

/**
 * A string statistic as text. parquetjs hands back a decoded value whose JS
 * type follows the column: bytes for a string column, a number for a numeric
 * one. Only string columns are read for text here, and anything else is
 * treated as no statistic rather than coerced (a DOUBLE column's numeric
 * statistic used to reach Buffer.from and throw, 2026-09-17).
 */
export function statText(v: unknown): string | null {
  if (typeof v === 'string') return v;
  if (v instanceof Uint8Array) return Buffer.from(v).toString('utf8');
  return null;
}

/**
 * The context and timestamp spans a footer's row groups state. Exported for
 * the truncated-statistics tests, which cannot make the plugin's writer
 * produce a truncated statistic on demand.
 */
export function readRowGroups(rowGroups: RowGroup[]): {
  context: string | null;
  timestampSpans: Array<[string, string]> | null;
} {
  let context: string | null = null;
  let contextExact = rowGroups.length > 0;
  const spans: Array<[string, string]> = [];
  let spansComplete = rowGroups.length > 0;
  for (const rg of rowGroups) {
    let rgContext: string | null = null;
    let rgSpan: [string, string] | null = null;
    for (const col of rg.columns) {
      const md = col.meta_data;
      const name = md?.path_in_schema?.[0];
      if (name !== 'context' && name !== 'signalk_timestamp') continue;
      const lo = statText(md?.statistics?.min_value);
      const hi = statText(md?.statistics?.max_value);
      if (lo === null || hi === null) continue;
      if (name === 'context') {
        if (lo === hi && lo.length > 0) rgContext = lo;
      } else {
        rgSpan = [lo, hi];
      }
    }
    // Every row group must answer on its own: one that lacks an exact
    // context statistic must not let a later group speak for it.
    if (rgContext === null || (context !== null && rgContext !== context)) {
      contextExact = false;
    } else {
      context = rgContext;
    }
    if (rgSpan === null) spansComplete = false;
    else spans.push(rgSpan);
  }
  return {
    context: contextExact ? context : null,
    timestampSpans: spansComplete ? spans : null,
  };
}

/** Read one file's footer. Rejects when the file cannot be opened or parsed. */
export async function readFooter(file: string): Promise<ParquetFooter> {
  if (!parquetjs) {
    throw new Error(
      'parquet footer reader unavailable: @dsnp/parquetjs missing'
    );
  }
  const reader = await parquetjs.ParquetReader.openFile(file);
  try {
    const columns = new Map<string, FooterColumn>();
    for (const field of reader.schema.fieldList) {
      // Top-level columns only; the plugin writes flat rows.
      if (field.path.length !== 1 || field.isNested) continue;
      columns.set(field.name, {
        name: field.name,
        physicalType: field.primitiveType ?? null,
        logicalType: field.originalType ?? null,
      });
    }
    const { context, timestampSpans } = readRowGroups(
      reader.metadata?.row_groups ?? []
    );
    return { file, columns, context, timestampSpans };
  } finally {
    await reader.close();
  }
}

export interface ReadFootersOptions {
  /**
   * Footers are tiny, so the cost is per-open latency, and a week of a busy
   * vessel is hundreds of files. Sixteen in flight keeps that sub-second
   * without opening a store's worth of descriptors at once.
   */
  concurrency?: number;
  /**
   * Called with each footer as it arrives; return true to stop launching
   * further reads (the ones in flight still complete and are included).
   */
  until?: (footer: ParquetFooter) => boolean;
}

/**
 * Read many footers with bounded parallelism. Rejects with the first file
 * that cannot be read, once the reads in flight have settled, so a caller
 * that wants "every file or nothing" gets exactly that.
 */
export async function readFooters(
  files: string[],
  options: ReadFootersOptions = {}
): Promise<ParquetFooter[]> {
  const concurrency = Math.max(1, options.concurrency ?? 16);
  const out: ParquetFooter[] = [];
  let next = 0;
  let stopped = false;
  let failure: unknown = null;
  const worker = async (): Promise<void> => {
    while (!stopped && failure === null) {
      const idx = next++;
      if (idx >= files.length) return;
      try {
        const footer = await readFooter(files[idx]);
        out.push(footer);
        if (options.until?.(footer)) stopped = true;
      } catch (error) {
        debugLogger.warn(
          `[Parquet Footer] Could not read footer of ${files[idx]}:`,
          error
        );
        failure = error;
      }
    }
  };
  await Promise.all(
    Array.from({ length: Math.min(concurrency, files.length) }, () => worker())
  );
  if (failure !== null) throw failure;
  return out;
}

/**
 * Whether any row group of the file overlaps `[fromIso, toIso)`, from the
 * timestamp statistics; null when the footer could not say.
 */
export function overlapsWindow(
  footer: ParquetFooter,
  fromIso: string,
  toIso: string
): boolean | null {
  if (footer.timestampSpans === null) return null;
  return footer.timestampSpans.some(([lo, hi]) => hi >= fromIso && lo < toIso);
}

/**
 * The category a column's parquet types put it in, matching what the DuckDB
 * type names used to give (a UTF8 BYTE_ARRAY is VARCHAR there, a raw
 * BYTE_ARRAY is BLOB, an INT96 is TIMESTAMP).
 */
export function dataTypeOf(column: FooterColumn): FooterComponent['dataType'] {
  const logical = column.logicalType;
  if (logical === 'UTF8' || logical === 'JSON' || logical === 'ENUM') {
    return 'string';
  }
  if (logical === 'DECIMAL') return 'numeric';
  switch (column.physicalType) {
    case 'BOOLEAN':
      return 'boolean';
    case 'INT32':
    case 'INT64':
    case 'FLOAT':
    case 'DOUBLE':
      return 'numeric';
    default:
      return 'unknown';
  }
}

/** Whether any of the footers has a top-level column of this name. */
export function hasColumn(footers: ParquetFooter[], name: string): boolean {
  return footers.some(f => f.columns.has(name));
}

/**
 * The union of the object components (`value_*` columns, minus the metadata
 * ones) across footers, keyed by component name. The first file to carry a
 * column decides its type, as DuckDB's union_by_name did. Empty for a scalar
 * path.
 */
export function componentColumns(
  footers: ParquetFooter[]
): Map<string, FooterComponent> {
  const components = new Map<string, FooterComponent>();
  for (const footer of footers) {
    for (const column of footer.columns.values()) {
      if (
        !column.name.startsWith('value_') ||
        NON_COMPONENT_COLUMNS.has(column.name)
      ) {
        continue;
      }
      const name = column.name.slice('value_'.length);
      if (components.has(name)) continue;
      components.set(name, {
        name,
        columnName: column.name,
        dataType: dataTypeOf(column),
      });
    }
  }
  return components;
}
