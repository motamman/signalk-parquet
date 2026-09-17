/**
 * One way to list parquet files, and one way to hand them to DuckDB.
 *
 * Every read site used to build its own glob, and most of them opened a
 * path's ENTIRE history to answer a question about a window: DuckDB has to
 * read every matched file's footer before it can prune by timestamp, and the
 * memory it allocates for that is retained by the process (measured on a
 * shore station, 2026-09-16: a dashboard's one-day query grew the server by
 * 366 MB per call through a `year=*\/day=*` glob, and by 1-3 MB once narrowed
 * to the window's days). A glob also descends into `processed/`,
 * `quarantine/`, `failed/` and `repaired/` only for a `filename NOT LIKE`
 * clause to filter them back out.
 *
 * `filesFor` returns the explicit files of exactly the day partitions a
 * window touches, walked with hive-walk (which never enters the special
 * directories), and `readParquetSql` is the only place that spells
 * `read_parquet(`, so the flags that keep a partition value from shadowing a
 * data column cannot be forgotten. The invariant test
 * test/unit/parquet-read-invariant.test.ts enforces both.
 *
 * Example:
 *   await filesFor({ dataDir, contexts: ['vessels.urn:mrn:imo:mmsi:1'],
 *                    paths: ['navigation.position'],
 *                    fromIso: '2026-09-15T00:00:00Z', toIso: '2026-09-17T00:00:00Z' })
 *   -> ['<dataDir>/tier=raw/context=vessels__urn-mrn-imo-mmsi-1/path=navigation__position/year=2026/day=258/signalk_data_....parquet',
 *       '…/day=259/….parquet']
 */

import { HivePathBuilder } from './hive-path-builder';
import {
  dayKey,
  listHiveDirs,
  listParquetFiles,
  yieldToEventLoop,
} from './hive-walk';
import { escapeSqlString } from './sql-escape';

const hive = new HivePathBuilder();

export interface FilesForOptions {
  dataDir: string;
  /** Tier partition value; the raw tier when omitted; 'all' for every tier. */
  tier?: string;
  /**
   * True context strings (sanitized here), or 'all' for every context
   * directory. `contextDirs` adds already-sanitized directory values for the
   * one caller that has only those (the contexts listing, whose directory
   * names are lossy and may hold several colliding contexts).
   */
  contexts: string[] | 'all';
  contextDirs?: string[];
  /** SignalK paths (sanitized here), or 'all' for every path directory. */
  paths: string[] | 'all';
  /**
   * Half-open window [fromIso, toIso), the same one the SQL predicates use.
   * Both or neither: without a window the listing is the WHOLE history of
   * the contexts and paths named, which is the cost this module exists to
   * stop paying. Omit it only when the whole history is what is meant.
   */
  fromIso?: string;
  toIso?: string;
}

/**
 * Past this many days the listing prunes by year only, so a multi-year
 * window costs one Set of years rather than thousands of day keys. The
 * caller's timestamp predicate still does the exact filtering.
 */
const MAX_DAY_KEYS = 366;

/**
 * The UTC day partitions a half-open window touches. A `to` on a midnight
 * boundary does not open that day: the last day is the one holding the final
 * included instant. Empty, reversed and unparseable windows touch nothing.
 */
export function partitionDays(
  fromIso: string,
  toIso: string
): Array<{ year: number; dayOfYear: number }> {
  const from = new Date(fromIso);
  const to = new Date(toIso);
  if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime())) return [];
  if (from >= to) return [];
  return hive.getDaysInRange(from, new Date(to.getTime() - 1));
}

/**
 * The parquet files of the day partitions `[fromIso, toIso)` touches, for
 * the given tier, contexts and paths: absolute paths, sorted, none from the
 * special directories. Missing directories contribute nothing; an empty
 * result means "no parquet side" and must not become a query.
 */
export async function filesFor(options: FilesForOptions): Promise<string[]> {
  const windowed = options.fromIso !== undefined || options.toIso !== undefined;
  const days = windowed
    ? partitionDays(options.fromIso ?? '', options.toIso ?? '')
    : [];
  if (windowed && days.length === 0) return [];

  const contexts =
    options.contexts === 'all'
      ? undefined
      : [
          ...options.contexts.map(c => hive.sanitizeContext(c)),
          ...(options.contextDirs ?? []),
        ];
  if (contexts && contexts.length === 0) return [];
  const paths =
    options.paths === 'all'
      ? undefined
      : options.paths.map(p => hive.sanitizePath(p));
  if (paths && paths.length === 0) return [];

  // Day directories for the window's days, plus every touched year
  // directory itself: compaction merges a year's day files into one file
  // that lives directly in `year=YYYY/` and deletes the days, so a window
  // in a compacted year finds its rows there and nowhere else. The SQL
  // predicate still does the exact time filtering.
  const dirs = await listHiveDirs(options.dataDir, {
    level: 'day',
    includeYearDirs: true,
    tiers: options.tier === 'all' ? undefined : [options.tier ?? 'raw'],
    contexts,
    paths,
    years: windowed ? [...new Set(days.map(d => d.year))] : undefined,
    days:
      !windowed || days.length > MAX_DAY_KEYS
        ? undefined
        : days.map(d => dayKey(d.year, d.dayOfYear)),
  });

  const files: string[] = [];
  let reads = 0;
  for (const dir of dirs) {
    reads += 1;
    if (reads % 100 === 0) await yieldToEventLoop();
    files.push(...(await listParquetFiles(dir.dayDir ?? dir.yearDir)));
  }
  return files.sort();
}

/**
 * The `read_parquet(...)` expression for an explicit file list.
 *
 * `hive_partitioning=false` is required: DuckDB otherwise derives columns
 * from the `key=value` path segments, and the sanitized `context` partition
 * value shadows the files' `context` data column (issue #71).
 * `union_by_name=true` reconciles schema drift across day files.
 *
 * An empty list is a caller error: it means "no parquet side", and the
 * caller decides what that means for its query (usually: buffer only, or an
 * empty answer). DuckDB would report it as an error about no files, which
 * is the exception path this module exists to remove.
 */
export function readParquetSql(
  files: string[],
  options: { filename?: boolean } = {}
): string {
  if (files.length === 0) {
    throw new Error(
      'readParquetSql: empty file list; an empty list is "no parquet side", not a query'
    );
  }
  const list = files.map(f => `'${escapeSqlString(f)}'`).join(', ');
  // `filename=true` adds a column naming each row's file, for a caller that
  // reads several files in one query and needs to know which said what.
  const filename = options.filename ? ', filename=true' : '';
  return `read_parquet([${list}], hive_partitioning=false, union_by_name=true${filename})`;
}

/**
 * The `read_parquet(...)` expression for an S3 glob from
 * HivePathBuilder.buildS3Glob, which already narrows to the window's day
 * partitions. Same flags as the local builder: a query that unions the two
 * sides needs the same columns from each, and the S3 key has the same
 * `key=value` segments DuckDB would otherwise turn into columns.
 */
export function readS3ParquetSql(glob: string): string {
  if (!glob.startsWith('s3://')) {
    throw new Error(`readS3ParquetSql: not an S3 URL: ${glob}`);
  }
  return `read_parquet('${escapeSqlString(glob)}', hive_partitioning=false, union_by_name=true)`;
}
