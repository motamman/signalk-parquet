import * as fs from 'fs-extra';
import * as path from 'path';
import { Context } from '@signalk/server-api';
import { ZonedDateTime } from '@js-joda/core';
import { debugLogger } from './debug-logger';
import { CACHE_TTL } from '../config/cache-defaults';
import { HivePathBuilder } from './hive-path-builder';
import { DuckDBPool } from './duckdb-pool';
import { escapeSqlString } from './sql-escape';
import { SpatialFilter, buildSpatialSqlClause } from './spatial-queries';

// Same guarded load as parquet-writer.ts: a missing library leaves the DuckDB
// data read as the only resolver rather than failing module load.
type FooterReader = {
  ParquetReader: {
    openFile(file: string): Promise<{
      metadata: {
        row_groups: Array<{
          columns: Array<{
            meta_data?: {
              path_in_schema?: string[];
              statistics?: {
                min_value?: unknown;
                max_value?: unknown;
              };
            };
          }>;
        }>;
      } | null;
      close(): Promise<void>;
    }>;
  };
};
let parquetjs: FooterReader | null = null;
try {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  parquetjs = require('@dsnp/parquetjs') as FooterReader;
} catch {
  parquetjs = null;
}

// Cache for context list
interface ContextListCache {
  contexts: string[];
  timestamp: number;
  dataDir: string;
}

let contextListCache: ContextListCache | null = null;

const hiveBuilder = new HivePathBuilder();

/**
 * Read context=* directory names under tier=raw/. Returns the SANITIZED
 * names — the true context strings are resolved later, per matching context,
 * from the parquet data itself (see resolveContextsFor).
 */
async function discoverContextDirsFromHive(dataDir: string): Promise<string[]> {
  const tierRawDir = path.join(dataDir, 'tier=raw');

  try {
    const entries = await fs.readdir(tierRawDir, { withFileTypes: true });
    const contexts: string[] = [];

    for (const entry of entries) {
      if (!entry.isDirectory()) continue;

      const match = entry.name.match(/^context=(.+)$/);
      if (match) {
        contexts.push(match[1]);
      }
    }

    return contexts.sort();
  } catch (error) {
    debugLogger.error(
      '[Context Discovery] Error reading tier=raw/ directory:',
      error
    );
    return [];
  }
}

/**
 * The data files of one sanitized context directory, narrowed to the days the
 * caller asked about.
 *
 * Without a range this is every day ever recorded, which is what the contexts
 * listing used to read in order to answer a question about one week. A day is
 * a directory, so restricting the glob to the window's day partitions is the
 * difference between opening a store's entire history and opening seven days
 * of it. Returns one glob per day, which DuckDB takes as a list.
 */
function contextGlobs(
  dataDir: string,
  sanitized: string,
  range?: { fromIso: string; toIso: string }
): string[] {
  const dir = path.join(dataDir, 'tier=raw', `context=${sanitized}`);
  if (!range) {
    return [path.join(dir, 'path=*', 'year=*', 'day=*', '*.parquet')];
  }
  const days = hiveBuilder.getDaysInRange(
    new Date(range.fromIso),
    new Date(range.toIso)
  );
  if (days.length === 0) {
    return [];
  }
  return days.map(d =>
    path.join(
      dir,
      'path=*',
      `year=${d.year}`,
      `day=${String(d.dayOfYear).padStart(3, '0')}`,
      '*.parquet'
    )
  );
}

/** A DuckDB list literal of globs. */
function globList(globs: string[]): string {
  return `[${globs.map(g => `'${escapeSqlString(g)}'`).join(', ')}]`;
}

/** DuckDB's "there is nothing here", which is an answer rather than a failure. */
function isNoFilesError(err: unknown): boolean {
  return err instanceof Error && /No files found/i.test(err.message);
}

/**
 * The distinct contexts of one directory, read from the parquet FOOTERS with
 * parquetjs — plain JavaScript, no DuckDB.
 *
 * A file holds exactly one context (the hive layout writes one file per
 * context/path/day), so the `context` column's min and max statistics are
 * that context. Reading them is a footer per file rather than a column scan,
 * and doing it in JavaScript keeps the allocations in V8's heap, which is
 * collected. That second point is the one that matters: the same work done
 * through DuckDB left its memory in native allocator arenas that glibc never
 * returns — measured on a shore station, one seven-day
 * `/signalk/v1/history/contexts` call took 7s and permanently added 504 MB to
 * the server, and repeating it kept climbing. With parquetjs the identical
 * work over the same 528 files took 0.5s and 3 MB, and a GC took that back.
 *
 * Only a chunk whose min equals max is trusted: parquet lets a writer truncate
 * string statistics, which would round min down and max up and yield a prefix
 * rather than the context. Any file that cannot be opened or lacks exact
 * context statistics (or, with a range, timestamp statistics) leaves the
 * whole directory unresolved: the answer is null rather than the contexts of
 * the other files, and the caller falls back to reading the data with DuckDB.
 *
 * `range` restricts to files with a row group whose `signalk_timestamp`
 * statistics overlap the window, decided from the same footer. Each row group
 * is tested on its own: the envelope of all of them would bridge the gap
 * between two groups and admit a file with no rows in the window. Files come
 * from the requested day partitions only, so a week's question opens a week's
 * files, not the store.
 *
 * Exported so a test can assert this path actually answers for files the
 * plugin writes. If the writer ever stops emitting usable statistics the
 * fallback would quietly take over and the cost would return unnoticed.
 */
export async function queryContextsFromFooters(
  dataDir: string,
  sanitized: string,
  range?: { fromIso: string; toIso: string }
): Promise<string[] | null> {
  if (!parquetjs) return null;
  const files = await listContextFiles(dataDir, sanitized, range);
  if (files === null) return null;
  const found = new Set<string>();
  // Bounded fan-out: footers are tiny, so the cost is per-open latency, and
  // a week of a busy vessel is hundreds of files. Sixteen in flight keeps
  // that sub-second without opening a store's worth of descriptors at once.
  // An unreadable file, or one without exact context statistics, aborts the
  // whole directory to the DuckDB data read rather than silently dropping a
  // vessel; a file whose timestamps miss the window is simply skipped.
  const lib = parquetjs;
  let next = 0;
  let aborted = false;
  const worker = async (): Promise<void> => {
    while (!aborted) {
      const idx = next++;
      if (idx >= files.length) return;
      const r = await contextOfFile(lib, files[idx], range);
      if (r.kind === 'abort') aborted = true;
      else if (r.kind === 'context') found.add(r.value);
    }
  };
  await Promise.all(
    Array.from({ length: Math.min(16, files.length) }, () => worker())
  );
  if (aborted) return null;
  return [...found];
}

type FooterVerdict =
  { kind: 'context'; value: string } | { kind: 'skip' } | { kind: 'abort' };

/** What one file's footer says: its context, nothing in range, or unusable. */
async function contextOfFile(
  lib: FooterReader,
  file: string,
  range?: { fromIso: string; toIso: string }
): Promise<FooterVerdict> {
  let reader: Awaited<ReturnType<FooterReader['ParquetReader']['openFile']>>;
  try {
    reader = await lib.ParquetReader.openFile(file);
  } catch (error) {
    debugLogger.warn(
      `[Context Discovery] Could not open footer of ${file}:`,
      error
    );
    return { kind: 'abort' };
  }
  try {
    let context: string | null = null;
    const spans: Array<[string, string]> = [];
    for (const rg of reader.metadata?.row_groups ?? []) {
      for (const col of rg.columns) {
        const md = col.meta_data;
        const name = md?.path_in_schema?.[0];
        if (name !== 'context' && name !== 'signalk_timestamp') continue;
        const st = md?.statistics;
        const lo = statText(st?.min_value);
        const hi = statText(st?.max_value);
        if (lo === null || hi === null) continue;
        if (name === 'context') {
          if (lo === hi && lo.length > 0) context = lo;
        } else if (name === 'signalk_timestamp') {
          spans.push([lo, hi]);
        }
      }
    }
    if (context === null) return { kind: 'abort' };
    if (range) {
      if (spans.length === 0) return { kind: 'abort' };
      const overlaps = spans.some(
        ([lo, hi]) => hi >= range.fromIso && lo <= range.toIso
      );
      if (!overlaps) return { kind: 'skip' };
    }
    return { kind: 'context', value: context };
  } finally {
    await reader.close();
  }
}

/**
 * The parquet files of one sanitized context directory, from its requested
 * day partitions (every day when no range is given). Null when the context
 * directory itself cannot be read; an empty list when it holds nothing.
 */
async function listContextFiles(
  dataDir: string,
  sanitized: string,
  range?: { fromIso: string; toIso: string }
): Promise<string[] | null> {
  const contextDir = path.join(dataDir, 'tier=raw', `context=${sanitized}`);
  let pathEntries: import('fs').Dirent[];
  try {
    pathEntries = await fs.readdir(contextDir, { withFileTypes: true });
  } catch {
    return null;
  }
  const wantedDays = range
    ? new Set(
        hiveBuilder
          .getDaysInRange(new Date(range.fromIso), new Date(range.toIso))
          .map(d => `${d.year}/${String(d.dayOfYear).padStart(3, '0')}`)
      )
    : null;
  if (wantedDays && wantedDays.size === 0) return [];

  const files: string[] = [];
  for (const pathEntry of pathEntries) {
    if (!pathEntry.isDirectory() || !pathEntry.name.startsWith('path='))
      continue;
    const pathDir = path.join(contextDir, pathEntry.name);
    for (const yearEntry of await readDirSafe(pathDir)) {
      if (!yearEntry.isDirectory() || !yearEntry.name.startsWith('year='))
        continue;
      const year = yearEntry.name.slice('year='.length);
      const yearDir = path.join(pathDir, yearEntry.name);
      for (const dayEntry of await readDirSafe(yearDir)) {
        if (!dayEntry.isDirectory() || !dayEntry.name.startsWith('day='))
          continue;
        const day = dayEntry.name.slice('day='.length);
        if (wantedDays && !wantedDays.has(`${year}/${day}`)) continue;
        const dayDir = path.join(yearDir, dayEntry.name);
        for (const f of await readDirSafe(dayDir)) {
          if (f.isFile() && f.name.endsWith('.parquet')) {
            files.push(path.join(dayDir, f.name));
          }
        }
      }
    }
  }
  return files;
}

/**
 * A string statistic as text. parquetjs hands back a decoded value whose JS
 * type follows the column: bytes for a string column, a number for a numeric
 * one. Only the two string columns read here are wanted, and anything else
 * is treated as no statistic rather than coerced.
 */
function statText(v: unknown): string | null {
  if (typeof v === 'string') return v;
  if (v instanceof Uint8Array) return Buffer.from(v).toString('utf8');
  return null;
}

async function readDirSafe(dir: string): Promise<import('fs').Dirent[]> {
  try {
    return await fs.readdir(dir, { withFileTypes: true });
  } catch {
    return [];
  }
}

/**
 * SELECT DISTINCT context over one sanitized context directory's data files,
 * optionally constrained to a signalk_timestamp range. Returns null when the
 * query fails (caller falls back).
 *
 * This reads the data through DuckDB and is therefore expensive in both time
 * and memory that is never returned — see queryContextsFromFooters, which
 * answers the same question from the file footers in JavaScript and is tried
 * first. This remains as the fallback for files written without usable
 * column statistics, or when parquetjs is unavailable.
 */
async function queryDistinctContexts(
  dataDir: string,
  sanitized: string,
  range?: { fromIso: string; toIso: string }
): Promise<string[] | null> {
  const globs = contextGlobs(dataDir, sanitized, range);
  if (globs.length === 0) return [];
  const rangeClause = range
    ? ` WHERE signalk_timestamp >= '${escapeSqlString(range.fromIso)}' AND signalk_timestamp <= '${escapeSqlString(range.toIso)}'`
    : '';
  try {
    const connection = await DuckDBPool.getConnection();
    try {
      // hive_partitioning=false is required: DuckDB auto-detects the
      // key=value path segments otherwise, and the (sanitized) partition
      // value shadows the files' `context` data column.
      const result = await connection.runAndReadAll(
        `SELECT DISTINCT context FROM read_parquet(${globList(globs)}, hive_partitioning=false, union_by_name=true)${rangeClause}`
      );
      return result
        .getRowObjects()
        .map(row => row.context)
        .filter((c): c is string => typeof c === 'string' && c.length > 0);
    } finally {
      connection.disconnectSync();
    }
  } catch (error) {
    if (isNoFilesError(error)) return [];
    debugLogger.warn(
      `[Context Discovery] Could not read context column under context=${sanitized}:`,
      error
    );
    return null;
  }
}

/**
 * The distinct contexts of one directory: from the footers when they answer,
 * from the data when they do not.
 */
async function resolveContextsFor(
  dataDir: string,
  sanitized: string,
  range?: { fromIso: string; toIso: string }
): Promise<string[] | null> {
  const fromFooters = await queryContextsFromFooters(dataDir, sanitized, range);
  if (fromFooters !== null && fromFooters.length > 0) {
    return fromFooters;
  }
  return queryDistinctContexts(dataDir, sanitized, range);
}

/**
 * Get available SignalK contexts that have data within a specific time range.
 * Reads context=* dirs under tier=raw/, then filters by checking if any
 * year=YYYY/day=DDD subdirectories fall within the from/to range.
 */
export async function getAvailableContextsForTimeRange(
  dataDir: string,
  from: ZonedDateTime,
  to: ZonedDateTime
): Promise<Context[]> {
  try {
    // Get all contexts (cached)
    const now = Date.now();
    let allContexts: string[];

    if (
      contextListCache &&
      contextListCache.dataDir === dataDir &&
      now - contextListCache.timestamp < CACHE_TTL.FILE_LIST
    ) {
      allContexts = contextListCache.contexts;
      debugLogger.log(
        `[Context Discovery] Using cached context list (${allContexts.length} contexts, age: ${Math.round((now - contextListCache.timestamp) / 1000)}s)`
      );
    } else {
      debugLogger.log(
        `[Context Discovery] Scanning hive directories for contexts...`
      );
      allContexts = await discoverContextDirsFromHive(dataDir);

      contextListCache = {
        contexts: allContexts,
        timestamp: now,
        dataDir,
      };
      debugLogger.log(
        `[Context Discovery] Cached ${allContexts.length} contexts`
      );
    }

    if (allContexts.length === 0) {
      return [];
    }

    // Compute from/to as year + dayOfYear for range comparison
    const fromDate = new Date(from.toInstant().toString());
    const toDate = new Date(to.toInstant().toString());
    const fromYearDay = dateToYearDay(fromDate);
    const toYearDay = dateToYearDay(toDate);

    // Filter contexts (still by their sanitized dir names) by checking if
    // they have any matching year/day subdirs
    const tierRawDir = path.join(dataDir, 'tier=raw');
    const matchingSanitized: string[] = [];

    for (const sanitized of allContexts) {
      const contextDir = path.join(tierRawDir, `context=${sanitized}`);

      const hasData = await contextHasDataInRange(
        contextDir,
        fromYearDay,
        toYearDay
      );
      if (hasData) {
        matchingSanitized.push(sanitized);
      }
    }

    // Resolve the true context strings from the data — the dir-name
    // reconstruction is lossy for ids containing literal dashes (issue #71),
    // and one sanitized directory can hold several colliding contexts.
    //
    // Each directory is resolved from the files of the requested days only,
    // with each footer's own timestamps deciding overlap. That answers the
    // question directly, collisions included (every file names its own
    // context), so there is no whole-history name lookup and no long-lived
    // per-directory name cache to go stale or to hide a collider whose data
    // falls outside the window. A week of footers is cheap enough that the
    // cache bought nothing but that risk.
    const fromIso = from.toInstant().toString();
    const toIso = to.toInstant().toString();
    const matchingContexts: string[] = [];
    for (const sanitized of matchingSanitized) {
      const resolved = await resolveContextsFor(dataDir, sanitized, {
        fromIso,
        toIso,
      });
      if (resolved === null) {
        // Nothing could be read at all: the lossy directory name beats
        // dropping a vessel the day-directory check says has data.
        matchingContexts.push(hiveBuilder.unsanitizeContext(sanitized));
      } else {
        matchingContexts.push(...resolved);
      }
    }

    debugLogger.log(
      `[Context Discovery] Found ${matchingContexts.length} contexts with data in time range`
    );

    return matchingContexts.sort() as Context[];
  } catch (error) {
    debugLogger.error('Error scanning contexts:', error);
    return [];
  }
}

/**
 * Convert a Date to { year, day } for range comparison.
 * Matches HivePathBuilder.getDayOfYear() logic exactly.
 */
function dateToYearDay(d: Date): { year: number; day: number } {
  const year = d.getUTCFullYear();
  const start = new Date(Date.UTC(year, 0, 0)); // day 0 — same as HivePathBuilder
  const day = Math.floor(
    (d.getTime() - start.getTime()) / (24 * 60 * 60 * 1000)
  );
  return { year, day };
}

/**
 * Check if a year/day falls within [from, to] range (inclusive).
 */
function yearDayInRange(
  year: number,
  day: number,
  from: { year: number; day: number },
  to: { year: number; day: number }
): boolean {
  const val = year * 1000 + day;
  return val >= from.year * 1000 + from.day && val <= to.year * 1000 + to.day;
}

/**
 * Check if a context directory has any path=* / year=YYYY / day=DDD
 * subdirectories that fall within the from/to range.
 */
async function contextHasDataInRange(
  contextDir: string,
  from: { year: number; day: number },
  to: { year: number; day: number }
): Promise<boolean> {
  try {
    const pathEntries = await fs.readdir(contextDir, { withFileTypes: true });

    for (const pathEntry of pathEntries) {
      if (!pathEntry.isDirectory() || !pathEntry.name.startsWith('path=')) {
        continue;
      }

      const pathDir = path.join(contextDir, pathEntry.name);

      let yearEntries;
      try {
        yearEntries = await fs.readdir(pathDir, { withFileTypes: true });
      } catch {
        continue;
      }

      for (const yearEntry of yearEntries) {
        if (!yearEntry.isDirectory()) continue;
        const yearMatch = yearEntry.name.match(/^year=(\d+)$/);
        if (!yearMatch) continue;

        const year = parseInt(yearMatch[1], 10);

        // Quick skip: if this year is entirely outside the range
        if (year < from.year || year > to.year) continue;

        const yearDir = path.join(pathDir, yearEntry.name);

        let dayEntries;
        try {
          dayEntries = await fs.readdir(yearDir, { withFileTypes: true });
        } catch {
          continue;
        }

        for (const dayEntry of dayEntries) {
          if (!dayEntry.isDirectory()) continue;
          const dayMatch = dayEntry.name.match(/^day=(\d+)$/);
          if (!dayMatch) continue;

          const day = parseInt(dayMatch[1], 10);
          if (yearDayInRange(year, day, from, to)) {
            return true;
          }
        }
      }
    }
  } catch {
    // Context dir doesn't exist or can't be read
  }

  return false;
}

/**
 * Find all vessel contexts with position data inside a spatial filter and time range.
 * Accepts bbox ("west,south,east,north") or radius ("lon,lat,meters") via SpatialFilter.
 * Uses a single DuckDB query with hive_partitioning on navigation__position files only.
 */
export async function getContextsInSpatialFilter(
  dataDir: string,
  from: ZonedDateTime,
  to: ZonedDateTime,
  filter: SpatialFilter
): Promise<Context[]> {
  const fromIso = from.toInstant().toString();
  const toIso = to.toInstant().toString();

  const glob = path.join(
    dataDir,
    'tier=raw',
    'context=*',
    'path=navigation__position',
    'year=*',
    'day=*',
    '*.parquet'
  );

  const spatialClause = buildSpatialSqlClause(filter);

  // hive_partitioning=false (explicitly — DuckDB auto-detects key=value path
  // segments otherwise): `context` must resolve to the DATA column (the
  // original, unsanitized context every record stores), never the sanitized
  // partition value from the directory name. Partition pruning isn't lost —
  // the query never filtered on partition columns.
  const query = `
    SELECT DISTINCT context
    FROM read_parquet('${escapeSqlString(glob)}', hive_partitioning=false, union_by_name=true)
    WHERE signalk_timestamp >= '${fromIso}'
      AND signalk_timestamp < '${toIso}'
      AND ${spatialClause}
  `;

  const connection = await DuckDBPool.getConnection();
  try {
    debugLogger.log(
      `[Context Discovery] Spatial query (${filter.type}): ${JSON.stringify(filter.bbox)}`
    );

    const result = await connection.runAndReadAll(query);
    const rows = result.getRowObjects();

    // row.context is the data column — already the true context string.
    // Running unsanitizeContext on it corrupted dash-bearing ids (issue #71:
    // it turned a UUID's literal dashes into colons).
    const contexts = rows.map(row => row.context as string).sort() as Context[];

    debugLogger.log(
      `[Context Discovery] Found ${contexts.length} contexts in spatial filter`
    );

    return contexts;
  } finally {
    connection.disconnectSync();
  }
}

/**
 * Clear the context list cache (useful for testing or when data structure changes)
 */
export function clearFileListCache(): void {
  contextListCache = null;
  debugLogger.log('[Context Discovery] Context list cache cleared');
}
