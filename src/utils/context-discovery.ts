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

// Cache for context list
interface ContextListCache {
  contexts: string[];
  timestamp: number;
  dataDir: string;
}

let contextListCache: ContextListCache | null = null;

const hiveBuilder = new HivePathBuilder();

/**
 * Sanitized context dir name -> ALL true context strings read from the data.
 * The directory encoding is lossy (issue #71: ':' and literal '-' both map to
 * '-', so a UUID vessel id can't be reconstructed from the dir name — and two
 * distinct contexts such as `a:b` and `a-b` can collide into one directory),
 * but every parquet record carries the original context as a data column.
 * A context's true name never changes, but the SET of contexts sharing one
 * directory can grow (a new colliding context starts recording), so entries
 * carry the same TTL as the directory-listing cache. Entries for other data
 * directories are purged on rescan so runtime reconfiguration can't
 * accumulate stale directories; clearFileListCache() clears everything.
 */
const trueContextCache = new Map<
  string,
  { contexts: string[]; timestamp: number }
>();

/**
 * Read context=* directory names under tier=raw/. Returns the SANITIZED
 * names — the true context strings are resolved later, per matching context,
 * from the parquet data itself (see resolveTrueContexts).
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
 * Resolve ALL true context strings for a sanitized context directory name by
 * reading them from the data (every record stores the original context).
 * The sanitization is many-to-one, so one directory can hold data for
 * multiple distinct contexts (e.g. `a:b` and `a-b`); a DISTINCT scan over
 * the directory's data files recovers every one. The glob only matches the
 * partition-shaped path=* / year=* / day=* subdirectories, so
 * quarantine/failed/processed/repaired siblings are never touched.
 * Falls back to the legacy lossy reconstruction from the directory name when
 * no file can be read; the fallback is not cached so a later successful read
 * can correct it.
 */
async function resolveTrueContexts(
  dataDir: string,
  sanitized: string
): Promise<string[]> {
  const key = JSON.stringify([dataDir, sanitized]);
  const cached = trueContextCache.get(key);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL.FILE_LIST) {
    return cached.contexts;
  }

  const contexts = await resolveContextsFor(dataDir, sanitized);
  if (contexts !== null && contexts.length > 0) {
    trueContextCache.set(key, { contexts, timestamp: Date.now() });
    return contexts;
  }
  return [hiveBuilder.unsanitizeContext(sanitized)];
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
 * The distinct contexts of one directory, read from the parquet FOOTERS.
 *
 * A file holds exactly one context (the hive layout writes one file per
 * context/path/day), so the `context` column's min and max statistics are
 * that context. Reading them costs a footer per file instead of the whole
 * column of every file, which is what made this the most expensive thing the
 * plugin did: on a store with thousands of AIS contexts a single
 * `/signalk/v1/history/contexts` call took 39s and permanently added 1.4 GB
 * to the server process, because DuckDB's freed buffers are not returned to
 * the OS by the allocator.
 *
 * Only rows where min equals max are trusted. Parquet allows a writer to
 * truncate string statistics, which would round min down and max up and yield
 * a prefix rather than the context; equality rules that out. A file whose
 * stats are missing or truncated leaves the directory unresolved here and the
 * caller falls back to reading the data.
 *
 * `range` restricts to files whose `signalk_timestamp` statistics overlap the
 * window, which is likewise decided from the footer.
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
  const globs = contextGlobs(dataDir, sanitized, range);
  if (globs.length === 0) return [];
  // A day directory bounds a file only to the day; the timestamp statistics
  // still decide whether it overlaps the requested window.
  const overlap = range
    ? `AND t0 IS NOT NULL AND t1 IS NOT NULL
         AND t1 >= '${escapeSqlString(range.fromIso)}'
         AND t0 <= '${escapeSqlString(range.toIso)}'`
    : '';
  try {
    const connection = await DuckDBPool.getConnection();
    try {
      const result = await connection.runAndReadAll(
        `SELECT DISTINCT ctx FROM (
           SELECT
             file_name,
             MIN(CASE WHEN path_in_schema = 'context'
                      AND stats_min = stats_max THEN stats_min END) AS ctx,
             MIN(CASE WHEN path_in_schema = 'signalk_timestamp'
                      THEN stats_min END) AS t0,
             MAX(CASE WHEN path_in_schema = 'signalk_timestamp'
                      THEN stats_max END) AS t1
           FROM parquet_metadata(${globList(globs)})
           GROUP BY file_name
         ) WHERE ctx IS NOT NULL ${overlap}`
      );
      return result
        .getRowObjects()
        .map(row => row.ctx)
        .filter((c): c is string => typeof c === 'string' && c.length > 0);
    } finally {
      connection.disconnectSync();
    }
  } catch (error) {
    if (isNoFilesError(error)) return [];
    debugLogger.warn(
      `[Context Discovery] Could not read context statistics under context=${sanitized}:`,
      error
    );
    return null;
  }
}

/**
 * SELECT DISTINCT context over one sanitized context directory's data files,
 * optionally constrained to a signalk_timestamp range. Returns null when the
 * query fails (caller falls back).
 *
 * This reads the data and is therefore expensive — see
 * queryContextsFromFooters, which answers the same question from the file
 * footers and is tried first. This remains as the fallback for files written
 * without usable column statistics.
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
      // Purge resolution-cache entries for other data directories so a
      // runtime setDataDir() reconfigure can't accumulate stale entries.
      for (const key of trueContextCache.keys()) {
        const [cachedDataDir] = JSON.parse(key) as [string, string];
        if (cachedDataDir !== dataDir) trueContextCache.delete(key);
      }
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
    // Sequential on purpose: each resolution opens a DuckDB connection, and a
    // large AIS store can have hundreds of context directories — a Promise.all
    // fan-out would open them all at once. After the first request the
    // resolutions are cached, so the sequential cost is a cold-start-only one.
    const fromIso = from.toInstant().toString();
    const toIso = to.toInstant().toString();
    const matchingContexts: string[] = [];
    for (const sanitized of matchingSanitized) {
      const resolved = await resolveTrueContexts(dataDir, sanitized);
      if (resolved.length > 1) {
        // Collided directory (e.g. `a:b` and `a-b` share it): the day-level
        // directory check above only proves SOME context in it has data in
        // range. Re-query constrained to the requested range so a collider
        // whose data lies entirely outside the range isn't reported. Rare
        // (requires ids differing only colon-vs-dash), so the extra query
        // costs nothing in the common single-context case.
        const inRange = await resolveContextsFor(dataDir, sanitized, {
          fromIso,
          toIso,
        });
        matchingContexts.push(...(inRange ?? resolved));
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
  trueContextCache.clear();
  debugLogger.log('[Context Discovery] Context list cache cleared');
}
