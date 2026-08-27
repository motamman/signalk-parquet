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
 * Sanitized context dir name -> true context string read from the data.
 * The directory encoding is lossy (issue #71: ':' and literal '-' both map to
 * '-', so a UUID vessel id can't be reconstructed from the dir name), but
 * every parquet record carries the original context as a data column. A
 * context's true name never changes, so entries live for the process;
 * clearFileListCache() clears it for tests.
 */
const trueContextCache = new Map<string, string>();

/**
 * Read context=* directory names under tier=raw/. Returns the SANITIZED
 * names — the true context strings are resolved later, per matching context,
 * from the parquet data itself (see resolveTrueContext).
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
 * Find one data parquet file under a context directory: first
 * path=* / year=* / day=* / *.parquet encountered. Only descends the
 * partition-shaped subdirectories, so quarantine/failed/processed/repaired
 * siblings are never touched.
 */
async function findFirstParquetFile(
  contextDir: string
): Promise<string | null> {
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
        if (!yearEntry.isDirectory() || !/^year=\d+$/.test(yearEntry.name)) {
          continue;
        }
        const yearDir = path.join(pathDir, yearEntry.name);
        let dayEntries;
        try {
          dayEntries = await fs.readdir(yearDir, { withFileTypes: true });
        } catch {
          continue;
        }
        for (const dayEntry of dayEntries) {
          if (!dayEntry.isDirectory() || !/^day=\d+$/.test(dayEntry.name)) {
            continue;
          }
          const dayDir = path.join(yearDir, dayEntry.name);
          let files;
          try {
            files = await fs.readdir(dayDir);
          } catch {
            continue;
          }
          const parquet = files.find(f => f.endsWith('.parquet'));
          if (parquet) {
            return path.join(dayDir, parquet);
          }
        }
      }
    }
  } catch {
    // context dir unreadable — caller falls back
  }
  return null;
}

/**
 * Resolve the true context string for a sanitized context directory name by
 * reading it from the data (every record stores the original context).
 * Falls back to the legacy lossy reconstruction from the directory name when
 * no file can be read; the fallback is not cached so a later successful read
 * can correct it.
 */
async function resolveTrueContext(
  dataDir: string,
  sanitized: string
): Promise<string> {
  const key = JSON.stringify([dataDir, sanitized]);
  const cached = trueContextCache.get(key);
  if (cached) return cached;

  const contextDir = path.join(dataDir, 'tier=raw', `context=${sanitized}`);
  const file = await findFirstParquetFile(contextDir);
  if (file) {
    try {
      const connection = await DuckDBPool.getConnection();
      try {
        // hive_partitioning=false is required: DuckDB auto-detects the
        // key=value path segments otherwise, and the (sanitized) partition
        // value shadows the file's `context` data column.
        const result = await connection.runAndReadAll(
          `SELECT context FROM read_parquet('${escapeSqlString(file)}', hive_partitioning=false) LIMIT 1`
        );
        const ctx = result.getRowObjects()[0]?.context;
        if (typeof ctx === 'string' && ctx.length > 0) {
          trueContextCache.set(key, ctx);
          return ctx;
        }
      } finally {
        connection.disconnectSync();
      }
    } catch (error) {
      debugLogger.warn(
        `[Context Discovery] Could not read context column from ${file}:`,
        error
      );
    }
  }
  return hiveBuilder.unsanitizeContext(sanitized);
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
    // reconstruction is lossy for ids containing literal dashes (issue #71).
    const matchingContexts = await Promise.all(
      matchingSanitized.map(s => resolveTrueContext(dataDir, s))
    );

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
