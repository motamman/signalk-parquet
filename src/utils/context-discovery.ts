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
import { filesFor, readParquetSql } from './parquet-files';
import { isoBound } from './iso-time';
import {
  footerReaderAvailable,
  overlapsWindow,
  readFooters,
} from './parquet-footer';

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
 * The data files of one sanitized context directory, narrowed to the days
 * the caller asked about (every day when no range is given, which only the
 * footer-path test does).
 */
function contextFiles(
  dataDir: string,
  sanitized: string,
  range?: { fromIso: string; toIso: string }
): Promise<string[]> {
  return filesFor({
    dataDir,
    contexts: [],
    contextDirs: [sanitized],
    paths: 'all',
    ...(range ?? {}),
  });
}

/**
 * The distinct contexts of one directory, read from the parquet FOOTERS —
 * plain JavaScript, no DuckDB (see parquet-footer.ts for why that is the
 * point: measured on a shore station, one seven-day contexts call took 7s
 * and permanently added 504 MB through DuckDB, 0.5s and a collectable 3 MB
 * through the footer reader).
 *
 * Any file that cannot be opened or lacks exact context statistics (or, with
 * a range, timestamp statistics) leaves the whole directory unresolved: the
 * answer is null rather than the contexts of the other files, and the caller
 * falls back to reading the data with DuckDB. A file whose timestamps miss
 * the window is skipped. Files come from the requested day partitions only,
 * so a week's question opens a week's files, not the store.
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
  if (!footerReaderAvailable()) return null;
  const files = await contextFiles(dataDir, sanitized, range);
  if (files.length === 0) return [];
  const unusable = (footer: {
    context: string | null;
    timestampSpans: unknown;
  }): boolean =>
    footer.context === null ||
    (range !== undefined && footer.timestampSpans === null);
  let footers;
  try {
    footers = await readFooters(files, { until: unusable });
  } catch {
    return null;
  }
  const found = new Set<string>();
  for (const footer of footers) {
    if (unusable(footer)) return null;
    if (range && !overlapsWindow(footer, range.fromIso, range.toIso)) continue;
    found.add(footer.context as string);
  }
  return [...found];
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
  const files = await contextFiles(dataDir, sanitized, range);
  if (files.length === 0) return [];
  const rangeClause = range
    ? ` WHERE signalk_timestamp >= '${escapeSqlString(range.fromIso)}' AND signalk_timestamp < '${escapeSqlString(range.toIso)}'`
    : '';
  try {
    const connection = await DuckDBPool.getConnection();
    try {
      const result = await connection.runAndReadAll(
        `SELECT DISTINCT context FROM ${readParquetSql(files)}${rangeClause}`
      );
      return result
        .getRowObjects()
        .map(row => row.context)
        .filter((c): c is string => typeof c === 'string' && c.length > 0);
    } finally {
      connection.disconnectSync();
    }
  } catch (error) {
    debugLogger.warn(
      `[Context Discovery] Could not read context column under context=${sanitized}:`,
      error
    );
    return null;
  }
}

/**
 * The distinct contexts of one directory: from the footers when they answer,
 * from the data when they cannot. An empty footer answer is an answer: the
 * window's files exist and none has a row in it, which the data read would
 * only confirm at the cost this path avoids.
 */
async function resolveContextsFor(
  dataDir: string,
  sanitized: string,
  range?: { fromIso: string; toIso: string }
): Promise<string[] | null> {
  const fromFooters = await queryContextsFromFooters(dataDir, sanitized, range);
  if (fromFooters !== null) {
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

    // Resolve the true context strings from the data — the dir-name
    // reconstruction is lossy for ids containing literal dashes (issue #71),
    // and one sanitized directory can hold several colliding contexts.
    //
    // Each directory is resolved from the files of the requested days only
    // (the lister knows both day files and compacted year files), with each
    // footer's own timestamps deciding overlap. A directory with no file in
    // the window costs one listing and contributes nothing; the earlier
    // pre-check for `year=/day=` subdirectories in range is gone because a
    // compacted year has none and was dropped (found on rima, 2026-09-17:
    // 17 of 90 contexts vanished after one compaction).
    const fromIso = isoBound(from);
    const toIso = isoBound(to);
    const matchingContexts: string[] = [];
    for (const sanitized of allContexts) {
      const resolved = await resolveContextsFor(dataDir, sanitized, {
        fromIso,
        toIso,
      });
      if (resolved === null) {
        // Files exist but nothing could be read at all: the lossy directory
        // name beats dropping a vessel that has data.
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
  const fromIso = isoBound(from);
  const toIso = isoBound(to);

  // Every vessel's position files, but only the window's days of them.
  const files = await filesFor({
    dataDir,
    contexts: 'all',
    paths: ['navigation.position'],
    fromIso,
    toIso,
  });
  if (files.length === 0) return [];

  const spatialClause = buildSpatialSqlClause(filter);

  // `context` is the DATA column (the original, unsanitized context every
  // record stores), never the sanitized partition value from the directory
  // name; readParquetSql sets hive_partitioning=false for exactly that.
  const query = `
    SELECT DISTINCT context
    FROM ${readParquetSql(files)}
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
