import { Router, Request, Response } from 'express';
import {
  AggregateMethod,
  DataResult,
  FromToContextRequest,
  PathSpec,
} from './HistoryAPI-types';
import { ZonedDateTime, ZoneOffset, ZoneId } from '@js-joda/core';
import { Context, Path, Timestamp } from '@signalk/server-api';
import { ParamsDictionary } from 'express-serve-static-core';
import { ParsedQs } from 'qs';
import { DuckDBInstance } from '@duckdb/node-api';
import { DuckDBPool } from './utils/duckdb-pool';
import path from 'path';
import {
  getAvailablePathsArray,
  getAvailablePathsForTimeRange,
} from './utils/path-discovery';
import {
  getCachedPaths,
  setCachedPaths,
  getCachedContexts,
  setCachedContexts,
} from './utils/path-cache';
import { getAvailableContextsForTimeRange } from './utils/context-discovery';
import {
  getPathComponentSchema,
  PathComponentSchema,
  ComponentInfo,
} from './utils/schema-cache';
import { ConcurrencyLimiter } from './utils/concurrency-limiter';
import { CONCURRENCY } from './config/cache-defaults';
import { SQLiteBufferInterface, DataRecord } from './types';
import { HivePathBuilder, AggregationTier } from './utils/hive-path-builder';
import {
  AutoDiscoveryService,
  AutoDiscoveryResult,
} from './services/auto-discovery';
import {
  SpatialFilter,
  parseSpatialParams,
  buildSpatialSqlClause,
  filterBufferRecordsSpatially,
  isPositionPath,
} from './utils/spatial-queries';
import {
  parseDurationToMillis,
  parseResolutionToMillis,
} from './utils/duration-parser';
import { isAngularPath } from './utils/angular-paths';

// ============================================================================
// Timestamp Conversion Helper Functions
// ============================================================================

/**
 * Get the target timezone ID
 * @param timezoneParam - Optional timezone from query parameter
 * @returns ZoneId object for the target timezone
 */
function getTargetTimezone(timezoneParam?: string): ZoneId {
  if (timezoneParam) {
    try {
      return ZoneId.of(timezoneParam);
    } catch (error) {
      console.error(
        `[Timestamp Conversion] Invalid timezone '${timezoneParam}', falling back to system default`
      );
    }
  }

  // Use system default timezone
  return ZoneId.systemDefault();
}

/**
 * Convert a UTC timestamp string to a target timezone
 * @param utcTimestamp - UTC timestamp string (e.g., "2025-10-18T20:44:09Z")
 * @param targetZone - Target timezone
 * @returns Converted timestamp string in target timezone (ISO 8601 format with offset)
 */
function convertTimestampToTimezone(
  utcTimestamp: Timestamp,
  targetZone: ZoneId
): Timestamp {
  try {
    // Parse the UTC timestamp
    const zonedDateTime = ZonedDateTime.parse(utcTimestamp);

    // Convert to target timezone
    const converted = zonedDateTime.withZoneSameInstant(targetZone);

    // Format to ISO 8601 with offset (e.g., "2025-10-20T08:12:14-04:00")
    // Use toOffsetDateTime() to get clean ISO format without [SYSTEM] suffix
    const offsetDateTime = converted.toOffsetDateTime();
    return offsetDateTime.toString() as Timestamp;
  } catch (error) {
    console.error(
      `[Timestamp Conversion] Error converting timestamp ${utcTimestamp}:`,
      error
    );
    return utcTimestamp; // Return original on error
  }
}

export function registerHistoryApiRoute(
  router: Pick<Router, 'get'>,
  selfId: string,
  dataDir: string,
  debug: (k: string) => void,
  app: any,
  sqliteBuffer?: SQLiteBufferInterface,
  autoDiscoveryService?: AutoDiscoveryService,
  s3Config?: S3QueryConfig,
  retentionDays: number = 7
) {
  const historyApi = new HistoryAPI(
    selfId,
    dataDir,
    sqliteBuffer,
    autoDiscoveryService,
    s3Config,
    retentionDays
  );
  // Handler for values endpoint
  const handleValues = (req: Request, res: Response) => {
    const { from, to, context, spatialFilter, shouldRefresh, positionPath } =
      getRequestParams(req as FromToContextRequest, selfId);
    const convertTimesToLocal =
      req.query.convertTimesToLocal === 'true' ||
      req.query.convertTimesToLocal === '1';
    const timezone = req.query.timezone as string | undefined;
    const source = (req.query.source as QuerySource) || 'auto';
    historyApi.getValues(
      context,
      from,
      to,
      shouldRefresh,
      convertTimesToLocal,
      timezone,
      spatialFilter,
      app,
      debug,
      req,
      res,
      source,
      positionPath
    );
  };

  // V1 route: Direct routes with all signalk-parquet extensions
  // (spatial filtering, timezone conversion, moving averages, etc.)
  router.get('/signalk/v1/history/values', handleValues);
  // Handler for contexts endpoint
  const handleContexts = async (req: Request, res: Response) => {
    try {
      // Check if time range parameters are provided
      const hasTimeParams =
        req.query.duration || req.query.from || req.query.to;

      if (hasTimeParams) {
        // Time-range-aware: return only contexts with data in the specified time range
        const { from, to } = getRequestParams(
          req as FromToContextRequest,
          selfId
        );

        // Check cache first
        let contexts = getCachedContexts(from, to);

        if (!contexts) {
          // Cache miss - query the parquet files
          contexts = await getAvailableContextsForTimeRange(dataDir, from, to);
          // Cache the result
          setCachedContexts(from, to, contexts);
        }

        res.json(contexts);
      } else {
        // No time range specified: return only self context (legacy behavior)
        res.json([`vessels.${selfId}`] as Context[]);
      }
    } catch (error) {
      debug(`Error in /contexts: ${error}`);
      res.status(500).json({ error: (error as Error).message });
    }
  };

  // V1 route: Direct routes with extensions (time-range-aware context discovery)
  router.get('/signalk/v1/history/contexts', handleContexts);
  // Note: V2 routes (/signalk/v2/api/history/*) are handled by the registered
  // HistoryApi provider in history-provider.ts via app.registerHistoryApiProvider()
  // Handler for paths endpoint
  const handlePaths = async (req: Request, res: Response) => {
    try {
      // Check if time range parameters are provided
      const hasTimeParams =
        req.query.duration || req.query.from || req.query.to;

      if (hasTimeParams) {
        // Time-range-aware: return only paths with data in the specified time range
        const { from, to, context } = getRequestParams(
          req as FromToContextRequest,
          selfId
        );

        // Check cache first
        let paths = getCachedPaths(context, from, to);

        if (!paths) {
          // Cache miss - query the parquet files
          paths = await getAvailablePathsForTimeRange(
            dataDir,
            context,
            from,
            to
          );
          // Cache the result
          setCachedPaths(context, from, to, paths);
        }

        res.json(paths);
      } else {
        // No time range specified: return all available paths (legacy behavior)
        const paths = getAvailablePathsArray(dataDir, app);
        res.json(paths);
      }
    } catch (error) {
      debug(`Error in /paths: ${error}`);
      res.status(500).json({ error: (error as Error).message });
    }
  };

  // V1 route: Direct routes with extensions (time-range-aware path discovery)
  router.get('/signalk/v1/history/paths', handlePaths);

  // Also register as plugin-style routes for testing
  router.get('/api/history/values', (req: Request, res: Response) => {
    const { from, to, context, spatialFilter, shouldRefresh, positionPath } =
      getRequestParams(req as FromToContextRequest, selfId);
    const convertTimesToLocal =
      req.query.convertTimesToLocal === 'true' ||
      req.query.convertTimesToLocal === '1';
    const timezone = req.query.timezone as string | undefined;
    const source = (req.query.source as QuerySource) || 'auto';
    historyApi.getValues(
      context,
      from,
      to,
      shouldRefresh,
      convertTimesToLocal,
      timezone,
      spatialFilter,
      app,
      debug,
      req,
      res,
      source,
      positionPath
    );
  });
  router.get('/api/history/contexts', async (req: Request, res: Response) => {
    try {
      // Check if time range parameters are provided
      const hasTimeParams =
        req.query.duration || req.query.from || req.query.to;

      if (hasTimeParams) {
        // Time-range-aware: return only contexts with data in the specified time range
        const { from, to } = getRequestParams(
          req as FromToContextRequest,
          selfId
        );

        // Check cache first
        let contexts = getCachedContexts(from, to);

        if (!contexts) {
          // Cache miss - query the parquet files
          contexts = await getAvailableContextsForTimeRange(dataDir, from, to);
          // Cache the result
          setCachedContexts(from, to, contexts);
        }

        res.json(contexts);
      } else {
        // No time range specified: return only self context (legacy behavior)
        res.json([`vessels.${selfId}`] as Context[]);
      }
    } catch (error) {
      debug(`Error in /api/history/contexts: ${error}`);
      res.status(500).json({ error: (error as Error).message });
    }
  });
  router.get('/api/history/paths', async (req: Request, res: Response) => {
    try {
      // Check if time range parameters are provided
      const hasTimeParams =
        req.query.duration || req.query.from || req.query.to;

      if (hasTimeParams) {
        // Time-range-aware: return only paths with data in the specified time range
        const { from, to, context } = getRequestParams(
          req as FromToContextRequest,
          selfId
        );

        // Check cache first
        let paths = getCachedPaths(context, from, to);

        if (!paths) {
          // Cache miss - query the parquet files
          paths = await getAvailablePathsForTimeRange(
            dataDir,
            context,
            from,
            to
          );
          // Cache the result
          setCachedPaths(context, from, to, paths);
        }

        res.json(paths);
      } else {
        // No time range specified: return all available paths (legacy behavior)
        const paths = getAvailablePathsArray(dataDir, app);
        res.json(paths);
      }
    } catch (error) {
      debug(`Error in /api/history/paths: ${error}`);
      res.status(500).json({ error: (error as Error).message });
    }
  });
}

const getRequestParams = ({ query }: FromToContextRequest, selfId: string) => {
  try {
    let from: ZonedDateTime;
    let to: ZonedDateTime;
    let shouldRefresh = false;

    // Check if user wants to work in UTC (default: false, use local timezone)
    const useUTC = query.useUTC === 'true' || query.useUTC === '1';

    // ============================================================================
    // STANDARD SIGNALK TIME RANGE PATTERNS
    // ============================================================================
    // Pattern 1: duration only → query back from now
    if (query.duration && !query.from && !query.to) {
      const durationMs = parseDuration(query.duration);
      to = ZonedDateTime.now(ZoneOffset.UTC);
      from = to.minusNanos(durationMs * 1000000);
      shouldRefresh = query.refresh === 'true' || query.refresh === '1';
    }
    // Pattern 2: from + duration → query forward from start
    else if (query.from && query.duration && !query.to) {
      from = parseDateTime(query.from, useUTC);
      const durationMs = parseDuration(query.duration);
      to = from.plusNanos(durationMs * 1000000);
    }
    // Pattern 3: to + duration → query backward to end
    else if (query.to && query.duration && !query.from) {
      to = parseDateTime(query.to, useUTC);
      const durationMs = parseDuration(query.duration);
      from = to.minusNanos(durationMs * 1000000);
    }
    // Pattern 4: from only → from start to now
    else if (query.from && !query.duration && !query.to) {
      from = parseDateTime(query.from, useUTC);
      to = ZonedDateTime.now(ZoneOffset.UTC);
    }
    // Pattern 5: from + to → specific range
    else if (query.from && query.to && !query.duration) {
      from = parseDateTime(query.from, useUTC);
      to = parseDateTime(query.to, useUTC);
    } else {
      throw new Error(
        'Invalid time range parameters. Use one of the following patterns:\n' +
          '  1. ?duration=1h (query back from now)\n' +
          '  2. ?from=2025-08-01T00:00:00Z&duration=1h (query forward)\n' +
          '  3. ?to=2025-08-01T12:00:00Z&duration=1h (query backward)\n' +
          '  4. ?from=2025-08-01T00:00:00Z (from start to now)\n' +
          '  5. ?from=2025-08-01T00:00:00Z&to=2025-08-02T00:00:00Z (specific range)'
      );
    }

    const context: Context = getContext(query.context, selfId);
    const spatialFilter = parseSpatialParams(query.bbox, query.radius);
    const positionPath =
      (query.positionPath as string) || 'navigation.position';
    return { from, to, context, spatialFilter, shouldRefresh, positionPath };
  } catch (e: unknown) {
    console.error('Full error details:', e);
    throw new Error(
      `Error extracting query parameters from ${JSON.stringify(query)}: ${e instanceof Error ? e.stack : e}`
    );
  }
};

// Parse duration string (supports ISO 8601, integer seconds, shorthand)
function parseDuration(duration: string | undefined): number {
  if (!duration) {
    throw new Error('Duration parameter is required');
  }
  return parseDurationToMillis(duration);
}

// Check if datetime string has timezone information
function hasTimezoneInfo(dateTimeStr: string): boolean {
  // Check for 'Z' at the end, or '+'/'-' followed by timezone offset pattern
  return (
    dateTimeStr.endsWith('Z') ||
    /[+-]\d{2}:?\d{2}$/.test(dateTimeStr) ||
    /[+-]\d{4}$/.test(dateTimeStr)
  );
}

// Parse datetime string and convert to UTC if needed
function parseDateTime(dateTimeStr: string, useUTC: boolean): ZonedDateTime {
  // Normalize the datetime string to include seconds if missing
  let normalizedStr = dateTimeStr;
  if (dateTimeStr.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/)) {
    // Add seconds if only HH:MM is provided
    normalizedStr = dateTimeStr + ':00';
  }

  if (useUTC) {
    // When useUTC=true, treat the datetime as UTC
    if (hasTimezoneInfo(normalizedStr)) {
      // Already has timezone info, parse as-is
      return ZonedDateTime.parse(normalizedStr);
    } else {
      // No timezone info, assume UTC by adding 'Z'
      return ZonedDateTime.parse(normalizedStr + 'Z');
    }
  } else {
    // When useUTC=false, handle timezone conversion
    if (hasTimezoneInfo(normalizedStr)) {
      // Already has timezone info, parse as-is (will be in UTC or specified timezone)
      return ZonedDateTime.parse(normalizedStr).withZoneSameInstant(
        ZoneOffset.UTC
      );
    } else {
      // No timezone info, treat as local time and convert to UTC
      try {
        // JavaScript Date constructor treats ISO strings without timezone as local time
        const localDate = new Date(normalizedStr);
        if (isNaN(localDate.getTime())) {
          throw new Error('Invalid date');
        }

        // Convert to UTC ISO string and parse with ZonedDateTime
        const utcIsoString = localDate.toISOString();
        return ZonedDateTime.parse(utcIsoString);
      } catch (e) {
        throw new Error(
          `Unable to parse datetime '${dateTimeStr}': ${e}. Use format like '2025-08-13T08:00:00' or '2025-08-13T08:00:00Z'`
        );
      }
    }
  }
}

function getContext(
  contextFromQuery: string | undefined,
  selfId: string
): Context {
  if (
    !contextFromQuery ||
    contextFromQuery === 'vessels.self' ||
    contextFromQuery === 'self'
  ) {
    return `vessels.${selfId}` as Context;
  }
  return contextFromQuery.replace(/ /gi, '') as Context;
}

export type QuerySource = 'local' | 's3' | 'hybrid' | 'auto';

export interface S3QueryConfig {
  enabled: boolean;
  bucket: string;
  keyPrefix: string;
  region: string;
}

export class HistoryAPI {
  private sqliteBuffer?: SQLiteBufferInterface;
  private hivePathBuilder: HivePathBuilder;
  private autoDiscoveryService?: AutoDiscoveryService;
  private s3Config?: S3QueryConfig;
  private retentionDays: number;

  constructor(
    private selfId: string,
    private dataDir: string,
    sqliteBuffer?: SQLiteBufferInterface,
    autoDiscoveryService?: AutoDiscoveryService,
    s3Config?: S3QueryConfig,
    retentionDays: number = 7
  ) {
    this.sqliteBuffer = sqliteBuffer;
    this.hivePathBuilder = new HivePathBuilder();
    this.autoDiscoveryService = autoDiscoveryService;
    this.s3Config = s3Config;
    this.retentionDays = retentionDays;
  }

  /**
   * Set S3 configuration for federated queries
   */
  setS3Config(config: S3QueryConfig | undefined): void {
    this.s3Config = config;
  }

  /**
   * Determine the query source based on time range and retention settings
   * - 'local': All data is within retention period (on local disk)
   * - 's3': All data is older than retention period (in S3)
   * - 'hybrid': Data spans retention boundary (need both local and S3)
   */
  private getQuerySource(
    from: ZonedDateTime,
    to: ZonedDateTime,
    forceSource?: QuerySource
  ): QuerySource {
    // Source routing is handled in getNumericValues:
    // local always queried, S3 supplements for dates before earliest local data.
    // forceSource only used for explicit API override (e.g. source=local or source=s3)
    if (forceSource && forceSource !== 'auto') {
      return forceSource;
    }
    return 'local';
  }

  /**
   * Set the auto-discovery service
   */
  setAutoDiscoveryService(service: AutoDiscoveryService | undefined): void {
    this.autoDiscoveryService = service;
  }

  /**
   * Set the SQLite buffer for federated queries
   */
  setSqliteBuffer(buffer: SQLiteBufferInterface | undefined): void {
    this.sqliteBuffer = buffer;
  }

  /**
   * Auto-select the optimal tier based on requested resolution
   * Returns undefined to use raw/flat data, or a tier name for aggregated data
   *
   * Logic:
   * - resolution >= 1 hour (3600000ms) → use 1h tier
   * - resolution >= 1 minute (60000ms) → use 60s tier
   * - resolution >= 5 seconds (5000ms) → use 5s tier
   * - resolution < 5 seconds → use raw data (no tier)
   *
   * Falls back through tiers if preferred tier doesn't exist
   */
  private selectOptimalTier(
    resolutionMillis: number
  ): AggregationTier | undefined {
    const fs = require('fs');

    // Determine preferred tier based on resolution
    let preferredTiers: AggregationTier[] = [];

    if (resolutionMillis >= 3600000) {
      preferredTiers = ['1h', '60s', '5s'];
    } else if (resolutionMillis >= 60000) {
      preferredTiers = ['60s', '5s'];
    } else if (resolutionMillis >= 5000) {
      preferredTiers = ['5s'];
    } else {
      // Use raw data for sub-5-second resolution
      return undefined;
    }

    // Check which tiers exist and return the best available
    for (const tier of preferredTiers) {
      const tierPath = path.join(this.dataDir, `tier=${tier}`);
      try {
        if (fs.existsSync(tierPath)) {
          return tier;
        }
      } catch {
        // Directory doesn't exist or not accessible
      }
    }

    // No aggregated tiers available, use raw data
    return undefined;
  }

  /**
   * Query recent data from SQLite buffer
   * With daily exports, data can sit in SQLite for up to 48 hours before export
   * Returns records for a specific context and path
   */
  private getRecentBufferData(
    context: Context,
    signalkPath: Path,
    from: ZonedDateTime,
    to: ZonedDateTime,
    debug: (k: string) => void
  ): DataRecord[] {
    if (!this.sqliteBuffer) {
      return [];
    }

    // With daily exports, SQLite buffer holds up to 48 hours of data
    // Query buffer for any data within retention period (not just export interval)
    const bufferRetentionHours = 48;
    const cutoffTime = new Date();
    cutoffTime.setHours(cutoffTime.getHours() - bufferRetentionHours);

    // Only query buffer if 'to' time is within buffer retention period
    const toDate = new Date(to.toInstant().toString());
    if (toDate < cutoffTime) {
      debug(
        `Query end time ${toDate.toISOString()} is before buffer cutoff ${cutoffTime.toISOString()}, skipping buffer query`
      );
      return [];
    }

    // Query from the requested time, buffer will return what it has
    const fromDate = new Date(from.toInstant().toString());

    debug(
      `Querying SQLite buffer for recent data: ${context}:${signalkPath} from ${fromDate.toISOString()}`
    );

    try {
      const records = this.sqliteBuffer.getRecordsForPath(
        context,
        signalkPath,
        fromDate.toISOString(),
        toDate.toISOString()
      );
      debug(`Found ${records.length} records in SQLite buffer`);
      return records;
    } catch (error) {
      debug(`Error querying SQLite buffer: ${(error as Error).message}`);
      return [];
    }
  }

  /**
   * Merge parquet results with buffer results, removing duplicates by timestamp.
   * Buffer records are bucketed and aggregated to match the parquet resolution.
   */
  private mergeWithBufferData(
    parquetData: Array<[Timestamp, unknown]>,
    bufferRecords: DataRecord[],
    debug: (k: string) => void,
    timeResolutionMillis: number = 0,
    aggregateMethod: string = 'average',
    pathName: string = '',
    isAngular: boolean = false
  ): Array<[Timestamp, unknown]> {
    if (bufferRecords.length === 0) {
      return parquetData;
    }

    // Bucket and aggregate buffer records to match parquet resolution
    const bufferData = this.bucketBufferRecords(
      bufferRecords,
      timeResolutionMillis,
      aggregateMethod,
      isAngular,
      debug
    );

    // Create a map of parquet data by timestamp for deduplication
    const resultMap = new Map<string, [Timestamp, unknown]>();

    // Add parquet data first
    for (const [timestamp, value] of parquetData) {
      resultMap.set(timestamp, [timestamp, value]);
    }

    // Add buffer data (will overwrite parquet data if same timestamp)
    for (const [timestamp, value] of bufferData) {
      resultMap.set(timestamp, [timestamp, value]);
    }

    // Sort by timestamp and return
    const merged = Array.from(resultMap.values()).sort((a, b) =>
      a[0].localeCompare(b[0])
    );

    debug(
      `Merged ${parquetData.length} parquet + ${bufferData.length} bucketed buffer (from ${bufferRecords.length} raw) = ${merged.length} total`
    );
    return merged;
  }

  /**
   * Bucket and aggregate raw buffer records into time-aligned buckets
   */
  private bucketBufferRecords(
    records: DataRecord[],
    resolutionMs: number,
    aggregateMethod: string,
    isAngular: boolean,
    debug: (k: string) => void
  ): Array<[Timestamp, unknown]> {
    // If no resolution or very small, just convert without bucketing
    if (resolutionMs <= 1000) {
      return records.map(record => {
        const timestamp = record.signalk_timestamp as Timestamp;
        const value = record.value_json
          ? typeof record.value_json === 'string'
            ? JSON.parse(record.value_json)
            : record.value_json
          : record.value;
        return [timestamp, value];
      });
    }

    // Group records into buckets by resolution
    const buckets = new Map<number, DataRecord[]>();
    for (const record of records) {
      const epochMs = new Date(record.signalk_timestamp).getTime();
      const bucketKey = Math.floor(epochMs / resolutionMs) * resolutionMs;
      let bucket = buckets.get(bucketKey);
      if (!bucket) {
        bucket = [];
        buckets.set(bucketKey, bucket);
      }
      bucket.push(record);
    }

    debug(
      `Bucketed ${records.length} buffer records into ${buckets.size} buckets (resolution=${resolutionMs}ms)`
    );

    // Aggregate each bucket
    const result: Array<[Timestamp, unknown]> = [];
    const sortedKeys = Array.from(buckets.keys()).sort((a, b) => a - b);

    for (const bucketKey of sortedKeys) {
      const bucket = buckets.get(bucketKey)!;
      const timestamp = new Date(bucketKey).toISOString().replace('.000Z', 'Z') as Timestamp;

      // Check if first record has object values (e.g., position)
      const firstRecord = bucket[0];
      const firstValue = firstRecord.value_json
        ? typeof firstRecord.value_json === 'string'
          ? JSON.parse(firstRecord.value_json)
          : firstRecord.value_json
        : firstRecord.value;

      if (typeof firstValue === 'object' && firstValue !== null) {
        result.push([timestamp, aggregateObjectBucket(bucket)]);
      } else {
        result.push([
          timestamp,
          aggregateNumericBucket(bucket, aggregateMethod, isAngular),
        ]);
      }
    }

    return result;
  }

  /**
   * Get timestamps where vessel position was within the spatial filter
   * Used to correlate non-position paths with spatial filtering
   */
  private async getSpatialTimestamps(
    context: Context,
    from: ZonedDateTime,
    to: ZonedDateTime,
    timeResolutionMillis: number,
    spatialFilter: SpatialFilter,
    positionPath: string,
    tier: AggregationTier | undefined,
    querySource: QuerySource,
    debug: (k: string) => void
  ): Promise<Set<string>> {
    const timestamps = new Set<string>();
    const effectiveTier = tier || 'raw';

    // Build file path for position data
    const sanitizedContext = this.hivePathBuilder.sanitizeContext(context);
    const sanitizedPath = this.hivePathBuilder.sanitizePath(positionPath);
    const localFilePath = path.join(
      this.dataDir,
      `tier=${effectiveTier}`,
      `context=${sanitizedContext}`,
      `path=${sanitizedPath}`,
      '**',
      '*.parquet'
    );

    const fromIso = from.toInstant().toString();
    const toIso = to.toInstant().toString();

    try {
      const connection = await DuckDBPool.getConnection();
      try {
        // Build spatial WHERE clause
        const spatialWhereClause = buildSpatialSqlClause(spatialFilter);

        // Query position data with spatial filter to get valid timestamps
        const spatialTsCol = getTierTimestampColumn(effectiveTier);
        const query = `
          SELECT DISTINCT
            strftime(DATE_TRUNC('seconds',
              EPOCH_MS(CAST(FLOOR(EPOCH_MS(${spatialTsCol}::TIMESTAMP) / ${timeResolutionMillis}) * ${timeResolutionMillis} AS BIGINT))
            ), '%Y-%m-%dT%H:%M:%SZ') as timestamp
          FROM read_parquet('${localFilePath}', union_by_name=true, filename=true)
          WHERE
            ${spatialTsCol} >= '${fromIso}'
            AND ${spatialTsCol} < '${toIso}'
            AND value_latitude IS NOT NULL
            AND value_longitude IS NOT NULL
            AND ${spatialWhereClause}
            AND filename NOT LIKE '%/processed/%'
            AND filename NOT LIKE '%/quarantine/%'
            AND filename NOT LIKE '%/failed/%'
            AND filename NOT LIKE '%/repaired/%'
          ORDER BY timestamp
        `;

        const result = await connection.runAndReadAll(query);
        const rows = result.getRowObjects();

        for (const row of rows) {
          timestamps.add((row as { timestamp: string }).timestamp);
        }
      } finally {
        connection.disconnectSync();
      }
    } catch (error) {
      debug(`[Spatial Correlation] Error querying position data: ${error}`);
      // On error, return empty set (no filtering will occur)
    }

    return timestamps;
  }

  async getValues(
    context: Context,
    from: ZonedDateTime,
    to: ZonedDateTime,
    shouldRefresh: boolean,
    convertTimesToLocal: boolean,
    timezone: string | undefined,
    spatialFilter: SpatialFilter | null,
    app: any,
    debug: (k: string) => void,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    req: Request<ParamsDictionary, any, any, ParsedQs, Record<string, any>>,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    res: Response<any, Record<string, any>>,
    source: QuerySource = 'auto',
    positionPath: string = 'navigation.position'
  ) {
    try {
      // Resolution now in SECONDS (breaking change from v0.7.0)
      const timeResolutionMillis = req.query.resolution
        ? parseResolutionToMillis(req.query.resolution as string)
        : ((to.toEpochSecond() - from.toEpochSecond()) / 500) * 1000;
      const pathExpressions = ((req.query.paths as string) || '')
        .replace(/[^0-9a-z.,:_]/gi, '')
        .split(',');
      const pathSpecs: PathSpec[] = pathExpressions.map(splitPathExpression);

      // Parse tier parameter (raw, 5s, 60s, 1h) or auto-select based on resolution
      const tierParam = req.query.tier as string | undefined;
      const validTiers: AggregationTier[] = ['raw', '5s', '60s', '1h'];
      let tier: AggregationTier | undefined;

      if (tierParam === 'auto' || !tierParam) {
        // Auto-select tier based on resolution
        tier = this.selectOptimalTier(timeResolutionMillis);
        if (tier) {
          debug(
            `Auto-selected tier=${tier} for resolution=${timeResolutionMillis}ms`
          );
        }
      } else if (validTiers.includes(tierParam as AggregationTier)) {
        tier = tierParam as AggregationTier;
      }

      // Log spatial filter if present
      if (spatialFilter) {
        debug(
          `Spatial filter: type=${spatialFilter.type}, bbox=[${spatialFilter.bbox.west},${spatialFilter.bbox.south},${spatialFilter.bbox.east},${spatialFilter.bbox.north}]`
        );
      }

      // Determine query source based on time range and configuration
      const querySource = this.getQuerySource(from, to, source);
      debug(`Query source determined: ${querySource} (requested: ${source})`);

      // Handle position and numeric paths together
      let allResult = pathSpecs.length
        ? await this.getNumericValues(
            context,
            from,
            to,
            timeResolutionMillis,
            pathSpecs,
            debug,
            tier,
            spatialFilter,
            querySource,
            positionPath,
            app
          )
        : {
            context,
            range: {
              from: from.toString() as Timestamp,
              to: to.toString() as Timestamp,
            },
            values: [],
            data: [],
          };

      // Check for auto-discovery on paths with no data
      debug(
        `[AutoDiscovery] Checking auto-discovery: service=${!!this.autoDiscoveryService}, pathSpecs.length=${pathSpecs.length}`
      );
      if (this.autoDiscoveryService && pathSpecs.length > 0) {
        const autoConfiguredPaths: AutoDiscoveryResult[] = [];

        for (let i = 0; i < pathSpecs.length; i++) {
          const pathSpec = pathSpecs[i];
          // Check if this path has any data in the result
          // Data array format: [timestamp, value1, value2, ...]
          // Each path corresponds to a position in the values array
          const hasData = allResult.data.some(row => {
            const valueIndex = i + 1; // +1 because index 0 is timestamp
            return row[valueIndex] !== null && row[valueIndex] !== undefined;
          });

          if (!hasData) {
            debug(
              `[AutoDiscovery] No data found for path ${pathSpec.path}, checking auto-discovery`
            );
            const result =
              await this.autoDiscoveryService.maybeAutoConfigurePath(
                pathSpec.path as Path,
                context
              );
            if (result.configured) {
              autoConfiguredPaths.push(result);
              debug(`[AutoDiscovery] Auto-configured path: ${pathSpec.path}`);
            } else {
              debug(
                `[AutoDiscovery] Path ${pathSpec.path} not auto-configured: ${result.reason}`
              );
            }
          }
        }

        // Add meta to response if any paths were auto-configured
        if (autoConfiguredPaths.length > 0) {
          allResult.meta = {
            autoConfigured: true,
            paths: autoConfiguredPaths.map(r => r.path),
            message: `${autoConfiguredPaths.length} path(s) auto-configured for recording. Data will be available shortly.`,
          };
        }
      }

      // Apply timestamp conversions if requested
      if (convertTimesToLocal) {
        allResult = this.convertTimestamps(allResult, timezone, debug);
      }

      // Add refresh headers if shouldRefresh is enabled
      if (shouldRefresh) {
        const refreshIntervalSeconds = Math.max(
          Math.round(timeResolutionMillis / 1000),
          1
        ); // At least 1 second
        res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');
        res.setHeader('Refresh', refreshIntervalSeconds.toString());

        // Add refresh info to response
        (allResult as any).refresh = {
          enabled: true,
          intervalSeconds: refreshIntervalSeconds,
          nextRefresh: new Date(
            Date.now() + refreshIntervalSeconds * 1000
          ).toISOString(),
        };
      }

      res.json(allResult);
    } catch (error) {
      debug(`Error in getValues: ${error}`);
      res.status(500).json({
        error: 'Internal server error',
        message: error instanceof Error ? error.message : String(error),
      });
    }
  }

  async getNumericValues(
    context: Context,
    from: ZonedDateTime,
    to: ZonedDateTime,
    timeResolutionMillis: number,
    pathSpecs: PathSpec[],
    debug: (k: string) => void,
    tier?: AggregationTier,
    spatialFilter?: SpatialFilter | null,
    querySource: QuerySource = 'local',
    positionPath: string = 'navigation.position',
    app?: any
  ): Promise<DataResult> {
    const allData: { [path: string]: Array<[Timestamp, unknown]> } = {};
    const objectPaths = new Set<string>(); // Track which paths are object paths

    // Convert ZonedDateTime to Date for S3 pattern building
    const fromDate = new Date(from.toInstant().toString());
    const toDate = new Date(to.toInstant().toString());

    // Calculate retention cutoff for hybrid queries
    const retentionCutoff = new Date();
    retentionCutoff.setUTCDate(
      retentionCutoff.getUTCDate() - this.retentionDays
    );

    // If spatial filter is set, get valid timestamps from position data
    // This allows filtering non-position paths by "when vessel was in this area"
    let spatialTimestamps: Set<string> | null = null;
    if (spatialFilter) {
      const hasNonPositionPaths = pathSpecs.some(
        ps => !isPositionPath(ps.path)
      );
      if (hasNonPositionPaths) {
        debug(
          `[Spatial Correlation] Querying ${positionPath} for valid timestamps within spatial filter`
        );
        spatialTimestamps = await this.getSpatialTimestamps(
          context,
          from,
          to,
          timeResolutionMillis,
          spatialFilter,
          positionPath,
          tier,
          querySource,
          debug
        );
        debug(
          `[Spatial Correlation] Found ${spatialTimestamps.size} valid timestamps`
        );
      }
    }

    // Process each path and collect data with concurrency limiting
    // Limit concurrent queries to prevent resource exhaustion (configured in cache-defaults)
    const limiter = new ConcurrencyLimiter(CONCURRENCY.MAX_QUERIES);
    await limiter.map(pathSpecs, async pathSpec => {
      try {
        // Build file patterns based on query source
        let localFilePath: string | null = null;
        let s3FilePath: string | null = null;
        const effectiveTier = tier || 'raw';

        // Always query local first
        const sanitizedContext =
          this.hivePathBuilder.sanitizeContext(context);
        const sanitizedSkPath = this.hivePathBuilder.sanitizePath(
          pathSpec.path
        );
        localFilePath = path.join(
          this.dataDir,
          `tier=${effectiveTier}`,
          `context=${sanitizedContext}`,
          `path=${sanitizedSkPath}`,
          '**',
          '*.parquet'
        );
        debug(
          `Querying local Hive tier=${effectiveTier} at: ${localFilePath}`
        );

        // S3 supplements local for dates before the earliest local data
        if (this.s3Config?.enabled) {
          // Find the earliest local data date
          const localEarliestDate = this.hivePathBuilder.findEarliestDate(
            this.dataDir,
            effectiveTier,
            sanitizedContext,
            sanitizedSkPath
          );

          if (localEarliestDate && fromDate < localEarliestDate) {
            // Only query S3 for the range before local data starts
            const s3ToDate = new Date(localEarliestDate.getTime() - 86400000); // day before local starts
            if (fromDate <= s3ToDate) {
              s3FilePath = this.hivePathBuilder.buildS3Glob(
                this.s3Config.bucket,
                this.s3Config.keyPrefix || '',
                effectiveTier,
                context,
                pathSpec.path,
                fromDate,
                s3ToDate
              );
              debug(`S3 supplement for ${fromDate.toISOString()} to ${s3ToDate.toISOString()}`);
            }
          } else if (!localEarliestDate) {
            // No local data at all — query S3 for full range
            s3FilePath = this.hivePathBuilder.buildS3Glob(
              this.s3Config.bucket,
              this.s3Config.keyPrefix || '',
              effectiveTier,
              context,
              pathSpec.path,
              fromDate,
              toDate
            );
            debug(`No local data, querying S3 for full range`);
          }
        }

        // Convert ZonedDateTime to ISO string format matching parquet schema
        const fromIso = from.toInstant().toString();
        const toIso = to.toInstant().toString();

        // Get connection from pool (spatial extension already loaded)
        const connection = await DuckDBPool.getConnection();

        try {
          // Build FROM clause based on available sources
          // For hybrid queries, we UNION local and S3 sources
          // Local files need filename filter to exclude processed/quarantine/etc directories
          const buildFromClause = (filePath: string): string => {
            const isS3 = filePath.startsWith('s3://');
            if (isS3) {
              return `read_parquet('${filePath}', union_by_name=true)`;
            }
            // Local files: exclude processed, quarantine, failed, repaired directories
            return `(SELECT * FROM read_parquet('${filePath}', union_by_name=true, filename=true) WHERE filename NOT LIKE '%/processed/%' AND filename NOT LIKE '%/quarantine/%' AND filename NOT LIKE '%/failed/%' AND filename NOT LIKE '%/repaired/%')`;
          };

          // Build FROM clause: local-only by default, hybrid only if S3 has data
          let fromClause: string;
          const localFromClause = localFilePath ? buildFromClause(localFilePath) : null;

          if (s3FilePath && localFromClause) {
            // Try hybrid: UNION local + S3, fall back to local if S3 glob has no files
            fromClause = `(
              SELECT * FROM ${localFromClause}
              UNION ALL
              SELECT * FROM ${buildFromClause(s3FilePath)}
            )`;
            debug(`Hybrid query: combining local and S3 sources`);
          } else if (s3FilePath) {
            fromClause = buildFromClause(s3FilePath);
          } else if (localFromClause) {
            fromClause = localFromClause;
          } else {
            debug(`No data source available for path ${pathSpec.path}`);
            allData[pathSpec.path] = [];
            return;
          }

          // Run query with S3 fallback — if hybrid/S3 query fails, retry local-only
          const runQueryWithFallback = async (buildQuery: (fc: string) => string): Promise<any> => {
            try {
              return await connection.runAndReadAll(buildQuery(fromClause));
            } catch (err) {
              if (s3FilePath && localFromClause) {
                debug(`Hybrid query failed, falling back to local-only: ${err}`);
                return await connection.runAndReadAll(buildQuery(localFromClause));
              }
              throw err;
            }
          };

          // Check if this path has object components (value_latitude, value_longitude, etc.)
          // Use local path for schema check (S3 schema should match)
          const schemaCheckPath = localFilePath || s3FilePath;
          const componentSchema = schemaCheckPath
            ? await getPathComponentSchema(this.dataDir, context, pathSpec.path)
            : null;

          if (componentSchema && componentSchema.components.size > 0) {
            // Object path with multiple components - aggregate each component separately
            debug(
              `Path ${pathSpec.path}: Object path with ${componentSchema.components.size} components`
            );
            objectPaths.add(pathSpec.path); // Mark as object path

            // Build SELECT clause with one aggregate per component
            const componentSelects = Array.from(
              componentSchema.components.values()
            )
              .map(comp => {
                const aggFunc = getComponentAggregateFunction(
                  pathSpec.aggregateMethod,
                  comp.dataType
                );
                return `${aggFunc}(${comp.columnName}) as ${comp.name}`;
              })
              .join(',\n              ');

            // Build WHERE clause to check for at least one non-null component
            const componentWhereConditions = Array.from(
              componentSchema.components.values()
            )
              .map(comp => `${comp.columnName} IS NOT NULL`)
              .join(' OR ');

            // Check if this is a position path and spatial filter applies
            const applyPositionSpatialFilter =
              spatialFilter &&
              isPositionPath(pathSpec.path) &&
              componentSchema.components.has('latitude') &&
              componentSchema.components.has('longitude');
            const spatialWhereClause = applyPositionSpatialFilter
              ? ` AND ${buildSpatialSqlClause(spatialFilter!)}`
              : '';

            if (applyPositionSpatialFilter) {
              debug(
                `Applying spatial filter to position path ${pathSpec.path}`
              );
            }

            const objTsCol = getTierTimestampColumn(effectiveTier);
            const buildObjQuery = (fc: string) => `
              SELECT
                strftime(DATE_TRUNC('seconds',
                  EPOCH_MS(CAST(FLOOR(EPOCH_MS(${objTsCol}::TIMESTAMP) / ${timeResolutionMillis}) * ${timeResolutionMillis} AS BIGINT))
                ), '%Y-%m-%dT%H:%M:%SZ') as timestamp,
                ${componentSelects}
              FROM ${fc} AS source_data
              WHERE
                ${objTsCol} >= '${fromIso}'
                AND
                ${objTsCol} < '${toIso}'
                AND (${componentWhereConditions})${spatialWhereClause}
              GROUP BY timestamp
              ORDER BY timestamp
              `;

            const result = await runQueryWithFallback(buildObjQuery);
            const rows = result.getRowObjects();

            // Reconstruct objects from aggregated components
            const pathData: Array<[Timestamp, unknown]> = rows.map(
              (row: any) => {
                const timestamp = row.timestamp as Timestamp;
                const reconstructedObject: any = {};

                // Build object from component values
                componentSchema.components.forEach((comp, componentName) => {
                  const value = (row as any)[componentName];
                  if (value !== null && value !== undefined) {
                    reconstructedObject[componentName] = value;
                  }
                });

                return [timestamp, reconstructedObject];
              }
            );

            // Merge with SQLite buffer data for recent records (federated query)
            let bufferRecords = this.getRecentBufferData(
              context,
              pathSpec.path as Path,
              from,
              to,
              debug
            );

            // Apply spatial filter to buffer records if this is a position path
            if (applyPositionSpatialFilter && bufferRecords.length > 0) {
              const originalCount = bufferRecords.length;
              bufferRecords = filterBufferRecordsSpatially(
                bufferRecords,
                spatialFilter!
              );
              debug(
                `Spatial filter on buffer: ${originalCount} -> ${bufferRecords.length} records`
              );
            }

            allData[pathSpec.path] = this.mergeWithBufferData(
              pathData,
              bufferRecords,
              debug,
              timeResolutionMillis,
              String(pathSpec.aggregateMethod || 'average'),
              pathSpec.path,
              false
            );
          } else {
            // Scalar path - use original logic
            // First, check if value_json column exists in the parquet files
            // Use local path for schema check, fall back to assuming no value_json for S3-only queries
            let hasValueJson = false;
            if (localFilePath) {
              try {
                const schemaQuery = `SELECT * FROM parquet_schema('${localFilePath}') WHERE name = 'value_json'`;
                const schemaResult =
                  await connection.runAndReadAll(schemaQuery);
                hasValueJson = schemaResult.getRowObjects().length > 0;
              } catch {
                // Schema check failed, assume no value_json
                hasValueJson = false;
              }
            }

            debug(
              `Path ${pathSpec.path}: value_json column ${hasValueJson ? 'exists' : 'does not exist'}`
            );

            // Rebuild the query based on actual column availability and tier
            const tsCol = getTierTimestampColumn(effectiveTier);
            const valueJsonSelect = hasValueJson && effectiveTier === 'raw'
              ? ', FIRST(value_json) as value_json'
              : '';
            const whereClause = getTierWhereClause(effectiveTier, hasValueJson);

            const buildScalarQuery = (fc: string) => `
              SELECT
                strftime(DATE_TRUNC('seconds',
                  EPOCH_MS(CAST(FLOOR(EPOCH_MS(${tsCol}::TIMESTAMP) / ${timeResolutionMillis}) * ${timeResolutionMillis} AS BIGINT))
                ), '%Y-%m-%dT%H:%M:%SZ') as timestamp,
                ${getTierAggregateExpression(pathSpec.aggregateMethod, pathSpec.path, effectiveTier, hasValueJson, app, context as string)} as value${valueJsonSelect}
              FROM ${fc} AS source_data
              WHERE
                ${tsCol} >= '${fromIso}'
                AND
                ${tsCol} < '${toIso}'
                AND ${whereClause}
              GROUP BY timestamp
              ORDER BY timestamp
              `;

            const result = await runQueryWithFallback(buildScalarQuery);
            const rows = result.getRowObjects();

            // Convert rows to the expected format using bucketed timestamps
            const pathData: Array<[Timestamp, unknown]> = rows.map(
              (row: any) => {
                const rowData = row as {
                  timestamp: Timestamp;
                  value: unknown;
                  value_json?: string;
                };
                const { timestamp } = rowData;
                // Handle both JSON values (like position objects) and simple values
                const value = rowData.value_json
                  ? JSON.parse(String(rowData.value_json))
                  : rowData.value;

                return [timestamp, value];
              }
            );

            // Merge with SQLite buffer data for recent records (federated query)
            let bufferRecords = this.getRecentBufferData(
              context,
              pathSpec.path as Path,
              from,
              to,
              debug
            );

            // Apply spatial filter to buffer records if this is a position path
            // (rare for scalar paths, but handle value_json position data)
            if (
              spatialFilter &&
              isPositionPath(pathSpec.path) &&
              bufferRecords.length > 0
            ) {
              const originalCount = bufferRecords.length;
              bufferRecords = filterBufferRecordsSpatially(
                bufferRecords,
                spatialFilter
              );
              debug(
                `Spatial filter on scalar buffer: ${originalCount} -> ${bufferRecords.length} records`
              );
            }

            allData[pathSpec.path] = this.mergeWithBufferData(
              pathData,
              bufferRecords,
              debug,
              timeResolutionMillis,
              String(pathSpec.aggregateMethod || 'average'),
              pathSpec.path,
              isAngularPath(pathSpec.path, app, context as string)
            );
          }
        } finally {
          connection.disconnectSync();
        }
      } catch (error) {
        console.error(
          `[HistoryAPI] Error querying path ${pathSpec.path}:`,
          error
        );
        debug(`Error querying path ${pathSpec.path}: ${error}`);

        // Even if parquet query fails, try to get buffer data
        let bufferRecords = this.getRecentBufferData(
          context,
          pathSpec.path as Path,
          from,
          to,
          debug
        );

        // Apply spatial filter if this is a position path
        if (
          spatialFilter &&
          isPositionPath(pathSpec.path) &&
          bufferRecords.length > 0
        ) {
          const originalCount = bufferRecords.length;
          bufferRecords = filterBufferRecordsSpatially(
            bufferRecords,
            spatialFilter
          );
          debug(
            `Spatial filter on fallback buffer: ${originalCount} -> ${bufferRecords.length} records`
          );
        }

        if (bufferRecords.length > 0) {
          allData[pathSpec.path] = this.mergeWithBufferData(
            [],
            bufferRecords,
            debug,
            timeResolutionMillis,
            String(pathSpec.aggregateMethod || 'average'),
            pathSpec.path,
            isAngularPath(pathSpec.path, app, context as string)
          );
        } else {
          allData[pathSpec.path] = [];
        }
      }
    });

    // Apply spatial timestamp filtering to non-position paths
    // This filters data to only include times when vessel was within spatial filter
    if (spatialTimestamps && spatialTimestamps.size > 0) {
      for (const pathSpec of pathSpecs) {
        if (!isPositionPath(pathSpec.path) && allData[pathSpec.path]) {
          const originalCount = allData[pathSpec.path].length;
          allData[pathSpec.path] = allData[pathSpec.path].filter(
            ([timestamp]) => spatialTimestamps!.has(timestamp)
          );
          debug(
            `[Spatial Correlation] Filtered ${pathSpec.path}: ${originalCount} -> ${allData[pathSpec.path].length} records`
          );
        }
      }
    }

    // Merge all path data into time-ordered rows
    const mergedData = this.mergePathData(allData, pathSpecs);

    // Check if any path has per-path smoothing defined
    const hasPerPathSmoothing = pathSpecs.some(
      ps => ps.smoothing !== undefined
    );

    // Determine final data and values based on smoothing mode
    let finalData: Array<[Timestamp, ...unknown[]]>;
    let finalValues: Array<{
      path: Path;
      method: AggregateMethod;
      smoothing?: string;
      window?: number;
    }>;

    if (hasPerPathSmoothing) {
      // Per-path smoothing mode: apply smoothing only to paths that have it defined
      // Use explicit syntax: path:sma:5 or path:ema:0.3
      finalData = this.addMovingAverages(mergedData, pathSpecs, true);
      finalValues = this.buildValuesWithMovingAverages(
        pathSpecs,
        objectPaths,
        true
      );
    } else {
      // No smoothing
      finalData = mergedData;
      finalValues = pathSpecs.map(({ path, aggregateMethod }) => ({
        path,
        method: aggregateMethod,
      }));
    }

    return {
      context,
      range: {
        from: from.toString() as Timestamp,
        to: to.toString() as Timestamp,
      },
      values: finalValues,
      data: finalData,
    } as DataResult;
  }

  private mergePathData(
    allData: { [path: string]: Array<[Timestamp, unknown]> },
    pathSpecs: PathSpec[]
  ): Array<[Timestamp, ...unknown[]]> {
    // Create a map of all unique timestamps
    const timestampMap = new Map<string, unknown[]>();

    pathSpecs.forEach((pathSpec, index) => {
      const pathData = allData[pathSpec.path] || [];
      pathData.forEach(([timestamp, value]) => {
        if (!timestampMap.has(timestamp)) {
          timestampMap.set(timestamp, new Array(pathSpecs.length).fill(null));
        }
        timestampMap.get(timestamp)![index] = value;
      });
    });

    // Convert to sorted array format
    return Array.from(timestampMap.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([timestamp, values]) => [timestamp as Timestamp, ...values]);
  }

  private addMovingAverages(
    data: Array<[Timestamp, ...unknown[]]>,
    pathSpecs: PathSpec[],
    usePerPathSmoothing: boolean = false
  ): Array<[Timestamp, ...unknown[]]> {
    if (data.length === 0) return data;

    // Default values for global moving averages mode
    const defaultSmaPeriod = 10;
    const defaultEmaAlpha = 0.2;

    // For each column, track EMA and SMA state
    // For objects, we need to track per-component state
    interface ComponentState {
      ema: number | null;
      smaWindow: number[];
    }

    const columnStates: Map<number, Map<string, ComponentState>> = new Map();

    // Initialize state for each column
    pathSpecs.forEach((_, colIndex) => {
      columnStates.set(colIndex, new Map());
    });

    return data.map((row, rowIndex) => {
      const [timestamp, ...values] = row;
      const enhancedValues: unknown[] = [];

      values.forEach((value, colIndex) => {
        const pathSpec = pathSpecs[colIndex];
        const hasSmoothing = pathSpec.smoothing !== undefined;

        // In per-path smoothing mode, only process paths with smoothing defined
        if (usePerPathSmoothing && !hasSmoothing) {
          enhancedValues.push(value);
          return;
        }

        // Get smoothing parameters
        const smoothingType = pathSpec.smoothing;
        const smaPeriod =
          smoothingType === 'sma' && pathSpec.smoothingParam
            ? pathSpec.smoothingParam
            : defaultSmaPeriod;
        const emaAlpha =
          smoothingType === 'ema' && pathSpec.smoothingParam
            ? pathSpec.smoothingParam
            : defaultEmaAlpha;

        // Check if this is an object value (like navigation.position)
        if (value && typeof value === 'object' && !Array.isArray(value)) {
          // Object with components - calculate smoothing for each numeric component
          const enhancedObject: any = { ...value };
          const colState = columnStates.get(colIndex)!;

          Object.entries(value).forEach(([componentName, componentValue]) => {
            if (typeof componentValue === 'number' && !isNaN(componentValue)) {
              // Get or create state for this component
              if (!colState.has(componentName)) {
                colState.set(componentName, { ema: null, smaWindow: [] });
              }
              const componentState = colState.get(componentName)!;

              if (usePerPathSmoothing) {
                // Per-path mode: only apply the specific smoothing requested
                if (smoothingType === 'ema') {
                  if (componentState.ema === null) {
                    componentState.ema = componentValue;
                  } else {
                    componentState.ema =
                      emaAlpha * componentValue +
                      (1 - emaAlpha) * componentState.ema;
                  }
                  enhancedObject[componentName] =
                    Math.round(componentState.ema * 1000) / 1000;
                } else if (smoothingType === 'sma') {
                  componentState.smaWindow.push(componentValue);
                  if (componentState.smaWindow.length > smaPeriod) {
                    componentState.smaWindow =
                      componentState.smaWindow.slice(-smaPeriod);
                  }
                  const sma =
                    componentState.smaWindow.reduce(
                      (sum, val) => sum + val,
                      0
                    ) / componentState.smaWindow.length;
                  enhancedObject[componentName] = Math.round(sma * 1000) / 1000;
                }
              } else {
                // Global mode: add both EMA and SMA as separate properties
                if (componentState.ema === null) {
                  componentState.ema = componentValue;
                } else {
                  componentState.ema =
                    defaultEmaAlpha * componentValue +
                    (1 - defaultEmaAlpha) * componentState.ema;
                }

                componentState.smaWindow.push(componentValue);
                if (componentState.smaWindow.length > defaultSmaPeriod) {
                  componentState.smaWindow =
                    componentState.smaWindow.slice(-defaultSmaPeriod);
                }
                const sma =
                  componentState.smaWindow.reduce((sum, val) => sum + val, 0) /
                  componentState.smaWindow.length;

                enhancedObject[`${componentName}_ema`] =
                  Math.round(componentState.ema * 1000) / 1000;
                enhancedObject[`${componentName}_sma`] =
                  Math.round(sma * 1000) / 1000;
              }
            }
          });

          enhancedValues.push(enhancedObject);
        } else if (typeof value === 'number' && !isNaN(value)) {
          // Scalar numeric value
          const colState = columnStates.get(colIndex)!;
          const scalarKey = '__scalar__';

          if (!colState.has(scalarKey)) {
            colState.set(scalarKey, { ema: null, smaWindow: [] });
          }
          const componentState = colState.get(scalarKey)!;

          if (usePerPathSmoothing) {
            // Check if using official SignalK syntax (smoothingOnly=true)
            // Official: path:sma:5 returns ONLY the smoothed value
            // Extension: path:average:sma:5 returns raw AND smoothed values
            const smoothingOnly = pathSpec.smoothingOnly === true;

            if (!smoothingOnly) {
              // Extension syntax: include raw value first
              enhancedValues.push(value);
            }

            if (smoothingType === 'ema') {
              if (componentState.ema === null) {
                componentState.ema = value;
              } else {
                componentState.ema =
                  emaAlpha * value + (1 - emaAlpha) * componentState.ema;
              }
              enhancedValues.push(Math.round(componentState.ema * 1000) / 1000);
            } else if (smoothingType === 'sma') {
              componentState.smaWindow.push(value);
              if (componentState.smaWindow.length > smaPeriod) {
                componentState.smaWindow =
                  componentState.smaWindow.slice(-smaPeriod);
              }
              const sma =
                componentState.smaWindow.reduce((sum, val) => sum + val, 0) /
                componentState.smaWindow.length;
              enhancedValues.push(Math.round(sma * 1000) / 1000);
            }
          } else {
            // Global mode: add value, EMA, and SMA as separate columns
            enhancedValues.push(value);

            if (componentState.ema === null) {
              componentState.ema = value;
            } else {
              componentState.ema =
                defaultEmaAlpha * value +
                (1 - defaultEmaAlpha) * componentState.ema;
            }

            componentState.smaWindow.push(value);
            if (componentState.smaWindow.length > defaultSmaPeriod) {
              componentState.smaWindow =
                componentState.smaWindow.slice(-defaultSmaPeriod);
            }
            const sma =
              componentState.smaWindow.reduce((sum, val) => sum + val, 0) /
              componentState.smaWindow.length;

            enhancedValues.push(Math.round(componentState.ema * 1000) / 1000); // EMA
            enhancedValues.push(Math.round(sma * 1000) / 1000); // SMA
          }
        } else {
          // Non-numeric, non-object values (null, string, etc.)
          const smoothingOnly = pathSpec.smoothingOnly === true;
          if (usePerPathSmoothing && smoothingOnly) {
            // Official syntax: only the smoothed value column
            enhancedValues.push(null);
          } else if (usePerPathSmoothing && hasSmoothing) {
            // Extension syntax: raw + smoothed columns
            enhancedValues.push(value);
            enhancedValues.push(null); // Smoothed value placeholder
          } else if (!usePerPathSmoothing) {
            enhancedValues.push(value);
            enhancedValues.push(null); // EMA
            enhancedValues.push(null); // SMA
          } else {
            enhancedValues.push(value);
          }
        }
      });

      return [timestamp, ...enhancedValues] as [Timestamp, ...unknown[]];
    });
  }

  private buildValuesWithMovingAverages(
    pathSpecs: PathSpec[],
    objectPaths: Set<string>,
    usePerPathSmoothing: boolean = false
  ): Array<{
    path: Path;
    method: AggregateMethod;
    smoothing?: string;
    window?: number;
  }> {
    const result: Array<{
      path: Path;
      method: AggregateMethod;
      smoothing?: string;
      window?: number;
    }> = [];

    pathSpecs.forEach(
      ({ path, aggregateMethod, smoothing, smoothingParam, smoothingOnly }) => {
        if (usePerPathSmoothing) {
          // Per-path smoothing mode
          // Check if using official SignalK syntax (smoothingOnly=true)
          // Official: path:sma:5 returns ONLY the smoothed value
          // Extension: path:average:sma:5 returns raw AND smoothed values

          if (!smoothingOnly) {
            // Extension syntax: add raw value entry first
            result.push({ path, method: aggregateMethod });
          }

          // Add smoothed entry if smoothing is defined
          if (smoothing) {
            const smoothedEntry: {
              path: Path;
              method: AggregateMethod;
              smoothing: string;
              window: number;
            } = {
              path,
              // For official syntax, use sma/ema as the method in response
              method: smoothingOnly
                ? (smoothing as AggregateMethod)
                : aggregateMethod,
              smoothing,
              window:
                smoothingParam !== undefined
                  ? smoothingParam
                  : smoothing === 'sma'
                    ? 10
                    : 0.2,
            };
            result.push(smoothedEntry);
          }
        } else if (objectPaths.has(path)) {
          // Object path - EMA/SMA are embedded in the object as component properties
          // Just add the single path entry
          result.push({ path, method: aggregateMethod });
        } else {
          // Scalar path - add separate entries for value, EMA, and SMA
          result.push({ path, method: aggregateMethod });
          result.push({
            path: `${path}.ema` as Path,
            method: 'ema' as AggregateMethod,
          });
          result.push({
            path: `${path}.sma` as Path,
            method: 'sma' as AggregateMethod,
          });
        }
      }
    );

    return result;
  }

  /**
   * Convert all timestamps in the data result to a target timezone
   */
  private convertTimestamps(
    result: DataResult,
    timezoneParam: string | undefined,
    debug: (k: string) => void
  ): DataResult {
    try {
      const targetZone = getTargetTimezone(timezoneParam);
      const targetZoneName = targetZone.toString();

      // Get current time in both UTC and target zone for verification
      const now = ZonedDateTime.now(ZoneOffset.UTC);
      const nowInTarget = now.withZoneSameInstant(targetZone);
      const offset = nowInTarget.offset().toString();

      debug(
        `[Timestamp Conversion] Converting timestamps to timezone: ${targetZoneName}`
      );
      console.log(`[Timestamp Conversion] Target timezone: ${targetZoneName}`);
      console.log(`[Timestamp Conversion] Current UTC time: ${now.toString()}`);
      console.log(
        `[Timestamp Conversion] Current local time: ${nowInTarget.toOffsetDateTime().toString()} (offset: ${offset})`
      );
      console.log(
        `[Timestamp Conversion] Converting ${result.data.length} rows`
      );

      // Convert all timestamps in the data array
      const convertedData = result.data.map(row => {
        const [timestamp, ...values] = row;
        const convertedTimestamp = convertTimestampToTimezone(
          timestamp,
          targetZone
        );
        return [convertedTimestamp, ...values] as [Timestamp, ...unknown[]];
      });

      // Also convert the range timestamps
      const convertedRange = {
        from: convertTimestampToTimezone(result.range.from, targetZone),
        to: convertTimestampToTimezone(result.range.to, targetZone),
      };

      console.log(
        `[Timestamp Conversion] ✅ Successfully converted timestamps to ${targetZoneName}`
      );

      // Get a sample timestamp to show the conversion
      const sampleOriginal =
        result.data.length > 0 ? result.data[0][0] : result.range.from;
      const sampleConverted =
        convertedData.length > 0 ? convertedData[0][0] : convertedRange.from;
      console.log(
        `[Timestamp Conversion] Example: ${sampleOriginal} → ${sampleConverted}`
      );

      return {
        ...result,
        data: convertedData,
        range: convertedRange,
        timezone: {
          converted: true,
          targetTimezone: targetZoneName,
          offset: offset,
          description: timezoneParam
            ? `Converted to user-specified timezone: ${targetZoneName} (${offset})`
            : `Converted to server local timezone: ${targetZoneName} (${offset}). To use a different timezone, add &timezone=America/New_York (or other IANA timezone ID)`,
        },
      };
    } catch (error) {
      console.error(
        '[Timestamp Conversion] Error converting timestamps:',
        error
      );
      debug(`Error converting timestamps: ${error}`);
      // Return original result if conversion fails
      return result;
    }
  }
}

function splitPathExpression(pathExpression: string): PathSpec {
  const parts = pathExpression.split(':');
  let aggregateMethod = (parts[1] || 'average') as AggregateMethod;

  // Validate the aggregation method
  const validMethods = [
    'average',
    'min',
    'max',
    'first',
    'last',
    'mid',
    'middle_index',
  ];

  let smoothing: 'sma' | 'ema' | undefined;
  let smoothingParam: number | undefined;
  let smoothingOnly = false;

  // Check for official SignalK spec syntax: path:sma:5 or path:ema:0.2
  // In official spec, sma/ema ARE aggregation methods (returns only smoothed value)
  if (parts[1] === 'sma' || parts[1] === 'ema') {
    smoothing = parts[1];
    smoothingOnly = true; // Official syntax: return only smoothed value
    aggregateMethod = 'average' as AggregateMethod; // SMA/EMA is based on average
    if (parts[2]) {
      smoothingParam = parseFloat(parts[2]);
      if (isNaN(smoothingParam)) smoothingParam = undefined;
    }
  } else {
    // Standard aggregation method validation
    if (parts[1] && !validMethods.includes(parts[1])) {
      aggregateMethod = 'average' as AggregateMethod;
    }

    // Parse extended smoothing syntax (parts[2]) and parameter (parts[3])
    // Extension syntax: path:aggregateMethod:smoothing:param
    // Example: navigation.speedOverGround:average:sma:5
    if (parts[2] === 'sma' || parts[2] === 'ema') {
      smoothing = parts[2];
      if (parts[3]) {
        smoothingParam = parseFloat(parts[3]);
        if (isNaN(smoothingParam)) smoothingParam = undefined;
      }
    }
  }

  return {
    path: parts[0] as Path,
    queryResultName: parts[0].replace(/\./g, '_'),
    aggregateMethod,
    aggregateFunction:
      (functionForAggregate[aggregateMethod] as string) || 'avg',
    smoothing,
    smoothingParam,
    smoothingOnly,
  };
}

const functionForAggregate: { [key: string]: string } = {
  average: 'avg',
  min: 'min',
  max: 'max',
  first: 'first',
  last: 'last',
  mid: 'median',
  middle_index: 'nth_value',
} as const;

function getAggregateFunction(method: AggregateMethod): string {
  switch (method) {
    case 'average':
      return 'AVG';
    case 'min':
      return 'MIN';
    case 'max':
      return 'MAX';
    case 'first':
      return 'FIRST';
    case 'last':
      return 'LAST';
    case 'mid':
      return 'MEDIAN';
    case 'middle_index':
      return 'NTH_VALUE';
    default:
      return 'AVG';
  }
}

function getValueExpression(pathName: string, hasValueJson: boolean): string {
  // For position data or other complex objects, use value_json if the column exists
  if (pathName === 'navigation.position' && hasValueJson) {
    return 'value_json';
  }

  // For numeric data, try to cast to DOUBLE, fallback to the original value
  return 'TRY_CAST(value AS DOUBLE)';
}

/**
 * Get the timestamp column name based on the tier.
 * Raw tier uses signalk_timestamp, aggregated tiers use bucket_time.
 */
function getTierTimestampColumn(tier: string): string {
  return tier === 'raw' ? 'signalk_timestamp' : 'bucket_time';
}

/**
 * Get the aggregate expression for a tier-aware query.
 * For raw tier, aggregates from raw value column.
 * For aggregated tiers, uses pre-computed value_avg (or re-aggregates from sin/cos for angular).
 */
function getTierAggregateExpression(
  method: AggregateMethod,
  pathName: string,
  tier: string,
  hasValueJson: boolean,
  app?: any,
  context?: string
): string {
  // Raw tier: use original aggregate expression
  if (tier === 'raw') {
    return getAggregateExpression(method, pathName, hasValueJson, app, context);
  }

  // Aggregated tier: use pre-computed columns
  const isAngular = app && context && isAngularPath(pathName, app, context);

  if (isAngular) {
    // Re-aggregate angular paths using pre-computed sin/cos averages
    // Weight by sample_count for correct weighted averaging
    return `ATAN2(
      SUM(COALESCE(value_sin_avg, SIN(value_avg)) * sample_count) / SUM(sample_count),
      SUM(COALESCE(value_cos_avg, COS(value_avg)) * sample_count) / SUM(sample_count)
    )`;
  }

  // For standard numeric aggregation methods on pre-aggregated data
  switch (method) {
    case 'min':
      return 'MIN(value_min)';
    case 'max':
      return 'MAX(value_max)';
    case 'average':
    case undefined:
      // Weighted average using sample_count
      return 'SUM(value_avg * sample_count) / SUM(sample_count)';
    default:
      // For other methods (first, last, median), fall back to value_avg
      return `${getAggregateFunction(method)}(value_avg)`;
  }
}

/**
 * Get the WHERE clause for null filtering based on tier.
 * Raw tier checks value column, aggregated tiers check value_avg.
 */
function getTierWhereClause(tier: string, hasValueJson: boolean): string {
  if (tier === 'raw') {
    return hasValueJson
      ? '(value IS NOT NULL OR value_json IS NOT NULL)'
      : 'value IS NOT NULL';
  }
  return 'value_avg IS NOT NULL';
}

/**
 * Aggregate numeric values in a buffer bucket according to the requested method
 */
function aggregateNumericBucket(
  records: DataRecord[],
  method: string,
  isAngular: boolean
): number | null {
  const values: number[] = [];
  for (const r of records) {
    const v = typeof r.value === 'number' ? r.value : parseFloat(r.value);
    if (!isNaN(v)) values.push(v);
  }
  if (values.length === 0) return null;

  switch (method) {
    case 'min':
      return Math.min(...values);
    case 'max':
      return Math.max(...values);
    case 'first':
      return values[0];
    case 'last':
      return values[values.length - 1];
    case 'average':
    default:
      if (isAngular) {
        let sinSum = 0;
        let cosSum = 0;
        for (const v of values) {
          sinSum += Math.sin(v);
          cosSum += Math.cos(v);
        }
        return Math.atan2(sinSum / values.length, cosSum / values.length);
      }
      return values.reduce((sum, v) => sum + v, 0) / values.length;
  }
}

/**
 * Aggregate object values (e.g., position {latitude, longitude}) in a buffer bucket
 */
function aggregateObjectBucket(
  records: DataRecord[]
): Record<string, unknown> | null {
  // Parse all values
  const objects: Record<string, unknown>[] = [];
  for (const r of records) {
    const val = r.value_json
      ? typeof r.value_json === 'string'
        ? JSON.parse(r.value_json)
        : r.value_json
      : r.value;
    if (typeof val === 'object' && val !== null) {
      objects.push(val as Record<string, unknown>);
    }
  }
  if (objects.length === 0) return null;

  // Average each numeric component
  const result: Record<string, unknown> = {};
  const keys = Object.keys(objects[0]);
  for (const key of keys) {
    const numericVals: number[] = [];
    for (const obj of objects) {
      const v = obj[key];
      if (typeof v === 'number') numericVals.push(v);
    }
    if (numericVals.length > 0) {
      result[key] =
        numericVals.reduce((sum, v) => sum + v, 0) / numericVals.length;
    } else {
      // Non-numeric: take first value
      result[key] = objects[0][key];
    }
  }
  return result;
}

function getAggregateExpression(
  method: AggregateMethod,
  pathName: string,
  hasValueJson: boolean,
  app?: any,
  context?: string
): string {
  const valueExpr = getValueExpression(pathName, hasValueJson);

  if (method === 'middle_index') {
    // For middle_index, use FIRST as a simple fallback for now
    // TODO: Implement proper middle index selection
    return `FIRST(${valueExpr})`;
  }

  // Use vector averaging for angular paths (heading, COG, wind direction, etc.)
  if (
    (method === 'average' || method === undefined) &&
    app &&
    context &&
    isAngularPath(pathName, app, context)
  ) {
    return `ATAN2(AVG(SIN(${valueExpr})), AVG(COS(${valueExpr})))`;
  }

  return `${getAggregateFunction(method)}(${valueExpr})`;
}

/**
 * Get the appropriate aggregate function for a component based on its data type
 * Numeric components use the requested method, non-numeric use middle_index
 */
function getComponentAggregateFunction(
  requestedMethod: AggregateMethod,
  dataType: ComponentInfo['dataType']
): string {
  // For numeric components, use the requested aggregation method
  if (dataType === 'numeric') {
    // Special case: middle_index requires window functions (NTH_VALUE)
    // Use FIRST as fallback, matching scalar path behavior
    if (requestedMethod === 'middle_index') {
      return 'FIRST';
    }
    return getAggregateFunction(requestedMethod);
  }

  // For non-numeric components (string, boolean, unknown), use FIRST
  // This ensures we get a representative value from the bucket
  return 'FIRST';
}
