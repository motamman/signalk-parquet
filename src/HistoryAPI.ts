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
import {
  getAvailableContextsForTimeRange,
  getContextsInSpatialFilter,
} from './utils/context-discovery';
import {
  getPathComponentSchema,
  PathComponentSchema,
  ComponentInfo,
} from './utils/schema-cache';
import { ConcurrencyLimiter } from './utils/concurrency-limiter';
import { CONCURRENCY } from './config/cache-defaults';
import { SQLiteBufferInterface } from './types';
import { HivePathBuilder, AggregationTier } from './utils/hive-path-builder';
import {
  AutoDiscoveryService,
  AutoDiscoveryResult,
} from './services/auto-discovery';
import {
  SpatialFilter,
  parseSpatialParams,
  buildSpatialSqlClause,
  isPositionPath,
} from './utils/spatial-queries';
import { calculateDistance } from './utils/geo-calculator';
import {
  buildBufferScalarSubquery,
  buildBufferObjectSubquery,
} from './utils/buffer-sql-builder';
import {
  parseDurationToMillis,
  parseResolutionToMillis,
} from './utils/duration-parser';
import { isAngularPath } from './utils/angular-paths';

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
    const { from, to, context, spatialFilter } = getRequestParams(
      req as FromToContextRequest,
      selfId
    );
    historyApi.getValues(
      context,
      from,
      to,
      spatialFilter,
      app,
      debug,
      req,
      res
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
        const context = req.query.context
          ? getContext(req.query.context as string, selfId)
          : undefined;
        const paths = getAvailablePathsArray(dataDir, app, context);
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
    const { from, to, context, spatialFilter } = getRequestParams(
      req as FromToContextRequest,
      selfId
    );
    historyApi.getValues(
      context,
      from,
      to,
      spatialFilter,
      app,
      debug,
      req,
      res
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
  router.get(
    '/api/history/contexts/spatial',
    async (req: Request, res: Response) => {
      try {
        const { from, to } = getRequestParams(
          req as FromToContextRequest,
          selfId
        );

        const spatialFilter = parseSpatialParams(
          req.query.bbox as string | undefined,
          req.query.radius as string | undefined
        );

        if (!spatialFilter) {
          res.status(400).json({
            error:
              'bbox (west,south,east,north) or radius (lon,lat,meters) is required',
          });
          return;
        }

        const contexts = await getContextsInSpatialFilter(
          dataDir,
          from,
          to,
          spatialFilter
        );
        res.json(contexts);
      } catch (error) {
        debug(`Error in /api/history/contexts/spatial: ${error}`);
        res.status(500).json({ error: (error as Error).message });
      }
    }
  );
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
        const context = req.query.context
          ? getContext(req.query.context as string, selfId)
          : undefined;
        const paths = getAvailablePathsArray(dataDir, app, context);
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

    // ============================================================================
    // STANDARD SIGNALK TIME RANGE PATTERNS
    // ============================================================================
    // Pattern 1: duration only → query back from now
    if (query.duration && !query.from && !query.to) {
      const durationMs = parseDuration(query.duration);
      to = ZonedDateTime.now(ZoneOffset.UTC);
      from = to.minusNanos(durationMs * 1000000);
    }
    // Pattern 2: from + duration → query forward from start
    else if (query.from && query.duration && !query.to) {
      from = parseDateTime(query.from);
      const durationMs = parseDuration(query.duration);
      to = from.plusNanos(durationMs * 1000000);
    }
    // Pattern 3: to + duration → query backward to end
    else if (query.to && query.duration && !query.from) {
      to = parseDateTime(query.to);
      const durationMs = parseDuration(query.duration);
      from = to.minusNanos(durationMs * 1000000);
    }
    // Pattern 4: from only → from start to now
    else if (query.from && !query.duration && !query.to) {
      from = parseDateTime(query.from);
      to = ZonedDateTime.now(ZoneOffset.UTC);
    }
    // Pattern 5: from + to → specific range
    else if (query.from && query.to && !query.duration) {
      from = parseDateTime(query.from);
      to = parseDateTime(query.to);
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
    return { from, to, context, spatialFilter };
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

// Parse datetime string per ISO 8601:
// - Bare timestamps (no Z, no offset) → local time, converted to UTC
// - Timestamps with Z or offset → parsed as-is
function parseDateTime(dateTimeStr: string): ZonedDateTime {
  // Normalize the datetime string to include seconds if missing
  let normalizedStr = dateTimeStr;
  if (dateTimeStr.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/)) {
    // Add seconds if only HH:MM is provided
    normalizedStr = dateTimeStr + ':00';
  }

  if (hasTimezoneInfo(normalizedStr)) {
    // Has timezone info (Z or offset), parse as-is and convert to UTC
    return ZonedDateTime.parse(normalizedStr).withZoneSameInstant(
      ZoneOffset.UTC
    );
  } else {
    // No timezone info — per ISO 8601, treat as local time and convert to UTC
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

type QuerySource = 'local' | 's3' | 'hybrid' | 'auto';

export interface S3QueryConfig {
  enabled: boolean;
  bucket: string;
  keyPrefix: string;
  region: string;
}

/**
 * Convert a UTC timestamp string to server-local time with offset suffix.
 * e.g. "2026-03-06T19:00:00Z" → "2026-03-06T14:00:00-05:00" (EST)
 */
function utcToLocalTimestamp(utcTs: Timestamp): Timestamp {
  try {
    const zdt = ZonedDateTime.parse(utcTs).withZoneSameInstant(ZoneId.systemDefault());
    // ZonedDateTime.toString() appends "[SYSTEM]" zone ID — strip it
    return zdt.toString().replace(/\[.*\]$/, '') as Timestamp;
  } catch {
    // If parsing fails (e.g. already has offset or unusual format), try via Date
    const d = new Date(utcTs);
    if (isNaN(d.getTime())) return utcTs;
    const offsetMin = d.getTimezoneOffset();
    const local = new Date(d.getTime() - offsetMin * 60000);
    const sign = offsetMin <= 0 ? '+' : '-';
    const absH = String(Math.floor(Math.abs(offsetMin) / 60)).padStart(2, '0');
    const absM = String(Math.abs(offsetMin) % 60).padStart(2, '0');
    return (local.toISOString().replace('Z', '') + sign + absH + ':' + absM) as Timestamp;
  }
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
   * Convert all timestamps in a DataResult from UTC to server local time.
   */
  private convertToLocalTime(result: DataResult): DataResult {
    return {
      ...result,
      range: {
        from: utcToLocalTimestamp(result.range.from),
        to: utcToLocalTimestamp(result.range.to),
      },
      data: result.data.map(row => {
        const newRow = [...row] as typeof row;
        newRow[0] = utcToLocalTimestamp(row[0]);
        return newRow;
      }),
    };
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
    _tier: AggregationTier | undefined,
    _querySource: QuerySource,
    debug: (k: string) => void
  ): Promise<Set<string>> {
    const timestamps = new Set<string>();

    // Build file path for raw position data
    const sanitizedContext = this.hivePathBuilder.sanitizeContext(context);
    const sanitizedPath = this.hivePathBuilder.sanitizePath(positionPath);
    const localFilePath = path.join(
      this.dataDir,
      'tier=raw',
      `context=${sanitizedContext}`,
      `path=${sanitizedPath}`,
      '**',
      '*.parquet'
    );

    const fromIso = from.toInstant().toString();
    const toIso = to.toInstant().toString();

    try {
      const hasBuffer = DuckDBPool.isSQLiteBufferInitialized();
      const connection = hasBuffer
        ? await DuckDBPool.getConnectionWithBuffer()
        : await DuckDBPool.getConnection();
      try {
        // Bucket-lookup approach: instead of scanning all raw position data,
        // bucket by time resolution, grab FIRST lat/lon per bucket, then filter by bbox/radius.
        // This reads far less data than a full scan with spatial SQL.
        const bucketExpr = `strftime(DATE_TRUNC('seconds',
          EPOCH_MS(CAST(FLOOR(EPOCH_MS(signalk_timestamp::TIMESTAMP) / ${timeResolutionMillis}) * ${timeResolutionMillis} AS BIGINT))
        ), '%Y-%m-%dT%H:%M:%SZ')`;

        // Build FROM: parquet UNION ALL buffer
        const parquetFrom = `SELECT signalk_timestamp, value_latitude, value_longitude FROM (
          SELECT * FROM read_parquet('${localFilePath}', union_by_name=true, filename=true)
          WHERE filename NOT LIKE '%/processed/%'
          AND filename NOT LIKE '%/quarantine/%'
          AND filename NOT LIKE '%/failed/%'
          AND filename NOT LIKE '%/repaired/%')`;

        let fromSource = `(${parquetFrom})`;
        if (hasBuffer && this.sqliteBuffer) {
          const knownPaths = this.sqliteBuffer.getKnownPaths();
          const bufferTableCols = this.sqliteBuffer.getTableColumns(positionPath);
          const bufferSubquery = buildBufferObjectSubquery(
            context, positionPath, fromIso, toIso,
            new Map([
              ['latitude', { name: 'latitude', columnName: 'value_latitude', dataType: 'numeric' as const }],
              ['longitude', { name: 'longitude', columnName: 'value_longitude', dataType: 'numeric' as const }],
            ]),
            knownPaths,
            bufferTableCols
          );
          if (bufferSubquery) {
            fromSource = `(${parquetFrom} UNION ALL SELECT signalk_timestamp, TRY_CAST(value_latitude AS DOUBLE) as value_latitude, TRY_CAST(value_longitude AS DOUBLE) as value_longitude FROM ${bufferSubquery})`;
          }
        }

        const query = `
          SELECT
            ${bucketExpr} as timestamp,
            FIRST(TRY_CAST(value_latitude AS DOUBLE)) as lat,
            FIRST(TRY_CAST(value_longitude AS DOUBLE)) as lon
          FROM ${fromSource}
          WHERE
            signalk_timestamp >= '${fromIso}'
            AND signalk_timestamp < '${toIso}'
            AND TRY_CAST(value_latitude AS DOUBLE) IS NOT NULL
            AND TRY_CAST(value_longitude AS DOUBLE) IS NOT NULL
          GROUP BY timestamp
          ORDER BY timestamp
        `;

        const result = await connection.runAndReadAll(query);
        const rows = result.getRowObjects() as Array<{ timestamp: string; lat: number; lon: number }>;

        // Filter buckets by spatial filter in JS
        const { bbox } = spatialFilter;
        for (const row of rows) {
          if (row.lat === null || row.lon === null) continue;

          // Lat check
          if (row.lat < bbox.south || row.lat > bbox.north) continue;

          // Lon check (handles 180° meridian crossing)
          if (bbox.west <= bbox.east) {
            if (row.lon < bbox.west || row.lon > bbox.east) continue;
          } else {
            if (row.lon < bbox.west && row.lon > bbox.east) continue;
          }

          // Precise radius check if applicable
          if (
            spatialFilter.type === 'radius' &&
            spatialFilter.centerLat !== undefined &&
            spatialFilter.centerLon !== undefined &&
            spatialFilter.radiusMeters !== undefined
          ) {
            const dist = calculateDistance(
              row.lat, row.lon,
              spatialFilter.centerLat, spatialFilter.centerLon
            );
            if (dist > spatialFilter.radiusMeters) continue;
          }

          timestamps.add(row.timestamp);
        }

        debug(
          `[Spatial Correlation] Bucketed ${rows.length} positions, ${timestamps.size} within spatial filter`
        );
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
    spatialFilter: SpatialFilter | null,
    app: any,
    debug: (k: string) => void,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    req: Request<ParamsDictionary, any, any, ParsedQs, Record<string, any>>,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    res: Response<any, Record<string, any>>
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

      // Auto-select tier based on resolution (provider selects automatically)
      const tier = this.selectOptimalTier(timeResolutionMillis);
      if (tier) {
        debug(
          `Auto-selected tier=${tier} for resolution=${timeResolutionMillis}ms`
        );
      }

      // Log spatial filter if present
      if (spatialFilter) {
        debug(
          `Spatial filter: type=${spatialFilter.type}, bbox=[${spatialFilter.bbox.west},${spatialFilter.bbox.south},${spatialFilter.bbox.east},${spatialFilter.bbox.north}]`
        );
      }

      // Handle position and numeric paths together
      const positionPath = 'navigation.position';
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
            'local',
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

      allResult = this.convertToLocalTime(allResult);
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
    const hasPositionPath = spatialFilter && pathSpecs.some(ps => isPositionPath(ps.path));
    if (spatialFilter) {
      const hasNonPositionPaths = pathSpecs.some(
        ps => !isPositionPath(ps.path)
      );
      if (hasNonPositionPaths && !hasPositionPath) {
        // Position not in requested paths — need a separate scan for correlation
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
          undefined, // Force raw tier — position is an object path, aggregated tiers lack value_latitude/value_longitude
          querySource,
          debug
        );
        debug(
          `[Spatial Correlation] Found ${spatialTimestamps.size} valid timestamps`
        );
      }

      if (hasPositionPath) {
        // Position IS in requested paths — use fast bucket+FIRST query for both
        // position results AND spatial correlation timestamps (one query, no full raw scan)
        const posPathSpec = pathSpecs.find(ps => isPositionPath(ps.path))!;
        debug(
          `[Spatial] Fast bucket query for ${posPathSpec.path} (FIRST lat/lon per bucket + JS filter)`
        );

        const sanitizedCtx = this.hivePathBuilder.sanitizeContext(context);
        const sanitizedPos = this.hivePathBuilder.sanitizePath(posPathSpec.path);
        const posFilePath = path.join(
          this.dataDir,
          'tier=raw',
          `context=${sanitizedCtx}`,
          `path=${sanitizedPos}`,
          '**',
          '*.parquet'
        );
        const fromIso = from.toInstant().toString();
        const toIso = to.toInstant().toString();

        try {
          const hasBuffer = DuckDBPool.isSQLiteBufferInitialized();
          const connection = hasBuffer
            ? await DuckDBPool.getConnectionWithBuffer()
            : await DuckDBPool.getConnection();
          try {
            const bucketExpr = `strftime(DATE_TRUNC('seconds',
              EPOCH_MS(CAST(FLOOR(EPOCH_MS(signalk_timestamp::TIMESTAMP) / ${timeResolutionMillis}) * ${timeResolutionMillis} AS BIGINT))
            ), '%Y-%m-%dT%H:%M:%SZ')`;

            // Build FROM: parquet UNION ALL buffer (for today's unexported data)
            const parquetFrom = `SELECT signalk_timestamp, value_latitude, value_longitude FROM (
              SELECT * FROM read_parquet('${posFilePath}', union_by_name=true, filename=true)
              WHERE filename NOT LIKE '%/processed/%'
              AND filename NOT LIKE '%/quarantine/%'
              AND filename NOT LIKE '%/failed/%'
              AND filename NOT LIKE '%/repaired/%')`;

            let fromSource = `(${parquetFrom})`;
            if (hasBuffer && this.sqliteBuffer) {
              const posPathSpec = pathSpecs.find(ps => isPositionPath(ps.path))!;
              const bufferTableCols = this.sqliteBuffer.getTableColumns(posPathSpec.path);
              const bufferSubquery = buildBufferObjectSubquery(
                context, posPathSpec.path, fromIso, toIso,
                new Map([
                  ['latitude', { name: 'latitude', columnName: 'value_latitude', dataType: 'numeric' as const }],
                  ['longitude', { name: 'longitude', columnName: 'value_longitude', dataType: 'numeric' as const }],
                ]),
                this.sqliteBuffer.getKnownPaths(),
                bufferTableCols
              );
              if (bufferSubquery) {
                fromSource = `(${parquetFrom} UNION ALL SELECT signalk_timestamp, TRY_CAST(value_latitude AS DOUBLE) as value_latitude, TRY_CAST(value_longitude AS DOUBLE) as value_longitude FROM ${bufferSubquery})`;
              }
            }

            const query = `
              SELECT
                ${bucketExpr} as timestamp,
                FIRST(TRY_CAST(value_latitude AS DOUBLE)) as lat,
                FIRST(TRY_CAST(value_longitude AS DOUBLE)) as lon
              FROM ${fromSource}
              WHERE
                signalk_timestamp >= '${fromIso}'
                AND signalk_timestamp < '${toIso}'
                AND TRY_CAST(value_latitude AS DOUBLE) IS NOT NULL
                AND TRY_CAST(value_longitude AS DOUBLE) IS NOT NULL
              GROUP BY timestamp
              ORDER BY timestamp
            `;

            const result = await connection.runAndReadAll(query);
            const rows = result.getRowObjects() as Array<{ timestamp: string; lat: number; lon: number }>;

            // Filter by spatial and build position results + timestamps
            const posData: Array<[Timestamp, unknown]> = [];
            spatialTimestamps = new Set<string>();
            const { bbox } = spatialFilter;

            for (const row of rows) {
              if (row.lat === null || row.lon === null) continue;

              let inArea = true;

              // Lat check
              if (row.lat < bbox.south || row.lat > bbox.north) inArea = false;

              // Lon check
              if (inArea) {
                if (bbox.west <= bbox.east) {
                  if (row.lon < bbox.west || row.lon > bbox.east) inArea = false;
                } else {
                  if (row.lon < bbox.west && row.lon > bbox.east) inArea = false;
                }
              }

              // Precise radius check
              if (
                inArea &&
                spatialFilter.type === 'radius' &&
                spatialFilter.centerLat !== undefined &&
                spatialFilter.centerLon !== undefined &&
                spatialFilter.radiusMeters !== undefined
              ) {
                const dist = calculateDistance(
                  row.lat, row.lon,
                  spatialFilter.centerLat, spatialFilter.centerLon
                );
                if (dist > spatialFilter.radiusMeters) inArea = false;
              }

              if (inArea) {
                spatialTimestamps.add(row.timestamp);
                posData.push([row.timestamp as Timestamp, { latitude: row.lat, longitude: row.lon }]);
              }
            }

            allData[posPathSpec.path] = posData;
            debug(
              `[Spatial] Bucketed ${rows.length} positions, ${posData.length} within filter`
            );
          } finally {
            connection.disconnectSync();
          }
        } catch (error) {
          debug(`[Spatial] Error in fast position query: ${error}`);
          allData[posPathSpec.path] = [];
          spatialTimestamps = new Set<string>();
        }
      }
    }

    // Process each path and collect data with concurrency limiting
    // Limit concurrent queries to prevent resource exhaustion (configured in cache-defaults)
    const limiter = new ConcurrencyLimiter(CONCURRENCY.MAX_QUERIES);
    await limiter.map(pathSpecs, async pathSpec => {
      try {
        // Skip position if already handled by fast spatial bucket query
        if (hasPositionPath && isPositionPath(pathSpec.path) && allData[pathSpec.path]) {
          return;
        }

        // Build file patterns based on query source
        let localFilePath: string | null = null;
        let s3FilePath: string | null = null;
        const effectiveTier = tier || 'raw';

        // Always query local first
        const sanitizedContext = this.hivePathBuilder.sanitizeContext(context);
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
        debug(`Querying local Hive tier=${effectiveTier} at: ${localFilePath}`);

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
              debug(
                `S3 supplement for ${fromDate.toISOString()} to ${s3ToDate.toISOString()}`
              );
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

        // Get connection from pool with SQLite buffer attached (spatial extension already loaded)
        const hasBuffer = DuckDBPool.isSQLiteBufferInitialized();
        const knownBufferPaths = hasBuffer && this.sqliteBuffer ? this.sqliteBuffer.getKnownPaths() : undefined;
        const connection = hasBuffer
          ? await DuckDBPool.getConnectionWithBuffer()
          : await DuckDBPool.getConnection();

        try {
          // Build FROM clause based on available sources
          // For hybrid queries, we UNION local and S3 sources
          // Local files need filename filter to exclude processed/quarantine/etc directories
          const buildFromClause = (filePath: string): string => {
            const isS3 = filePath.startsWith('s3://');
            if (isS3) {
              return `read_parquet('${filePath}', union_by_name=true, filename=true)`;
            }
            // Local files: exclude processed, quarantine, failed, repaired directories
            return `(SELECT * FROM read_parquet('${filePath}', union_by_name=true, filename=true) WHERE filename NOT LIKE '%/processed/%' AND filename NOT LIKE '%/quarantine/%' AND filename NOT LIKE '%/failed/%' AND filename NOT LIKE '%/repaired/%')`;
          };

          // Build FROM clause: local-only by default, hybrid only if S3 has data
          let fromClause: string;
          let localFromClause = localFilePath
            ? buildFromClause(localFilePath)
            : null;

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
          const runQueryWithFallback = async (
            buildQuery: (fc: string) => string
          ): Promise<any> => {
            try {
              return await connection.runAndReadAll(buildQuery(fromClause));
            } catch (err) {
              if (s3FilePath && localFromClause) {
                debug(
                  `Hybrid query failed, falling back to local-only: ${err}`
                );
                return await connection.runAndReadAll(
                  buildQuery(localFromClause)
                );
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
            // Aggregated tiers (5s/60s/1h) collapse object paths into scalar value_avg,
            // so always query raw tier for object paths
            if (effectiveTier !== 'raw') {
              debug(`Path ${pathSpec.path}: Object path — overriding tier=${effectiveTier} to raw`);
              localFilePath = path.join(
                this.dataDir,
                'tier=raw',
                `context=${sanitizedContext}`,
                `path=${sanitizedSkPath}`,
                '**',
                '*.parquet'
              );
              // Rebuild S3 path with raw tier too
              let rawS3FilePath: string | null = null;
              if (this.s3Config?.enabled) {
                const rawEarliestDate = this.hivePathBuilder.findEarliestDate(
                  this.dataDir, 'raw', sanitizedContext, sanitizedSkPath
                );
                if (rawEarliestDate && fromDate < rawEarliestDate) {
                  const s3ToDate = new Date(rawEarliestDate.getTime() - 86400000);
                  if (fromDate <= s3ToDate) {
                    rawS3FilePath = this.hivePathBuilder.buildS3Glob(
                      this.s3Config.bucket, this.s3Config.keyPrefix || '',
                      'raw', context, pathSpec.path, fromDate, s3ToDate
                    );
                  }
                } else if (!rawEarliestDate) {
                  rawS3FilePath = this.hivePathBuilder.buildS3Glob(
                    this.s3Config.bucket, this.s3Config.keyPrefix || '',
                    'raw', context, pathSpec.path, fromDate, toDate
                  );
                }
              }
              // Rebuild fromClause with raw tier local + S3
              const rawLocalFrom = buildFromClause(localFilePath);
              localFromClause = rawLocalFrom; // Update fallback for S3 failure
              if (rawS3FilePath) {
                fromClause = `(
                  SELECT * FROM ${rawLocalFrom}
                  UNION ALL
                  SELECT * FROM ${buildFromClause(rawS3FilePath)}
                )`;
                debug(`Hybrid query (raw tier): combining local and S3 sources`);
              } else {
                fromClause = rawLocalFrom;
              }
            }
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
                // TRY_CAST handles mixed-type parquet files (some store lat/lon as VARCHAR)
                const colExpr = comp.dataType === 'numeric'
                  ? `TRY_CAST(${comp.columnName} AS DOUBLE)`
                  : comp.columnName;
                return `${aggFunc}(${colExpr}) as ${comp.name}`;
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

            // Each source does its own aggregation into (timestamp, components..., priority).
            // UNION ALL the results, then ROW_NUMBER to pick highest-priority source per bucket.
            // Priority: buffer(3) > raw tier gap(2) > tier parquet(1).
            const objTsCol = getTierTimestampColumn('raw');
            const componentCols = Array.from(componentSchema.components.values()).map(c => c.columnName).join(', ');
            const objBucketExpr = (col: string) =>
              `strftime(DATE_TRUNC('seconds', EPOCH_MS(CAST(FLOOR(EPOCH_MS(${col}::TIMESTAMP) / ${timeResolutionMillis}) * ${timeResolutionMillis} AS BIGINT))), '%Y-%m-%dT%H:%M:%SZ')`;

            const buildObjQuery = (fc: string) => {
              const subqueries: string[] = [];

              // Source 1: tier parquet
              subqueries.push(`
                SELECT ${objBucketExpr(objTsCol)} as timestamp, ${componentSelects}, 1 as priority
                FROM ${fc} AS source_data
                WHERE ${objTsCol} >= '${fromIso}' AND ${objTsCol} < '${toIso}' AND (${componentWhereConditions})${spatialWhereClause}
                GROUP BY timestamp`);

              // Source 2: SQLite buffer (today's live data not yet exported)
              if (hasBuffer) {
                const bufferTableCols = this.sqliteBuffer?.getTableColumns(pathSpec.path);
                const bufferSubquery = buildBufferObjectSubquery(
                  context,
                  pathSpec.path,
                  fromIso,
                  toIso,
                  componentSchema.components,
                  knownBufferPaths,
                  bufferTableCols
                );
                if (bufferSubquery) {
                  subqueries.push(`
                SELECT ${objBucketExpr('signalk_timestamp')} as timestamp, ${componentSelects}, 2 as priority
                FROM ${bufferSubquery} AS source_data
                WHERE (${componentWhereConditions})${spatialWhereClause}
                GROUP BY timestamp`);
                }
              }

              if (subqueries.length === 1) {
                return `SELECT timestamp, ${componentCols} FROM (${subqueries[0]}) ORDER BY timestamp`;
              }

              // Pick highest priority per timestamp bucket
              const compNames = Array.from(componentSchema.components.keys()).join(', ');
              return `
              SELECT timestamp, ${compNames} FROM (
                SELECT timestamp, ${compNames},
                  ROW_NUMBER() OVER (PARTITION BY timestamp ORDER BY priority DESC) as rn
                FROM (${subqueries.join('\n              UNION ALL')})
              ) WHERE rn = 1
              ORDER BY timestamp`;
            };

            const result = await runQueryWithFallback(buildObjQuery);
            const rows = result.getRowObjects();

            // Reconstruct objects from aggregated components
            const pathData: Array<[Timestamp, unknown]> = rows.map((row: any) => {
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
            });

            allData[pathSpec.path] = pathData;
          } else {
            // Scalar path
            // First, check if value_json column exists in the parquet files
            let hasValueJson = false;
            if (localFilePath) {
              try {
                const schemaQuery = `SELECT * FROM parquet_schema('${localFilePath}') WHERE name = 'value_json'`;
                const schemaResult =
                  await connection.runAndReadAll(schemaQuery);
                hasValueJson = schemaResult.getRowObjects().length > 0;
              } catch {
                hasValueJson = false;
              }
            }

            debug(
              `Path ${pathSpec.path}: value_json column ${hasValueJson ? 'exists' : 'does not exist'}`
            );

            // Each source does its own aggregation into (timestamp, value, priority).
            // UNION ALL the results, then ROW_NUMBER to pick highest-priority source per bucket.
            // Priority: buffer(3) > raw tier gap(2) > aggregated/raw tier parquet(1).
            const tsCol = getTierTimestampColumn(effectiveTier);
            const whereClause = getTierWhereClause(effectiveTier, hasValueJson);
            const bucketExpr = (col: string) =>
              `strftime(DATE_TRUNC('seconds', EPOCH_MS(CAST(FLOOR(EPOCH_MS(${col}::TIMESTAMP) / ${timeResolutionMillis}) * ${timeResolutionMillis} AS BIGINT))), '%Y-%m-%dT%H:%M:%SZ')`;

            const buildScalarQuery = (fc: string) => {
              const subqueries: string[] = [];

              // Source 1: tier parquet (uses tier-native columns and aggregate)
              const tierAggExpr = getTierAggregateExpression(pathSpec.aggregateMethod, pathSpec.path, effectiveTier, hasValueJson, app, context as string);
              subqueries.push(`
                SELECT ${bucketExpr(tsCol)} as timestamp, ${tierAggExpr} as value, 1 as priority
                FROM ${fc} AS source_data
                WHERE ${tsCol} >= '${fromIso}' AND ${tsCol} < '${toIso}' AND ${whereClause}
                GROUP BY timestamp`);

              // Source 2: SQLite buffer (today's live data not yet exported)
              if (hasBuffer) {
                const bufferSubquery = buildBufferScalarSubquery(
                  context,
                  pathSpec.path,
                  fromIso,
                  toIso,
                  knownBufferPaths
                );
                if (bufferSubquery) {
                  const bufferAggExpr = getTierAggregateExpression(pathSpec.aggregateMethod, pathSpec.path, 'raw', false, app, context as string);
                  subqueries.push(`
                SELECT ${bucketExpr('signalk_timestamp')} as timestamp, ${bufferAggExpr} as value, 2 as priority
                FROM ${bufferSubquery} AS source_data
                WHERE value IS NOT NULL
                GROUP BY timestamp`);
                }
              }

              if (subqueries.length === 1) {
                // Single source — no priority logic needed
                return `SELECT timestamp, value FROM (${subqueries[0]}) ORDER BY timestamp`;
              }

              // UNION ALL pre-aggregated results, pick highest priority per timestamp bucket
              return `
              SELECT timestamp, value FROM (
                SELECT timestamp, value,
                  ROW_NUMBER() OVER (PARTITION BY timestamp ORDER BY priority DESC) as rn
                FROM (${subqueries.join('\n              UNION ALL')})
              ) WHERE rn = 1
              ORDER BY timestamp`;
            };

            const result = await runQueryWithFallback(buildScalarQuery);
            const rows = result.getRowObjects();

            const pathData: Array<[Timestamp, unknown]> = rows.map((row: any) => {
              const rowData = row as {
                timestamp: Timestamp;
                value: unknown;
                value_json?: string;
              };
              const { timestamp } = rowData;
              const value = rowData.value_json
                ? JSON.parse(String(rowData.value_json))
                : rowData.value;
              return [timestamp, value];
            });

            allData[pathSpec.path] = pathData;
          }
        } finally {
          connection.disconnectSync();
        }
      } catch (error) {
        debug(`Error querying path ${pathSpec.path}: ${error}`);

        // Fallback: if parquet failed but buffer is available, query buffer only
        if (DuckDBPool.isSQLiteBufferInitialized()) {
          try {
            const fallbackFromIso = from.toInstant().toString();
            const fallbackToIso = to.toInstant().toString();
            const bufferConn = await DuckDBPool.getConnectionWithBuffer();
            try {
              const fallbackKnownPaths = this.sqliteBuffer ? this.sqliteBuffer.getKnownPaths() : undefined;
              const bufferSubquery = buildBufferScalarSubquery(
                context,
                pathSpec.path,
                fallbackFromIso,
                fallbackToIso,
                fallbackKnownPaths
              );
              if (bufferSubquery) {
                const bufferQuery = `
                  SELECT
                    strftime(DATE_TRUNC('seconds',
                      EPOCH_MS(CAST(FLOOR(EPOCH_MS(signalk_timestamp::TIMESTAMP) / ${timeResolutionMillis}) * ${timeResolutionMillis} AS BIGINT))
                    ), '%Y-%m-%dT%H:%M:%SZ') as timestamp,
                    AVG(TRY_CAST(value AS DOUBLE)) as value
                  FROM ${bufferSubquery} AS source_data
                  WHERE value IS NOT NULL
                  GROUP BY timestamp
                  ORDER BY timestamp
                `;
                const bufResult = await bufferConn.runAndReadAll(bufferQuery);
                const bufRows = bufResult.getRowObjects();
                allData[pathSpec.path] = bufRows.map((row: any) => [
                  row.timestamp as Timestamp,
                  row.value,
                ]);
                debug(`Buffer-only fallback: ${bufRows.length} rows for ${pathSpec.path}`);
              }
            } finally {
              bufferConn.disconnectSync();
            }
          } catch (bufErr) {
            debug(`Buffer-only fallback also failed for ${pathSpec.path}: ${bufErr}`);
            allData[pathSpec.path] = [];
          }
        } else {
          allData[pathSpec.path] = [];
        }
      }
    });

    // Apply spatial timestamp filtering to non-position paths
    // This filters data to only include times when vessel was within spatial filter
    if (spatialTimestamps !== null) {
      for (const pathSpec of pathSpecs) {
        if (!isPositionPath(pathSpec.path) && allData[pathSpec.path]) {
          if (spatialTimestamps.size === 0) {
            // No position data in area — return empty for all correlated paths
            allData[pathSpec.path] = [];
            debug(
              `[Spatial Correlation] No matching positions — cleared ${pathSpec.path}`
            );
          } else {
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
