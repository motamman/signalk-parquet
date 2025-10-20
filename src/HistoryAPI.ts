import { Router, Request, Response } from 'express';
import {
  AggregateMethod,
  DataResult,
  FromToContextRequest,
  PathSpec,
} from './HistoryAPI-types';
import { ZonedDateTime, ZoneOffset, ZoneId, LocalDateTime } from '@js-joda/core';
import { Context, Path, Timestamp } from '@signalk/server-api';
import { ParamsDictionary } from 'express-serve-static-core';
import { ParsedQs } from 'qs';
import { DuckDBInstance } from '@duckdb/node-api';
import { toContextFilePath } from '.';
import path from 'path';
import { getAvailablePathsArray } from './utils/path-discovery';

export function registerHistoryApiRoute(
  router: Pick<Router, 'get'>,
  selfId: string,
  dataDir: string,
  debug: (k: string) => void,
  app: any
) {
  const historyApi = new HistoryAPI(selfId, dataDir);
  router.get('/signalk/v1/history/values', (req: Request, res: Response) => {
    const { from, to, context, shouldRefresh } = getRequestParams(
      req as FromToContextRequest,
      selfId
    );
    const includeMovingAverages =
      req.query.includeMovingAverages === 'true' ||
      req.query.includeMovingAverages === '1';
    historyApi.getValues(context, from, to, shouldRefresh, includeMovingAverages, debug, req, res);
  });
  router.get('/signalk/v1/history/contexts', (req: Request, res: Response) => {
    //TODO implement retrieval of contexts for the given period
    res.json([`vessels.${selfId}`] as Context[]);
  });
  router.get('/signalk/v1/history/paths', (req: Request, res: Response) => {
    try {
      const paths = getAvailablePathsArray(dataDir, app);
      res.json(paths);
    } catch (error) {
      res.status(500).json({ error: (error as Error).message });
    }
  });

  // Also register as plugin-style routes for testing
  router.get('/api/history/values', (req: Request, res: Response) => {
    const { from, to, context, shouldRefresh } = getRequestParams(
      req as FromToContextRequest,
      selfId
    );
    const includeMovingAverages =
      req.query.includeMovingAverages === 'true' ||
      req.query.includeMovingAverages === '1';
    historyApi.getValues(context, from, to, shouldRefresh, includeMovingAverages, debug, req, res);
  });
  router.get('/api/history/contexts', (req: Request, res: Response) => {
    res.json([`vessels.${selfId}`] as Context[]);
  });
  router.get('/api/history/paths', (req: Request, res: Response) => {
    res.json(['navigation.speedOverGround']);
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
    // BACKWARD COMPATIBILITY: Support legacy 'start' parameter
    // ============================================================================
    if (query.start) {
      console.warn(
        '[DEPRECATED] Query parameter "start" is deprecated and will be removed in v2.0. ' +
        'Use standard SignalK time range parameters instead:\n' +
        '  - duration only: ?duration=1h (query back from now)\n' +
        '  - from + duration: ?from=2025-08-01T00:00:00Z&duration=1h (query forward)\n' +
        '  - to + duration: ?to=2025-08-01T12:00:00Z&duration=1h (query backward)\n' +
        '  - from only: ?from=2025-08-01T00:00:00Z (from start to now)\n' +
        '  - from + to: ?from=...&to=... (specific range)'
      );

      if (query.duration) {
        const durationMs = parseDuration(query.duration);

        if (query.start === 'now') {
          // Map 'start=now&duration=X' to standard pattern 1: duration only
          to = ZonedDateTime.now(ZoneOffset.UTC);
          from = to.minusNanos(durationMs * 1000000);
          shouldRefresh = query.refresh === 'true' || query.refresh === '1';
        } else {
          // Map 'start=TIME&duration=X' to standard pattern 3: to + duration
          to = parseDateTime(query.start, useUTC);
          from = to.minusNanos(durationMs * 1000000);
        }
      } else {
        throw new Error('Legacy "start" parameter requires "duration" parameter');
      }
    }
    // ============================================================================
    // STANDARD SIGNALK TIME RANGE PATTERNS
    // ============================================================================
    // Pattern 1: duration only → query back from now
    else if (query.duration && !query.from && !query.to) {
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
    }
    else {
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
    const bbox = query.bbox;
    return { from, to, context, bbox, shouldRefresh };
  } catch (e: unknown) {
    console.error('Full error details:', e);
    throw new Error(
      `Error extracting query parameters from ${JSON.stringify(query)}: ${e instanceof Error ? e.stack : e}`
    );
  }
};

// Parse duration string (e.g., "1h", "30m", "5s", "2d")
function parseDuration(duration: string | undefined): number {
  if (!duration) {
    throw new Error('Duration parameter is required');
  }

  const match = duration.match(/^(\d+)([smhd])$/);
  if (!match) {
    throw new Error(`Invalid duration format: ${duration}. Use format like "1h", "30m", "5s", "2d"`);
  }

  const value = parseInt(match[1]);
  const unit = match[2];

  switch (unit) {
    case 's': return value * 1000;        // seconds to milliseconds
    case 'm': return value * 60 * 1000;   // minutes to milliseconds
    case 'h': return value * 60 * 60 * 1000; // hours to milliseconds
    case 'd': return value * 24 * 60 * 60 * 1000; // days to milliseconds
    default: throw new Error(`Unknown duration unit: ${unit}`);
  }
}

// Check if datetime string has timezone information
function hasTimezoneInfo(dateTimeStr: string): boolean {
  // Check for 'Z' at the end, or '+'/'-' followed by timezone offset pattern
  return dateTimeStr.endsWith('Z') || 
         /[+-]\d{2}:?\d{2}$/.test(dateTimeStr) || 
         /[+-]\d{4}$/.test(dateTimeStr);
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
      return ZonedDateTime.parse(normalizedStr).withZoneSameInstant(ZoneOffset.UTC);
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
        throw new Error(`Unable to parse datetime '${dateTimeStr}': ${e}. Use format like '2025-08-13T08:00:00' or '2025-08-13T08:00:00Z'`);
      }
    }
  }
}

function getContext(contextFromQuery: string | undefined, selfId: string): Context {
  if (
    !contextFromQuery ||
    contextFromQuery === 'vessels.self' ||
    contextFromQuery === 'self'
  ) {
    return `vessels.${selfId}` as Context;
  }
  return contextFromQuery.replace(/ /gi, '') as Context;
}

export class HistoryAPI {
  readonly selfContextPath: string;
  constructor(
    private selfId: string,
    private dataDir: string
  ) {
    this.selfContextPath = toContextFilePath(`vessels.${selfId}` as Context);
  }
  async getValues(
    context: Context,
    from: ZonedDateTime,
    to: ZonedDateTime,
    shouldRefresh: boolean,
    includeMovingAverages: boolean,
    debug: (k: string) => void,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    req: Request<ParamsDictionary, any, any, ParsedQs, Record<string, any>>,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    res: Response<any, Record<string, any>>
  ) {
    try {
      const timeResolutionMillis =
        req.query.resolution
          ? Number.parseFloat(req.query.resolution as string)
          : (to.toEpochSecond() - from.toEpochSecond()) / 500 * 1000;
      const pathExpressions = ((req.query.paths as string) || '')
        .replace(/[^0-9a-z.,:_]/gi, '')
        .split(',');
      const pathSpecs: PathSpec[] = pathExpressions.map(splitPathExpression);

      // Handle position and numeric paths together
      const allResult = pathSpecs.length
        ? await this.getNumericValues(
            context,
            from,
            to,
            timeResolutionMillis,
            pathSpecs,
            includeMovingAverages,
            debug
          )
        : Promise.resolve({
            context,
            range: {
              from: from.toString() as Timestamp,
              to: to.toString() as Timestamp,
            },
            values: [],
            data: [],
          });

      // Add refresh headers if shouldRefresh is enabled
      if (shouldRefresh) {
        const refreshIntervalSeconds = Math.max(Math.round(timeResolutionMillis / 1000), 1); // At least 1 second
        res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');
        res.setHeader('Refresh', refreshIntervalSeconds.toString());
        
        // Add refresh info to response
        (allResult as any).refresh = {
          enabled: true,
          intervalSeconds: refreshIntervalSeconds,
          nextRefresh: new Date(Date.now() + refreshIntervalSeconds * 1000).toISOString()
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
    includeMovingAverages: boolean,
    debug: (k: string) => void
  ): Promise<DataResult> {
    const allData: { [path: string]: Array<[Timestamp, unknown]> } = {};

    // Process each path and collect data
    await Promise.all(
      pathSpecs.map(async pathSpec => {
        try {
          // Sanitize the path to prevent directory traversal and SQL injection
          const sanitizedPath = pathSpec.path
            .replace(/[^a-zA-Z0-9._]/g, '') // Only allow alphanumeric, dots, underscores
            .replace(/\./g, '/');

          const filePath = path.join(
            this.dataDir,
            this.selfContextPath,
            sanitizedPath,
            '*.parquet'
          );

          debug(`Querying parquet files at: ${filePath}`);

          // Convert ZonedDateTime to ISO string format matching parquet schema
          const fromIso = from.toInstant().toString();
          const toIso = to.toInstant().toString();


          const duckDB = await DuckDBInstance.create();
          const connection = await duckDB.connect();

          // Load spatial extension for geographic queries
          await connection.runAndReadAll("INSTALL spatial;");
          await connection.runAndReadAll("LOAD spatial;");

          try {
            // First, check if value_json column exists in the parquet files
            const schemaQuery = `SELECT * FROM parquet_schema('${filePath}') WHERE name = 'value_json'`;
            const schemaResult = await connection.runAndReadAll(schemaQuery);
            const hasValueJson = schemaResult.getRowObjects().length > 0;

            debug(`Path ${pathSpec.path}: value_json column ${hasValueJson ? 'exists' : 'does not exist'}`);

            // Rebuild the query based on actual column availability
            const valueJsonSelect = hasValueJson ? ', FIRST(value_json) as value_json' : '';
            const whereClause = hasValueJson
              ? '(value IS NOT NULL OR value_json IS NOT NULL)'
              : 'value IS NOT NULL';

            const dynamicQuery = `
            SELECT
              strftime(DATE_TRUNC('seconds',
                EPOCH_MS(CAST(FLOOR(EPOCH_MS(signalk_timestamp::TIMESTAMP) / ${timeResolutionMillis}) * ${timeResolutionMillis} AS BIGINT))
              ), '%Y-%m-%dT%H:%M:%SZ') as timestamp,
              ${getAggregateExpression(pathSpec.aggregateMethod, pathSpec.path, hasValueJson)} as value${valueJsonSelect}
            FROM read_parquet('${filePath}', union_by_name=true)
            WHERE
              signalk_timestamp >= '${fromIso}'
              AND
              signalk_timestamp < '${toIso}'
              AND ${whereClause}
            GROUP BY timestamp
            ORDER BY timestamp
            `;

            const result = await connection.runAndReadAll(dynamicQuery);
            const rows = result.getRowObjects();

            // Convert rows to the expected format using bucketed timestamps
            const pathData: Array<[Timestamp, unknown]> = rows.map(
              (row) => {
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

                // For position paths, ensure we return the full position object
                if (
                  pathSpec.path === 'navigation.position' &&
                  value &&
                  typeof value === 'object'
                ) {
                  // Position data is already an object with latitude/longitude
                  // No reassignment needed, keeping original value
                }

                return [timestamp, value];
              }
            );

            allData[pathSpec.path] = pathData;
          } finally {
            connection.disconnectSync();
          }
        } catch (error) {
          console.error(`[HistoryAPI] Error querying path ${pathSpec.path}:`, error);
          debug(`Error querying path ${pathSpec.path}: ${error}`);
          allData[pathSpec.path] = [];
        }
      })
    );

    // Merge all path data into time-ordered rows
    const mergedData = this.mergePathData(allData, pathSpecs);

    // Conditionally add EMA and SMA calculations based on includeMovingAverages parameter
    const finalData = includeMovingAverages
      ? this.addMovingAverages(mergedData, pathSpecs)
      : mergedData;

    const finalValues = includeMovingAverages
      ? this.buildValuesWithMovingAverages(pathSpecs)
      : pathSpecs.map(({ path, aggregateMethod }) => ({ path, method: aggregateMethod }));

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
          timestampMap.set(
            timestamp,
            new Array(pathSpecs.length).fill(null)
          );
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
    pathSpecs: PathSpec[]
  ): Array<[Timestamp, ...unknown[]]> {
    if (data.length === 0) return data;

    const smaPeriod = 10;
    const emaAlpha = 0.2;
    
    // For each column, track EMA and SMA state
    const columnEMAs: (number | null)[] = new Array(pathSpecs.length).fill(null);
    const columnSMAWindows: number[][] = pathSpecs.map(() => []);

    return data.map((row, rowIndex) => {
      const [timestamp, ...values] = row;
      const enhancedValues: unknown[] = [];

      values.forEach((value, colIndex) => {
        enhancedValues.push(value);

        // Calculate EMA and SMA for numeric values only
        if (typeof value === 'number' && !isNaN(value)) {
          // Calculate EMA
          if (columnEMAs[colIndex] === null) {
            columnEMAs[colIndex] = value; // First value
          } else {
            columnEMAs[colIndex] = emaAlpha * value + (1 - emaAlpha) * columnEMAs[colIndex]!;
          }

          // Calculate SMA
          columnSMAWindows[colIndex].push(value);
          if (columnSMAWindows[colIndex].length > smaPeriod) {
            columnSMAWindows[colIndex] = columnSMAWindows[colIndex].slice(-smaPeriod);
          }
          const sma = columnSMAWindows[colIndex].reduce((sum, val) => sum + val, 0) / columnSMAWindows[colIndex].length;

          // Add EMA and SMA as additional values (rounded to 3 decimal places)
          enhancedValues.push(Math.round(columnEMAs[colIndex]! * 1000) / 1000); // EMA
          enhancedValues.push(Math.round(sma * 1000) / 1000); // SMA
        } else {
          // Non-numeric values get null for EMA/SMA
          enhancedValues.push(null); // EMA
          enhancedValues.push(null); // SMA
        }
      });

      return [timestamp, ...enhancedValues] as [Timestamp, ...unknown[]];
    });
  }

  private buildValuesWithMovingAverages(pathSpecs: PathSpec[]): Array<{path: Path; method: AggregateMethod}> {
    const result: Array<{path: Path; method: AggregateMethod}> = [];
    
    pathSpecs.forEach(({ path, aggregateMethod }) => {
      // Add original path
      result.push({ path, method: aggregateMethod });
      
      // Add EMA and SMA paths for this column
      result.push({ path: `${path}.ema` as Path, method: 'ema' as AggregateMethod });
      result.push({ path: `${path}.sma` as Path, method: 'sma' as AggregateMethod });
    });
    
    return result;
  }
}

function splitPathExpression(pathExpression: string): PathSpec {
  const parts = pathExpression.split(':');
  let aggregateMethod = (parts[1] || 'average') as AggregateMethod;
  
  // Auto-select appropriate default method for complex data types
  if (parts[0] === 'navigation.position' && !parts[1]) {
    aggregateMethod = 'first' as AggregateMethod;
  }
  
  // Validate the aggregation method
  const validMethods = ['average', 'min', 'max', 'first', 'last', 'mid', 'middle_index'];
  if (parts[1] && !validMethods.includes(parts[1])) {
    aggregateMethod = 'average' as AggregateMethod;
  }
  
  return {
    path: parts[0] as Path,
    queryResultName: parts[0].replace(/\./g, '_'),
    aggregateMethod,
    aggregateFunction:
      (functionForAggregate[aggregateMethod] as string) || 'avg',
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

function getAggregateExpression(method: AggregateMethod, pathName: string, hasValueJson: boolean): string {
  const valueExpr = getValueExpression(pathName, hasValueJson);

  if (method === 'middle_index') {
    // For middle_index, use FIRST as a simple fallback for now
    // TODO: Implement proper middle index selection
    return `FIRST(${valueExpr})`;
  }

  return `${getAggregateFunction(method)}(${valueExpr})`;
}
