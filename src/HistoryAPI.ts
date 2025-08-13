import { Router, Request, Response } from 'express';
import {
  AggregateMethod,
  DataResult,
  FromToContextRequest,
  PathSpec,
} from './HistoryAPI-types';
import { ZonedDateTime, ZoneOffset } from '@js-joda/core';
import { Context, Path, Timestamp } from '@signalk/server-api';
import { ParamsDictionary } from 'express-serve-static-core';
import { ParsedQs } from 'qs';
import { DuckDBInstance } from '@duckdb/node-api';
import { toContextFilePath } from '.';
import path from 'path';

export function registerHistoryApiRoute(
  router: Pick<Router, 'get'>,
  selfId: string,
  dataDir: string,
  debug: (k: string) => void
) {
  const historyApi = new HistoryAPI(selfId, dataDir);
  router.get('/signalk/v1/history/values', (req: Request, res: Response) => {
    const { from, to, context, shouldRefresh } = getRequestParams(
      req as FromToContextRequest,
      selfId
    );
    historyApi.getValues(context, from, to, shouldRefresh, debug, req, res);
  });
  router.get('/signalk/v1/history/contexts', (req: Request, res: Response) => {
    //TODO implement retrieval of contexts for the given period
    res.json([`vessels.${selfId}`] as Context[]);
  });
  router.get('/signalk/v1/history/paths', (req: Request, res: Response) => {
    //TODO implement retrieval of paths for the given period
    // const { from, to } = getRequestParams(req as FromToContextRequest, selfId);
    // getPaths(influx, from, to, res);
    res.json(['navigation.speedOverGround']);
  });

  // Also register as plugin-style routes for testing
  router.get('/api/history/values', (req: Request, res: Response) => {
    const { from, to, context, shouldRefresh } = getRequestParams(
      req as FromToContextRequest,
      selfId
    );
    historyApi.getValues(context, from, to, shouldRefresh, debug, req, res);
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

    // Handle new backwards querying with start + duration
    if (query.start && query.duration) {
      const durationMs = parseDuration(query.duration);
      
      if (query.start === 'now') {
        // Use current UTC time as start and go backwards
        to = ZonedDateTime.now(ZoneOffset.UTC);
        from = to.minusNanos(durationMs * 1000000); // Convert ms to nanoseconds
        shouldRefresh = query.refresh === 'true' || query.refresh === '1';
      } else {
        // Use specified start time and go backwards
        to = ZonedDateTime.parse(query.start);
        from = to.minusNanos(durationMs * 1000000);
      }
    } else if (query.from && query.to) {
      // Traditional from/to querying (forward in time)
      from = ZonedDateTime.parse(query.from);
      to = ZonedDateTime.parse(query.to);
    } else {
      throw new Error('Either (from + to) or (start + duration) parameters are required');
    }

    const context: Context = getContext(query.context, selfId);
    const bbox = query.bbox;
    return { from, to, context, bbox, shouldRefresh };
  } catch (e: unknown) {
    throw new Error(
      `Error extracting query parameters from ${JSON.stringify(query)}: ${e}`
    );
  }
};

// Parse duration string (e.g., "1h", "30m", "5s", "2d")
function parseDuration(duration: string): number {
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

function getContext(contextFromQuery: string, selfId: string): Context {
  if (
    !contextFromQuery ||
    contextFromQuery === 'vessels.self' ||
    contextFromQuery === 'self'
  ) {
    return `vessels.${selfId}` as Context;
  }
  return contextFromQuery.replace(/ /gi, '') as Context;
}

class HistoryAPI {
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
    debug: (k: string) => void,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    req: Request<ParamsDictionary, any, any, ParsedQs, Record<string, any>>,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    res: Response<any, Record<string, any>>
  ) {
    try {
      const timeResolutionMillis =
        (req.query.resolution
          ? Number.parseFloat(req.query.resolution as string)
          : (to.toEpochSecond() - from.toEpochSecond()) / 500) * 1000;
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
        
        debug(`Refresh enabled: resolution=${timeResolutionMillis}ms, interval=${refreshIntervalSeconds}s`);
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

          debug(`Looking for data files at: ${filePath}`);
          debug(`Context: ${context}, SelfContextPath: ${this.selfContextPath}`);

          // Convert ZonedDateTime to ISO string format matching parquet schema
          const fromIso = from.toInstant().toString();
          const toIso = to.toInstant().toString();
          
          debug(`Time range: ${fromIso} to ${toIso}`);

          // Build query with time bucketing - fix type casting
          const query = `
          SELECT
            DATE_TRUNC('seconds', 
              EPOCH_MS(CAST(FLOOR(EPOCH_MS(signalk_timestamp::TIMESTAMP) / ${timeResolutionMillis}) * ${timeResolutionMillis} AS BIGINT))
            ) as time_bucket,
            ${getAggregateExpression(pathSpec.aggregateMethod, pathSpec.path)} as value,
            FIRST(value_json) as value_json
          FROM '${filePath}'
          WHERE
            signalk_timestamp >= '${fromIso}'
            AND 
            signalk_timestamp < '${toIso}'
            AND (value IS NOT NULL OR value_json IS NOT NULL)
          GROUP BY time_bucket
          ORDER BY time_bucket
          `;

          debug(`Executing query for path ${pathSpec.path}: ${query}`);
          const duckDB = await DuckDBInstance.create();
          const connection = await duckDB.connect();

          try {
            const result = await connection.runAndReadAll(query);
            const rows = result.getRowObjects();

            // Convert rows to the expected format using bucketed timestamps
            const pathData: Array<[Timestamp, unknown]> = rows.map(
              (row: unknown) => {
                const rowData = row as {
                  time_bucket: Timestamp;
                  value: unknown;
                  value_json?: string;
                };
                const timestamp = rowData.time_bucket;
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
            debug(
              `Retrieved ${pathData.length} data points for ${pathSpec.path}`
            );
          } finally {
            connection.disconnectSync();
          }
        } catch (error) {
          debug(`Error querying path ${pathSpec.path}: ${error}`);
          allData[pathSpec.path] = [];
        }
      })
    );

    // Merge all path data into time-ordered rows
    const mergedData = this.mergePathData(allData, pathSpecs);

    return {
      context,
      range: {
        from: from.toString() as Timestamp,
        to: to.toString() as Timestamp,
      },
      values: pathSpecs.map(({ path, aggregateMethod }: PathSpec) => ({
        path,
        method: aggregateMethod,
      })),
      data: mergedData,
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
        const timestampStr = timestamp.toString();
        if (!timestampMap.has(timestampStr)) {
          timestampMap.set(
            timestampStr,
            new Array(pathSpecs.length).fill(null)
          );
        }
        timestampMap.get(timestampStr)![index] = value;
      });
    });

    // Convert to sorted array format
    return Array.from(timestampMap.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([timestamp, values]) => [timestamp as Timestamp, ...values]);
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

function getValueExpression(pathName: string): string {
  // For position data, use value_json since the value is an object
  if (pathName === 'navigation.position') {
    return 'value_json';
  }
  
  // For numeric data, try to cast to DOUBLE, fallback to the original value
  return 'TRY_CAST(value AS DOUBLE)';
}

function getAggregateExpression(method: AggregateMethod, pathName: string): string {
  const valueExpr = getValueExpression(pathName);
  
  if (method === 'middle_index') {
    // For middle_index, use FIRST as a simple fallback for now
    // TODO: Implement proper middle index selection
    return `FIRST(${valueExpr})`;
  }
  
  return `${getAggregateFunction(method)}(${valueExpr})`;
}
