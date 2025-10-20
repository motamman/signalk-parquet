import { Router, Request, Response } from 'express';
import {
  AggregateMethod,
  DataResult,
  FromToContextRequest,
  PathSpec,
  ConversionMetadata,
  UnitConversionInfo,
} from './HistoryAPI-types';
import { ZonedDateTime, ZoneOffset, ZoneId } from '@js-joda/core';
import { Context, Path, Timestamp } from '@signalk/server-api';
import { ParamsDictionary } from 'express-serve-static-core';
import { ParsedQs } from 'qs';
import { DuckDBInstance } from '@duckdb/node-api';
import { toContextFilePath } from '.';
import path from 'path';
import { getAvailablePathsArray, getAvailablePathsForTimeRange } from './utils/path-discovery';
import { getCachedPaths, setCachedPaths, getCachedContexts, setCachedContexts } from './utils/path-cache';
import { getAvailableContextsForTimeRange } from './utils/context-discovery';
import { getPathComponentSchema, PathComponentSchema, ComponentInfo } from './utils/schema-cache';

// ============================================================================
// Unit Conversion Helper Functions
// ============================================================================

// Cache for all paths conversion metadata - loaded once when available
let allPathsConversions: Map<string, ConversionMetadata> | null = null;
let conversionsLoadedAt: number = 0;
let hasLoggedUnavailable = false; // Track if we've already logged the unavailable message

// Cache TTL in milliseconds - configurable, defaults to 5 minutes
let CONVERSIONS_CACHE_TTL_MS = 5 * 60 * 1000;

/**
 * Load all paths conversion metadata from units-preference plugin
 * This is called lazily and will retry until successful or permanently unavailable
 * Cache expires after 5 minutes to allow unit preference changes to take effect
 */
async function loadAllPathsConversions(app: any): Promise<void> {
  const now = Date.now();

  // Reload if cache is older than TTL
  if (allPathsConversions !== null && (now - conversionsLoadedAt) > CONVERSIONS_CACHE_TTL_MS) {
    console.log('[Unit Conversion] Cache expired, reloading conversions...');
    allPathsConversions = null;
    hasLoggedUnavailable = false; // Reset logging flag
  }

  // If already successfully loaded and fresh, don't reload
  if (allPathsConversions !== null && allPathsConversions.size > 0) {
    return;
  }

  try {
    // Debug: Log what we see on the app object
    console.log('[Unit Conversion] DEBUG: Checking app object...');
    console.log('[Unit Conversion] DEBUG: typeof app.getAllUnitsConversions =', typeof app.getAllUnitsConversions);
    console.log('[Unit Conversion] DEBUG: typeof app.getUnitsConversion =', typeof app.getUnitsConversion);

    // Try to find the functions in multiple locations (different plugins may receive different app instances)
    let getAllUnitsConversions = app.getAllUnitsConversions;

    // Check server instance
    if (!getAllUnitsConversions) {
      const serverInstance = (app as any).server || (app as any)._server;
      if (serverInstance) {
        console.log('[Unit Conversion] DEBUG: Checking server instance...');
        getAllUnitsConversions = serverInstance.getAllUnitsConversions;
        console.log('[Unit Conversion] DEBUG: server.getAllUnitsConversions =', typeof getAllUnitsConversions);
      }
    }

    // Check global
    if (!getAllUnitsConversions && (global as any).signalkApp) {
      console.log('[Unit Conversion] DEBUG: Checking global.signalkApp...');
      getAllUnitsConversions = (global as any).signalkApp.getAllUnitsConversions;
      console.log('[Unit Conversion] DEBUG: global.signalkApp.getAllUnitsConversions =', typeof getAllUnitsConversions);
    }

    // Check if the units-preference plugin has exposed its conversion functions
    if (typeof getAllUnitsConversions === 'function') {
      console.log('[Unit Conversion] ✅ Found units-preference plugin, loading conversions...');

      const pathsData = await getAllUnitsConversions() as Record<string, any>;
      console.log(`[Unit Conversion] Successfully loaded ${Object.keys(pathsData).length} paths via direct call`);

      // Convert to our ConversionMetadata format
      allPathsConversions = new Map();

      for (const [pathName, pathInfo] of Object.entries(pathsData)) {
        const info = pathInfo;

        // Find the user's preferred target unit from the categories endpoint
        // For now, we'll just use the first available conversion
        const conversions = info.conversions || {};
        const firstConversionKey = Object.keys(conversions)[0];

        if (firstConversionKey) {
          const conversion = conversions[firstConversionKey];
          allPathsConversions.set(pathName, {
            path: pathName,
            baseUnit: info.baseUnit,
            targetUnit: firstConversionKey,
            formula: conversion.formula,
            inverseFormula: conversion.inverseFormula,
            symbol: conversion.symbol,
            displayFormat: '0.0', // Default format
            category: info.category,
            valueType: 'number',
          });
        }
      }

      console.log(`[Unit Conversion] ✅ Successfully initialized ${allPathsConversions.size} conversions`);

      // If we got 0 conversions, the units-preference plugin may not be fully initialized yet
      // Reset to null so we retry on the next request
      if (allPathsConversions.size === 0) {
        console.log('[Unit Conversion] No conversions loaded - units-preference may still be initializing. Will retry on next request.');
        allPathsConversions = null;
      } else {
        // Update the cache timestamp
        conversionsLoadedAt = Date.now();
      }
    } else {
      // Only log once to avoid spamming the logs
      if (!hasLoggedUnavailable) {
        console.log('[Unit Conversion] Units preference plugin not yet available (getAllUnitsConversions function not found)');
        console.log('[Unit Conversion] Will retry on next request. Make sure signalk-units-preference plugin is installed.');
        hasLoggedUnavailable = true;
      }
    }
  } catch (error) {
    console.error('[Unit Conversion] Error loading paths conversions:', error);
  }
}

/**
 * Check if the signalk-units-preference plugin is available
 */
async function isUnitsPreferencePluginAvailable(app: any): Promise<boolean> {
  await loadAllPathsConversions(app);
  return allPathsConversions !== null && allPathsConversions.size > 0;
}

/**
 * Fetch conversion metadata for a specific path from the cached conversions
 */
async function getConversionMetadata(
  signalkPath: string,
  app: any
): Promise<ConversionMetadata | null> {
  // Ensure conversions are loaded
  await loadAllPathsConversions(app);

  if (!allPathsConversions) {
    return null;
  }

  // Look up the path in our cached conversions
  const conversion = allPathsConversions.get(signalkPath);

  if (conversion) {
    console.log(`[Unit Conversion] Found cached conversion for ${signalkPath}: ${conversion.baseUnit} → ${conversion.targetUnit}`);
  }

  return conversion || null;
}

/**
 * Apply conversion formula to a numeric value
 * Uses a safe eval approach similar to the units-preference plugin
 */
function applyConversionFormula(value: number, formula: string): number {
  try {
    // Simple formula evaluation - replace 'value' with the actual value
    // This is safe because the formula comes from the trusted units-preference plugin
    const result = eval(formula.replace(/value/g, String(value)));
    return typeof result === 'number' ? result : value;
  } catch (error) {
    console.error(`Error applying conversion formula "${formula}" to value ${value}:`, error);
    return value;
  }
}

/**
 * Format a number according to the display format (e.g., "0.0", "0.00", "0")
 */
function formatNumber(value: number, displayFormat: string): string {
  if (displayFormat === '0') {
    return Math.round(value).toString();
  }

  const decimals = displayFormat.includes('.')
    ? displayFormat.split('.')[1].length
    : 0;

  return value.toFixed(decimals);
}

/**
 * Convert a single numeric value using conversion metadata
 */
function convertNumericValue(
  value: number,
  metadata: ConversionMetadata
): { converted: number; formatted: string } {
  const converted = applyConversionFormula(value, metadata.formula);
  const formatted = formatNumber(converted, metadata.displayFormat);

  return { converted, formatted };
}

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
      console.error(`[Timestamp Conversion] Invalid timezone '${timezoneParam}', falling back to system default`);
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
function convertTimestampToTimezone(utcTimestamp: Timestamp, targetZone: ZoneId): Timestamp {
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
    console.error(`[Timestamp Conversion] Error converting timestamp ${utcTimestamp}:`, error);
    return utcTimestamp; // Return original on error
  }
}

export function registerHistoryApiRoute(
  router: Pick<Router, 'get'>,
  selfId: string,
  dataDir: string,
  debug: (k: string) => void,
  app: any,
  unitConversionCacheMinutes: number = 5
) {
  // Set the cache TTL from configuration
  CONVERSIONS_CACHE_TTL_MS = unitConversionCacheMinutes * 60 * 1000;
  console.log(`[Unit Conversion] Cache TTL set to ${unitConversionCacheMinutes} minutes`);
  const historyApi = new HistoryAPI(selfId, dataDir);
  router.get('/signalk/v1/history/values', (req: Request, res: Response) => {
    const { from, to, context, shouldRefresh } = getRequestParams(
      req as FromToContextRequest,
      selfId
    );
    const includeMovingAverages =
      req.query.includeMovingAverages === 'true' ||
      req.query.includeMovingAverages === '1';
    const convertUnits =
      req.query.convertUnits === 'true' ||
      req.query.convertUnits === '1';
    const convertTimesToLocal =
      req.query.convertTimesToLocal === 'true' ||
      req.query.convertTimesToLocal === '1';
    const timezone = req.query.timezone as string | undefined;
    historyApi.getValues(context, from, to, shouldRefresh, includeMovingAverages, convertUnits, convertTimesToLocal, timezone, app, debug, req, res);
  });
  router.get('/signalk/v1/history/contexts', async (req: Request, res: Response) => {
    try {
      // Check if time range parameters are provided
      const hasTimeParams = req.query.duration || req.query.from || req.query.to || req.query.start;

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
  });
  router.get('/signalk/v1/history/paths', async (req: Request, res: Response) => {
    try {
      // Check if time range parameters are provided
      const hasTimeParams = req.query.duration || req.query.from || req.query.to || req.query.start;

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
          paths = await getAvailablePathsForTimeRange(dataDir, context, from, to);
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
    const convertUnits =
      req.query.convertUnits === 'true' ||
      req.query.convertUnits === '1';
    const convertTimesToLocal =
      req.query.convertTimesToLocal === 'true' ||
      req.query.convertTimesToLocal === '1';
    const timezone = req.query.timezone as string | undefined;
    historyApi.getValues(context, from, to, shouldRefresh, includeMovingAverages, convertUnits, convertTimesToLocal, timezone, app, debug, req, res);
  });
  router.get('/api/history/contexts', async (req: Request, res: Response) => {
    try {
      // Check if time range parameters are provided
      const hasTimeParams = req.query.duration || req.query.from || req.query.to || req.query.start;

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
      const hasTimeParams = req.query.duration || req.query.from || req.query.to || req.query.start;

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
          paths = await getAvailablePathsForTimeRange(dataDir, context, from, to);
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
    convertUnits: boolean,
    convertTimesToLocal: boolean,
    timezone: string | undefined,
    app: any,
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
      let allResult = pathSpecs.length
        ? await this.getNumericValues(
            context,
            from,
            to,
            timeResolutionMillis,
            pathSpecs,
            includeMovingAverages,
            debug
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

      // Apply unit conversions if requested
      if (convertUnits) {
        allResult = await this.applyUnitConversions(allResult, pathSpecs, app, debug);
      }

      // Apply timestamp conversions if requested
      if (convertTimesToLocal) {
        allResult = this.convertTimestamps(allResult, timezone, debug);
      }

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
    const objectPaths = new Set<string>(); // Track which paths are object paths

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
            // Check if this path has object components (value_latitude, value_longitude, etc.)
            const componentSchema = await getPathComponentSchema(this.dataDir, context, pathSpec.path);

            if (componentSchema && componentSchema.components.size > 0) {
              // Object path with multiple components - aggregate each component separately
              debug(`Path ${pathSpec.path}: Object path with ${componentSchema.components.size} components`);
              objectPaths.add(pathSpec.path); // Mark as object path

              // Build SELECT clause with one aggregate per component
              const componentSelects = Array.from(componentSchema.components.values()).map(comp => {
                const aggFunc = getComponentAggregateFunction(pathSpec.aggregateMethod, comp.dataType);
                return `${aggFunc}(${comp.columnName}) as ${comp.name}`;
              }).join(',\n              ');

              // Build WHERE clause to check for at least one non-null component
              const componentWhereConditions = Array.from(componentSchema.components.values())
                .map(comp => `${comp.columnName} IS NOT NULL`)
                .join(' OR ');

              const dynamicQuery = `
              SELECT
                strftime(DATE_TRUNC('seconds',
                  EPOCH_MS(CAST(FLOOR(EPOCH_MS(signalk_timestamp::TIMESTAMP) / ${timeResolutionMillis}) * ${timeResolutionMillis} AS BIGINT))
                ), '%Y-%m-%dT%H:%M:%SZ') as timestamp,
                ${componentSelects}
              FROM read_parquet('${filePath}', union_by_name=true)
              WHERE
                signalk_timestamp >= '${fromIso}'
                AND
                signalk_timestamp < '${toIso}'
                AND (${componentWhereConditions})
              GROUP BY timestamp
              ORDER BY timestamp
              `;

              const result = await connection.runAndReadAll(dynamicQuery);
              const rows = result.getRowObjects();

              // Reconstruct objects from aggregated components
              const pathData: Array<[Timestamp, unknown]> = rows.map(row => {
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
              // Scalar path - use original logic
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

                  return [timestamp, value];
                }
              );

              allData[pathSpec.path] = pathData;
            }
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
      ? this.buildValuesWithMovingAverages(pathSpecs, objectPaths)
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
        // Check if this is an object value (like navigation.position)
        if (value && typeof value === 'object' && !Array.isArray(value)) {
          // Object with components - calculate EMA/SMA for each numeric component
          const enhancedObject: any = { ...value };
          const colState = columnStates.get(colIndex)!;

          Object.entries(value).forEach(([componentName, componentValue]) => {
            if (typeof componentValue === 'number' && !isNaN(componentValue)) {
              // Get or create state for this component
              if (!colState.has(componentName)) {
                colState.set(componentName, { ema: null, smaWindow: [] });
              }
              const componentState = colState.get(componentName)!;

              // Calculate EMA
              if (componentState.ema === null) {
                componentState.ema = componentValue;
              } else {
                componentState.ema = emaAlpha * componentValue + (1 - emaAlpha) * componentState.ema;
              }

              // Calculate SMA
              componentState.smaWindow.push(componentValue);
              if (componentState.smaWindow.length > smaPeriod) {
                componentState.smaWindow = componentState.smaWindow.slice(-smaPeriod);
              }
              const sma = componentState.smaWindow.reduce((sum, val) => sum + val, 0) / componentState.smaWindow.length;

              // Add EMA and SMA to the object with _ema and _sma suffixes
              enhancedObject[`${componentName}_ema`] = Math.round(componentState.ema * 1000) / 1000;
              enhancedObject[`${componentName}_sma`] = Math.round(sma * 1000) / 1000;
            }
            // Non-numeric components don't get EMA/SMA
          });

          enhancedValues.push(enhancedObject);

        } else if (typeof value === 'number' && !isNaN(value)) {
          // Scalar numeric value - use simple column-based tracking
          enhancedValues.push(value);

          const colState = columnStates.get(colIndex)!;
          const scalarKey = '__scalar__';

          if (!colState.has(scalarKey)) {
            colState.set(scalarKey, { ema: null, smaWindow: [] });
          }
          const componentState = colState.get(scalarKey)!;

          // Calculate EMA
          if (componentState.ema === null) {
            componentState.ema = value;
          } else {
            componentState.ema = emaAlpha * value + (1 - emaAlpha) * componentState.ema;
          }

          // Calculate SMA
          componentState.smaWindow.push(value);
          if (componentState.smaWindow.length > smaPeriod) {
            componentState.smaWindow = componentState.smaWindow.slice(-smaPeriod);
          }
          const sma = componentState.smaWindow.reduce((sum, val) => sum + val, 0) / componentState.smaWindow.length;

          // Add EMA and SMA as additional values
          enhancedValues.push(Math.round(componentState.ema * 1000) / 1000); // EMA
          enhancedValues.push(Math.round(sma * 1000) / 1000); // SMA

        } else {
          // Non-numeric, non-object values (null, string, etc.)
          enhancedValues.push(value);
          enhancedValues.push(null); // EMA
          enhancedValues.push(null); // SMA
        }
      });

      return [timestamp, ...enhancedValues] as [Timestamp, ...unknown[]];
    });
  }

  private buildValuesWithMovingAverages(
    pathSpecs: PathSpec[],
    objectPaths: Set<string>
  ): Array<{path: Path; method: AggregateMethod}> {
    const result: Array<{path: Path; method: AggregateMethod}> = [];

    pathSpecs.forEach(({ path, aggregateMethod }) => {
      if (objectPaths.has(path)) {
        // Object path - EMA/SMA are embedded in the object as component properties
        // Just add the single path entry
        result.push({ path, method: aggregateMethod });
      } else {
        // Scalar path - add separate entries for value, EMA, and SMA
        result.push({ path, method: aggregateMethod });
        result.push({ path: `${path}.ema` as Path, method: 'ema' as AggregateMethod });
        result.push({ path: `${path}.sma` as Path, method: 'sma' as AggregateMethod });
      }
    });

    return result;
  }

  /**
   * Apply unit conversions to the data result
   */
  private async applyUnitConversions(
    result: DataResult,
    pathSpecs: PathSpec[],
    app: any,
    debug: (k: string) => void
  ): Promise<DataResult> {
    try {
      debug('[Unit Conversion] Starting unit conversion process');

      // Check if the units-preference plugin is available
      const pluginAvailable = await isUnitsPreferencePluginAvailable(app);
      debug(`[Unit Conversion] Plugin available: ${pluginAvailable}`);

      if (!pluginAvailable) {
        console.log('[Unit Conversion] Units preference plugin not available, skipping conversions');
        debug('Units preference plugin not available, skipping conversions');
        return result;
      }

      debug('[Unit Conversion] Applying unit conversions to history data');
      console.log(`[Unit Conversion] Processing ${pathSpecs.length} paths for conversion`);

      // Fetch conversion metadata for all paths
      const conversions: Map<string, ConversionMetadata> = new Map();
      await Promise.all(
        pathSpecs.map(async (pathSpec) => {
          debug(`[Unit Conversion] Fetching metadata for ${pathSpec.path}`);
          const metadata = await getConversionMetadata(pathSpec.path, app);

          if (metadata) {
            debug(`[Unit Conversion] Metadata received for ${pathSpec.path}: type=${metadata.valueType}`);
            console.log(`[Unit Conversion] Metadata for ${pathSpec.path}:`, JSON.stringify(metadata, null, 2));

            if (metadata.valueType === 'number') {
              conversions.set(pathSpec.path, metadata);
              debug(`[Unit Conversion] Conversion available for ${pathSpec.path}: ${metadata.baseUnit} → ${metadata.targetUnit}`);
            } else {
              debug(`[Unit Conversion] Skipping ${pathSpec.path}: not a numeric type (${metadata.valueType})`);
            }
          } else {
            debug(`[Unit Conversion] No metadata available for ${pathSpec.path}`);
            console.log(`[Unit Conversion] No metadata returned for ${pathSpec.path}`);
          }
        })
      );

      // If no conversions available, return original result
      if (conversions.size === 0) {
        console.log('[Unit Conversion] No unit conversions available for any requested paths');
        debug('No unit conversions available for any requested paths');
        return result;
      }

      console.log(`[Unit Conversion] Successfully loaded ${conversions.size} conversions`);

      // Apply conversions to data array
      const convertedData = result.data.map((row) => {
        const [timestamp, ...values] = row;
        const convertedValues = values.map((value, index) => {
          // Get the path for this column index
          const pathSpec = pathSpecs[Math.floor(index / (result.values.length / pathSpecs.length))];
          const metadata = conversions.get(pathSpec.path);

          // Only convert numeric values
          if (metadata && typeof value === 'number' && !isNaN(value)) {
            const { converted } = convertNumericValue(value, metadata);
            return converted;
          }

          // Return non-numeric values unchanged
          return value;
        });

        return [timestamp, ...convertedValues] as [Timestamp, ...unknown[]];
      });

      // Update values metadata to include unit information
      const updatedValues = result.values.map((valueSpec) => {
        const pathWithoutSuffix = valueSpec.path.replace(/\.(ema|sma)$/, '') as Path;
        const metadata = conversions.get(pathWithoutSuffix);

        if (metadata) {
          return {
            ...valueSpec,
            unit: metadata.symbol,
            displayFormat: metadata.displayFormat,
          };
        }

        return valueSpec;
      });

      const convertedResult = {
        ...result,
        data: convertedData,
        values: updatedValues,
        units: {
          converted: true,
          conversions: Array.from(conversions.entries()).map(([path, metadata]) => ({
            path: path as Path,
            baseUnit: metadata.baseUnit,
            targetUnit: metadata.targetUnit,
            symbol: metadata.symbol,
          })),
        },
      };

      console.log(`[Unit Conversion] Successfully converted ${convertedData.length} data rows`);
      debug(`[Unit Conversion] Conversion complete with ${conversions.size} conversions applied`);

      return convertedResult;
    } catch (error) {
      console.error('[Unit Conversion] Error applying unit conversions:', error);
      debug(`Error applying unit conversions: ${error}`);
      // Return original result if conversion fails
      return result;
    }
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

      debug(`[Timestamp Conversion] Converting timestamps to timezone: ${targetZoneName}`);
      console.log(`[Timestamp Conversion] Target timezone: ${targetZoneName}`);
      console.log(`[Timestamp Conversion] Current UTC time: ${now.toString()}`);
      console.log(`[Timestamp Conversion] Current local time: ${nowInTarget.toOffsetDateTime().toString()} (offset: ${offset})`);
      console.log(`[Timestamp Conversion] Converting ${result.data.length} rows`);

      // Convert all timestamps in the data array
      const convertedData = result.data.map((row) => {
        const [timestamp, ...values] = row;
        const convertedTimestamp = convertTimestampToTimezone(timestamp, targetZone);
        return [convertedTimestamp, ...values] as [Timestamp, ...unknown[]];
      });

      // Also convert the range timestamps
      const convertedRange = {
        from: convertTimestampToTimezone(result.range.from, targetZone),
        to: convertTimestampToTimezone(result.range.to, targetZone),
      };

      console.log(`[Timestamp Conversion] ✅ Successfully converted timestamps to ${targetZoneName}`);

      // Get a sample timestamp to show the conversion
      const sampleOriginal = result.data.length > 0 ? result.data[0][0] : result.range.from;
      const sampleConverted = convertedData.length > 0 ? convertedData[0][0] : convertedRange.from;
      console.log(`[Timestamp Conversion] Example: ${sampleOriginal} → ${sampleConverted}`);

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
      console.error('[Timestamp Conversion] Error converting timestamps:', error);
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
