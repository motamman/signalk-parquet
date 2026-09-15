/**
 * SignalK History API Provider
 *
 * This module implements the SignalK HistoryApi interface to register
 * this plugin as the official history data provider for the SignalK server.
 */

import { Temporal } from '@js-temporal/polyfill';
import { ZonedDateTime, ZoneOffset, Instant } from '@js-joda/core';
import { Context, Timestamp, ServerAPI } from '@signalk/server-api';
import {
  HistoryApi,
  ValuesRequest,
  ValuesResponse,
  ContextsRequest,
  ContextsResponse,
  PathsRequest,
  PathsResponse,
  PathSpec as SignalKPathSpec,
  AggregateMethod,
} from '@signalk/server-api/dist/history';
import {
  PathFilter,
  filtersFromFields,
  buildParquetFilterClause,
  availableFilterColumns,
} from './utils/path-filters';
import { getAvailablePathsArray } from './utils/path-discovery';
import { getAvailableContextsForTimeRange } from './utils/context-discovery';
import { DuckDBPool } from './utils/duckdb-pool';
import { escapeSqlString } from './utils/sql-escape';
import {
  validateContext,
  validateSignalKPath,
} from './utils/signalk-validation';
import { getPathComponentSchema } from './utils/schema-cache';
import { HivePathBuilder } from './utils/hive-path-builder';
import { isAngularPath } from './utils/angular-paths';
import { middleIndexSql } from './utils/aggregate-sql';
import {
  buildBufferScalarSubquery,
  buildBufferObjectSubquery,
} from './utils/buffer-sql-builder';
import { stageBufferTable, BufferStagingSource } from './utils/buffer-staging';
import {
  smoothLinear,
  smoothCircularRad,
  smoothCircularDeg,
  SmoothMethod,
} from './utils/smoothing';

/** The slice of SQLiteBuffer the provider needs: staging plus schema lookups. */
type ProviderBufferSource = BufferStagingSource & {
  getKnownPaths(): Set<string>;
  getTableColumns(path: string): Set<string> | undefined;
};

/**
 * Convert Temporal.Instant or ISO string to ZonedDateTime (UTC)
 */
function temporalToZonedDateTime(
  instant: Temporal.Instant | string
): ZonedDateTime {
  // Handle ISO string (e.g., "2025-06-06T00:00:00Z")
  if (typeof instant === 'string') {
    return ZonedDateTime.parse(instant);
  }
  // Handle Temporal.Instant
  const epochMillis = instant.epochMilliseconds;
  return ZonedDateTime.ofInstant(
    Instant.ofEpochMilli(epochMillis),
    ZoneOffset.UTC
  );
}

/**
 * Convert Temporal.Duration to milliseconds
 */
function durationToMillis(duration: Temporal.Duration | number): number {
  if (typeof duration === 'number') {
    return duration;
  }
  return duration.total({ unit: 'milliseconds' });
}

/**
 * Parse time range parameters into from/to ZonedDateTime
 */
function parseTimeRange(
  params: ValuesRequest | ContextsRequest | PathsRequest
): { from: ZonedDateTime; to: ZonedDateTime } {
  const now = ZonedDateTime.now(ZoneOffset.UTC);

  if ('from' in params && params.from && 'to' in params && params.to) {
    // Both from and to specified
    return {
      from: temporalToZonedDateTime(params.from),
      to: temporalToZonedDateTime(params.to),
    };
  } else if (
    'from' in params &&
    params.from &&
    'duration' in params &&
    params.duration
  ) {
    // From + duration: query forward
    const from = temporalToZonedDateTime(params.from);
    const durationMs = durationToMillis(params.duration);
    const to = from.plusNanos(durationMs * 1_000_000);
    return { from, to };
  } else if (
    'to' in params &&
    params.to &&
    'duration' in params &&
    params.duration
  ) {
    // To + duration: query backward
    const to = temporalToZonedDateTime(params.to);
    const durationMs = durationToMillis(params.duration);
    const from = to.minusNanos(durationMs * 1_000_000);
    return { from, to };
  } else if ('from' in params && params.from) {
    // From only: query to now
    return {
      from: temporalToZonedDateTime(params.from),
      to: now,
    };
  } else if ('duration' in params && params.duration) {
    // Duration only: query back from now
    const durationMs = durationToMillis(params.duration);
    return {
      from: now.minusNanos(durationMs * 1_000_000),
      to: now,
    };
  }

  // Default: last hour
  return {
    from: now.minusHours(1),
    to: now,
  };
}

/**
 * One column of a values response: the requested spec, the filters that
 * narrow it, and the source it claims in the response (`$source`), if any.
 *
 * Columns are positional. The same path may appear more than once with a
 * different filter, aggregate or parameters, and under `sourcePolicy=all`
 * one spec fans out into several columns, so results are keyed by column
 * index rather than by path.
 */
interface ColumnSpec {
  spec: SignalKPathSpec;
  filters: PathFilter[];
  source?: string;
}

/**
 * Upper bound on the columns one path may expand into under
 * `sourcePolicy=all`. A busy bus can accumulate many source refs for one
 * path; each becomes a query, so the fan-out is capped and the first
 * sources in sorted order win.
 */
const MAX_EXPANDED_SOURCES = 16;

/**
 * Upper bound on the columns a whole request may expand into. Exceeding it
 * fails the request rather than truncating it: returning some of the asked-
 * for series without saying so would be worse than refusing.
 */
const MAX_EXPANDED_COLUMNS = 64;

/** The stored column a source ref lives in, as registered in path-filters. */
const SOURCE_FILTER = { field: 'sourceRef', column: 'source_label' } as const;

/**
 * History API Provider implementation
 */
export class HistoryProvider implements HistoryApi {
  private sqliteBuffer?: ProviderBufferSource;

  constructor(
    private selfId: string,
    private dataDir: string,
    private app: ServerAPI,
    private debug: (msg: string) => void
  ) {}

  setSqliteBuffer(buffer: ProviderBufferSource): void {
    this.sqliteBuffer = buffer;
  }

  /**
   * Get historical values for the specified query
   */
  async getValues(query: ValuesRequest): Promise<ValuesResponse> {
    this.debug(
      `[HistoryProvider] getValues called with: ${JSON.stringify(
        query,
        (_, v) => (typeof v === 'bigint' ? v.toString() : v)
      )}`
    );
    const { from, to } = parseTimeRange(query);
    // Translate 'vessels.self' to actual vessel URN (same as HTTP endpoint)
    const context =
      !query.context ||
      query.context === 'vessels.self' ||
      query.context === ('self' as Context)
        ? (`vessels.${this.selfId}` as Context)
        : validateContext(query.context as string);
    // query.resolution is in seconds per the SignalK History API spec;
    // the DuckDB bucketing SQL below works in milliseconds. Reject 0,
    // negative, NaN, and Infinity here so they don't reach SQL as a
    // divide-by-zero, negative bucket, or unbounded cardinality query.
    const r = query.resolution;
    const resolutionFromQuery = r != null && Number.isFinite(r) && r > 0;
    // Compute the auto fallback in milliseconds so a sub-second range
    // doesn't truncate to a 0 ms divisor in the SQL bucketing.
    const autoResolutionMs = Math.max(
      1,
      Math.round(
        (to.toInstant().toEpochMilli() - from.toInstant().toEpochMilli()) / 500
      )
    );
    const resolutionMs = resolutionFromQuery ? r * 1000 : autoResolutionMs;

    this.debug(
      `[HistoryProvider] getValues: context=${context}, from=${from}, to=${to}, resolution=${resolutionMs}ms (${resolutionFromQuery ? 'from query' : 'auto'}), paths=${query.pathSpecs.length}`
    );

    const fromIso = from.toInstant().toString();
    const toIso = to.toInstant().toString();

    const columns = await this.resolveColumns(query, context, fromIso, toIso);

    // Query each column
    const allData: Array<Array<[Timestamp, unknown]>> = [];

    for (const column of columns) {
      const { spec } = column;
      try {
        let pathData = await this.queryPath(
          context,
          spec,
          column.filters,
          fromIso,
          toIso,
          resolutionMs
        );
        // Apply the sma/ema moving window on this path's time-ordered,
        // federated (parquet + buffer) bucket series, before mergePathData
        // combines paths into columns.
        if (spec.aggregate === 'sma' || spec.aggregate === 'ema') {
          pathData = this.applySmoothing(pathData, spec, context);
        }
        allData.push(pathData);
      } catch (error) {
        this.debug(
          `[HistoryProvider] Error querying path ${spec.path}: ${error}`
        );
        allData.push([]);
      }
    }

    // Merge all column data into time-ordered rows
    const mergedData = this.mergePathData(allData);

    return {
      context,
      range: {
        from: fromIso as Timestamp,
        to: toIso as Timestamp,
      },
      // `$source` is the per-column source in the response (signalk-server
      // #2817); the request side keeps the name `sourceRef`. A column that
      // merges every source, or holds the unattributed rows, makes no claim.
      values: columns.map(({ spec, source }) => ({
        path: spec.path,
        method: spec.aggregate,
        ...(source !== undefined ? { $source: source } : {}),
      })) as ValuesResponse['values'],
      data: mergedData,
    };
  }

  /**
   * The columns a request produces. Without `sourcePolicy=all`, one per
   * spec, filtered by any explicit `sourceRef`. With it, a spec that names no
   * source fans out into one column per source that recorded the path in
   * range (named sources first, sorted, so the order is stable between
   * requests), plus a trailing unattributed column when rows exist with no
   * source, or in files that predate the column. An explicit `sourceRef` is
   * a filter and is never expanded, per the contract.
   */
  private async resolveColumns(
    query: ValuesRequest,
    context: Context,
    fromIso: string,
    toIso: string
  ): Promise<ColumnSpec[]> {
    const expand = query.sourcePolicy === 'all';
    const columns: ColumnSpec[] = [];

    for (const spec of query.pathSpecs) {
      const filters = filtersFromFields(
        spec as unknown as Record<string, unknown>
      );
      if (!expand || spec.sourceRef) {
        columns.push({ spec, filters, source: spec.sourceRef });
        continue;
      }
      // Checked before discovery as well as after: discovery costs a query
      // per path, so a request far past the ceiling should be refused
      // before paying for it.
      if (columns.length > MAX_EXPANDED_COLUMNS) {
        throw new Error(
          `sourcePolicy=all expands these paths past the limit (max ${MAX_EXPANDED_COLUMNS} columns); request fewer paths or name the sources with paths=<path>|<sourceRef>`
        );
      }
      const sources = await this.discoverSources(context, spec, fromIso, toIso);
      if (sources.length === 0) {
        // Nothing recorded in range: keep the unexpanded column so the path
        // still appears in the response, empty, rather than vanishing.
        columns.push({ spec, filters });
        continue;
      }
      for (const source of sources) {
        const filter: PathFilter = {
          field: SOURCE_FILTER.field,
          column: SOURCE_FILTER.column,
          value: source,
        };
        columns.push({
          spec,
          filters: [...filters, filter],
          ...(source !== null ? { source } : {}),
        });
      }
    }

    if (columns.length > MAX_EXPANDED_COLUMNS) {
      throw new Error(
        `sourcePolicy=all expands these paths into ${columns.length} columns (max ${MAX_EXPANDED_COLUMNS} columns); request fewer paths or name the sources with paths=<path>|<sourceRef>`
      );
    }
    return columns;
  }

  /**
   * Distinct sources that recorded `spec.path` for the context in range:
   * the raw-tier parquet files (where the column exists) unioned with the
   * live buffer. Named sources come back sorted; a trailing `null` means
   * rows with no source were found, which includes every row of a parquet
   * file that predates the column. Capped at MAX_EXPANDED_SOURCES.
   */
  private async discoverSources(
    context: Context,
    spec: SignalKPathSpec,
    fromIso: string,
    toIso: string
  ): Promise<(string | null)[]> {
    validateSignalKPath(spec.path as string);
    const filePath = new HivePathBuilder().getGlobPattern(
      this.dataDir,
      'raw',
      context,
      spec.path
    );
    const column = SOURCE_FILTER.column;
    const found = new Set<string | null>();
    const connection = await DuckDBPool.getConnection();
    try {
      const timeWindow = `signalk_timestamp >= '${fromIso}' AND signalk_timestamp < '${toIso}'`;
      const excluded = `filename NOT LIKE '%/processed/%' AND filename NOT LIKE '%/quarantine/%' AND filename NOT LIKE '%/failed/%' AND filename NOT LIKE '%/repaired/%'`;
      // Parquet side. A glob that matches no files throws; that is "no data
      // here", not a failure, and the buffer may still answer.
      const available = await availableFilterColumns(
        connection,
        [filePath],
        [{ field: SOURCE_FILTER.field, column, value: null }]
      );
      try {
        const selectExpr = available.has(column) ? column : 'NULL';
        const result = await connection.runAndReadAll(
          `SELECT DISTINCT ${selectExpr} AS src FROM read_parquet('${escapeSqlString(filePath)}', union_by_name=true, filename=true) WHERE ${excluded} AND ${timeWindow}`
        );
        for (const row of result.getRowObjects() as Array<{ src: unknown }>) {
          found.add(typeof row.src === 'string' ? row.src : null);
        }
      } catch (error) {
        this.debug(
          `[HistoryProvider] source discovery on parquet skipped for ${spec.path}: ${error}`
        );
      }
      // Buffer side.
      if (DuckDBPool.isSQLiteBufferInitialized() && this.sqliteBuffer) {
        const staged = await stageBufferTable(
          connection,
          this.sqliteBuffer,
          String(context),
          String(spec.path),
          fromIso,
          toIso,
          (msg: string) => this.debug(msg)
        );
        if (staged) {
          const result = await connection.runAndReadAll(
            `SELECT DISTINCT ${column} AS src FROM ${staged} WHERE context = '${escapeSqlString(String(context))}' AND ${timeWindow} AND exported = 0`
          );
          for (const row of result.getRowObjects() as Array<{ src: unknown }>) {
            found.add(typeof row.src === 'string' ? row.src : null);
          }
        }
      }
    } finally {
      connection.disconnectSync();
    }

    const named = [...found].filter((s): s is string => s !== null).sort();
    const all: (string | null)[] = found.has(null) ? [...named, null] : named;
    if (all.length > MAX_EXPANDED_SOURCES) {
      this.debug(
        `[HistoryProvider] ${spec.path} has ${all.length} sources in range; expanding the first ${MAX_EXPANDED_SOURCES} only`
      );
      return all.slice(0, MAX_EXPANDED_SOURCES);
    }
    return all;
  }

  /**
   * Get available contexts for the time range
   */
  async getContexts(query: ContextsRequest): Promise<ContextsResponse> {
    const { from, to } = parseTimeRange(query);

    this.debug(`[HistoryProvider] getContexts: from=${from}, to=${to}`);

    const contexts = await getAvailableContextsForTimeRange(
      this.dataDir,
      from,
      to
    );
    return contexts as ContextsResponse;
  }

  /**
   * Get available paths for the time range
   */
  async getPaths(query: PathsRequest): Promise<PathsResponse> {
    const { from, to } = parseTimeRange(query);

    this.debug(`[HistoryProvider] getPaths: from=${from}, to=${to}`);

    // Extract context if present (PathsRequest type doesn't include context, but callers may pass it)
    const queryContext = (query as any).context;
    const context = queryContext
      ? !queryContext ||
        queryContext === 'vessels.self' ||
        queryContext === 'self'
        ? `vessels.${this.selfId}`
        : validateContext(queryContext.replace(/ /gi, ''))
      : undefined;
    const paths = getAvailablePathsArray(this.dataDir, this.app, context);
    return paths as PathsResponse;
  }

  /**
   * Query a single path from parquet files
   */
  private async queryPath(
    context: Context,
    pathSpec: SignalKPathSpec,
    filters: PathFilter[],
    fromIso: string,
    toIso: string,
    resolutionMs: number
  ): Promise<Array<[Timestamp, unknown]>> {
    // Reject a malformed path before it reaches the read_parquet glob.
    validateSignalKPath(pathSpec.path as string);

    // Use HivePathBuilder for correct Hive-partitioned paths
    const hiveBuilder = new HivePathBuilder();

    // Build glob pattern for Hive partitions
    const filePath = hiveBuilder.getGlobPattern(
      this.dataDir,
      'raw',
      context,
      pathSpec.path
    );
    this.debug(`[HistoryProvider] Querying Hive path: ${filePath}`);

    // Stage this path's buffer rows into a temp table if the buffer is available
    const hasBuffer = DuckDBPool.isSQLiteBufferInitialized();
    const connection = await DuckDBPool.getConnection();

    try {
      const stagedBufferTable =
        hasBuffer && this.sqliteBuffer
          ? await stageBufferTable(
              connection,
              this.sqliteBuffer,
              String(context),
              String(pathSpec.path),
              fromIso,
              toIso,
              (msg: string) => this.debug(msg)
            )
          : null;
      // Check if this is an object path (has value_* columns)
      const componentSchema = await getPathComponentSchema(
        this.dataDir,
        context,
        pathSpec.path
      );

      // sma/ema bucket like average, so angular data needs the same
      // circular-mean bucket value before the moving window is applied.
      const averageLike =
        !pathSpec.aggregate ||
        pathSpec.aggregate === 'average' ||
        pathSpec.aggregate === 'sma' ||
        pathSpec.aggregate === 'ema';

      // `filters` carries the column's source filter, explicit or expanded.
      // This provider only queries raw-tier parquet; probe it for the filter
      // columns so files without them are excluded rather than throwing.
      const available = await availableFilterColumns(
        connection,
        [filePath],
        filters
      );
      const sourceFilter = buildParquetFilterClause(filters, available);

      // Build parquet FROM clause with filename filtering
      const parquetFrom = `(SELECT * FROM read_parquet('${escapeSqlString(filePath)}', union_by_name=true, filename=true) WHERE filename NOT LIKE '%/processed/%' AND filename NOT LIKE '%/quarantine/%' AND filename NOT LIKE '%/failed/%' AND filename NOT LIKE '%/repaired/%'${sourceFilter})`;

      if (componentSchema && componentSchema.components.size > 0) {
        // Object path - aggregate each component
        const componentSelects = Array.from(
          componentSchema.components.entries()
        )
          .map(([name, comp]) => {
            if (comp.dataType !== 'numeric') {
              // middle_index takes the same sample as the numeric components.
              if (pathSpec.aggregate === 'middle_index') {
                return `${middleIndexSql(comp.columnName, 'signalk_timestamp')} as ${name}`;
              }
              // FIRST returns the physically-first row's value, which may be
              // NULL even when a later row in the same bucket has one;
              // ANY_VALUE skips NULLs and the ORDER BY makes "earliest
              // non-NULL in the bucket" deterministic.
              return `ANY_VALUE(${comp.columnName} ORDER BY signalk_timestamp) as ${name}`;
            }
            // TRY_CAST handles mixed-type parquet files (some store lat/lon as VARCHAR)
            const colExpr = `TRY_CAST(${comp.columnName} AS DOUBLE)`;
            if (averageLike) {
              // Longitude wraps at the ±180° antimeridian, so a plain AVG
              // pulls 179°/−179° toward 0°; use the circular mean in degrees.
              if (
                pathSpec.path === 'navigation.position' &&
                name === 'longitude'
              ) {
                return `DEGREES(ATAN2(AVG(SIN(RADIANS(${colExpr}))), AVG(COS(RADIANS(${colExpr}))))) as ${name}`;
              }
              if (
                isAngularPath(
                  `${pathSpec.path}.${name}`,
                  this.app,
                  context as string
                )
              ) {
                return `ATAN2(AVG(SIN(${colExpr})), AVG(COS(${colExpr}))) as ${name}`;
              }
            }
            return `${this.aggregateSql(pathSpec.aggregate, colExpr)} as ${name}`;
          })
          .join(', ');

        const componentWhereConditions = Array.from(
          componentSchema.components.values()
        )
          .map(comp => `${comp.columnName} IS NOT NULL`)
          .join(' OR ');

        const componentCols = Array.from(componentSchema.components.values())
          .map(c => c.columnName)
          .join(', ');

        // Build federated FROM: parquet UNION ALL buffer
        let federatedFrom: string;
        if (stagedBufferTable) {
          const bufferTableCols = this.sqliteBuffer?.getTableColumns(
            pathSpec.path as string
          );
          const bufferSubquery = buildBufferObjectSubquery(
            stagedBufferTable,
            context,
            fromIso,
            toIso,
            componentSchema.components,
            bufferTableCols,
            filters
          );
          federatedFrom = `(
              SELECT signalk_timestamp, ${componentCols} FROM ${parquetFrom}
              UNION ALL
              SELECT signalk_timestamp, ${componentCols} FROM ${bufferSubquery}
            )`;
        } else {
          federatedFrom = parquetFrom;
        }

        const query = `
          SELECT
            strftime(DATE_TRUNC('seconds',
              EPOCH_MS(CAST(FLOOR(EPOCH_MS(signalk_timestamp::TIMESTAMP) / ${resolutionMs}) * ${resolutionMs} AS BIGINT))
            ), '%Y-%m-%dT%H:%M:%SZ') as timestamp,
            ${componentSelects}
          FROM ${federatedFrom} AS source_data
          WHERE
            signalk_timestamp >= '${fromIso}'
            AND signalk_timestamp < '${toIso}'
            AND (${componentWhereConditions})
          GROUP BY timestamp
          ORDER BY timestamp
        `;

        const result = await connection.runAndReadAll(query);
        const rows = result.getRowObjects();

        return rows.map((row: any) => {
          const timestamp = row.timestamp as Timestamp;
          // For navigation.position, return as [longitude, latitude] array for compatibility
          // with plugins like signalk-pmtiles-plugin that expect this format
          if (
            pathSpec.path === 'navigation.position' &&
            row.longitude !== undefined &&
            row.latitude !== undefined
          ) {
            return [timestamp, [row.longitude, row.latitude]];
          }
          // For other object paths, return as object
          const obj: any = {};
          componentSchema.components.forEach((_, name) => {
            if (row[name] !== null && row[name] !== undefined) {
              obj[name] = row[name];
            }
          });
          return [timestamp, obj];
        });
      } else {
        // Scalar path — use vector averaging for angular paths when aggregating by average
        const angular = isAngularPath(
          pathSpec.path,
          this.app,
          context as string
        );
        const valueExpression =
          angular && averageLike
            ? 'ATAN2(AVG(SIN(TRY_CAST(value AS DOUBLE))), AVG(COS(TRY_CAST(value AS DOUBLE))))'
            : this.aggregateSql(
                pathSpec.aggregate,
                'TRY_CAST(value AS DOUBLE)'
              );

        // Build federated FROM: parquet UNION ALL buffer
        let federatedFrom: string;
        if (stagedBufferTable) {
          const bufferSubquery = buildBufferScalarSubquery(
            stagedBufferTable,
            context,
            pathSpec.path,
            fromIso,
            toIso,
            filters
          );
          federatedFrom = `(
              SELECT signalk_timestamp, value FROM ${parquetFrom}
              UNION ALL
              SELECT signalk_timestamp, value FROM ${bufferSubquery}
            )`;
        } else {
          federatedFrom = parquetFrom;
        }

        const query = `
          SELECT
            strftime(DATE_TRUNC('seconds',
              EPOCH_MS(CAST(FLOOR(EPOCH_MS(signalk_timestamp::TIMESTAMP) / ${resolutionMs}) * ${resolutionMs} AS BIGINT))
            ), '%Y-%m-%dT%H:%M:%SZ') as timestamp,
            ${valueExpression} as value
          FROM ${federatedFrom} AS source_data
          WHERE
            signalk_timestamp >= '${fromIso}'
            AND signalk_timestamp < '${toIso}'
            AND value IS NOT NULL
          GROUP BY timestamp
          ORDER BY timestamp
        `;

        const result = await connection.runAndReadAll(query);
        const rows = result.getRowObjects();

        return rows.map((row: any) => [row.timestamp as Timestamp, row.value]);
      }
    } finally {
      connection.disconnectSync();
    }
  }

  /**
   * Aggregate SQL for one column of a bucketed query.
   */
  private aggregateSql(method: AggregateMethod, colExpr: string): string {
    if (method === 'middle_index') {
      return middleIndexSql(colExpr, 'signalk_timestamp');
    }
    return `${this.getAggregateFunction(method)}(${colExpr})`;
  }

  /**
   * Convert aggregate method to SQL function
   */
  private getAggregateFunction(method: AggregateMethod): string {
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
        // Needs the bucket's row count as well as its values, so it has no
        // single-function form.
        throw new Error('middle_index must be built with middleIndexSql()');
      case 'sma':
      case 'ema':
        // Moving averages bucket identically to `average`; the sma/ema window
        // is then applied in TS post-processing (see applySmoothing).
        return 'AVG';
      default:
        return 'AVG';
    }
  }

  /**
   * Apply the sma/ema moving window to one path's already-bucketed,
   * time-ordered series (post-bucket, post parquet+buffer federation, before
   * mergePathData). Buckets were computed identically to `average`.
   *
   *  - Scalar numbers: smoothed angular-aware — angular paths (units=rad) use
   *    the circular (sin/cos) variant so the 0/2π wrap doesn't corrupt the mean.
   *  - navigation.position ([lon, lat] array): latitude smoothed linearly;
   *    longitude smoothed on the circle (degrees) so the ±180° antimeridian is
   *    handled instead of linear-averaging 179°/−179° toward 0°.
   *  - Other object paths: each component that is a finite number in every
   *    row is smoothed — circularly for angular (units=rad) components,
   *    linearly otherwise; components missing/non-finite in any row pass
   *    through untouched (keeps index alignment, avoids NaN contamination).
   *  - Non-numeric values pass through unchanged.
   */
  private applySmoothing(
    rows: Array<[Timestamp, unknown]>,
    pathSpec: SignalKPathSpec,
    context: Context
  ): Array<[Timestamp, unknown]> {
    if (rows.length === 0) return rows;
    const method = pathSpec.aggregate as SmoothMethod;
    const param = pathSpec.parameter;
    const timestamps = rows.map(r => r[0]);
    const sample = rows.find(r => r[1] !== null && r[1] !== undefined)?.[1];

    // Scalar numeric series. Only finite values enter the moving window —
    // a null/NaN bucket (e.g. AVG over values that failed TRY_CAST) would
    // otherwise coerce to 0 or poison the recursive EMA; such rows pass
    // through unchanged instead.
    if (typeof sample === 'number') {
      const validIdx: number[] = [];
      const nums: number[] = [];
      rows.forEach((r, i) => {
        const v = r[1] === null || r[1] === undefined ? NaN : Number(r[1]);
        if (Number.isFinite(v)) {
          validIdx.push(i);
          nums.push(v);
        }
      });
      const angular = isAngularPath(pathSpec.path, this.app, context as string);
      const smoothed = angular
        ? smoothCircularRad(nums, method, param)
        : smoothLinear(nums, method, param);
      const out: Array<[Timestamp, unknown]> = rows.map(r => [r[0], r[1]]);
      validIdx.forEach((rowIdx, j) => {
        out[rowIdx] = [timestamps[rowIdx], smoothed[j]];
      });
      return out;
    }

    // navigation.position — queryPath returns [longitude, latitude]
    if (
      pathSpec.path === 'navigation.position' &&
      Array.isArray(sample) &&
      sample.length >= 2
    ) {
      // Same finite-value guard as the scalar branch: rows with a null/NaN
      // longitude or latitude pass through unchanged rather than entering
      // the moving window.
      const validIdx: number[] = [];
      const lons: number[] = [];
      const lats: number[] = [];
      rows.forEach((r, i) => {
        const v = r[1];
        if (!Array.isArray(v)) return;
        const lonV = v[0] === null || v[0] === undefined ? NaN : Number(v[0]);
        const latV = v[1] === null || v[1] === undefined ? NaN : Number(v[1]);
        if (Number.isFinite(lonV) && Number.isFinite(latV)) {
          validIdx.push(i);
          lons.push(lonV);
          lats.push(latV);
        }
      });
      const lon = smoothCircularDeg(lons, method, param);
      const lat = smoothLinear(lats, method, param);
      const out: Array<[Timestamp, unknown]> = rows.map(r => [r[0], r[1]]);
      validIdx.forEach((rowIdx, j) => {
        out[rowIdx] = [timestamps[rowIdx], [lon[j], lat[j]]];
      });
      return out;
    }

    // Other object paths — smooth each component that is a finite number in
    // every row (NaN/Infinity would poison the recursive EMA, same as the
    // scalar branch); other components pass through untouched.
    if (sample && typeof sample === 'object' && !Array.isArray(sample)) {
      const keys = Object.keys(sample as Record<string, unknown>).filter(k =>
        rows.every(r => {
          const v = (r[1] as Record<string, unknown>)?.[k];
          return typeof v === 'number' && Number.isFinite(v);
        })
      );
      const smoothedByKey: Record<string, number[]> = {};
      for (const k of keys) {
        const series = rows.map(r => (r[1] as Record<string, number>)[k]);
        // Angular components bucket as circular means (see queryPath); the
        // moving window must stay on the circle too, or a 0/2π wrap smooths
        // through a false midpoint.
        smoothedByKey[k] = isAngularPath(
          `${pathSpec.path}.${k}`,
          this.app,
          context as string
        )
          ? smoothCircularRad(series, method, param)
          : smoothLinear(series, method, param);
      }
      return timestamps.map((ts, i) => {
        const obj: Record<string, unknown> = {
          ...(rows[i][1] as Record<string, unknown>),
        };
        for (const k of keys) obj[k] = smoothedByKey[k][i];
        return [ts, obj];
      });
    }

    // Non-numeric — cannot smooth.
    return rows;
  }

  /**
   * Merge the per-column series (one entry per column, in column order) into
   * time-aligned rows.
   */
  private mergePathData(
    allData: Array<Array<[Timestamp, unknown]>>
  ): Array<[Timestamp, ...unknown[]]> {
    // Collect all unique timestamps
    const timestampSet = new Set<string>();
    allData.forEach(pathData => {
      pathData.forEach(([ts]) => timestampSet.add(ts));
    });

    // Sort timestamps
    const timestamps = Array.from(timestampSet).sort();

    // One lookup map per column, by position.
    const columnMaps = allData.map(pathData => {
      const map = new Map<string, unknown>();
      pathData.forEach(([ts, val]) => map.set(ts, val));
      return map;
    });

    // Build merged rows
    return timestamps.map(ts => {
      const row: [Timestamp, ...unknown[]] = [ts as Timestamp];
      columnMaps.forEach(map => row.push(map.get(ts) ?? null));
      return row;
    });
  }
}

/**
 * Register this plugin as the History API provider
 */
export function registerHistoryApiProvider(
  app: ServerAPI,
  selfId: string,
  dataDir: string,
  debug: (msg: string) => void,
  sqliteBuffer?: ProviderBufferSource
): void {
  const provider = new HistoryProvider(selfId, dataDir, app, debug);
  if (sqliteBuffer) {
    provider.setSqliteBuffer(sqliteBuffer);
  }

  // Debug: Check if registerHistoryApiProvider exists on app
  console.log(
    '[signalk-parquet] app.registerHistoryApiProvider exists:',
    typeof (app as any).registerHistoryApiProvider
  );

  if (typeof (app as any).registerHistoryApiProvider !== 'function') {
    console.error(
      '[signalk-parquet] ERROR: app.registerHistoryApiProvider is not a function!'
    );
    console.error(
      '[signalk-parquet] Available app methods:',
      Object.keys(app)
        .filter(k => typeof (app as any)[k] === 'function')
        .join(', ')
    );
    return;
  }

  try {
    app.registerHistoryApiProvider(provider);
    debug('[HistoryProvider] Successfully registered as History API provider');
    console.log('[signalk-parquet] Registered as SignalK History API provider');
  } catch (error) {
    console.error(
      '[signalk-parquet] Failed to register as History API provider:',
      error
    );
    debug(`[HistoryProvider] Registration failed: ${error}`);
  }
}

/**
 * Unregister this plugin as the History API provider
 */
export function unregisterHistoryApiProvider(app: ServerAPI): void {
  try {
    app.unregisterHistoryApiProvider();
    console.log(
      '[signalk-parquet] Unregistered as SignalK History API provider'
    );
  } catch (error) {
    // Ignore errors during unregistration
  }
}
