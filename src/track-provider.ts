/**
 * SignalK Track API provider (SignalK/signalk-server#2995).
 *
 * A track is the recorded, time-ordered series of a vessel's positions. This
 * provider answers the Track API from the same store the History API reads:
 * raw-tier `navigation.position` parquet, federated with today's not-yet-
 * exported rows in the SQLite buffer through a staged TEMP table (never an
 * ATTACH of the live buffer — see buffer-staging.ts for why).
 *
 * Contract notes that shaped the implementation:
 *
 * - The requested time range is always returned in full. Where the result
 *   would be too large, points are thinned by time bucketing and the bucket
 *   size actually used is reported in `properties.resolution`. Thinning keeps
 *   the first fix in each bucket rather than averaging: an averaged position
 *   cuts the corners off a track, a real fix does not.
 * - Position lives only in the raw tier (the aggregated tiers collapse object
 *   paths to a scalar), so the query always reads tier=raw and buckets in
 *   DuckDB rather than picking a tier.
 * - A bounding box selects tracks, it does not clip them (settled on #2995):
 *   a context whose track touches the box anywhere in the window is returned
 *   whole, approach and departure included, rather than as a line that stops
 *   at an invisible edge. A gap in recording longer than a few buckets starts
 *   a new segment, so a line is never drawn across a stretch the vessel did
 *   not travel.
 * - `properties` are co-recorded paths returned nested to match coordinates.
 *   Each is bucketed with the same expression as the positions and joined on
 *   the bucket, so a value lands on the fix it was recorded alongside. Paths
 *   the store does not hold are left out of `appliedProperties`.
 *
 * The request/response typings below mirror `@signalk/server-api/tracks` from
 * the server PR. They are copied rather than imported because the published
 * server-api this plugin depends on predates the Track API, and registration
 * is duck-typed so the plugin still loads on servers without it.
 */

import * as fs from 'fs-extra';
import * as path from 'path';
import { Context, Path, ServerAPI } from '@signalk/server-api';
import { DuckDBPool } from './utils/duckdb-pool';
import { escapeSqlString } from './utils/sql-escape';
import { HivePathBuilder } from './utils/hive-path-builder';
import {
  validateContext,
  validateSignalKPath,
} from './utils/signalk-validation';
import { stageBufferTable, BufferStagingSource } from './utils/buffer-staging';
import {
  buildBufferObjectSubquery,
  buildBufferScalarSubquery,
} from './utils/buffer-sql-builder';
import { ComponentInfo } from './utils/schema-cache';
import { SpatialFilter, buildSpatialSqlClause } from './utils/spatial-queries';
import {
  calculateDistance,
  isPointInBoundingBox,
} from './utils/geo-calculator';
import { isAngularPath } from './utils/angular-paths';
import { parseDurationToMillis } from './utils/duration-parser';
import {
  boundingBoxOf,
  millisToIsoDuration,
  simplifyIndices,
  splitIntoSegments,
} from './utils/track-geometry';

// ---------------------------------------------------------------------------
// Contract (mirrors packages/server-api/src/tracks.ts on the server branch)
// ---------------------------------------------------------------------------

/** `[west, south, east, north]`; west > east crosses the antimeridian. */
export type TrackBoundingBox = [number, number, number, number];

/**
 * A point in time as the server passes it (a Temporal.Instant, matched
 * structurally so the polyfill is not a runtime dependency here), an ISO 8601
 * string, or epoch milliseconds.
 */
export type TrackInstant = { epochMilliseconds: number } | string | number;

/**
 * A duration as the server passes it (a Temporal.Duration, matched by its
 * unit fields), an ISO 8601 string, or a number of seconds as the History API
 * uses.
 */
export interface TemporalDurationLike {
  years?: number;
  months?: number;
  weeks?: number;
  days?: number;
  hours?: number;
  minutes?: number;
  seconds?: number;
  milliseconds?: number;
  microseconds?: number;
  nanoseconds?: number;
}
export type TrackDuration = TemporalDurationLike | string | number;

export interface TracksRequest {
  contexts?: Context[];
  from?: TrackInstant;
  to?: TrackInstant;
  duration?: TrackDuration;
  bbox?: TrackBoundingBox;
  resolution?: TrackDuration;
  maxPoints?: number;
  simplify?: boolean;
  epsilon?: number;
  times?: boolean;
  properties?: Path[];
  geometry?: boolean;
}

export interface TrackProperties {
  context: Context;
  isSelf: boolean;
  providerId?: string;
  contextName?: string;
  from: string;
  to: string;
  bbox?: TrackBoundingBox;
  pointCount: number;
  resolution?: string;
  epsilon?: number;
  coordTimes?: string[][];
  appliedProperties?: Path[];
  values?: Record<string, (number | string | null)[][]>;
}

export interface TrackFeature {
  type: 'Feature';
  geometry: {
    type: 'MultiLineString';
    coordinates: [number, number][][];
  } | null;
  properties: TrackProperties;
}

export interface TracksResponse {
  type: 'FeatureCollection';
  features: TrackFeature[];
}

export interface TrackApi {
  getTracks(query: TracksRequest): Promise<TracksResponse>;
  getTrackContexts(query: TracksRequest): Promise<Context[]>;
}

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

const POSITION_PATH = 'navigation.position';

/**
 * Points per track when the caller gives neither `resolution` nor
 * `maxPoints`. Enough for a map at any zoom; a client that wants more says so
 * with `maxPoints`, one that wants less with `resolution`.
 */
export const DEFAULT_POINT_BUDGET = 5000;

/**
 * A gap between consecutive fixes longer than this many buckets starts a new
 * segment, never less than MIN_GAP_MS so a fine resolution over a short
 * window does not shatter a track at every dropped GPS sentence.
 */
const GAP_BUCKETS = 5;
const MIN_GAP_MS = 5 * 60 * 1000;

/** Douglas-Peucker tolerance when `simplify` is asked for without `epsilon`. */
const DEFAULT_EPSILON_M = 10;

/**
 * How far back to look when a request has no start, no duration, and the
 * context has no parquet to anchor the start on: the buffer's own retention.
 */
const BUFFER_LOOKBACK_MS = 48 * 60 * 60 * 1000;

/** Excludes the sidecar directories the write path leaves next to data. */
const FILENAME_EXCLUSIONS =
  "filename NOT LIKE '%/processed/%' AND filename NOT LIKE '%/quarantine/%' " +
  "AND filename NOT LIKE '%/failed/%' AND filename NOT LIKE '%/repaired/%'";

const POSITION_COMPONENTS = new Map<string, ComponentInfo>([
  [
    'latitude',
    { name: 'latitude', columnName: 'value_latitude', dataType: 'numeric' },
  ],
  [
    'longitude',
    { name: 'longitude', columnName: 'value_longitude', dataType: 'numeric' },
  ],
]);

/** The slice of SQLiteBuffer the provider needs. */
export type TrackBufferSource = BufferStagingSource & {
  getTableColumns(signalkPath: string): Set<string> | undefined;
  hasTable(signalkPath: string): boolean;
};

interface TimeWindow {
  fromMs: number;
  toMs: number;
  fromIso: string;
  toIso: string;
  /** False when the request gave neither `from` nor `duration`. */
  bounded: boolean;
}

interface TrackPoint {
  bucketMs: number;
  tMs: number;
  lon: number;
  lat: number;
}

type PropertyValue = number | string | null;

/** One co-recorded path, bucketed like the positions, sorted by bucket. */
export interface PropertySeries {
  bucketsMs: number[];
  values: PropertyValue[];
}

/**
 * How far from a fix a property sample may be and still be returned with it.
 *
 * Different sensors stamp their sentences at different instants: on a real
 * feed position (GP talker) and speed over ground (II talker) each arrive
 * about once a second, offset by a few hundred milliseconds. At a fine bucket
 * they never share one, so an exact bucket join returned nothing for either.
 * A sample within a few seconds of a fix was recorded alongside it; one
 * minutes away was not.
 */
export const PROPERTY_MATCH_WINDOW_MS = 5000;

/**
 * The property value that belongs with a fix at `tMs`: the bucket containing
 * the fix if it has a value, otherwise the nearest bucket start within
 * `toleranceMs`, otherwise null. `series` is sorted ascending by bucket.
 */
export function nearestPropertyValue(
  series: PropertySeries,
  tMs: number,
  resolutionMs: number,
  toleranceMs: number
): PropertyValue {
  const { bucketsMs, values } = series;
  if (bucketsMs.length === 0) {
    return null;
  }
  // Index of the last bucket starting at or before tMs.
  let lo = 0;
  let hi = bucketsMs.length - 1;
  let at = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    if (bucketsMs[mid] <= tMs) {
      at = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  if (at !== -1 && tMs < bucketsMs[at] + resolutionMs) {
    return values[at];
  }
  const candidates = [at, at + 1].filter(i => i >= 0 && i < bucketsMs.length);
  let best = -1;
  let bestDistance = Infinity;
  for (const i of candidates) {
    const distance = Math.abs(bucketsMs[i] - tMs);
    if (distance < bestDistance) {
      best = i;
      bestDistance = distance;
    }
  }
  return best !== -1 && bestDistance <= toleranceMs ? values[best] : null;
}

// ---------------------------------------------------------------------------
// Request decoding
// ---------------------------------------------------------------------------

function isoOf(ms: number): string {
  return new Date(ms).toISOString();
}

export function instantToMillis(value: TrackInstant): number {
  if (typeof value === 'number') {
    return value;
  }
  if (typeof value === 'string') {
    const ms = Date.parse(value);
    if (Number.isNaN(ms)) {
      throw new Error(`Invalid timestamp: ${JSON.stringify(value)}`);
    }
    return ms;
  }
  return Number(value.epochMilliseconds);
}

/**
 * Milliseconds for a duration in any of the accepted forms. Calendar units
 * (months, years) have no fixed length and are refused rather than guessed.
 */
export function durationToMillis(value: TrackDuration): number {
  if (typeof value === 'number') {
    return value * 1000;
  }
  if (typeof value === 'string') {
    return parseDurationToMillis(value);
  }
  if ((value.years ?? 0) !== 0 || (value.months ?? 0) !== 0) {
    throw new Error('Durations in months or years are not supported');
  }
  return (
    (value.weeks ?? 0) * 7 * 86_400_000 +
    (value.days ?? 0) * 86_400_000 +
    (value.hours ?? 0) * 3_600_000 +
    (value.minutes ?? 0) * 60_000 +
    (value.seconds ?? 0) * 1_000 +
    (value.milliseconds ?? 0) +
    (value.microseconds ?? 0) / 1_000 +
    (value.nanoseconds ?? 0) / 1_000_000
  );
}

/**
 * Resolve `from`/`to`/`duration` to one window. The server already folds
 * `duration` into `from` before calling a provider; this mirrors its rule so a
 * direct caller gets the same answer: `duration` measures back from the end,
 * and where `from` is also given the later start wins, since `from` is a
 * floor the caller set deliberately.
 */
export function resolveWindow(
  query: Pick<TracksRequest, 'from' | 'to' | 'duration'>,
  nowMs: number = Date.now()
): TimeWindow {
  const toMs = query.to !== undefined ? instantToMillis(query.to) : nowMs;
  let fromMs: number | undefined =
    query.from !== undefined ? instantToMillis(query.from) : undefined;
  if (query.duration !== undefined) {
    const back = toMs - durationToMillis(query.duration);
    fromMs = fromMs === undefined ? back : Math.max(fromMs, back);
  }
  if (fromMs === undefined) {
    return {
      fromMs: 0,
      toMs,
      fromIso: isoOf(0),
      toIso: isoOf(toMs),
      bounded: false,
    };
  }
  return {
    fromMs,
    toMs,
    fromIso: isoOf(fromMs),
    toIso: isoOf(toMs),
    bounded: true,
  };
}

function toSpatialFilter(bbox: TrackBoundingBox): SpatialFilter {
  const [west, south, east, north] = bbox;
  const finite = [west, south, east, north].every(Number.isFinite);
  if (
    !finite ||
    south > north ||
    Math.abs(south) > 90 ||
    Math.abs(north) > 90
  ) {
    throw new Error(`Invalid bbox: ${JSON.stringify(bbox)}`);
  }
  return { type: 'bbox', bbox: { west, south, east, north } };
}

/**
 * Tolerance for `simplify` without an explicit `epsilon`: a thousandth of the
 * box's diagonal when there is a box (so a harbour view keeps its detail and
 * an ocean view does not carry every wobble), otherwise a fixed 10 m.
 */
function defaultEpsilonMetres(bbox?: TrackBoundingBox): number {
  if (!bbox) {
    return DEFAULT_EPSILON_M;
  }
  const [west, south, east, north] = bbox;
  const diagonal = calculateDistance(south, west, north, east);
  return Math.max(1, diagonal / 1000);
}

function isNoFilesError(err: unknown): boolean {
  return (err as Error)?.message?.includes('No files found') ?? false;
}

/** Root-level paths without dots (name, mmsi, ...) are string properties. */
function isStringPath(signalkPath: string): boolean {
  return !signalkPath.includes('.');
}

function bucketExpression(resolutionMs: number): string {
  return `CAST(FLOOR(EPOCH_MS(signalk_timestamp::TIMESTAMP) / ${resolutionMs}) * ${resolutionMs} AS BIGINT)`;
}

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

export class TrackProvider implements TrackApi {
  private readonly hive = new HivePathBuilder();
  private sqliteBuffer?: TrackBufferSource;

  constructor(
    private readonly selfId: string,
    private dataDir: string,
    private readonly app: ServerAPI,
    private readonly debug: (msg: string) => void,
    sqliteBuffer?: TrackBufferSource
  ) {
    this.sqliteBuffer = sqliteBuffer;
  }

  setSqliteBuffer(buffer: TrackBufferSource | undefined): void {
    this.sqliteBuffer = buffer;
  }

  setDataDir(dataDir: string): void {
    this.dataDir = dataDir;
  }

  private get selfContext(): Context {
    return `vessels.${this.selfId}` as Context;
  }

  async getTracks(query: TracksRequest): Promise<TracksResponse> {
    // Snapshot mutable config once so a reconfigure mid-request cannot pair
    // one directory with another's buffer.
    const dataDir = this.dataDir;
    const buffer = this.sqliteBuffer;

    const window = resolveWindow(query);
    const contexts = await this.resolveContexts(query, window, dataDir, buffer);
    const wantGeometry = query.geometry !== false;
    const propertyPaths = this.validPropertyPaths(query.properties);

    this.debug(
      `[TrackProvider] getTracks: contexts=${contexts.length} window=${window.fromIso}..${window.toIso} bbox=${JSON.stringify(query.bbox ?? null)} properties=${propertyPaths.length}`
    );

    const features: TrackFeature[] = [];
    for (const context of contexts) {
      const feature = await this.buildFeature(
        context,
        query,
        window,
        propertyPaths,
        wantGeometry,
        dataDir,
        buffer
      );
      if (feature) {
        features.push(feature);
      }
    }
    return { type: 'FeatureCollection', features };
  }

  async getTrackContexts(query: TracksRequest): Promise<Context[]> {
    const dataDir = this.dataDir;
    const buffer = this.sqliteBuffer;
    const window = resolveWindow(query);
    const filter = query.bbox ? toSpatialFilter(query.bbox) : undefined;
    return this.contextsWithPositions(window, dataDir, buffer, filter);
  }

  // -- contexts -------------------------------------------------------------

  private async resolveContexts(
    query: TracksRequest,
    window: TimeWindow,
    dataDir: string,
    buffer: TrackBufferSource | undefined
  ): Promise<Context[]> {
    if (query.contexts && query.contexts.length > 0) {
      const unique = new Set<Context>();
      for (const context of query.contexts) {
        unique.add(this.normalizeContext(String(context)));
      }
      return [...unique];
    }
    if (query.bbox) {
      return this.contextsWithPositions(
        window,
        dataDir,
        buffer,
        toSpatialFilter(query.bbox)
      );
    }
    return [this.selfContext];
  }

  /**
   * `self` and `vessels.self` become the own vessel; a bare id is qualified
   * with `vessels.`; everything is validated before it can reach a glob.
   */
  private normalizeContext(context: string): Context {
    const trimmed = context.trim();
    if (!trimmed || trimmed === 'self' || trimmed === 'vessels.self') {
      return this.selfContext;
    }
    return validateContext(
      trimmed.includes('.') ? trimmed : `vessels.${trimmed}`
    );
  }

  /**
   * Contexts with a position fix in the window (and inside the box, when
   * given): one DISTINCT scan over every context's raw position partitions,
   * plus the buffer for fixes recorded today and not yet exported.
   */
  private async contextsWithPositions(
    window: TimeWindow,
    dataDir: string,
    buffer: TrackBufferSource | undefined,
    filter?: SpatialFilter
  ): Promise<Context[]> {
    const found = new Set<string>();

    const glob = path.join(
      dataDir,
      'tier=raw',
      'context=*',
      `path=${this.hive.sanitizePath(POSITION_PATH)}`,
      'year=*',
      'day=*',
      '*.parquet'
    );
    const spatial = filter ? ` AND ${buildSpatialSqlClause(filter)}` : '';
    // hive_partitioning=false so `context` is the data column holding the
    // true context string, not the lossy sanitized directory name.
    const sql = `
      SELECT DISTINCT context
      FROM read_parquet('${escapeSqlString(glob)}', hive_partitioning=false, union_by_name=true)
      WHERE signalk_timestamp >= '${escapeSqlString(window.fromIso)}'
        AND signalk_timestamp < '${escapeSqlString(window.toIso)}'${spatial}`;

    const connection = await DuckDBPool.getConnection();
    try {
      const result = await connection.runAndReadAll(sql);
      for (const row of result.getRowObjects()) {
        if (typeof row.context === 'string' && row.context.length > 0) {
          found.add(row.context);
        }
      }
    } catch (err) {
      if (!isNoFilesError(err)) {
        throw err;
      }
    } finally {
      connection.disconnectSync();
    }

    // The buffer is keyed by path, not context, and its unexported rows are
    // in practice the own vessel's, so only self is probed here. Other
    // contexts' buffered fixes still reach getTracks through staging; they
    // are just not listed until the day's export lands them in parquet.
    if (buffer?.hasTable(POSITION_PATH)) {
      if (this.bufferHasPositionIn(buffer, this.selfContext, window, filter)) {
        found.add(this.selfContext);
      }
    }

    return [...found].sort() as Context[];
  }

  private bufferHasPositionIn(
    buffer: TrackBufferSource,
    context: Context,
    window: TimeWindow,
    filter?: SpatialFilter
  ): boolean {
    const PAGE = 5000;
    const MAX_ROWS = 200_000;
    let afterId = 0;
    let scanned = 0;
    for (;;) {
      const rows = buffer.getRowsForFederation(
        POSITION_PATH,
        context,
        window.fromIso,
        window.toIso,
        afterId,
        PAGE
      );
      if (rows.length === 0) return false;
      for (const row of rows) {
        if (!filter) return true;
        const lat = Number(row.value_latitude);
        const lon = Number(row.value_longitude);
        if (
          Number.isFinite(lat) &&
          Number.isFinite(lon) &&
          isPointInBoundingBox(lat, lon, filter.bbox)
        ) {
          return true;
        }
      }
      scanned += rows.length;
      afterId = Number(rows[rows.length - 1].id);
      if (rows.length < PAGE || scanned >= MAX_ROWS) return false;
    }
  }

  // -- one context's track --------------------------------------------------

  private validPropertyPaths(properties: Path[] | undefined): Path[] {
    const valid: Path[] = [];
    for (const p of properties ?? []) {
      try {
        valid.push(validateSignalKPath(String(p)));
      } catch (err) {
        // A malformed path is dropped rather than failing the request; it is
        // absent from appliedProperties, which is how a client learns it was
        // not honoured.
        this.debug(
          `[TrackProvider] Ignoring property ${JSON.stringify(p)}: ${err}`
        );
      }
    }
    return valid;
  }

  /**
   * A request without `from` or `duration` asks for a context's entire
   * recorded history. Anchor the window on the earliest raw position
   * partition so the point budget is spread over data rather than over the
   * decades since the epoch; with no parquet at all, fall back to the
   * buffer's own retention.
   */
  private boundWindowToData(
    context: Context,
    window: TimeWindow,
    dataDir: string
  ): TimeWindow {
    if (window.bounded) {
      return window;
    }
    const earliest = this.hive.findEarliestDate(
      dataDir,
      'raw',
      this.hive.sanitizeContext(context),
      this.hive.sanitizePath(POSITION_PATH)
    );
    const fromMs = earliest
      ? earliest.getTime()
      : window.toMs - BUFFER_LOOKBACK_MS;
    return { ...window, fromMs, fromIso: isoOf(fromMs), bounded: true };
  }

  private async buildFeature(
    context: Context,
    query: TracksRequest,
    requestWindow: TimeWindow,
    propertyPaths: Path[],
    wantGeometry: boolean,
    dataDir: string,
    buffer: TrackBufferSource | undefined
  ): Promise<TrackFeature | null> {
    const window = this.boundWindowToData(context, requestWindow, dataDir);
    if (window.toMs <= window.fromMs) {
      return null;
    }

    // Bucket size: whatever the caller asked for, widened as needed so the
    // window fits the point budget. The budget is a bound on bucket count and
    // so on points; a fine bucket over a short window returns raw fixes.
    const budget =
      query.maxPoints !== undefined && query.maxPoints > 0
        ? Math.floor(query.maxPoints)
        : DEFAULT_POINT_BUDGET;
    const requestedMs =
      query.resolution !== undefined ? durationToMillis(query.resolution) : 0;
    const resolutionMs = Math.max(
      1,
      Math.ceil(requestedMs),
      Math.ceil((window.toMs - window.fromMs) / budget)
    );

    // The box selects tracks, it does not clip them: probe for any fix inside
    // it over the whole window as a single bucket, and if there is one, return
    // the context's track in full.
    if (query.bbox) {
      const probe = await this.queryPositions(
        context,
        window,
        window.toMs - window.fromMs,
        toSpatialFilter(query.bbox),
        dataDir,
        buffer
      );
      if (probe.length === 0) {
        return null;
      }
    }

    const points = await this.queryPositions(
      context,
      window,
      resolutionMs,
      undefined,
      dataDir,
      buffer
    );
    if (points.length === 0) {
      return null;
    }

    const gapMs = Math.max(GAP_BUCKETS * resolutionMs, MIN_GAP_MS);
    let segments = splitIntoSegments(points, gapMs);

    let epsilon: number | undefined;
    if (query.simplify || query.epsilon !== undefined) {
      epsilon =
        query.epsilon !== undefined && query.epsilon >= 0
          ? query.epsilon
          : defaultEpsilonMetres(query.bbox);
      const tolerance = epsilon;
      segments = segments.map(segment =>
        simplifyIndices(segment, tolerance).map(i => segment[i])
      );
    }

    const flat = segments.flat();
    const properties: TrackProperties = {
      context,
      isSelf: context === this.selfContext,
      contextName: this.lookupContextName(context),
      from: isoOf(flat[0].tMs),
      to: isoOf(flat[flat.length - 1].tMs),
      bbox: boundingBoxOf(flat),
      pointCount: flat.length,
      resolution: millisToIsoDuration(resolutionMs),
    };
    if (epsilon !== undefined) {
      properties.epsilon = epsilon;
    }

    if (!wantGeometry) {
      return { type: 'Feature', geometry: null, properties };
    }

    if (query.times) {
      properties.coordTimes = segments.map(segment =>
        segment.map(p => isoOf(p.tMs))
      );
    }

    if (propertyPaths.length > 0) {
      const applied: Path[] = [];
      const values: Record<string, PropertyValue[][]> = {};
      for (const propertyPath of propertyPaths) {
        const series = await this.queryPropertySeries(
          context,
          propertyPath,
          window,
          resolutionMs,
          dataDir,
          buffer
        );
        if (!series) {
          continue;
        }
        applied.push(propertyPath);
        const toleranceMs = Math.max(resolutionMs, PROPERTY_MATCH_WINDOW_MS);
        values[propertyPath] = segments.map(segment =>
          segment.map(p =>
            nearestPropertyValue(series, p.tMs, resolutionMs, toleranceMs)
          )
        );
      }
      properties.appliedProperties = applied;
      if (applied.length > 0) {
        properties.values = values;
      }
    }

    return {
      type: 'Feature',
      geometry: {
        type: 'MultiLineString',
        coordinates: segments.map(segment =>
          segment.map(p => [p.lon, p.lat] as [number, number])
        ),
      },
      properties,
    };
  }

  private lookupContextName(context: Context): string | undefined {
    try {
      const host = this.app as unknown as {
        getSelfPath?: (p: string) => unknown;
        getPath?: (p: string) => unknown;
      };
      const raw =
        context === this.selfContext
          ? host.getSelfPath?.('name')
          : host.getPath?.(`${context}.name`);
      if (typeof raw === 'string') {
        return raw;
      }
      if (raw && typeof raw === 'object') {
        const value = (raw as { value?: unknown }).value;
        if (typeof value === 'string') {
          return value;
        }
      }
    } catch {
      // Name is a nicety; a host without the lookup still gets a track.
    }
    return undefined;
  }

  // -- queries --------------------------------------------------------------

  private positionDir(dataDir: string, context: Context): string {
    return path.join(
      dataDir,
      'tier=raw',
      `context=${this.hive.sanitizeContext(context)}`,
      `path=${this.hive.sanitizePath(POSITION_PATH)}`
    );
  }

  /**
   * First fix per time bucket, in time order, from raw parquet unioned with
   * the staged buffer. `t_ms` is the fix's own timestamp rather than the
   * bucket start, so `coordTimes` says when the vessel was really there.
   */
  private async queryPositions(
    context: Context,
    window: TimeWindow,
    resolutionMs: number,
    filter: SpatialFilter | undefined,
    dataDir: string,
    buffer: TrackBufferSource | undefined
  ): Promise<TrackPoint[]> {
    const hasParquet = await fs.pathExists(this.positionDir(dataDir, context));
    const hasBufferTable = buffer?.hasTable(POSITION_PATH) ?? false;
    if (!hasParquet && !hasBufferTable) {
      return [];
    }

    const connection = await DuckDBPool.getConnection();
    try {
      const sources: string[] = [];
      if (hasParquet) {
        const glob = this.hive.getGlobPattern(
          dataDir,
          'raw',
          context,
          POSITION_PATH
        );
        sources.push(
          `SELECT signalk_timestamp, TRY_CAST(value_latitude AS DOUBLE) AS lat, TRY_CAST(value_longitude AS DOUBLE) AS lon ` +
            `FROM (SELECT * FROM read_parquet('${escapeSqlString(glob)}', union_by_name=true, filename=true) WHERE ${FILENAME_EXCLUSIONS})`
        );
      }
      if (hasBufferTable && buffer) {
        const staged = await stageBufferTable(
          connection,
          buffer,
          String(context),
          POSITION_PATH,
          window.fromIso,
          window.toIso,
          this.debug
        );
        if (staged) {
          const subquery = buildBufferObjectSubquery(
            staged,
            context,
            window.fromIso,
            window.toIso,
            POSITION_COMPONENTS,
            buffer.getTableColumns(POSITION_PATH)
          );
          sources.push(
            `SELECT signalk_timestamp, value_latitude AS lat, value_longitude AS lon FROM ${subquery}`
          );
        }
      }
      if (sources.length === 0) {
        return [];
      }

      const spatial = filter
        ? ` AND ${buildSpatialSqlClause(filter, 'lat', 'lon')}`
        : '';
      const buildSql = (from: string[]): string => `
        SELECT
          ${bucketExpression(resolutionMs)} AS bucket_ms,
          MIN(EPOCH_MS(signalk_timestamp::TIMESTAMP)) AS t_ms,
          ARG_MIN(lat, signalk_timestamp::TIMESTAMP) AS lat,
          ARG_MIN(lon, signalk_timestamp::TIMESTAMP) AS lon
        FROM (${from.join(' UNION ALL ')}) AS src
        WHERE signalk_timestamp >= '${escapeSqlString(window.fromIso)}'
          AND signalk_timestamp < '${escapeSqlString(window.toIso)}'
          AND lat IS NOT NULL AND lon IS NOT NULL${spatial}
        GROUP BY bucket_ms
        ORDER BY bucket_ms`;

      const rows = await this.runWithParquetFallback(
        connection,
        sources,
        hasParquet,
        buildSql
      );
      return rows.map(row => ({
        bucketMs: Number(row.bucket_ms),
        tMs: Number(row.t_ms),
        lat: Number(row.lat),
        lon: Number(row.lon),
      }));
    } finally {
      connection.disconnectSync();
    }
  }

  /**
   * One co-recorded path bucketed like the positions, keyed by bucket start.
   * Null when the store has nothing for the path, so the caller leaves it out
   * of `appliedProperties`. A query failure (an object-valued path with no
   * scalar `value` column, say) is logged and treated the same way rather
   * than failing the whole track.
   */
  private async queryPropertySeries(
    context: Context,
    signalkPath: Path,
    window: TimeWindow,
    resolutionMs: number,
    dataDir: string,
    buffer: TrackBufferSource | undefined
  ): Promise<PropertySeries | null> {
    const hasParquet = await fs.pathExists(
      path.join(
        dataDir,
        'tier=raw',
        `context=${this.hive.sanitizeContext(context)}`,
        `path=${this.hive.sanitizePath(signalkPath)}`
      )
    );
    const hasBufferTable = buffer?.hasTable(signalkPath) ?? false;
    if (!hasParquet && !hasBufferTable) {
      return null;
    }

    const stringValued = isStringPath(signalkPath);
    const angular =
      !stringValued && isAngularPath(signalkPath, this.app, context);
    const parquetValue = stringValued
      ? 'CAST(value AS VARCHAR)'
      : 'TRY_CAST(value AS DOUBLE)';
    // Angular paths take the circular mean, folded back into [0, 2π) since
    // ATAN2 answers in (-π, π] and SignalK angles are never negative.
    const aggregate = stringValued
      ? 'FIRST(value ORDER BY signalk_timestamp)'
      : angular
        ? 'MOD(ATAN2(AVG(SIN(value)), AVG(COS(value))) + 2 * PI(), 2 * PI())'
        : 'AVG(value)';

    const connection = await DuckDBPool.getConnection();
    try {
      const sources: string[] = [];
      if (hasParquet) {
        const glob = this.hive.getGlobPattern(
          dataDir,
          'raw',
          context,
          signalkPath
        );
        sources.push(
          `SELECT signalk_timestamp, ${parquetValue} AS value ` +
            `FROM (SELECT * FROM read_parquet('${escapeSqlString(glob)}', union_by_name=true, filename=true) WHERE ${FILENAME_EXCLUSIONS})`
        );
      }
      if (hasBufferTable && buffer) {
        const staged = await stageBufferTable(
          connection,
          buffer,
          String(context),
          signalkPath,
          window.fromIso,
          window.toIso,
          this.debug
        );
        if (staged) {
          const subquery = buildBufferScalarSubquery(
            staged,
            context,
            signalkPath,
            window.fromIso,
            window.toIso
          );
          sources.push(`SELECT signalk_timestamp, value FROM ${subquery}`);
        }
      }
      if (sources.length === 0) {
        return null;
      }

      const buildSql = (from: string[]): string => `
        SELECT ${bucketExpression(resolutionMs)} AS bucket_ms, ${aggregate} AS value
        FROM (${from.join(' UNION ALL ')}) AS src
        WHERE signalk_timestamp >= '${escapeSqlString(window.fromIso)}'
          AND signalk_timestamp < '${escapeSqlString(window.toIso)}'
          AND value IS NOT NULL
        GROUP BY bucket_ms
        ORDER BY bucket_ms`;

      const rows = await this.runWithParquetFallback(
        connection,
        sources,
        hasParquet,
        buildSql
      );
      const series: PropertySeries = { bucketsMs: [], values: [] };
      for (const row of rows) {
        const value = row.value;
        series.bucketsMs.push(Number(row.bucket_ms));
        series.values.push(
          typeof value === 'bigint'
            ? Number(value)
            : typeof value === 'number' || typeof value === 'string'
              ? value
              : value === null || value === undefined
                ? null
                : String(value)
        );
      }
      return series;
    } catch (err) {
      this.debug(
        `[TrackProvider] property ${signalkPath} not applied for ${context}: ${err}`
      );
      return null;
    } finally {
      connection.disconnectSync();
    }
  }

  /**
   * Run a query over the given sources. A position directory that exists but
   * holds no day-partition files (only quarantined ones, say) makes
   * read_parquet fail with "No files found"; in that case the parquet source
   * is dropped and the buffer alone answers, matching the History API.
   */
  private async runWithParquetFallback(
    connection: Awaited<ReturnType<typeof DuckDBPool.getConnection>>,
    sources: string[],
    parquetFirst: boolean,
    buildSql: (from: string[]) => string
  ): Promise<Array<Record<string, unknown>>> {
    try {
      const result = await connection.runAndReadAll(buildSql(sources));
      return result.getRowObjects() as Array<Record<string, unknown>>;
    } catch (err) {
      if (!isNoFilesError(err) || !parquetFirst) {
        throw err;
      }
      const remaining = sources.slice(1);
      if (remaining.length === 0) {
        return [];
      }
      const result = await connection.runAndReadAll(buildSql(remaining));
      return result.getRowObjects() as Array<Record<string, unknown>>;
    }
  }
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

type TrackRegistryHost = {
  registerTrackApiProvider?: (provider: TrackApi) => void;
  unregisterTrackApiProvider?: () => void;
};

/**
 * Register with the server's Track API registry. Returns false, without
 * throwing, on a server that predates the Track API: the plugin keeps working
 * as before, it just does not answer `/signalk/v2/api/tracks`.
 */
export function registerTrackApiProvider(
  app: ServerAPI,
  provider: TrackApi,
  debug: (msg: string) => void
): boolean {
  const host = app as unknown as TrackRegistryHost;
  if (typeof host.registerTrackApiProvider !== 'function') {
    debug(
      '[TrackProvider] Server exposes no Track API registry (needs signalk-server with #2995); not registering'
    );
    return false;
  }
  host.registerTrackApiProvider(provider);
  debug('[TrackProvider] Registered as SignalK Track API provider');
  return true;
}

/** Best-effort unregistration; the server also unregisters on plugin stop. */
export function unregisterTrackApiProvider(app: ServerAPI): void {
  try {
    (app as unknown as TrackRegistryHost).unregisterTrackApiProvider?.();
  } catch {
    // Ignore errors during unregistration
  }
}
