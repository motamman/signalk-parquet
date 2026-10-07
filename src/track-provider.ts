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
 * - A bounding box selects the contexts whose track touches it anywhere in
 *   the window. What comes back of each is set by `clip` (#3081): clipped,
 *   only the stretches inside the box, each its own segment and each keeping
 *   the recorded fix just outside at either crossing, with the point budget
 *   spent on them; not clipped (`clip=false`, or a server that predates
 *   `clip`), the whole track, approach and departure included (#2995). A gap
 *   in recording longer than a few buckets starts a new segment either way,
 *   so a line is never drawn across a stretch the vessel did not travel.
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

import { Context, Path, ServerAPI } from '@signalk/server-api';
import { DuckDBPool } from './utils/duckdb-pool';
import { escapeSqlString } from './utils/sql-escape';
import { HivePathBuilder } from './utils/hive-path-builder';
import {
  validateContext,
  validateSignalKPath,
} from './utils/signalk-validation';
import {
  IN_PROCESS_BATCH_SIZE,
  stageBufferTable,
  BufferStagingSource,
} from './utils/buffer-staging';
import { yieldToEventLoop } from './utils/hive-walk';
import { federationCursor } from './utils/sqlite-buffer';
import { FederationCursor } from './types';
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
import { firstSql } from './utils/aggregate-sql';
import { filesFor, readParquetSql } from './utils/parquet-files';
import {
  footerReaderAvailable,
  overlapsWindow,
  readFooters,
} from './utils/parquet-footer';
import { parseDurationToMillis } from './utils/duration-parser';
import {
  boundingBoxOf,
  capPoints,
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
  /**
   * Return only the parts of each track inside `bbox` (SignalK/signalk-server
   * #3081): a segment per stretch inside, each keeping the recorded fix just
   * outside the box at either crossing, clipped before `resolution`,
   * `maxPoints` and simplification. The server resolves it (true with a
   * `bbox` unless the client sent `clip=false`); a server without #3081
   * never sends it, and its `bbox` keeps selecting whole tracks.
   */
  clip?: boolean;
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

/**
 * Whether a property path holds angles, so its bucket takes the circular
 * mean. The server's metadata answers it (`isAngularPath`); a provider with
 * no server to ask — the one in track-worker.ts — is handed the answer.
 */
export type AngularPathTest = (
  signalkPath: string,
  context: Context
) => boolean;

export interface TrackProviderOptions {
  isAngular?: AngularPathTest;
}

interface TimeWindow {
  fromMs: number;
  toMs: number;
  fromIso: string;
  toIso: string;
  /** False when the request gave neither `from` nor `duration`. */
  bounded: boolean;
}

type Connection = Awaited<ReturnType<typeof DuckDBPool.getConnection>>;

interface TrackPoint {
  bucketMs: number;
  tMs: number;
  lon: number;
  lat: number;
  /** A clipped track's fix just outside the box, at an entry or exit. */
  crossing?: boolean;
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
  let ms: number;
  if (typeof value === 'number') {
    ms = value;
  } else if (typeof value === 'string') {
    ms = Date.parse(value);
  } else {
    ms = Number(value.epochMilliseconds);
  }
  // A NaN or infinite instant would flow straight into the window arithmetic
  // and the SQL; refuse it here rather than produce a query that means nothing.
  if (!Number.isFinite(ms)) {
    throw new Error(`Invalid timestamp: ${JSON.stringify(value)}`);
  }
  return ms;
}

/**
 * Milliseconds for a duration in any of the accepted forms. Calendar units
 * (months, years) have no fixed length and are refused rather than guessed.
 */
export function durationToMillis(value: TrackDuration): number {
  if (typeof value === 'number') {
    return finiteMillis(value * 1000, value);
  }
  if (typeof value === 'string') {
    return finiteMillis(parseDurationToMillis(value), value);
  }
  if ((value.years ?? 0) !== 0 || (value.months ?? 0) !== 0) {
    throw new Error('Durations in months or years are not supported');
  }
  return finiteMillis(
    (value.weeks ?? 0) * 7 * 86_400_000 +
      (value.days ?? 0) * 86_400_000 +
      (value.hours ?? 0) * 3_600_000 +
      (value.minutes ?? 0) * 60_000 +
      (value.seconds ?? 0) * 1_000 +
      (value.milliseconds ?? 0) +
      (value.microseconds ?? 0) / 1_000 +
      (value.nanoseconds ?? 0) / 1_000_000,
    value
  );
}

/** A duration that is not a finite number of milliseconds is refused. */
function finiteMillis(ms: number, source: unknown): number {
  if (!Number.isFinite(ms)) {
    throw new Error(`Invalid duration: ${JSON.stringify(source)}`);
  }
  return ms;
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
  // West greater than east is legal (a box across the antimeridian), so only
  // the ranges are checked, matching parseBboxParam on the History API side.
  const finite = [west, south, east, north].every(Number.isFinite);
  if (
    !finite ||
    south > north ||
    Math.abs(south) > 90 ||
    Math.abs(north) > 90 ||
    Math.abs(west) > 180 ||
    Math.abs(east) > 180
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

/**
 * The vessel's name as the server currently has it, for
 * `properties.contextName`. Undefined when the server has none or cannot be
 * asked.
 */
export function contextNameFor(
  app: ServerAPI,
  selfContext: Context,
  context: Context
): string | undefined {
  try {
    const host = app as unknown as {
      getSelfPath?: (p: string) => unknown;
      getPath?: (p: string) => unknown;
    };
    const raw =
      context === selfContext
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

export class TrackProvider implements TrackApi {
  private readonly hive = new HivePathBuilder();
  private sqliteBuffer?: TrackBufferSource;
  private readonly isAngular: AngularPathTest;

  constructor(
    private readonly selfId: string,
    private dataDir: string,
    private readonly app: ServerAPI,
    private readonly debug: (msg: string) => void,
    sqliteBuffer?: TrackBufferSource,
    options: TrackProviderOptions = {}
  ) {
    this.sqliteBuffer = sqliteBuffer;
    this.isAngular =
      options.isAngular ??
      ((signalkPath, context) => isAngularPath(signalkPath, app, context));
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
   * given): every context's raw position files for the window's days, plus
   * the buffer for fixes recorded today and not yet exported.
   *
   * Without a box the footers answer on their own: each file holds exactly
   * one context, and its timestamp span says whether it is in the window.
   * With a box the data has to be read, so DuckDB scans the bounded list;
   * a file whose footer cannot say goes to DuckDB either way.
   */
  private async contextsWithPositions(
    window: TimeWindow,
    dataDir: string,
    buffer: TrackBufferSource | undefined,
    filter?: SpatialFilter
  ): Promise<Context[]> {
    const found = new Set<string>();

    const files = await filesFor({
      dataDir,
      contexts: 'all',
      paths: [POSITION_PATH],
      fromIso: window.fromIso,
      toIso: window.toIso,
    });
    let viaDuckDb = files;
    if (files.length > 0 && !filter && footerReaderAvailable()) {
      try {
        const footers = await readFooters(files);
        const undecided: string[] = [];
        for (const footer of footers) {
          const overlaps = overlapsWindow(footer, window.fromIso, window.toIso);
          if (footer.context === null || overlaps === null) {
            undecided.push(footer.file);
          } else if (overlaps) {
            found.add(footer.context);
          }
        }
        viaDuckDb = undecided;
      } catch {
        // An unreadable file: DuckDB decides over all of them rather than a
        // guess at which one failed.
        viaDuckDb = files;
      }
    }
    if (viaDuckDb.length > 0) {
      const spatial = filter ? ` AND ${buildSpatialSqlClause(filter)}` : '';
      // `context` is the data column holding the true context string, not
      // the lossy sanitized directory name; readParquetSql sets
      // hive_partitioning=false for exactly that.
      const sql = `
        SELECT DISTINCT context
        FROM ${readParquetSql(viaDuckDb)}
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
      } finally {
        connection.disconnectSync();
      }
    }

    // The buffer is keyed by path, not context, and its unexported rows are
    // in practice the own vessel's, so only self is probed here. Other
    // contexts' buffered fixes still reach getTracks through staging; they
    // are just not listed until the day's export lands them in parquet.
    if (buffer?.hasTable(POSITION_PATH)) {
      if (
        await this.bufferHasPositionIn(buffer, this.selfContext, window, filter)
      ) {
        found.add(this.selfContext);
      }
    }

    return [...found].sort() as Context[];
  }

  /**
   * Whether the buffer holds a fix for the context in the window, inside the
   * box when there is one. Without a box the first row answers; with one, a
   * vessel whose buffered fixes all lie outside it is read to the end of the
   * window.
   *
   * Each page is a synchronous SQLite read on the server's thread, so the
   * pages are the staging read's size (the 10 ms budget in buffer-staging.ts)
   * and the loop yields between them: read whole, the 200,000-row cap was
   * about 1.6 s of frozen server at the ~8 µs a row measured there.
   */
  private async bufferHasPositionIn(
    buffer: TrackBufferSource,
    context: Context,
    window: TimeWindow,
    filter?: SpatialFilter
  ): Promise<boolean> {
    const MAX_ROWS = 200_000;
    let after: FederationCursor | null = null;
    let scanned = 0;
    for (;;) {
      const rows = buffer.getRowsForFederation(
        POSITION_PATH,
        context,
        window.fromIso,
        window.toIso,
        after,
        IN_PROCESS_BATCH_SIZE
      );
      if (rows.length === 0) {
        // A buffer closed since the last page (plugin stop, reconfigure)
        // returns no rows, which must not be read as "no fix".
        if (after !== null && !buffer.isOpen()) {
          throw new Error(
            `[TrackProvider] buffer closed mid-scan; refusing to answer with incomplete data`
          );
        }
        return false;
      }
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
      after = federationCursor(rows[rows.length - 1]);
      if (rows.length < IN_PROCESS_BATCH_SIZE || scanned >= MAX_ROWS) {
        return false;
      }
      await yieldToEventLoop();
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

    // Bucket size: whatever the caller asked for, widened as needed so what
    // is returned fits the point budget: the window, or when clipping, the
    // clipped part. The budget is a bound on bucket count and so on points; a
    // fine bucket over a short window returns raw fixes.
    const budget =
      query.maxPoints !== undefined && query.maxPoints > 0
        ? Math.floor(query.maxPoints)
        : DEFAULT_POINT_BUDGET;
    const requestedMs =
      query.resolution !== undefined ? durationToMillis(query.resolution) : 0;

    let resolutionMs: number;
    let segments: TrackPoint[][];
    if (query.bbox && query.clip === true) {
      // Clipped: the parts inside the box, the budget spent on them.
      const clipped = await this.queryClippedPositions(
        context,
        window,
        toSpatialFilter(query.bbox),
        budget,
        requestedMs,
        dataDir,
        buffer
      );
      if (!clipped) {
        return null;
      }
      resolutionMs = clipped.resolutionMs;
      const gapMs = Math.max(GAP_BUCKETS * resolutionMs, MIN_GAP_MS);
      segments = clipped.segments.flatMap(segment =>
        splitIntoSegments(segment, gapMs)
      );
    } else {
      // Bucket size: whatever the caller asked for, widened as needed so the
      // window fits the point budget.
      resolutionMs = Math.max(
        1,
        Math.ceil(requestedMs),
        Math.ceil((window.toMs - window.fromMs) / budget)
      );

      // Not clipped, the box selects tracks: probe for any fix inside it over
      // the whole window as a single bucket, and if there is one, return the
      // context's track in full.
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
      segments = splitIntoSegments(points, gapMs);
    }

    // The budget is an upper bound (`maxPoints`), which the buckets only aim
    // at; see capPoints.
    segments = capPoints(segments, budget);

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
      contextName: contextNameFor(this.app, this.selfContext, context),
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

  // -- queries --------------------------------------------------------------

  /** The raw files of one path of one context for the window's days. */
  private rawFiles(
    dataDir: string,
    context: Context,
    signalkPath: string,
    window: TimeWindow
  ): Promise<string[]> {
    return filesFor({
      dataDir,
      contexts: [context],
      paths: [signalkPath],
      fromIso: window.fromIso,
      toIso: window.toIso,
    });
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
    const files = await this.rawFiles(dataDir, context, POSITION_PATH, window);
    const hasBufferTable = buffer?.hasTable(POSITION_PATH) ?? false;
    if (files.length === 0 && !hasBufferTable) {
      return [];
    }

    const connection = await DuckDBPool.getConnection();
    try {
      const sources = await this.positionSources(
        connection,
        context,
        window,
        files,
        buffer
      );
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

      const rows = (
        await connection.runAndReadAll(buildSql(sources))
      ).getRowObjects();
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
   * The SELECTs a position query unions: raw parquet, and the buffer staged
   * onto `connection`, each as `signalk_timestamp, lat, lon`.
   */
  private async positionSources(
    connection: Connection,
    context: Context,
    window: TimeWindow,
    files: string[],
    buffer: TrackBufferSource | undefined
  ): Promise<string[]> {
    const sources: string[] = [];
    if (files.length > 0) {
      sources.push(
        `SELECT signalk_timestamp, TRY_CAST(value_latitude AS DOUBLE) AS lat, TRY_CAST(value_longitude AS DOUBLE) AS lon ` +
          `FROM ${readParquetSql(files)}`
      );
    }
    if (buffer?.hasTable(POSITION_PATH)) {
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
    return sources;
  }

  /**
   * The parts of the context's track inside the box, as the Track API's
   * `clip` asks (SignalK/signalk-server#3081): each stretch inside is its own
   * segment, opened by the recorded fix just before it entered and closed by
   * the one just after it left, so a line reaches the edge of the view
   * without invented points. A single fix outside between two visits closes
   * one and opens the next, so it is in both.
   *
   * The budget is spent on what is returned: the bucket size comes from the
   * clipped segments' time span, not the window's, with room reserved for
   * every segment's entry and exit fix, which are kept whatever the bucket
   * and marked `crossing` so the caller's cap (capPoints) keeps them while
   * they fit. Null when no fix is inside the box.
   */
  private async queryClippedPositions(
    context: Context,
    window: TimeWindow,
    box: SpatialFilter,
    budget: number,
    requestedMs: number,
    dataDir: string,
    buffer: TrackBufferSource | undefined
  ): Promise<{ segments: TrackPoint[][]; resolutionMs: number } | null> {
    const files = await this.rawFiles(dataDir, context, POSITION_PATH, window);
    if (files.length === 0 && !buffer?.hasTable(POSITION_PATH)) {
      return null;
    }

    const connection = await DuckDBPool.getConnection();
    try {
      const sources = await this.positionSources(
        connection,
        context,
        window,
        files,
        buffer
      );
      if (sources.length === 0) {
        return null;
      }

      // Every fix in the window, flagged inside or not, then only those a
      // clipped track keeps: inside, or next to an inside fix in time. `seg`
      // counts entries: a kept fix starts a segment when it is the fix
      // before an entry, or an inside fix with nothing before it in the
      // window. A fix outside with an inside fix on both sides also closes
      // the segment before it, so it is copied there.
      const inside = `(${buildSpatialSqlClause(box, 'lat', 'lon')})`;
      await connection.run(`
        CREATE OR REPLACE TEMP TABLE track_clip AS
        WITH fixes AS (
          SELECT
            EPOCH_MS(signalk_timestamp::TIMESTAMP) AS t_ms,
            lat,
            lon,
            ${inside} AS inside
          FROM (${sources.join(' UNION ALL ')}) AS src
          WHERE signalk_timestamp >= '${escapeSqlString(window.fromIso)}'
            AND signalk_timestamp < '${escapeSqlString(window.toIso)}'
            AND lat IS NOT NULL AND lon IS NOT NULL
        ),
        flagged AS (
          SELECT
            t_ms, lat, lon, inside,
            LAG(inside) OVER w AS prev_inside,
            LEAD(inside) OVER w AS next_inside
          FROM fixes
          WINDOW w AS (ORDER BY t_ms, lat, lon)
        ),
        kept AS (
          SELECT
            t_ms, lat, lon, inside,
            SUM(
              CASE
                WHEN (NOT inside AND next_inside)
                  OR (inside AND prev_inside IS NULL) THEN 1
                ELSE 0
              END
            ) OVER (ORDER BY t_ms, lat, lon ROWS UNBOUNDED PRECEDING) AS seg,
            NOT inside AND prev_inside AND next_inside AS between_visits
          FROM flagged
          WHERE inside OR prev_inside OR next_inside
        )
        SELECT t_ms, lat, lon, inside, seg FROM kept
        UNION ALL
        SELECT t_ms, lat, lon, inside, seg - 1 FROM kept WHERE between_visits`);

      const spans = (
        await connection.runAndReadAll(`
          SELECT COUNT(*) AS segments,
                 SUM(span) AS total_span
          FROM (
            SELECT MAX(t_ms) - MIN(t_ms) AS span
            FROM track_clip
            GROUP BY seg
          )`)
      ).getRowObjects()[0];
      const segmentCount = Number(spans?.segments ?? 0);
      if (segmentCount === 0) {
        return null;
      }
      // Entry and exit fixes are kept whatever the bucket, so they come out of
      // the budget first.
      const buckets = Math.max(1, budget - 2 * segmentCount);
      const resolutionMs = Math.max(
        1,
        Math.ceil(requestedMs),
        Math.ceil(Number(spans.total_span ?? 0) / buckets)
      );

      // The first inside fix per bucket within each segment, and every fix
      // outside (the entry and exit fixes), in time order.
      const rows = (
        await connection.runAndReadAll(`
          SELECT seg, t_ms, lat, lon, crossing,
                 CAST(FLOOR(t_ms / ${resolutionMs}) * ${resolutionMs} AS BIGINT) AS bucket_ms
          FROM (
            SELECT seg, t_ms, lat, lon, TRUE AS crossing
            FROM track_clip
            WHERE NOT inside
            UNION ALL
            SELECT seg,
                   MIN(t_ms),
                   ARG_MIN(lat, t_ms),
                   ARG_MIN(lon, t_ms),
                   FALSE
            FROM track_clip
            WHERE inside
            GROUP BY seg, FLOOR(t_ms / ${resolutionMs})
          )
          ORDER BY seg, t_ms`)
      ).getRowObjects();

      const bySegment = new Map<number, TrackPoint[]>();
      for (const row of rows) {
        const seg = Number(row.seg);
        let points = bySegment.get(seg);
        if (!points) {
          points = [];
          bySegment.set(seg, points);
        }
        points.push({
          bucketMs: Number(row.bucket_ms),
          tMs: Number(row.t_ms),
          lat: Number(row.lat),
          lon: Number(row.lon),
          crossing: row.crossing === true,
        });
      }
      return { segments: [...bySegment.values()], resolutionMs };
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
    const files = await this.rawFiles(dataDir, context, signalkPath, window);
    const hasBufferTable = buffer?.hasTable(signalkPath) ?? false;
    if (files.length === 0 && !hasBufferTable) {
      return null;
    }

    const stringValued = isStringPath(signalkPath);
    const angular = !stringValued && this.isAngular(signalkPath, context);
    const parquetValue = stringValued
      ? 'CAST(value AS VARCHAR)'
      : 'TRY_CAST(value AS DOUBLE)';
    // Angular paths take the circular mean, folded back into [0, 2π) since
    // ATAN2 answers in (-π, π] and SignalK angles are never negative.
    const aggregate = stringValued
      ? firstSql('value', 'signalk_timestamp')
      : angular
        ? 'MOD(ATAN2(AVG(SIN(value)), AVG(COS(value))) + 2 * PI(), 2 * PI())'
        : 'AVG(value)';

    const connection = await DuckDBPool.getConnection();
    try {
      const sources: string[] = [];
      if (files.length > 0) {
        sources.push(
          `SELECT signalk_timestamp, ${parquetValue} AS value ` +
            `FROM ${readParquetSql(files)}`
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

      const rows = (
        await connection.runAndReadAll(buildSql(sources))
      ).getRowObjects();
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
