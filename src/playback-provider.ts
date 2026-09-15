/**
 * Signal K v1 history playback provider.
 *
 * A client opens `/signalk/v1/playback?startTime=…&playbackRate=…` and the
 * server asks the registered provider two things: `hasAnyData` for the
 * start time, then `streamHistory`, handing over the same delta sink it uses
 * for live data. The server applies the connection's `subscribe` rule
 * (missing or `self` means the own vessel only, `none` means nothing) and
 * security filtering; the provider's job is to replay stored rows as the
 * delta messages they came from, paced by the playback rate, until the
 * client disconnects.
 *
 * One extension: a `context` query parameter (a signalk-parquet addition,
 * not in the spec) names the vessels to replay, comma-separated, each a
 * full context, a bare MMSI or `self`. It narrows every read to those
 * vessels; the client pairs it with `subscribe=all` so the server's own
 * filter lets them through.
 *
 * Reading is done in windows of playback time. For each window the raw-tier
 * parquet files that overlap it are read (a per-day index of every file's
 * time span, built from parquet metadata alone, says which) together with
 * the unexported rows still in the SQLite buffer. Empty stretches are
 * skipped by jumping to the next recorded instant rather than waited out.
 * When the window reaches the present, the buffer is polled once a second
 * so playback runs on into live data.
 *
 * Nothing here runs at plugin start; the first directory read happens when
 * a playback connection opens.
 */

import * as fs from 'fs-extra';
import * as path from 'path';
import { ZonedDateTime, ZoneOffset, Instant } from '@js-joda/core';
import { ServerAPI } from '@signalk/server-api';
import { DuckDBPool } from './utils/duckdb-pool';
import { HivePathBuilder } from './utils/hive-path-builder';
import { escapeSqlString } from './utils/sql-escape';
import { getAvailableContextsForTimeRange } from './utils/context-discovery';
import { IDENTITY_PATH, IdentityComponents } from './utils/vessel-identity';
import {
  PlaybackRow,
  PlaybackDelta,
  ScalarKind,
  decodeScalar,
  decodeJson,
  groupRowsIntoDeltas,
  scalarKindFromColumnType,
  unfoldIdentity,
} from './utils/playback-deltas';

/** Playback time covered by one read, at most. */
export const WINDOW_MS = 60_000;
/** A window that returns more rows than this is halved and re-read. */
export const MAX_ROWS_PER_WINDOW = 50_000;
/** The smallest window; past this a flood is played as it is. */
const MIN_WINDOW_MS = 1_000;
/** How far behind the wall clock the live tail stays, for late rows. */
const LIVE_LAG_MS = 2_000;
/** Poll interval once playback has caught up with the present. */
const LIVE_POLL_MS = 1_000;
/** Slowest and fastest playback honoured. */
const MIN_RATE = 0.1;
const MAX_RATE = 1_000;

const FILENAME_EXCLUSIONS =
  "filename NOT LIKE '%/processed/%' AND filename NOT LIKE '%/quarantine/%' " +
  "AND filename NOT LIKE '%/failed/%' AND filename NOT LIKE '%/repaired/%'";

/** What the server passes to the provider. */
export interface PlaybackOptions {
  startTime: Date;
  playbackRate?: number;
  subscribe?: string;
}

/** The slice of the server's connection object the provider reads. */
interface PlaybackSpark {
  query?: Record<string, unknown>;
}

/**
 * Parse the `context` query extension: a comma-separated list of full
 * contexts, bare MMSIs or `self`. Null when absent or empty.
 */
export function parseContextParam(
  raw: unknown,
  selfContext: string
): string[] | null {
  if (typeof raw !== 'string') return null;
  const out = new Set<string>();
  for (const part of raw.split(',')) {
    const p = part.trim();
    if (!p) continue;
    if (p === 'self') out.add(selfContext);
    else if (/^\d{9}$/.test(p)) out.add(`vessels.urn:mrn:imo:mmsi:${p}`);
    else if (p.includes('.')) out.add(p);
    else out.add(`vessels.${p}`);
  }
  return out.size > 0 ? [...out] : null;
}

/** The slice of SQLiteBuffer playback needs. */
export interface PlaybackBufferSource {
  isOpen(): boolean;
  getRowsForPlayback(
    fromIso: string,
    toIso: string,
    contexts: string[] | null,
    limitPerPath: number
  ): Array<{
    path: string;
    context: string;
    signalk_timestamp: string;
    source_label: string | null;
    value: string | null;
    value_json: string | null;
  }>;
  hasRowsSince(fromIso: string, contexts: string[] | null): boolean;
  getNextRowTime(fromIso: string, contexts: string[] | null): string | null;
  getLatestObjectRowAt(
    signalkPath: string,
    context: string,
    atIso: string
  ):
    | {
        signalk_timestamp: string;
        source_label: string | null;
        value_json: string | null;
      }
    | undefined;
}

/** One raw-tier parquet file and what its metadata says it holds. */
interface IndexedFile {
  filename: string;
  context: string;
  path: string;
  /** Time span from row-group statistics; null when the file has none. */
  t0: string | null;
  t1: string | null;
  /** How the scalar `value` column is typed; 'object' when there is none. */
  kind: ScalarKind | 'object';
  hasContext: boolean;
  hasSource: boolean;
  /** Object paths: the flattened `value_*` component columns, sans prefix. */
  components: string[];
  hasValueJson: boolean;
}

interface DayIndex {
  key: string;
  files: IndexedFile[];
}

interface Chunk {
  fromIso: string;
  toIso: string;
  rows: PlaybackRow[];
  /** True when a row cap was hit and the window must shrink. */
  capped: boolean;
  /**
   * True when the window ends at the present (the wall clock less the live
   * lag) as `fetch` saw it: there is nothing further to read yet, and the
   * next read must wait rather than follow at once.
   */
  live: boolean;
}

type DeltaSink = (delta: PlaybackDelta) => void;

type Connection = Awaited<ReturnType<typeof DuckDBPool.getConnection>>;

/**
 * Rebuild an object value from its flattened `value_<component>` columns.
 * DuckDB hands integers back as bigint; they become numbers. Null when no
 * component has a value.
 */
function objectFromComponents(
  row: Record<string, unknown>,
  components: string[]
): Record<string, unknown> | null {
  const out: Record<string, unknown> = {};
  let any = false;
  for (const c of components) {
    const v = row[`value_${c}`];
    if (v === null || v === undefined) continue;
    out[c] = typeof v === 'bigint' ? Number(v) : v;
    any = true;
  }
  return any ? out : null;
}

/** Directory entries, or none when the directory is missing. */
async function readDirNames(dir: string): Promise<string[]> {
  try {
    return await fs.readdir(dir);
  } catch {
    return [];
  }
}

function isNoFilesError(err: unknown): boolean {
  return err instanceof Error && /No files found/i.test(err.message);
}

function toZoned(date: Date): ZonedDateTime {
  return ZonedDateTime.ofInstant(
    Instant.ofEpochMilli(date.getTime()),
    ZoneOffset.UTC
  );
}

function dayKey(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function startOfNextDay(d: Date): Date {
  return new Date(
    Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 1)
  );
}

export class PlaybackProvider {
  private readonly hive = new HivePathBuilder();
  private readonly selfContext: string;

  constructor(
    private readonly app: Pick<ServerAPI, 'selfId' | 'selfContext'>,
    private readonly dataDir: string,
    private readonly buffer: PlaybackBufferSource | undefined,
    private readonly debug: (msg: string) => void
  ) {
    this.selfContext = app.selfContext || `vessels.${app.selfId}`;
  }

  /** Vessels a connection may see: the own vessel, or every vessel. */
  private scope(options: PlaybackOptions): string[] | null {
    const sub = options.subscribe;
    return !sub || sub === 'self' ? [this.selfContext] : null;
  }

  /**
   * Whether anything at all was recorded at or after the start time for the
   * vessels in scope. Cheap: a buffer probe, then the cached context scan.
   */
  hasAnyData(
    options: PlaybackOptions,
    callback: (hasResults: boolean) => void
  ): void {
    this.checkAnyData(options)
      .then(callback)
      .catch(err => {
        this.debug(`[Playback] hasAnyData failed: ${err}`);
        callback(false);
      });
  }

  /** The server-api typing spells it this way. */
  hasAnydata(
    options: PlaybackOptions,
    callback: (hasResults: boolean) => void
  ): void {
    this.hasAnyData(options, callback);
  }

  /** Legacy snapshot hook; the server no longer calls it. */
  getHistory(
    _date: Date,
    _path: string,
    callback: (deltas: PlaybackDelta[]) => void
  ): void {
    callback([]);
  }

  private async checkAnyData(options: PlaybackOptions): Promise<boolean> {
    const start = options.startTime;
    if (!(start instanceof Date) || Number.isNaN(start.getTime())) {
      return false;
    }
    const contexts = this.scope(options);
    if (
      this.buffer?.isOpen() &&
      this.buffer.hasRowsSince(start.toISOString(), contexts)
    ) {
      return true;
    }
    const found = await getAvailableContextsForTimeRange(
      this.dataDir,
      toZoned(start),
      ZonedDateTime.now(ZoneOffset.UTC)
    );
    if (contexts === null) return found.length > 0;
    return found.some(c => contexts.includes(String(c)));
  }

  /**
   * Start replaying. Returns the function the server calls on disconnect.
   */
  streamHistory(
    spark: unknown,
    options: PlaybackOptions,
    onChange: DeltaSink
  ): () => void {
    const query = (spark as PlaybackSpark | null | undefined)?.query;
    const named = parseContextParam(query?.context, this.selfContext);
    const session = new PlaybackSession(
      this,
      this.dataDir,
      this.buffer,
      named ?? this.scope(options),
      options,
      onChange,
      this.debug
    );
    session.run().catch(err => {
      this.debug(`[Playback] stream ended with error: ${err}`);
    });
    return () => session.stop();
  }

  // ---- storage access used by sessions ------------------------------------

  /**
   * Index the raw-tier files of one UTC day for the vessels in scope, from
   * parquet metadata only (no row is read): each file's time span and how
   * its value column is typed.
   */
  async indexDay(day: Date, contexts: string[] | null): Promise<IndexedFile[]> {
    const year = day.getUTCFullYear();
    const dayOfYear = this.hive.getDayOfYear(day);
    const globs =
      contexts === null
        ? [
            this.hive.getGlobPattern(
              this.dataDir,
              'raw',
              undefined,
              undefined,
              year,
              dayOfYear
            ),
          ]
        : contexts.map(c =>
            this.hive.getGlobPattern(
              this.dataDir,
              'raw',
              c,
              undefined,
              year,
              dayOfYear
            )
          );
    if (!(await fs.pathExists(path.join(this.dataDir, 'tier=raw')))) return [];

    const files = new Map<string, IndexedFile>();
    let connection: Connection;
    try {
      connection = await DuckDBPool.getConnection();
    } catch (err) {
      this.debug(`[Playback] DuckDB unavailable, parquet skipped: ${err}`);
      return [];
    }
    try {
      for (const glob of globs) {
        const g = escapeSqlString(glob);
        let schemaRows: Array<Record<string, unknown>>;
        try {
          const schema = await connection.runAndReadAll(
            `SELECT file_name, name, type, converted_type FROM parquet_schema('${g}')`
          );
          schemaRows = schema.getRowObjects() as Array<Record<string, unknown>>;
        } catch (err) {
          if (isNoFilesError(err)) continue;
          throw err;
        }
        for (const row of schemaRows) {
          const filename = String(row.file_name);
          if (!/\.parquet$/i.test(filename)) continue;
          let file = files.get(filename);
          if (!file) {
            const parsed = this.hive.detectPathStyle(filename);
            if (!parsed.isHive || !parsed.context || !parsed.signalkPath)
              continue;
            file = {
              filename,
              context: parsed.context,
              path: parsed.signalkPath,
              t0: null,
              t1: null,
              kind: 'object',
              hasContext: false,
              hasSource: false,
              components: [],
              hasValueJson: false,
            };
            files.set(filename, file);
          }
          const name = String(row.name);
          if (name === 'value') {
            const physical = String(row.type ?? '');
            const converted = String(row.converted_type ?? '');
            file.kind =
              physical === 'BYTE_ARRAY' || /UTF8|STRING/i.test(converted)
                ? 'string'
                : scalarKindFromColumnType(physical);
          } else if (name === 'context') {
            file.hasContext = true;
          } else if (name === 'source_label') {
            file.hasSource = true;
          } else if (name === 'value_json') {
            file.hasValueJson = true;
          } else if (name.startsWith('value_')) {
            file.components.push(name.slice('value_'.length));
          }
        }
        // Time spans from row-group statistics.
        const stats = await connection.runAndReadAll(
          `SELECT file_name, MIN(stats_min) AS t0, MAX(stats_max) AS t1
           FROM parquet_metadata('${g}')
           WHERE path_in_schema = 'signalk_timestamp'
           GROUP BY file_name`
        );
        for (const row of stats.getRowObjects() as Array<
          Record<string, unknown>
        >) {
          const file = files.get(String(row.file_name));
          if (!file) continue;
          file.t0 = row.t0 == null ? null : String(row.t0);
          file.t1 = row.t1 == null ? null : String(row.t1);
        }
      }
    } finally {
      connection.disconnectSync();
    }
    return [...files.values()];
  }

  /**
   * The first UTC day at or after `after` that has any raw-tier day
   * directory for the vessels in scope, or null. A directory walk only
   * (context, path, year, day), no file is opened; used to jump over long
   * silences instead of indexing every empty day in between.
   */
  async nextParquetDay(
    after: Date,
    contexts: string[] | null
  ): Promise<Date | null> {
    const rawDir = path.join(this.dataDir, 'tier=raw');
    const contextDirs =
      contexts === null
        ? (await readDirNames(rawDir)).filter(n => n.startsWith('context='))
        : contexts.map(c => `context=${this.hive.sanitizeContext(c)}`);
    const afterYear = after.getUTCFullYear();
    const afterDay = this.hive.getDayOfYear(after);
    const found: { best: { year: number; day: number } | null } = {
      best: null,
    };
    const better = (y: number, d: number) => {
      const b = found.best;
      return b === null || y < b.year || (y === b.year && d < b.day);
    };
    for (const cd of contextDirs) {
      const contextDir = path.join(rawDir, cd);
      for (const pd of (await readDirNames(contextDir)).filter(n =>
        n.startsWith('path=')
      )) {
        const pathDir = path.join(contextDir, pd);
        const years = (await readDirNames(pathDir))
          .filter(n => n.startsWith('year='))
          .map(n => parseInt(n.slice(5), 10))
          .filter(
            y =>
              Number.isFinite(y) &&
              y >= afterYear &&
              (found.best === null || y <= found.best.year)
          )
          .sort((a, b) => a - b);
        for (const y of years) {
          const days = (await readDirNames(path.join(pathDir, `year=${y}`)))
            .filter(n => n.startsWith('day='))
            .map(n => parseInt(n.slice(4), 10))
            .filter(
              d => Number.isFinite(d) && (y > afterYear || d >= afterDay)
            );
          for (const d of days) {
            if (better(y, d)) found.best = { year: y, day: d };
          }
          if (found.best && found.best.year === y) break;
        }
      }
    }
    const best = found.best;
    return best ? this.hive.dateFromDayOfYear(best.year, best.day) : null;
  }

  /** Rows from the given files inside the window, decoded. */
  async readFiles(
    files: IndexedFile[],
    fromIso: string,
    toIso: string,
    limit: number
  ): Promise<{ rows: PlaybackRow[]; capped: boolean }> {
    const rows: PlaybackRow[] = [];
    if (files.length === 0) return { rows, capped: false };
    const groups = new Map<IndexedFile['kind'], IndexedFile[]>();
    for (const f of files) {
      const list = groups.get(f.kind) ?? [];
      list.push(f);
      groups.set(f.kind, list);
    }
    const byName = new Map(files.map(f => [f.filename, f]));
    let capped = false;
    const connection = await DuckDBPool.getConnection();
    try {
      for (const [kind, group] of groups) {
        const list = group
          .map(f => `'${escapeSqlString(f.filename)}'`)
          .join(', ');
        // Exported object paths carry their flattened `value_*` components;
        // `value_json` is kept only where a file has it.
        const components =
          kind === 'object'
            ? [...new Set(group.flatMap(f => f.components))].sort()
            : [];
        const valueExpr =
          kind === 'object'
            ? [
                group.some(f => f.hasValueJson)
                  ? 'value_json AS v'
                  : 'NULL AS v',
                ...components.map(c => `"value_${c}"`),
              ].join(', ')
            : 'CAST(value AS VARCHAR) AS v';
        const contextExpr = group.some(f => f.hasContext)
          ? 'context'
          : 'NULL AS context';
        const sourceExpr = group.some(f => f.hasSource)
          ? 'source_label'
          : 'NULL AS source_label';
        const sql = `SELECT filename, ${contextExpr}, signalk_timestamp, ${sourceExpr}, ${valueExpr}
          FROM read_parquet([${list}], hive_partitioning=false, union_by_name=true, filename=true)
          WHERE ${FILENAME_EXCLUSIONS}
            AND signalk_timestamp >= '${escapeSqlString(fromIso)}'
            AND signalk_timestamp < '${escapeSqlString(toIso)}'
          ORDER BY signalk_timestamp
          LIMIT ${limit + 1}`;
        let result: Array<Record<string, unknown>>;
        try {
          result = (
            await connection.runAndReadAll(sql)
          ).getRowObjects() as Array<Record<string, unknown>>;
        } catch (err) {
          if (isNoFilesError(err)) continue;
          throw err;
        }
        if (result.length > limit) {
          capped = true;
          result = result.slice(0, limit);
        }
        for (const r of result) {
          const file = byName.get(String(r.filename));
          if (!file) continue;
          const text = r.v == null ? null : String(r.v);
          const value =
            kind === 'object'
              ? (decodeJson(text) ?? objectFromComponents(r, components))
              : decodeScalar(text, kind);
          rows.push({
            context:
              typeof r.context === 'string' && r.context
                ? r.context
                : file.context,
            path: file.path,
            timestamp: String(r.signalk_timestamp),
            source: typeof r.source_label === 'string' ? r.source_label : null,
            value,
          });
        }
      }
    } finally {
      connection.disconnectSync();
    }
    return { rows, capped };
  }

  /** A vessel's identity as last recorded at or before `atIso`, if any. */
  async identityAt(
    context: string,
    atIso: string
  ): Promise<IdentityComponents | undefined> {
    if (this.buffer?.isOpen()) {
      const row = this.buffer.getLatestObjectRowAt(
        IDENTITY_PATH,
        context,
        atIso
      );
      const parsed = row ? decodeJson(row.value_json) : null;
      if (parsed && typeof parsed === 'object')
        return parsed as IdentityComponents;
    }
    const dir = path.join(
      this.dataDir,
      'tier=raw',
      `context=${this.hive.sanitizeContext(context)}`,
      `path=${this.hive.sanitizePath(IDENTITY_PATH)}`
    );
    if (!(await fs.pathExists(dir))) return undefined;
    const glob = this.hive.getGlobPattern(
      this.dataDir,
      'raw',
      context,
      IDENTITY_PATH
    );
    let connection: Connection;
    try {
      connection = await DuckDBPool.getConnection();
    } catch {
      return undefined;
    }
    try {
      const result = await connection.runAndReadAll(
        `SELECT value_json FROM read_parquet('${escapeSqlString(glob)}', hive_partitioning=false, union_by_name=true, filename=true)
         WHERE ${FILENAME_EXCLUSIONS} AND context = '${escapeSqlString(context)}'
           AND signalk_timestamp <= '${escapeSqlString(atIso)}'
         ORDER BY signalk_timestamp DESC LIMIT 1`
      );
      const rows = result.getRowObjects() as Array<{ value_json: unknown }>;
      const parsed = rows.length
        ? decodeJson(String(rows[0].value_json))
        : null;
      return parsed && typeof parsed === 'object'
        ? (parsed as IdentityComponents)
        : undefined;
    } catch (err) {
      if (!isNoFilesError(err)) {
        this.debug(`[Playback] identity lookup failed for ${context}: ${err}`);
      }
      return undefined;
    } finally {
      connection.disconnectSync();
    }
  }
}

/**
 * One playback connection: reads windows ahead of the clock, paces deltas
 * by the playback rate, announces each vessel's identity before its first
 * delta, and stops the moment the server says so.
 */
class PlaybackSession {
  private stopped = false;
  /** Every sleep still pending, each with its own timer and resolver. */
  private readonly sleepers = new Set<{
    timer: NodeJS.Timeout;
    resolve: () => void;
  }>();
  private readonly rate: number;
  private windowMs = WINDOW_MS;
  private dayIndex: DayIndex | undefined;
  private readonly announced = new Set<string>();

  constructor(
    private readonly provider: PlaybackProvider,
    private readonly dataDir: string,
    private readonly buffer: PlaybackBufferSource | undefined,
    private readonly contexts: string[] | null,
    private readonly options: PlaybackOptions,
    private readonly onChange: DeltaSink,
    private readonly debug: (msg: string) => void
  ) {
    const r = Number(options.playbackRate);
    this.rate =
      Number.isFinite(r) && r > 0
        ? Math.min(MAX_RATE, Math.max(MIN_RATE, r))
        : 1;
  }

  stop(): void {
    this.stopped = true;
    for (const s of this.sleepers) {
      clearTimeout(s.timer);
      s.resolve();
    }
    this.sleepers.clear();
  }

  async run(): Promise<void> {
    const start = this.options.startTime;
    if (!(start instanceof Date) || Number.isNaN(start.getTime())) {
      this.debug('[Playback] invalid startTime; nothing to replay');
      return;
    }
    this.debug(
      `[Playback] start ${start.toISOString()} rate ${this.rate} scope ${this.contexts ? this.contexts.join(',') : 'all vessels'}`
    );
    let cursor = start;
    let next = this.fetch(cursor);
    while (!this.stopped) {
      const chunk = await next;
      if (this.stopped) break;
      if (chunk.capped) {
        this.windowMs = Math.max(MIN_WINDOW_MS, Math.floor(this.windowMs / 2));
        if (this.windowMs > MIN_WINDOW_MS || chunk.rows.length === 0) {
          next = this.fetch(cursor);
          continue;
        }
        // At the smallest window the flood plays as read.
      } else if (this.windowMs < WINDOW_MS) {
        this.windowMs = Math.min(WINDOW_MS, this.windowMs * 2);
      }
      const chunkEnd = new Date(chunk.toIso);
      // Every turn of this loop must either move the cursor forward or
      // sleep. An empty window at the present cannot move it, so it waits;
      // an empty window in the past is silence and is skipped at once. A
      // window that is neither is a bug, and waits rather than spins.
      let caughtUp = chunk.live;
      if (
        !caughtUp &&
        chunk.rows.length === 0 &&
        chunkEnd.getTime() <= cursor.getTime()
      ) {
        this.debug(
          `[Playback] window ${chunk.fromIso}..${chunk.toIso} made no progress; waiting`
        );
        caughtUp = true;
      }
      // Read the next window while this one plays; once caught up, wait a
      // moment first so the buffer has something new.
      next = caughtUp
        ? this.sleep(LIVE_POLL_MS).then(() => this.fetch(chunkEnd))
        : this.fetch(chunkEnd);
      await this.play(chunk);
      cursor = chunkEnd;
    }
    this.debug('[Playback] stopped');
  }

  /**
   * Read one window starting at `from`. Silence is skipped: when nothing is
   * recorded between `from` and the next known row, the window starts there.
   */
  private async fetch(from: Date): Promise<Chunk> {
    let fromMs = from.getTime();
    const now = Date.now() - LIVE_LAG_MS;
    if (fromMs > now) fromMs = now;
    const day = new Date(fromMs);
    await this.ensureDayIndex(day);
    const files = this.dayIndex?.files ?? [];

    // Jump over silence to the next recorded instant, within this day.
    const fromIso = new Date(fromMs).toISOString();
    const dayEndMs = Math.min(startOfNextDay(day).getTime(), now);
    let firstMs = Number.POSITIVE_INFINITY;
    for (const f of files) {
      if (f.t1 !== null && f.t1 < fromIso) continue;
      const t0 =
        f.t0 === null ? fromMs : Math.max(fromMs, Date.parse(f.t0) || fromMs);
      if (t0 < firstMs) firstMs = t0;
    }
    if (this.buffer?.isOpen()) {
      const t = this.buffer.getNextRowTime(fromIso, this.contexts);
      const ms = t ? Date.parse(t) : NaN;
      if (Number.isFinite(ms) && ms < firstMs) firstMs = Math.max(fromMs, ms);
    }
    if (firstMs === Number.POSITIVE_INFINITY || firstMs >= dayEndMs) {
      // Nothing more today. Hand back an empty window that ends where the
      // next recorded data can start: the next day with files or the next
      // buffer row, whichever is first, or the present when there is none.
      let jumpMs = dayEndMs;
      if (dayEndMs < now) {
        const nextDay = await this.provider.nextParquetDay(
          startOfNextDay(day),
          this.contexts
        );
        let candidate = nextDay ? nextDay.getTime() : Number.POSITIVE_INFINITY;
        if (Number.isFinite(firstMs) && firstMs < candidate)
          candidate = firstMs;
        jumpMs = Number.isFinite(candidate)
          ? Math.max(dayEndMs, Math.min(candidate, now))
          : now;
      }
      const toIso = new Date(jumpMs).toISOString();
      return { fromIso, toIso, rows: [], capped: false, live: jumpMs >= now };
    }
    const startMs = firstMs;
    const endMs = Math.min(startMs + this.windowMs, dayEndMs);
    const startIso = new Date(startMs).toISOString();
    const endIso = new Date(endMs).toISOString();

    const overlapping = files.filter(
      f =>
        !(f.t1 !== null && f.t1 < startIso) &&
        !(f.t0 !== null && f.t0 >= endIso)
    );
    const parquet = await this.provider.readFiles(
      overlapping,
      startIso,
      endIso,
      MAX_ROWS_PER_WINDOW
    );
    const rows = parquet.rows;
    let capped = parquet.capped;
    if (this.buffer?.isOpen()) {
      const perPath = MAX_ROWS_PER_WINDOW;
      const bufferRows = this.buffer.getRowsForPlayback(
        startIso,
        endIso,
        this.contexts,
        perPath + 1
      );
      let count = 0;
      for (const r of bufferRows) {
        count += 1;
        if (count > perPath) {
          capped = true;
          break;
        }
        rows.push({
          context: r.context,
          path: r.path,
          timestamp: r.signalk_timestamp,
          source: r.source_label,
          value:
            r.value_json !== null
              ? decodeJson(r.value_json)
              : decodeScalar(r.value, 'unknown'),
        });
      }
      if (rows.length > MAX_ROWS_PER_WINDOW) capped = true;
    }
    return {
      fromIso: startIso,
      toIso: endIso,
      rows,
      capped,
      live: endMs >= now,
    };
  }

  private async ensureDayIndex(day: Date): Promise<void> {
    const key = dayKey(day);
    if (this.dayIndex?.key === key) return;
    const files = await this.provider.indexDay(day, this.contexts);
    this.dayIndex = { key, files };
    this.debug(
      `[Playback] indexed ${files.length} file(s) for ${key}: ${files
        .map(f => `${f.path}=${f.kind}${f.t0 ? '' : '(no stats)'}`)
        .join(' ')}`
    );
  }

  /** Emit a window's deltas, each at its moment on the playback clock. */
  private async play(chunk: Chunk): Promise<void> {
    if (chunk.rows.length === 0) return;
    const deltas = groupRowsIntoDeltas(chunk.rows);
    const wallStart = Date.now();
    const dataStart = Date.parse(deltas[0].updates[0].timestamp);
    for (const delta of deltas) {
      if (this.stopped) return;
      const at = Date.parse(delta.updates[0].timestamp);
      const due =
        Number.isFinite(at) && Number.isFinite(dataStart)
          ? wallStart + (at - dataStart) / this.rate
          : wallStart;
      const wait = due - Date.now();
      if (wait > 0) await this.sleep(wait);
      if (this.stopped) return;
      await this.announce(delta.context, delta.updates[0].timestamp);
      if (this.stopped) return;
      this.onChange(delta);
    }
  }

  /** Before a vessel's first delta, replay its last known identity. */
  private async announce(context: string, timestamp: string): Promise<void> {
    if (this.announced.has(context)) return;
    this.announced.add(context);
    const identity = await this.provider.identityAt(context, timestamp);
    if (!identity || this.stopped) return;
    const values = unfoldIdentity(identity);
    if (values.length === 0) return;
    this.onChange({ context, updates: [{ timestamp, values }] });
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => {
      if (this.stopped) {
        resolve();
        return;
      }
      const sleeper = {
        timer: setTimeout(() => {
          this.sleepers.delete(sleeper);
          resolve();
        }, ms),
        resolve,
      };
      sleeper.timer.unref();
      this.sleepers.add(sleeper);
    });
  }
}

/**
 * Register with the server as its v1 playback provider. Returns false, with
 * a debug line, on a host that has no such registry.
 */
export function registerPlaybackProvider(
  app: ServerAPI,
  provider: PlaybackProvider,
  debug: (msg: string) => void
): boolean {
  const host = app as unknown as {
    registerHistoryProvider?: (p: unknown) => void;
  };
  if (typeof host.registerHistoryProvider !== 'function') {
    debug(
      '[Playback] server has no registerHistoryProvider; playback not offered'
    );
    return false;
  }
  host.registerHistoryProvider(provider);
  debug('[Playback] registered as v1 history playback provider');
  return true;
}
