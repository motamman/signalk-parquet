/**
 * Records vessel identity (see utils/vessel-identity.ts) for every vessel the
 * server hears, independent of the path configuration.
 *
 * Restraint rules, so that a fleet of passing AIS targets does not turn into
 * a file per target per path per day:
 *   - identity is written once when a vessel is first heard and again only
 *     when a component changes, never on the six-minute repeat of an AIS
 *     static report;
 *   - a write that only completes the last row (more fields, nothing
 *     different) extends that row in place while it is still in the buffer,
 *     so a vessel whose identity arrives one path per message, as an
 *     upstream Signal K server replays its cache, still ends up as one row.
 *     The buffer decides that, in one operation, from what is stored;
 *     this service holds no row ids;
 *   - what was last written per vessel is read back from the buffer at
 *     start, so a restart does not rewrite known vessels however the
 *     previous run ended. A small JSON file in the data directory keeps the
 *     same record for vessels older than the buffer's retention.
 *
 * Rows go through the ordinary SQLite buffer and daily export, so the history
 * providers read them like any other object path.
 */

import * as fs from 'fs-extra';
import * as path from 'path';
import { NormalizedDelta, Path, ServerAPI } from '@signalk/server-api';
import { DataRecord, PluginState } from '../types';
import { HivePathBuilder } from '../utils/hive-path-builder';
import { latestStoredIdentity } from '../utils/stored-identity';
import {
  disposeStreamSubscription,
  StreamSubscription,
} from '../utils/stream-subscription';
import {
  IDENTITY_PATH,
  IDENTITY_PATHS,
  IDENTITY_ROOT_KEYS,
  IdentityComponents,
  identityFromDelta,
  mergeIdentity,
} from '../utils/vessel-identity';

/**
 * Vessels held in memory: the working set (`tracked`, emptied when full) and
 * the last-written identities (`lastWritten`, least recently written dropped
 * first). Memory is only a cache of what is stored: a vessel no longer held is
 * looked up in the buffer and its parquet files before anything is written,
 * so dropping one never writes its identity again (#137).
 */
const MAX_TRACKED_CONTEXTS = 20_000;

/** How often the last-written map is flushed to disk while running. */
const PERSIST_INTERVAL_MS = 10 * 60 * 1000;

const STATE_FILE = 'identity-state.json';

interface TrackedVessel {
  known: IdentityComponents;
  /** The delta that last changed `known`; its stamp and source go on the row. */
  timestamp?: string;
  source?: string;
  /** True when `known` differs from what was last written. */
  dirty: boolean;
  /**
   * True while its stored identity is being read from parquet: values are
   * folded into `known` meanwhile, and whether to write is decided once the
   * stored identity is in.
   */
  loading?: boolean;
}

export class VesselIdentityService {
  private readonly tracked = new Map<string, TrackedVessel>();
  /**
   * Canonical JSON of the components last written per context (persisted),
   * least recently written first, at most `maxRemembered` of them.
   */
  private lastWritten = new Map<string, string>();
  private subscriptions: StreamSubscription[] = [];
  private persistTimer?: NodeJS.Timeout;
  private stateDirty = false;
  private running = false;
  /** Contexts whose report is still arriving; written once it has (flushPending). */
  private readonly pending = new Set<string>();
  private flushScheduled = false;
  /** Parquet lookups, one at a time; stop() waits for the one in flight. */
  private lookups: Promise<void> = Promise.resolve();
  private lookupsInFlight = 0;
  private readonly hive = new HivePathBuilder();
  private readonly maxRemembered: number;

  constructor(
    private readonly app: ServerAPI,
    private readonly state: PluginState,
    private readonly dataDir: string,
    private readonly debug: (msg: string) => void,
    options: { maxRemembered?: number } = {}
  ) {
    this.maxRemembered = options.maxRemembered ?? MAX_TRACKED_CONTEXTS;
  }

  private get stateFile(): string {
    return path.join(this.dataDir, STATE_FILE);
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loadState();
    this.readBackFromBuffer();

    // The identity paths, through the plugin API's per-path buses, as the
    // data handler subscribes. The server splits every delta into one bus
    // event per value (streambundle pushDelta), each carrying the update's
    // timestamp and $source, and pushes them all synchronously, so an AIS
    // static report arrives as several events that share a stamp within one
    // pass; `absorb` folds them in and the write waits for the end of that
    // pass (flushPending), so a report is one row. Root keys (name, mmsi)
    // arrive on the root bus as one object. No debounce: it would drop the
    // later pieces of a report.
    this.subscribe('' as Path, (d: NormalizedDelta) => {
      const v = d.value as Record<string, unknown> | null;
      return (
        v !== null &&
        typeof v === 'object' &&
        IDENTITY_ROOT_KEYS.some(key => v[key] !== undefined)
      );
    });
    for (const p of IDENTITY_PATHS) {
      this.subscribe(p as Path, () => true);
    }

    // The server emits its defaults (self name, mmsi, design.*) once at
    // start, before plugins load, and AIS targets heard before a plugin
    // restart do not repeat for up to six minutes. Seed from the in-memory
    // model so neither is missed; a seeded vessel is written like any other,
    // and only if it differs from what was last written.
    this.seedFromModel();

    this.persistTimer = setInterval(
      () => this.persistState(),
      PERSIST_INTERVAL_MS
    );
    this.persistTimer.unref();
    this.debug(
      `[Identity] Recording vessel identity on '${IDENTITY_PATH}' for ${this.lastWritten.size} known vessel(s)`
    );
  }

  /**
   * Resolves once stopped. A parquet lookup still in flight finishes first,
   * while the service is running, so a vessel heard just before the stop is
   * decided and written like any other; it also holds a DuckDB connection,
   * and the plugin closes the pool after this.
   */
  stop(): Promise<void> {
    if (!this.running) return Promise.resolve();
    // Without a lookup in flight the stop is immediate, as it always was.
    if (this.lookupsInFlight === 0) {
      this.shutdown();
      return Promise.resolve();
    }
    return this.lookups.then(() => {
      if (this.running) this.shutdown();
    });
  }

  private shutdown(): void {
    // A report that arrived in this same pass is written, not dropped: the
    // plugin stops this service before it closes the buffer.
    this.flushPending();
    this.running = false;
    for (const s of this.subscriptions) {
      disposeStreamSubscription(s);
    }
    this.subscriptions = [];
    if (this.persistTimer) {
      clearInterval(this.persistTimer);
      this.persistTimer = undefined;
    }
    this.persistState();
    this.tracked.clear();
  }

  /** One bus subscription; vessel contexts and real values only. */
  private subscribe(
    busPath: Path,
    accept: (d: NormalizedDelta) => boolean
  ): void {
    const stream = this.app.streambundle
      .getBus(busPath)
      .filter((d: NormalizedDelta) => {
        if (!this.running || d.isMeta) return false;
        const context = String(d.context ?? '');
        return context.startsWith('vessels.') && accept(d);
      })
      .onValue((d: NormalizedDelta) => {
        this.absorb(
          String(d.context),
          busPath,
          d.value,
          d.timestamp,
          d.$source
        );
      });
    this.subscriptions.push(stream as StreamSubscription);
  }

  /**
   * Fold one value for `busPath` (a Signal K path, '' for the root) into the
   * vessel's known identity. From the buses, a change is written once the
   * report it belongs to has fully arrived (see flushPending). Callers that
   * fold several values themselves pass `writeNow = false` and write once
   * afterwards.
   */
  private absorb(
    context: string,
    busPath: string,
    value: unknown,
    timestamp: string | undefined,
    source: string | undefined,
    writeNow = true
  ): void {
    const incoming = identityFromDelta(busPath, value);
    if (Object.keys(incoming).length === 0) return;

    // A value stamped differently is a different report. The one still
    // pending is written first, so two reports are two rows even when they
    // arrive in one pass.
    if (this.pending.has(context)) {
      const waiting = this.tracked.get(context);
      if (waiting && waiting.timestamp !== timestamp) {
        this.pending.delete(context);
        if (waiting.dirty) this.write(context, waiting);
      }
    }

    let vessel = this.tracked.get(context);
    if (!vessel) {
      if (this.tracked.size >= MAX_TRACKED_CONTEXTS) {
        this.tracked.clear();
      }
      // Start from what was last written, so a replay of known values after
      // a restart (an upstream cache sent one path per message) is not
      // mistaken for a change. What is not in memory is asked of the
      // buffer, synchronously; only a vessel whose identity is on disk but
      // no longer in the buffer waits for a parquet read.
      vessel = { known: {}, dirty: false };
      this.tracked.set(context, vessel);
      const written =
        this.lastWritten.get(context) ?? this.recallFromBuffer(context);
      if (written !== undefined) {
        vessel.known = parseIdentity(written) ?? {};
      } else if (this.hasIdentityOnDisk(context)) {
        vessel.loading = true;
        this.recallFromParquet(context, vessel);
      }
    }
    if (mergeIdentity(vessel.known, incoming)) {
      vessel.timestamp = timestamp;
      vessel.source = source;
      // Decided once the stored identity is in (recallFromParquet).
      if (vessel.loading) return;
      vessel.dirty = canonical(vessel.known) !== this.lastWritten.get(context);
      if (vessel.dirty && writeNow) this.schedule(context);
    }
  }

  /**
   * The vessel's last identity row in the buffer, exported or not, as
   * canonical JSON, remembered; undefined when the buffer has none.
   */
  private recallFromBuffer(context: string): string | undefined {
    const buffer = this.state.sqliteBuffer;
    if (!buffer || !buffer.isOpen()) return undefined;
    let row: ReturnType<typeof buffer.getLatestObjectRowAt>;
    try {
      row = buffer.getLatestObjectRowAt(
        IDENTITY_PATH,
        context,
        '9999-12-31T23:59:59.999Z'
      );
    } catch {
      return undefined;
    }
    const known = parseIdentity(row?.value_json ?? undefined);
    if (!known) return undefined;
    const written = canonical(known);
    this.remember(context, written);
    return written;
  }

  /** Whether the vessel has an identity directory in the raw tier. */
  private hasIdentityOnDisk(context: string): boolean {
    return fs.existsSync(
      path.join(
        this.dataDir,
        'tier=raw',
        `context=${this.hive.sanitizeContext(context)}`,
        `path=${this.hive.sanitizePath(IDENTITY_PATH)}`
      )
    );
  }

  /**
   * Read the vessel's newest identity from its parquet files, then decide
   * as for any vessel: write only if what has arrived since differs from it.
   * A read that fails is taken as nothing on file, as for a new vessel: an
   * identity is not left unrecorded because a file could not be read.
   */
  private recallFromParquet(context: string, vessel: TrackedVessel): void {
    this.lookupsInFlight += 1;
    this.lookups = this.lookups.then(async () => {
      try {
        await this.decideFromParquet(context, vessel);
      } catch (error) {
        // Caught here so the chain stays resolved: one rejected link would
        // skip every lookup queued after it, leaving those vessels waiting
        // for good, and make stop() reject before it shuts down.
        this.debug(
          `[Identity] Deciding the identity of ${context} failed: ${(error as Error).message}`
        );
        // Its next report is then decided as for any vessel, not held.
        vessel.loading = false;
      } finally {
        this.lookupsInFlight -= 1;
      }
    });
  }

  private async decideFromParquet(
    context: string,
    vessel: TrackedVessel
  ): Promise<void> {
    if (!this.running) return;
    let stored: IdentityComponents | null = null;
    try {
      const raw = await latestStoredIdentity(
        this.dataDir,
        context,
        new Date().toISOString()
      );
      stored = raw ? fromStoredRow(raw) : null;
    } catch (error) {
      this.app.error(
        `[Identity] Could not read the stored identity of ${context}: ${(error as Error).message}`
      );
    }
    if (!this.running || this.tracked.get(context) !== vessel) return;
    vessel.loading = false;
    const known: IdentityComponents = stored ? { ...stored } : {};
    if (stored) this.remember(context, canonical(stored));
    mergeIdentity(known, vessel.known);
    vessel.known = known;
    vessel.dirty = canonical(known) !== this.lastWritten.get(context);
    if (vessel.dirty) this.schedule(context);
  }

  /**
   * Record what was last written for a context as the most recent entry,
   * dropping the least recently written past the limit.
   */
  private remember(context: string, written: string): void {
    if (this.lastWritten.get(context) !== written) this.stateDirty = true;
    this.lastWritten.delete(context);
    this.lastWritten.set(context, written);
    this.trimLastWritten();
  }

  private trimLastWritten(): void {
    while (this.lastWritten.size > this.maxRemembered) {
      this.lastWritten.delete(this.lastWritten.keys().next().value as string);
      this.stateDirty = true;
    }
  }

  /**
   * Write the context's identity once the current pass is over. The server
   * pushes every value of a delta onto its bus synchronously, so by the time a
   * microtask runs the whole report has been folded in. Writing after each
   * value instead wrote a partial identity first whenever a report changed
   * two fields already known: it could not extend the stored row (a field
   * had changed), so it inserted, and the next value inserted again.
   */
  private schedule(context: string): void {
    this.pending.add(context);
    if (this.flushScheduled) return;
    this.flushScheduled = true;
    queueMicrotask(() => this.flushPending());
  }

  private flushPending(): void {
    this.flushScheduled = false;
    const contexts = [...this.pending];
    this.pending.clear();
    for (const context of contexts) {
      const vessel = this.tracked.get(context);
      if (vessel?.dirty) this.write(context, vessel);
    }
  }

  /**
   * Seed known identities from the server's in-memory full model. In-memory
   * only, once per start: no filesystem or network access.
   */
  private seedFromModel(): void {
    let vessels: unknown;
    try {
      vessels = this.app.getPath('vessels');
    } catch {
      return;
    }
    if (!vessels || typeof vessels !== 'object') return;
    let seeded = 0;
    for (const [id, model] of Object.entries(
      vessels as Record<string, unknown>
    )) {
      if (!model || typeof model !== 'object') continue;
      if (seeded >= MAX_TRACKED_CONTEXTS) break;
      const context = `vessels.${id}`;
      const root = model as Record<string, unknown>;
      // Root keys (name, mmsi) sit bare on the vessel object.
      this.absorb(
        context,
        '',
        { name: unwrapLeaf(root.name), mmsi: unwrapLeaf(root.mmsi) },
        undefined,
        undefined,
        false
      );
      // The snapshot row carries the newest leaf stamp and its source, not
      // whichever leaf happened to come last in IDENTITY_PATHS order.
      let newestMs = Number.NEGATIVE_INFINITY;
      let newest: { timestamp: string; source?: string } | undefined;
      for (const p of IDENTITY_PATHS) {
        const leaf = getDotted(root, p);
        if (leaf === undefined || leaf === null) continue;
        const timestamp = leafField(leaf, 'timestamp');
        const source = leafField(leaf, '$source');
        this.absorb(context, p, unwrapLeaf(leaf), timestamp, source, false);
        if (timestamp === undefined) continue;
        const ms = Date.parse(timestamp);
        if (Number.isNaN(ms) || ms <= newestMs) continue;
        newestMs = ms;
        newest = { timestamp, source };
      }
      // One row per vessel for the whole seeded identity, not one per part.
      const vessel = this.tracked.get(context);
      if (vessel?.dirty) {
        if (newest) {
          vessel.timestamp = newest.timestamp;
          vessel.source = newest.source;
        }
        this.write(context, vessel);
      }
      seeded += 1;
    }
    this.debug(
      `[Identity] Seeded identity from the model for ${seeded} vessel(s)`
    );
  }

  /**
   * Write the vessel's identity. When the last row is still in the buffer
   * and the new identity only completes it, that row is extended in place;
   * otherwise a new row is inserted.
   */
  private write(context: string, vessel: TrackedVessel): void {
    const buffer = this.state.sqliteBuffer;
    if (!buffer || !buffer.isOpen()) {
      // No buffer, no identity row; the next change or restart tries again.
      return;
    }
    const value = { ...vessel.known };
    const now = new Date().toISOString();
    const record: DataRecord = {
      received_timestamp: now,
      signalk_timestamp: vessel.timestamp || now,
      context,
      path: IDENTITY_PATH,
      value: null,
      value_json: value,
      source_label: vessel.source,
    };
    for (const [key, v] of Object.entries(value)) {
      if (v !== undefined) record[`value_${key}`] = v;
    }
    try {
      // One buffer operation decides between extending the vessel's latest
      // unexported row and inserting a new one, because that decision depends
      // on what is stored. The service does not track row ids: it cannot know
      // whether the row it last wrote is still there to extend, and asking
      // then writing would be two calls with a gap in between.
      buffer.insertOrExtendLatest(record);
    } catch (error) {
      this.app.error(
        `[Identity] Failed to record identity for ${context}: ${(error as Error).message}`
      );
      return;
    }
    vessel.dirty = false;
    this.remember(context, canonical(vessel.known));
  }

  /**
   * The buffer holds what was actually written, however the previous run
   * ended; the state file holds only what a clean stop or the periodic
   * flush managed to save. Lay the buffer over the file, so a killed server
   * does not rewrite the vessels it heard in its last minutes.
   */
  private readBackFromBuffer(): void {
    const buffer = this.state.sqliteBuffer;
    if (!buffer || !buffer.isOpen()) return;
    let rows: ReturnType<typeof buffer.getLatestRowPerContext>;
    try {
      rows = buffer.getLatestRowPerContext(IDENTITY_PATH);
    } catch (error) {
      this.app.error(
        `[Identity] Could not read identity rows back from the buffer: ${(error as Error).message}`
      );
      return;
    }
    for (const row of rows) {
      const known = parseIdentity(row.value_json ?? undefined);
      if (!known) continue;
      // Rows in the buffer are the newest written, so they go last.
      this.remember(row.context, canonical(known));
    }
    this.persistState();
  }

  private loadState(): void {
    try {
      const raw = fs.readJsonSync(this.stateFile) as {
        lastWritten?: Record<string, string>;
      };
      if (raw && raw.lastWritten && typeof raw.lastWritten === 'object') {
        this.lastWritten = new Map();
        for (const [context, json] of Object.entries(raw.lastWritten)) {
          const known = parseIdentity(json);
          if (known) this.lastWritten.set(context, canonical(known));
        }
        // A file saved before the limit existed is cut to it here, the
        // least recently written first, as the file keeps the map's order.
        this.trimLastWritten();
      }
    } catch {
      this.lastWritten = new Map();
    }
  }

  private persistState(): void {
    if (!this.stateDirty) return;
    try {
      // Write beside the file and rename over it, so a crash mid-write
      // leaves the previous state intact rather than a truncated file.
      const tmp = `${this.stateFile}.tmp`;
      fs.outputJsonSync(tmp, {
        lastWritten: Object.fromEntries(this.lastWritten),
      });
      fs.renameSync(tmp, this.stateFile);
      this.stateDirty = false;
    } catch (error) {
      this.debug(
        `[Identity] Could not persist identity state: ${(error as Error).message}`
      );
    }
  }
}

/** JSON of the components with keys in a fixed order, so equal identities compare equal. */
function canonical(components: IdentityComponents): string {
  const sorted: Record<string, unknown> = {};
  for (const key of Object.keys(components).sort()) {
    const v = (components as Record<string, unknown>)[key];
    if (v !== undefined) sorted[key] = v;
  }
  return JSON.stringify(sorted);
}

/** Expected value type of every supported identity component. */
const IDENTITY_FIELD_TYPES: Record<
  keyof IdentityComponents,
  'string' | 'number'
> = {
  name: 'string',
  mmsi: 'string',
  aisShipTypeId: 'number',
  aisShipTypeName: 'string',
  lengthOverall: 'number',
  beam: 'number',
  callsignVhf: 'string',
  aisClass: 'string',
};

/**
 * Parse persisted identity JSON (state file or buffer row). Only the
 * supported components with values of the expected type are accepted; an
 * unknown key or a wrongly typed value makes the whole record invalid, so
 * arbitrary persisted keys never reach `write` as `value_*` columns.
 */
function parseIdentity(json: string | undefined): IdentityComponents | null {
  if (!json) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(json);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return null;
  }
  const out: IdentityComponents = {};
  for (const [key, value] of Object.entries(
    parsed as Record<string, unknown>
  )) {
    if (!Object.prototype.hasOwnProperty.call(IDENTITY_FIELD_TYPES, key)) {
      return null;
    }
    const field = key as keyof IdentityComponents;
    if (value === undefined || value === null) continue;
    const expected = IDENTITY_FIELD_TYPES[field];
    if (typeof value !== expected) return null;
    if (expected === 'number' && !Number.isFinite(value as number)) return null;
    (out as Record<string, unknown>)[field] = value;
  }
  return out;
}

/**
 * Identity components from a stored parquet row, as the file typed them: a
 * numeric-looking text column such as the MMSI comes back a number and is
 * turned back into text. Keys that are not identity components are dropped.
 */
function fromStoredRow(
  row: Record<string, unknown>
): IdentityComponents | null {
  const out: Record<string, unknown> = {};
  for (const [key, expected] of Object.entries(IDENTITY_FIELD_TYPES)) {
    const v = row[key];
    if (v === null || v === undefined) continue;
    if (expected === 'string') {
      if (typeof v === 'string') out[key] = v;
      else if (typeof v === 'number' && Number.isFinite(v))
        out[key] = String(v);
    } else {
      const n = Number(v);
      if (Number.isFinite(n)) out[key] = n;
    }
  }
  return Object.keys(out).length > 0 ? (out as IdentityComponents) : null;
}

/** Walk a dotted path through nested plain objects. */
function getDotted(root: Record<string, unknown>, dotted: string): unknown {
  let cur: unknown = root;
  for (const key of dotted.split('.')) {
    if (!cur || typeof cur !== 'object') return undefined;
    cur = (cur as Record<string, unknown>)[key];
  }
  return cur;
}

/**
 * A full-model leaf is `{ value, timestamp, $source, meta }`; root keys such
 * as `name` are bare. Return the bare value either way.
 */
function unwrapLeaf(leaf: unknown): unknown {
  if (leaf && typeof leaf === 'object' && 'value' in leaf) {
    return (leaf as { value: unknown }).value;
  }
  return leaf;
}

function leafField(leaf: unknown, field: string): string | undefined {
  if (leaf && typeof leaf === 'object') {
    const v = (leaf as Record<string, unknown>)[field];
    return typeof v === 'string' ? v : undefined;
  }
  return undefined;
}

export { IDENTITY_ROOT_KEYS };
