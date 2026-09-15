/**
 * Records vessel identity (see utils/vessel-identity.ts) for every vessel the
 * server hears, independent of the path configuration.
 *
 * Restraint rules, so that a fleet of passing AIS targets does not turn into
 * a file per target per path per day:
 *   - identity is written once when a vessel is first heard and again only
 *     when a component changes, never on the six-minute repeat of an AIS
 *     static report;
 *   - what was last written per vessel is persisted to a small JSON file in
 *     the data directory, so a plugin restart does not rewrite every vessel.
 *
 * Rows go through the ordinary SQLite buffer and daily export, so the history
 * providers read them like any other object path.
 */

import * as fs from 'fs-extra';
import * as path from 'path';
import { ServerAPI } from '@signalk/server-api';
import { DataRecord, PluginState } from '../types';
import {
  IDENTITY_PATH,
  IDENTITY_PATHS,
  IDENTITY_ROOT_KEYS,
  IdentityComponents,
  identityFromDelta,
  mergeIdentity,
} from '../utils/vessel-identity';

/** Vessels tracked in memory before the maps are cleared and rebuilt. */
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
}

/** Minimal shape of a whole delta message as emitted on `app.signalk`. */
interface DeltaMessage {
  context?: string;
  updates?: Array<{
    timestamp?: string;
    $source?: string;
    values?: Array<{ path?: string; value?: unknown }>;
    meta?: unknown[];
  }>;
}

/** The delta emitter the server keeps on the plugin app object. */
interface DeltaEmitter {
  on(event: 'delta', handler: (delta: DeltaMessage) => void): unknown;
  removeListener(
    event: 'delta',
    handler: (delta: DeltaMessage) => void
  ): unknown;
}

export class VesselIdentityService {
  private readonly tracked = new Map<string, TrackedVessel>();
  /** JSON of the components last written per context (persisted). */
  private lastWritten = new Map<string, string>();
  private readonly onDeltaBound = (delta: DeltaMessage) => this.onDelta(delta);
  private persistTimer?: NodeJS.Timeout;
  private stateDirty = false;
  private running = false;

  constructor(
    private readonly app: ServerAPI,
    private readonly state: PluginState,
    private readonly dataDir: string,
    private readonly debug: (msg: string) => void
  ) {}

  private get stateFile(): string {
    return path.join(this.dataDir, STATE_FILE);
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loadState();

    // Whole delta messages, before the server splits them per path, so one
    // AIS static report folds into one row.
    this.emitter.on('delta', this.onDeltaBound);

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

  stop(): void {
    if (!this.running) return;
    this.running = false;
    try {
      this.emitter.removeListener('delta', this.onDeltaBound);
    } catch {
      // Best-effort.
    }
    if (this.persistTimer) {
      clearInterval(this.persistTimer);
      this.persistTimer = undefined;
    }
    this.persistState();
    this.tracked.clear();
  }

  private get emitter(): DeltaEmitter {
    return (this.app as unknown as { signalk: DeltaEmitter }).signalk;
  }

  private onDelta(delta: DeltaMessage): void {
    if (!this.running || !delta || !Array.isArray(delta.updates)) return;
    const context = delta.context;
    if (!context || !context.startsWith('vessels.')) return;
    for (const update of delta.updates) {
      if (!update || !Array.isArray(update.values)) continue;
      for (const pv of update.values) {
        if (!pv) continue;
        this.absorb(
          context,
          pv.path ?? '',
          pv.value,
          update.timestamp,
          update.$source,
          false
        );
      }
      // One row per update, however many identity values it carried.
      const vessel = this.tracked.get(context);
      if (vessel?.dirty) this.write(context, vessel);
    }
  }

  /**
   * Fold one value for `busPath` (a Signal K path, '' for the root) into the
   * vessel's known identity. Callers that fold several values pass
   * `writeNow = false` and write once afterwards.
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

    let vessel = this.tracked.get(context);
    if (!vessel) {
      if (this.tracked.size >= MAX_TRACKED_CONTEXTS) {
        this.tracked.clear();
      }
      // Start from what was last written, so a replay of known values after
      // a restart (an upstream cache sent one path per message) is not
      // mistaken for a change.
      vessel = { known: this.lastWrittenIdentity(context), dirty: false };
      this.tracked.set(context, vessel);
    }
    if (mergeIdentity(vessel.known, incoming)) {
      vessel.timestamp = timestamp;
      vessel.source = source;
      vessel.dirty =
        JSON.stringify(vessel.known) !== this.lastWritten.get(context);
      if (vessel.dirty && writeNow) this.write(context, vessel);
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
      for (const p of IDENTITY_PATHS) {
        const leaf = getDotted(root, p);
        if (leaf === undefined || leaf === null) continue;
        this.absorb(
          context,
          p,
          unwrapLeaf(leaf),
          leafField(leaf, 'timestamp'),
          leafField(leaf, '$source'),
          false
        );
      }
      // One row per vessel for the whole seeded identity, not one per part.
      const vessel = this.tracked.get(context);
      if (vessel?.dirty) this.write(context, vessel);
      seeded += 1;
    }
    this.debug(`[Identity] Seeded identity from the model for ${seeded} vessel(s)`);
  }

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
      buffer.insert(record);
    } catch (error) {
      this.app.error(
        `[Identity] Failed to record identity for ${context}: ${(error as Error).message}`
      );
      return;
    }
    vessel.dirty = false;
    this.lastWritten.set(context, JSON.stringify(vessel.known));
    this.stateDirty = true;
  }

  private lastWrittenIdentity(context: string): IdentityComponents {
    const raw = this.lastWritten.get(context);
    if (!raw) return {};
    try {
      const parsed = JSON.parse(raw) as unknown;
      return parsed && typeof parsed === 'object'
        ? (parsed as IdentityComponents)
        : {};
    } catch {
      return {};
    }
  }

  private loadState(): void {
    try {
      const raw = fs.readJsonSync(this.stateFile) as {
        lastWritten?: Record<string, string>;
      };
      if (raw && raw.lastWritten && typeof raw.lastWritten === 'object') {
        this.lastWritten = new Map(Object.entries(raw.lastWritten));
      }
    } catch {
      this.lastWritten = new Map();
    }
  }

  private persistState(): void {
    if (!this.stateDirty) return;
    try {
      fs.outputJsonSync(this.stateFile, {
        lastWritten: Object.fromEntries(this.lastWritten),
      });
      this.stateDirty = false;
    } catch (error) {
      this.debug(
        `[Identity] Could not persist identity state: ${(error as Error).message}`
      );
    }
  }
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
