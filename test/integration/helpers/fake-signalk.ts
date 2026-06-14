/**
 * In-memory test doubles for a Signal K server host.
 *
 * The real plugin talks to a `ServerAPI` object provided by the Signal K
 * server: it logs through `debug`/`error`, reads self data via `getSelfPath`,
 * resolves its data directory via `getDataDirPath`, subscribes to command
 * paths through `subscriptionmanager`, and streams data values through
 * `streambundle`. This module provides a controllable fake of that surface so
 * integration tests can drive the plugin's ingestion path deterministically:
 * tests push deltas in by hand and inspect what the plugin buffered.
 *
 * It deliberately models only the slice of ServerAPI the plugin actually
 * calls; everything else throws via the Proxy so an untested dependency
 * surfaces loudly instead of silently returning undefined.
 */
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs-extra';
import type { ServerAPI } from '@signalk/server-api';

/** A single registered streambundle handler for one bus (path). */
interface BusHandler {
  filter: (delta: unknown) => boolean;
  onValue: (delta: unknown) => void;
  active: boolean;
}

/** A registered command-subscription callback. */
interface CommandSub {
  deltaCb: (delta: unknown) => void;
  active: boolean;
}

export interface FakeSignalK {
  app: ServerAPI;
  dataDir: string;
  /** Captured debug/error log lines, newest last. */
  logs: { debug: string[]; error: string[] };
  /** PUT handlers registered via registerPutHandler, keyed by path. */
  putHandlers: Map<
    string,
    (context: string, path: string, value: unknown, cb?: unknown) => unknown
  >;
  /** Express-style routes registered via app.get(path, handler). */
  routes: Map<string, (req: unknown, res: unknown) => void>;
  /** Deltas published back to the server via handleMessage. */
  published: Array<{ source: string; delta: unknown }>;
  /** Push a streambundle delta to every active handler for its path. */
  emitBus(path: string, delta: Record<string, unknown>): void;
  /** Push a command delta to the active command subscription callback(s). */
  emitCommand(delta: Record<string, unknown>): void;
  /** Remove the temp data directory. */
  cleanup(): Promise<void>;
}

export interface FakeSignalKOptions {
  /** selfId used by the History API to resolve `vessels.self`. */
  selfId?: string;
  /** Values returned by getSelfPath(key); '' returns the whole self object. */
  selfPaths?: Record<string, unknown>;
  /** Metadata returned by getMetadata(path). */
  metadata?: Record<string, unknown>;
}

/**
 * Create a fake Signal K host backed by a fresh temp data directory.
 * Call `cleanup()` (and close any plugin resources) in an afterEach.
 */
export function createFakeSignalK(
  options: FakeSignalKOptions = {}
): FakeSignalK {
  const selfId = options.selfId ?? 'test-self';
  const selfPaths = options.selfPaths ?? {};
  const metadata = options.metadata ?? {};

  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sk-parquet-it-'));
  const logs = { debug: [] as string[], error: [] as string[] };
  const putHandlers = new Map<
    string,
    (context: string, path: string, value: unknown, cb?: unknown) => unknown
  >();
  const routes = new Map<string, (req: unknown, res: unknown) => void>();
  const published: Array<{ source: string; delta: unknown }> = [];

  const busHandlers = new Map<string, BusHandler[]>();
  const commandSubs: CommandSub[] = [];

  function makeBus(busPath: string) {
    let filter: (delta: unknown) => boolean = () => true;
    const chain = {
      filter(fn: (delta: unknown) => boolean) {
        filter = fn;
        return chain;
      },
      // The real stream debounces; tests drive timing explicitly, so this is
      // a pass-through that returns the same chain.
      debounceImmediate() {
        return chain;
      },
      onValue(cb: (delta: unknown) => void) {
        const handler: BusHandler = { filter, onValue: cb, active: true };
        const list = busHandlers.get(busPath) ?? [];
        list.push(handler);
        busHandlers.set(busPath, list);
        // Subscription handle: data-handler disposes via .unsubscribe().
        return {
          unsubscribe() {
            handler.active = false;
          },
        };
      },
    };
    return chain;
  }

  const partialApp = {
    selfId,
    selfContext: `vessels.${selfId}`,
    debug: (msg: string) => {
      logs.debug.push(msg);
    },
    error: (msg: string) => {
      logs.error.push(msg);
    },
    getDataDirPath: () => dataDir,
    getSelfPath: (key: string) => {
      if (key === '') return selfPaths;
      return selfPaths[key];
    },
    getMetadata: (key: string) => metadata[key],
    handleMessage: (source: string, delta: unknown) => {
      published.push({ source, delta });
    },
    savePluginOptions: (_opts: unknown, cb?: (err?: unknown) => void) => {
      if (cb) cb();
    },
    setPluginStatus: () => {},
    setPluginError: () => {},
    registerPutHandler: (
      _context: string,
      putPath: string,
      handler: (
        context: string,
        path: string,
        value: unknown,
        cb?: unknown
      ) => unknown
    ) => {
      putHandlers.set(putPath, handler);
    },
    // Express Router surface used when the plugin passes `app` as the router.
    get: (routePath: string, handler: (req: unknown, res: unknown) => void) => {
      routes.set(routePath, handler);
    },
    subscriptionmanager: {
      subscribe: (
        _subscription: unknown,
        unsubscribes: Array<() => void>,
        _errorCb: (err: unknown) => void,
        deltaCb: (delta: unknown) => void
      ) => {
        const sub: CommandSub = { deltaCb, active: true };
        commandSubs.push(sub);
        unsubscribes.push(() => {
          sub.active = false;
        });
      },
    },
    streambundle: {
      getBus: (busPath: string) => makeBus(busPath),
      getSelfBus: (busPath: string) => makeBus(busPath),
    },
  };

  // Proxy so any unmodelled ServerAPI access fails loudly rather than
  // silently returning undefined and masking an untested dependency.
  const app = new Proxy(partialApp, {
    get(target, prop: string) {
      if (prop in target) {
        return (target as Record<string, unknown>)[prop];
      }
      if (prop === 'then') return undefined; // not a thenable
      throw new Error(
        `FakeSignalK: unmodelled ServerAPI member accessed: '${String(prop)}'`
      );
    },
  }) as unknown as ServerAPI;

  return {
    app,
    dataDir,
    logs,
    putHandlers,
    routes,
    published,
    emitBus(busPath, delta) {
      const list = busHandlers.get(busPath) ?? [];
      for (const handler of list) {
        if (!handler.active) continue;
        if (handler.filter(delta)) {
          handler.onValue(delta);
        }
      }
    },
    emitCommand(delta) {
      // Snapshot the active subscribers first: a real server delivers a delta
      // only to subscriptions that exist when it arrives, not to ones created
      // re-entrantly while this same delta is being dispatched.
      const active = commandSubs.filter(sub => sub.active);
      for (const sub of active) {
        sub.deltaCb(delta);
      }
    },
    async cleanup() {
      await fs.remove(dataDir);
    },
  };
}

/**
 * Build a PluginConfig with sensible test defaults. Only the fields the
 * ingestion and export paths read are meaningful; the rest satisfy the type.
 */
export function makeTestConfig(
  dataDir: string,
  overrides: Record<string, unknown> = {}
): import('../../../src/types').PluginConfig {
  const base = {
    bufferSize: 1000,
    saveIntervalSeconds: 30,
    outputDirectory: dataDir,
    filenamePrefix: 'signalk_data',
    retentionDays: 0,
    fileFormat: 'parquet' as const,
    vesselMMSI: '368204530',
    cloudUpload: { provider: 'none' as const },
    homePortLatitude: 0,
    homePortLongitude: 0,
    useSqliteBuffer: true,
    bufferRetentionHours: 48,
    useHivePartitioning: true,
    autoDiscovery: {
      enabled: false,
      requireLiveData: true,
      maxAutoConfiguredPaths: 100,
      excludePatterns: [],
    },
    exportBatchSize: 50000,
    enableRawSql: false,
    dailyExportHour: 4,
  };
  return {
    ...base,
    ...overrides,
  } as unknown as import('../../../src/types').PluginConfig;
}
