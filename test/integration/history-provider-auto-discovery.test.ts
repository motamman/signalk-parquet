/**
 * The v2 History API provider must offer an empty path to auto-discovery,
 * as the v1 route does. The plugin used to serve /signalk/v2/api/history/*
 * from its own express routes, bound to the v1 handler and its auto-discovery
 * hook; once those routes gave way to the server's built-in v2 API and this
 * provider, a client that queried through v2 never triggered discovery, so a
 * requested-but-unrecorded path was never configured for recording.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as path from 'path';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { LRUCache } from '../../src/utils/lru-cache';
import { HistoryProvider } from '../../src/history-provider';
import { AutoDiscoveryService } from '../../src/services/auto-discovery';
import { clearSchemaCache } from '../../src/utils/schema-cache';
import {
  createFakeSignalK,
  makeTestConfig,
  FakeSignalK,
} from './helpers/fake-signalk';
import type { DataRecord, PathConfig, PluginState } from '../../src/types';
import type { ValuesRequest } from '@signalk/server-api/dist/history';

const SELF_ID = 'autodiscoverself';
const STORED_CONTEXT = `vessels.${SELF_ID}`;
const RECORDED = 'navigation.speedOverGround';
const UNRECORDED = 'environment.moon.fraction';

function sogRecord(isoTime: string, value: number): DataRecord {
  return {
    received_timestamp: isoTime,
    signalk_timestamp: isoTime,
    context: STORED_CONTEXT,
    path: RECORDED,
    value,
    source_label: 'gps.main',
  };
}

function request(paths: string[]): ValuesRequest {
  return {
    from: '2024-06-01T00:00:00Z',
    to: '2024-06-01T23:59:59Z',
    context: 'vessels.self',
    resolution: 60,
    pathSpecs: paths.map(p => ({
      path: p,
      aggregate: 'average',
      parameter: [],
    })),
  } as unknown as ValuesRequest;
}

describe('History API v2 provider: auto-discovery of empty paths', function () {
  this.timeout(60000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;
  let provider: HistoryProvider;
  let currentPaths: PathConfig[];

  const configFile = () =>
    path.join(host.dataDir, 'signalk-parquet', 'webapp-config.json');

  const providerApp = {
    debug: () => {},
    error: () => {},
    getMetadata: () => undefined,
    getSelfPath: () => undefined,
    selfId: SELF_ID,
    selfContext: STORED_CONTEXT,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } as any;

  beforeEach(async () => {
    // Live data exists for both paths; only RECORDED is configured and has
    // rows in the buffer.
    host = createFakeSignalK({
      selfId: SELF_ID,
      selfPaths: {
        [RECORDED]: { value: 5 },
        [UNRECORDED]: { value: 0.5 },
      },
    });
    buffer = new SQLiteBuffer({ dbPath: path.join(host.dataDir, 'buffer.db') });
    await DuckDBPool.initialize();
    clearSchemaCache();
    buffer.insert(sogRecord('2024-06-01T10:00:10.000Z', 5));
    buffer.insert(sogRecord('2024-06-01T10:01:10.000Z', 6));

    currentPaths = [
      { path: RECORDED, name: 'SOG', enabled: true, context: 'vessels.self' },
    ] as unknown as PathConfig[];
    const state = {
      unsubscribes: [],
      dataBuffers: new LRUCache<string, DataRecord[]>(100),
      activeRegimens: new Set<string>(),
      subscribedPaths: new Set<string>(),
      sqliteBuffer: buffer,
      getDataDirPath: () => host.dataDir,
      commandState: { registeredCommands: new Map(), putHandlers: new Map() },
    } as unknown as PluginState;
    const config = makeTestConfig(host.dataDir, {
      autoDiscovery: {
        enabled: true,
        requireLiveData: true,
        maxAutoConfiguredPaths: 100,
        excludePatterns: [],
      },
    });
    const service = new AutoDiscoveryService(
      host.app,
      config,
      state,
      currentPaths
    );

    provider = new HistoryProvider(
      SELF_ID,
      host.dataDir,
      providerApp,
      () => {}
    );
    provider.setSqliteBuffer(buffer);
    provider.setAutoDiscoveryService(service);
    DuckDBPool.initializeSQLiteBuffer(path.join(host.dataDir, 'buffer.db'));
  });

  afterEach(async () => {
    clearSchemaCache();
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  it('configures a requested path that returned nothing', async () => {
    const res = await provider.getValues(request([RECORDED, UNRECORDED]));
    // The recorded path answered; the other column is empty.
    expect(res.data.map(r => [r[1], r[2]])).to.deep.equal([
      [5, null],
      [6, null],
    ]);

    const added = currentPaths.find(p => p.path === UNRECORDED);
    expect(added, 'path added to the live config').to.not.equal(undefined);
    expect(added!.autoDiscovered).to.equal(true);
    expect(added!.enabled).to.equal(true);

    // Persisted, so it survives a restart, and subscribed, so it records.
    const stored = fs.readJsonSync(configFile()) as { paths: PathConfig[] };
    expect(stored.paths.map(p => p.path)).to.include(UNRECORDED);
    expect(
      host.logs.debug.some(l =>
        l.includes(`auto-configured path: ${UNRECORDED}`)
      )
    ).to.equal(true);
  });

  it('leaves a path alone when the query returned data for it', async () => {
    await provider.getValues(request([RECORDED]));
    expect(currentPaths.map(p => p.path)).to.deep.equal([RECORDED]);
    expect(fs.existsSync(configFile())).to.equal(false);
  });

  it('does nothing when no auto-discovery service is attached', async () => {
    provider.setAutoDiscoveryService(undefined);
    const res = await provider.getValues(request([UNRECORDED]));
    expect(res.data).to.deep.equal([]);
    expect(currentPaths.map(p => p.path)).to.deep.equal([RECORDED]);
  });
});
