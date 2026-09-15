/**
 * The v2 History API provider must answer from the live SQLite buffer alone
 * for a path that has no raw parquet yet: a vessel or AIS target recorded
 * for the first time since the last daily export. Before this, the provider
 * read the parquet glob unconditionally, DuckDB threw "No files found", and
 * every such query came back empty while the v1 routes and the Track API
 * answered from the buffer.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as path from 'path';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { HistoryProvider } from '../../src/history-provider';
import { clearSchemaCache } from '../../src/utils/schema-cache';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import type { DataRecord } from '../../src/types';
import type { ValuesRequest } from '@signalk/server-api/dist/history';

const SELF_ID = 'bufferonlyself';
const STORED_CONTEXT = `vessels.${SELF_ID}`;
const SOG = 'navigation.speedOverGround';
const POSITION = 'navigation.position';

function sogRecord(isoTime: string, value: number, source: string): DataRecord {
  return {
    received_timestamp: isoTime,
    signalk_timestamp: isoTime,
    context: STORED_CONTEXT,
    path: SOG,
    value,
    source_label: source,
  };
}

function positionRecord(
  isoTime: string,
  latitude: number,
  longitude: number,
  source: string
): DataRecord {
  return {
    received_timestamp: isoTime,
    signalk_timestamp: isoTime,
    context: STORED_CONTEXT,
    path: POSITION,
    value: { latitude, longitude },
    value_json: { latitude, longitude },
    value_latitude: latitude,
    value_longitude: longitude,
    source_label: source,
  };
}

function request(
  pathSpecs: Array<Record<string, unknown>>,
  extra: Record<string, unknown> = {}
): ValuesRequest {
  return {
    from: '2024-06-01T00:00:00Z',
    to: '2024-06-01T23:59:59Z',
    context: 'vessels.self',
    resolution: 60,
    pathSpecs,
    ...extra,
  } as unknown as ValuesRequest;
}

describe('History API v2 provider: buffer-only paths (no raw parquet yet)', function () {
  this.timeout(60000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;
  let provider: HistoryProvider;

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
    host = createFakeSignalK({ selfId: SELF_ID });
    buffer = new SQLiteBuffer({ dbPath: path.join(host.dataDir, 'buffer.db') });
    await DuckDBPool.initialize();
    clearSchemaCache();

    // Nothing is exported: the data directory has no tier=raw at all.
    buffer.insert(sogRecord('2024-06-01T10:00:10.000Z', 5, 'gps.main'));
    buffer.insert(sogRecord('2024-06-01T10:00:20.000Z', 7, 'gps.backup'));
    buffer.insert(sogRecord('2024-06-01T10:01:10.000Z', 6, 'gps.main'));
    buffer.insert(positionRecord('2024-06-01T10:00:10.000Z', 47.5, 8.7, 'gps.main'));
    buffer.insert(positionRecord('2024-06-01T10:01:10.000Z', 47.6, 8.8, 'gps.main'));

    provider = new HistoryProvider(SELF_ID, host.dataDir, providerApp, () => {});
    provider.setSqliteBuffer(buffer);
    DuckDBPool.initializeSQLiteBuffer(path.join(host.dataDir, 'buffer.db'));
  });

  afterEach(async () => {
    clearSchemaCache();
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  it('returns a scalar path from the buffer alone', async () => {
    expect(await fs.pathExists(path.join(host.dataDir, 'tier=raw'))).to.equal(
      false
    );
    const res = await provider.getValues(
      request([{ path: SOG, aggregate: 'average', parameter: [] }])
    );
    expect(res.data.map(r => [r[0], r[1]])).to.deep.equal([
      ['2024-06-01T10:00:00Z', 6],
      ['2024-06-01T10:01:00Z', 6],
    ]);
  });

  it('returns navigation.position from the buffer alone, as [lon, lat]', async () => {
    const res = await provider.getValues(
      request([{ path: POSITION, aggregate: 'first', parameter: [] }])
    );
    expect(res.data).to.deep.equal([
      ['2024-06-01T10:00:00Z', [8.7, 47.5]],
      ['2024-06-01T10:01:00Z', [8.8, 47.6]],
    ]);
  });

  it('filters and expands by source from the buffer alone', async () => {
    const filtered = await provider.getValues(
      request([
        { path: SOG, aggregate: 'average', parameter: [], sourceRef: 'gps.backup' },
      ])
    );
    expect(filtered.values[0].$source).to.equal('gps.backup');
    expect(filtered.data).to.deep.equal([['2024-06-01T10:00:00Z', 7]]);

    const expanded = await provider.getValues(
      request([{ path: SOG, aggregate: 'average', parameter: [] }], {
        sourcePolicy: 'all',
      })
    );
    expect(expanded.values.map(v => v.$source)).to.deep.equal([
      'gps.backup',
      'gps.main',
    ]);
    expect(expanded.data).to.deep.equal([
      ['2024-06-01T10:00:00Z', 7, 5],
      ['2024-06-01T10:01:00Z', null, 6],
    ]);
  });

  it('still answers when the raw directory exists but holds no day files', async () => {
    // A quarantined file is the only thing under the path directory, so the
    // glob matches nothing and read_parquet throws.
    const quarantine = path.join(
      host.dataDir,
      'tier=raw',
      `context=vessels__${SELF_ID}`,
      'path=navigation__speedOverGround',
      'year=2024',
      'day=153',
      'quarantine'
    );
    await fs.ensureDir(quarantine);
    await fs.writeFile(path.join(quarantine, 'bad.parquet'), 'not parquet');

    const res = await provider.getValues(
      request([{ path: SOG, aggregate: 'max', parameter: [] }])
    );
    expect(res.data.map(r => r[1])).to.deep.equal([7, 6]);
  });

  it('returns an empty series for a path in neither store', async () => {
    const res = await provider.getValues(
      request([{ path: 'environment.depth.belowKeel', aggregate: 'average', parameter: [] }])
    );
    expect(res.data).to.deep.equal([]);
  });
});
