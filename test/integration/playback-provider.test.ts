/**
 * v1 history playback end to end: rows exported to parquet and rows still
 * in the SQLite buffer replay as delta messages in time order, grouped by
 * instant, vessel and source, with each vessel's identity announced before
 * its first delta, scoped by the connection's subscribe rule, paced by the
 * playback rate, jumping over silence, and stopping when told.
 */
import { expect } from 'chai';
import * as path from 'path';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { clearSchemaCache } from '../../src/utils/schema-cache';
import { clearFileListCache } from '../../src/utils/context-discovery';
import { PlaybackProvider } from '../../src/playback-provider';
import type { PlaybackDelta } from '../../src/utils/playback-deltas';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import type { DataRecord } from '../../src/types';

const SELF_ID = 'playbackself';
const SELF = `vessels.${SELF_ID}`;
const OTHER = 'vessels.urn:mrn:imo:mmsi:244813000';
const DAY = new Date('2024-06-01T00:00:00.000Z');
const SOG = 'navigation.speedOverGround';
const POSITION = 'navigation.position';

function sog(context: string, iso: string, value: number, source: string | null): DataRecord {
  return {
    received_timestamp: iso,
    signalk_timestamp: iso,
    context,
    path: SOG,
    value,
    source_label: source ?? undefined,
  } as DataRecord;
}

function position(context: string, iso: string, lat: number, lon: number, source: string): DataRecord {
  return {
    received_timestamp: iso,
    signalk_timestamp: iso,
    context,
    path: POSITION,
    value: { latitude: lat, longitude: lon },
    value_json: { latitude: lat, longitude: lon },
    value_latitude: lat,
    value_longitude: lon,
    source_label: source,
  };
}

function identity(context: string, iso: string, value: Record<string, unknown>): DataRecord {
  const record: DataRecord = {
    received_timestamp: iso,
    signalk_timestamp: iso,
    context,
    path: 'identity',
    value: null,
    value_json: value,
    source_label: 'ais.1',
  };
  for (const [k, v] of Object.entries(value)) record[`value_${k}`] = v;
  return record;
}

/** Collect deltas from a stream until `count` arrive or `timeoutMs` passes. */
function collect(
  provider: PlaybackProvider,
  options: { startTime: Date; playbackRate?: number; subscribe?: string },
  count: number,
  timeoutMs = 15000
): Promise<{ deltas: PlaybackDelta[]; stop: () => void }> {
  return new Promise(resolve => {
    const deltas: PlaybackDelta[] = [];
    let done = false;
    let stop: () => void = () => {};
    const finish = () => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      resolve({ deltas, stop });
    };
    const timer = setTimeout(finish, timeoutMs);
    stop = provider.streamHistory({}, options, delta => {
      if (done) return;
      deltas.push(delta);
      if (deltas.length >= count) finish();
    });
  });
}

describe('v1 history playback provider', function () {
  this.timeout(60000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;
  let provider: PlaybackProvider;
  const stops: Array<() => void> = [];

  beforeEach(async () => {
    host = createFakeSignalK({ selfId: SELF_ID });
    buffer = new SQLiteBuffer({ dbPath: path.join(host.dataDir, 'buffer.db') });
    await DuckDBPool.initialize();
    clearSchemaCache();
    clearFileListCache();

    const writer = new ParquetWriter({ format: 'parquet', app: host.app });
    const exportService = new ParquetExportService(
      buffer,
      writer,
      {
        outputDirectory: host.dataDir,
        filenamePrefix: 'signalk_data',
        useHivePartitioning: true,
        dailyExportHour: 4,
      },
      host.app
    );

    // Exported day: the own vessel at 10:00:00-10:00:02 from two sources, an
    // AIS target at 10:00:01, identities recorded an hour earlier.
    buffer.insert(identity(SELF, '2024-06-01T09:00:00.000Z', { name: 'Zennora', mmsi: '368396230', beam: 5 }));
    buffer.insert(identity(OTHER, '2024-06-01T09:30:00.000Z', { name: 'Ariel', aisClass: 'B' }));
    buffer.insert(sog(SELF, '2024-06-01T10:00:00.000Z', 5, 'gps.main'));
    buffer.insert(position(SELF, '2024-06-01T10:00:00.000Z', 47.5, 8.7, 'gps.main'));
    buffer.insert(sog(SELF, '2024-06-01T10:00:00.000Z', 5.1, 'gps.backup'));
    buffer.insert(sog(SELF, '2024-06-01T10:00:01.000Z', 6, 'gps.main'));
    buffer.insert(position(OTHER, '2024-06-01T10:00:01.000Z', 47.9, 8.9, 'ais.1'));
    buffer.insert(sog(SELF, '2024-06-01T10:00:02.000Z', 7, null));
    await exportService.exportDayToParquet(DAY);
    // Still in the buffer, not yet exported.
    buffer.insert(sog(SELF, '2024-06-01T10:00:03.000Z', 8, 'gps.main'));

    provider = new PlaybackProvider(
      { selfId: SELF_ID, selfContext: SELF },
      host.dataDir,
      buffer,
      msg => {
        if (process.env.PLAYBACK_DEBUG) console.log(msg);
      }
    );
  });

  afterEach(async () => {
    for (const stop of stops.splice(0)) stop();
    clearSchemaCache();
    clearFileListCache();
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  it('reports data at or after the start time for the vessels in scope', async () => {
    const has = (startTime: Date, subscribe?: string) =>
      new Promise<boolean>(resolve =>
        provider.hasAnyData({ startTime, playbackRate: 1, subscribe }, resolve)
      );
    expect(await has(new Date('2024-06-01T09:00:00Z'))).to.equal(true);
    expect(await has(new Date('2024-06-01T10:00:02.500Z'))).to.equal(true); // buffer row
    expect(await has(new Date('2030-01-01T00:00:00Z'))).to.equal(false);
    expect(await has(new Date('2024-06-01T09:00:00Z'), 'all')).to.equal(true);
  });

  it('replays exported and buffered rows as grouped deltas with identities first', async () => {
    const { deltas, stop } = await collect(
      provider,
      { startTime: new Date('2024-06-01T10:00:00Z'), playbackRate: 1000, subscribe: 'all' },
      8
    );
    stops.push(stop);
    stop();

    const summary = deltas.map(d => [
      d.context,
      d.updates[0].timestamp,
      d.updates[0].$source ?? null,
      d.updates[0].values.map(v => v.path).join(','),
    ]);
    expect(summary).to.deep.equal([
      // Own vessel announced, then its 10:00:00 updates, one per source.
      [SELF, '2024-06-01T10:00:00.000Z', null, ',design.beam'],
      [SELF, '2024-06-01T10:00:00.000Z', 'gps.backup', SOG],
      [SELF, '2024-06-01T10:00:00.000Z', 'gps.main', `${POSITION},${SOG}`],
      [SELF, '2024-06-01T10:00:01.000Z', 'gps.main', SOG],
      // AIS target announced before its first delta.
      [OTHER, '2024-06-01T10:00:01.000Z', null, ',sensors.ais.class'],
      [OTHER, '2024-06-01T10:00:01.000Z', 'ais.1', POSITION],
      // Unattributed row replays without $source; buffered row follows.
      [SELF, '2024-06-01T10:00:02.000Z', null, SOG],
      [SELF, '2024-06-01T10:00:03.000Z', 'gps.main', SOG],
    ]);
    expect(deltas[0].updates[0].values[0]).to.deep.equal({
      path: '',
      value: { name: 'Zennora', mmsi: '368396230' },
    });
    expect(deltas[2].updates[0].values).to.deep.equal([
      { path: POSITION, value: { latitude: 47.5, longitude: 8.7 } },
      { path: SOG, value: 5 },
    ]);
    // Parquet scalars keep their type; buffered scalars are parsed back.
    expect(deltas[3].updates[0].values[0].value).to.equal(6);
    expect(deltas[7].updates[0].values[0].value).to.equal(8);
  });

  it('limits a self subscription to the own vessel', async () => {
    const { deltas, stop } = await collect(
      provider,
      { startTime: new Date('2024-06-01T10:00:00Z'), playbackRate: 1000, subscribe: 'self' },
      6
    );
    stops.push(stop);
    stop();
    expect(deltas.map(d => d.context)).to.deep.equal(Array(6).fill(SELF));
  });

  it('jumps over silence to the first recorded instant', async () => {
    const started = Date.now();
    const { deltas, stop } = await collect(
      provider,
      { startTime: new Date('2022-05-20T00:00:00Z'), playbackRate: 1, subscribe: 'self' },
      2
    );
    stops.push(stop);
    stop();
    // The first recorded instant is the 09:00 identity row (announced, then
    // replayed), two years after the requested start; the empty years are
    // jumped, not walked day by day.
    expect(deltas[0].updates[0].timestamp).to.equal('2024-06-01T09:00:00.000Z');
    expect(deltas[1].updates[0].timestamp).to.equal('2024-06-01T09:00:00.000Z');
    expect(Date.now() - started).to.be.below(5000);
  });

  it('paces deltas by the playback rate', async () => {
    const started = Date.now();
    const { deltas, stop } = await collect(
      provider,
      { startTime: new Date('2024-06-01T10:00:00Z'), playbackRate: 4, subscribe: 'self' },
      5
    );
    stops.push(stop);
    stop();
    // Two seconds of data at 4x is half a second of wall time.
    expect(deltas[4].updates[0].timestamp).to.equal('2024-06-01T10:00:02.000Z');
    const elapsed = Date.now() - started;
    expect(elapsed).to.be.at.least(400);
    expect(elapsed).to.be.below(3000);
  });

  it('emits nothing after stop', async () => {
    const seen: PlaybackDelta[] = [];
    const stop = provider.streamHistory(
      {},
      { startTime: new Date('2024-06-01T10:00:00Z'), playbackRate: 1, subscribe: 'self' },
      d => seen.push(d)
    );
    stop();
    await new Promise(r => setTimeout(r, 1500));
    expect(seen).to.have.lengthOf(0);
  });
});
