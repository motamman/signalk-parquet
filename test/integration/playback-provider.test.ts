/**
 * v1 history playback end to end: rows exported to parquet and rows still
 * in the SQLite buffer replay as delta messages in time order, grouped by
 * instant, vessel and source, with each vessel's identity announced before
 * its first delta, scoped by the connection's subscribe rule, paced by the
 * playback rate, jumping over silence, and stopping when told.
 */
import { expect } from 'chai';
import { EventEmitter } from 'events';
import * as path from 'path';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { clearFileListCache } from '../../src/utils/context-discovery';
import { PlaybackProvider, parseContextParam } from '../../src/playback-provider';
import type { PlaybackDelta } from '../../src/utils/playback-deltas';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import type { DataRecord } from '../../src/types';
import { filesFor } from '../../src/utils/parquet-files';
import { readFooters } from '../../src/utils/parquet-footer';

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
  timeoutMs = 15000,
  query: Record<string, unknown> = {}
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
    stop = provider.streamHistory({ query }, options, delta => {
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

  it('replays only the vessels named by the context query extension', async () => {
    const { deltas, stop } = await collect(
      provider,
      { startTime: new Date('2024-06-01T10:00:00Z'), playbackRate: 1000, subscribe: 'all' },
      2,
      15000,
      { context: '244813000' }
    );
    stops.push(stop);
    stop();
    expect(deltas.map(d => d.context)).to.deep.equal([OTHER, OTHER]);
    expect(deltas[0].updates[0].values.map(v => v.path)).to.include('');
    expect(deltas[1].updates[0].values[0].path).to.equal(POSITION);
  });

  it('parses the context query extension', () => {
    expect(parseContextParam(undefined, SELF)).to.equal(null);
    expect(parseContextParam('', SELF)).to.equal(null);
    expect(parseContextParam(' , ', SELF)).to.equal(null);
    expect(parseContextParam('self', SELF)).to.deep.equal([SELF]);
    expect(parseContextParam('244813000', SELF)).to.deep.equal([OTHER]);
    expect(parseContextParam(OTHER, SELF)).to.deep.equal([OTHER]);
    expect(parseContextParam(` self ,244813000, ${OTHER}`, SELF)).to.deep.equal([SELF, OTHER]);
    expect(parseContextParam('urn:mrn:signalk:uuid:abc', SELF)).to.deep.equal([
      'vessels.urn:mrn:signalk:uuid:abc',
    ]);
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

  it('waits between reads once it reaches the present instead of spinning', async () => {
    // One fresh row just behind the live lag; nothing after it. Once the
    // session has replayed it every further read is at the present and
    // must be paced by the live poll, about one a second.
    const recent = new Date(Date.now() - 4000).toISOString();
    buffer.insert(sog(SELF, recent, 9, 'gps.main'));
    // Reads take a few milliseconds each, as they do against a real buffer
    // of any size; the caught-up decision must not depend on their speed.
    let reads = 0;
    const counting = new Proxy(buffer, {
      get(target, prop, receiver) {
        const v = Reflect.get(target, prop, receiver);
        if (prop !== 'getNextRowTime') {
          return typeof v === 'function' ? v.bind(target) : v;
        }
        return (...args: unknown[]) => {
          reads += 1;
          const until = Date.now() + 3;
          while (Date.now() < until) {
            /* busy: a synchronous query */
          }
          return (v as (...a: unknown[]) => unknown).apply(target, args);
        };
      },
    });
    const live = new PlaybackProvider(
      { selfId: SELF_ID, selfContext: SELF },
      host.dataDir,
      counting,
      () => {}
    );
    const { deltas, stop } = await collect(
      live,
      { startTime: new Date(Date.now() - 5000), playbackRate: 1000, subscribe: 'self' },
      2
    );
    stops.push(stop);
    // Identity first, then the row.
    expect(deltas[1].updates[0].values[0].value).to.equal(9);
    const before = reads;
    await new Promise(r => setTimeout(r, 2500));
    stop();
    expect(reads - before).to.be.at.most(4);
  });

  it('returns the earliest rows under the cap, whatever order the paths were registered in', () => {
    // `buffer_navigation_speedOverGround` is registered first (the fixture
    // inserts it before anything else), so a first-come budget would fill up
    // on its rows and never reach the depth table registered here, whose rows
    // are earlier. The merge must prefer the earlier rows regardless.
    const from = '2024-06-01T14:00:00.000Z';
    const to = '2024-06-01T15:00:00.000Z';
    for (let i = 0; i < 10; i++) {
      const iso = `2024-06-01T14:00:${String(30 + i).padStart(2, '0')}.000Z`;
      buffer.insert(sog(SELF, iso, i, 'gps.main'));
    }
    for (let i = 0; i < 10; i++) {
      const iso = `2024-06-01T14:00:${String(i).padStart(2, '0')}.000Z`;
      buffer.insert({
        received_timestamp: iso,
        signalk_timestamp: iso,
        context: SELF,
        path: 'environment.depth.belowKeel',
        value: 3 + i,
        source_label: 'sounder.1',
      } as DataRecord);
    }

    const capped = buffer.getRowsForPlayback(from, to, null, 5);
    expect(capped).to.have.lengthOf(5);
    expect(capped.map(r => r.signalk_timestamp)).to.deep.equal([
      '2024-06-01T14:00:00.000Z',
      '2024-06-01T14:00:01.000Z',
      '2024-06-01T14:00:02.000Z',
      '2024-06-01T14:00:03.000Z',
      '2024-06-01T14:00:04.000Z',
    ]);
    expect(new Set(capped.map(r => r.path))).to.deep.equal(
      new Set(['environment.depth.belowKeel'])
    );

    // Uncapped, both paths come back and the whole window is in time order.
    const all = buffer.getRowsForPlayback(from, to, null, 1000);
    expect(all).to.have.lengthOf(20);
    const times = all.map(r => r.signalk_timestamp);
    expect([...times].sort()).to.deep.equal(times);
  });

  it('caps buffer rows per window across all paths, not per path', () => {
    // Three paths with rows in the window; a budget of 4 must yield at most 4
    // rows in total, however many paths have rows.
    const t = '2024-06-01T12:00:00.000Z';
    for (let i = 0; i < 5; i++) {
      const iso = `2024-06-01T12:00:0${i}.000Z`;
      buffer.insert(sog(SELF, iso, i, 'gps.main'));
      buffer.insert(position(SELF, iso, 47 + i, 8, 'gps.main'));
      buffer.insert(identity(OTHER, iso, { name: `v${i}` }));
    }
    const rows = buffer.getRowsForPlayback(t, '2024-06-01T13:00:00.000Z', null, 4);
    expect(rows.length).to.be.at.most(4);
    const all = buffer.getRowsForPlayback(t, '2024-06-01T13:00:00.000Z', null, 1000);
    expect(all.length).to.equal(15);
  });

  it('announces an identity stored only as components, with no value_json', async () => {
    const VESSEL = 'vessels.urn:mrn:imo:mmsi:235000001';
    const NEXT_DAY = new Date('2024-06-02T00:00:00.000Z');
    // As brain's identity files are: the fields as value_* columns only.
    buffer.insert({
      received_timestamp: '2024-06-02T09:00:00.000Z',
      signalk_timestamp: '2024-06-02T09:00:00.000Z',
      context: VESSEL,
      path: 'identity',
      value: null,
      value_name: 'Solent',
      value_mmsi: '235000001',
      source_label: 'ais.1',
    } as DataRecord);
    buffer.insert(sog(VESSEL, '2024-06-02T10:00:00.000Z', 4, 'ais.1'));
    const exporter = new ParquetExportService(
      buffer,
      new ParquetWriter({ format: 'parquet', app: host.app }),
      {
        outputDirectory: host.dataDir,
        filenamePrefix: 'signalk_data',
        useHivePartitioning: true,
        dailyExportHour: 4,
      },
      host.app
    );
    await exporter.exportDayToParquet(NEXT_DAY);
    const identityFiles = await filesFor({
      dataDir: host.dataDir,
      contexts: [VESSEL],
      paths: ['identity'],
    });
    expect(identityFiles).to.have.lengthOf(1);
    const [footer] = await readFooters(identityFiles);
    expect(footer.columns.has('value_json'), 'precondition').to.equal(false);

    // No buffer, so the identity can only come from that file.
    const fromParquet = new PlaybackProvider(
      { selfId: SELF_ID, selfContext: SELF },
      host.dataDir,
      undefined,
      () => {}
    );
    const { deltas, stop } = await collect(
      fromParquet,
      { startTime: new Date('2024-06-02T10:00:00Z'), playbackRate: 1000, subscribe: 'all' },
      2,
      15000,
      { context: '235000001' }
    );
    stops.push(stop);
    stop();
    expect(deltas[0].context).to.equal(VESSEL);
    // The writer types a column of numeric-looking text as DOUBLE, so the
    // mmsi comes back a number; brain's identity files are all typed so.
    expect(deltas[0].updates[0].values[0]).to.deep.equal({
      path: '',
      value: { name: 'Solent', mmsi: 235000001 },
    });
    expect(deltas[1].updates[0].values[0]).to.deep.equal({ path: SOG, value: 4 });
  });

  describe('sessions the server cannot stop (#141)', () => {
    /** A Primus spark as far as playback sees one: 3 is open, 2 closed. */
    const spark = (readyState: number) =>
      Object.assign(new EventEmitter(), { readyState, query: {} });
    const logged = (): { provider: PlaybackProvider; logs: string[] } => {
      const logs: string[] = [];
      return {
        logs,
        provider: new PlaybackProvider(
          { selfId: SELF_ID, selfContext: SELF },
          host.dataDir,
          buffer,
          msg => logs.push(msg)
        ),
      };
    };
    const opts = {
      startTime: new Date('2024-06-01T10:00:00Z'),
      playbackRate: 1,
      subscribe: 'self',
    };

    it('starts no session for a client that left before playback began', async () => {
      const { provider: p, logs } = logged();
      const seen: PlaybackDelta[] = [];
      p.streamHistory(spark(2), opts, d => seen.push(d));
      await new Promise(r => setTimeout(r, 1000));
      expect(seen).to.have.lengthOf(0);
      expect(logs).to.include('[Playback] client left before playback began');
      expect(logs.some(l => l.startsWith('[Playback] start '))).to.equal(false);
    });

    it('stops when its client disconnects, with no call from the server', async () => {
      const { provider: p, logs } = logged();
      const client = spark(3);
      const seen: PlaybackDelta[] = [];
      // The returned stop function is dropped, as the server drops it when
      // the disconnect was handled before streamHistory was called.
      p.streamHistory(client, opts, d => seen.push(d));
      while (seen.length === 0) await new Promise(r => setTimeout(r, 20));
      client.emit('end');
      await new Promise(r => setTimeout(r, 100));
      const count = seen.length;
      await new Promise(r => setTimeout(r, 1500));
      expect(seen).to.have.lengthOf(count);
      expect(logs).to.include('[Playback] stopped');
    });

    it('stopAll ends every session and waits for each', async () => {
      const { provider: p, logs } = logged();
      for (let i = 0; i < 3; i++) p.streamHistory(spark(3), opts, () => {});
      await new Promise(r => setTimeout(r, 200));
      await p.stopAll();
      expect(logs.filter(l => l === '[Playback] stopped')).to.have.lengthOf(3);
      // Nothing left to stop.
      await p.stopAll();
      expect(logs.filter(l => l === '[Playback] stopped')).to.have.lengthOf(3);
    });
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
