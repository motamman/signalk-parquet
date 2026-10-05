/**
 * Vessel identity capture end to end: identity values arriving on the
 * plugin API's per-path buses (one event per value, each carrying the
 * update's timestamp and $source, as the server's streambundle splits a
 * delta) are folded into one `identity` row per vessel in the SQLite
 * buffer, written for every vessel heard, once per report that changes
 * something, and not rewritten after a restart. The rows are readable
 * through the v2 History API provider.
 */
import { expect } from 'chai';
import * as path from 'path';
import * as fs from 'fs-extra';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { HistoryProvider } from '../../src/history-provider';
import { VesselIdentityService } from '../../src/services/vessel-identity-service';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { ParquetWriter } from '../../src/parquet-writer';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import type { PluginState } from '../../src/types';
import type { ValuesRequest } from '@signalk/server-api/dist/history';

const SELF_ID = 'identityself';
const SELF = `vessels.${SELF_ID}`;
const OTHER = 'vessels.urn:mrn:imo:mmsi:244813000';
const T0 = '2024-06-01T10:00:00.000Z';

/**
 * Deliver one delta the way the server's streambundle does: one bus event
 * per value, on that value's path bus, each carrying the update's timestamp
 * and $source, all in one synchronous pass. Resolves once that pass is over
 * and its microtasks have run, which is when the service writes.
 */
async function emit(
  host: FakeSignalK,
  context: string,
  values: Array<{ path: string; value: unknown }>,
  timestamp = T0,
  $source?: string
): Promise<void> {
  for (const { path: p, value } of values) {
    host.emitBus(p, {
      context,
      path: p,
      value,
      timestamp,
      $source,
      isMeta: false,
    });
  }
  await Promise.resolve();
}

function rows(buffer: SQLiteBuffer, context: string) {
  return buffer.getRowsForFederation(
    'identity',
    context,
    '2024-01-01T00:00:00.000Z',
    '2030-01-01T00:00:00.000Z',
    null,
    100
  );
}

describe('vessel identity capture', function () {
  this.timeout(30000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;
  let state: PluginState;
  let service: VesselIdentityService;

  beforeEach(async () => {
    host = createFakeSignalK({ selfId: SELF_ID });
    buffer = new SQLiteBuffer({ dbPath: path.join(host.dataDir, 'buffer.db') });
    state = { sqliteBuffer: buffer } as unknown as PluginState;
    service = new VesselIdentityService(
      host.app,
      state,
      host.dataDir,
      () => {}
    );
    service.start();
  });

  afterEach(async () => {
    service.stop();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  it('folds one AIS static report into one row, then writes only on change', async () => {
    // One AIS type 5 report: the server emits it as a single delta carrying
    // several identity values.
    const report = [
      { path: '', value: { name: 'Ariel', mmsi: 244813000 } },
      { path: 'design.length', value: { overall: 12.5 } },
      { path: 'design.beam', value: 4.1 },
      { path: 'communication.callsignVhf', value: 'PD1234' },
      { path: 'sensors.ais.class', value: 'B' },
    ];
    await emit(host, OTHER, report, T0, 'ais.1');
    const first = rows(buffer, OTHER);
    expect(first).to.have.lengthOf(1);
    expect(first[0].value_name).to.equal('Ariel');
    expect(first[0].value_lengthOverall).to.equal(12.5);
    expect(first[0].value_beam).to.equal(4.1);
    expect(first[0].value_callsignVhf).to.equal('PD1234');
    expect(first[0].value_aisClass).to.equal('B');

    // The static report repeats: no new row.
    await emit(host, OTHER, report, '2024-06-01T10:06:00.000Z', 'ais.1');
    expect(rows(buffer, OTHER)).to.have.lengthOf(1);

    // Ship type arrives: it only completes the identity, so the row still in
    // the buffer is extended rather than joined by a second one.
    await emit(
      host,
      OTHER,
      [{ path: 'design.aisShipType', value: { id: 36, name: 'Sailing' } }],
      '2024-06-01T10:07:00.000Z',
      'ais.1'
    );
    const completed = rows(buffer, OTHER);
    expect(completed).to.have.lengthOf(1);
    expect(completed[0].value_name).to.equal('Ariel');
    expect(completed[0].value_mmsi).to.equal('244813000');
    expect(completed[0].value_aisShipTypeId).to.equal(36);
    expect(completed[0].value_aisShipTypeName).to.equal('Sailing');
    expect(completed[0].source_label).to.equal('ais.1');
    expect(completed[0].signalk_timestamp).to.equal('2024-06-01T10:07:00.000Z');

    // A component changes: one more row carrying the whole identity.
    await emit(
      host,
      OTHER,
      [{ path: '', value: { name: 'Ariel II' } }],
      '2024-06-01T10:08:00.000Z',
      'ais.1'
    );
    const after = rows(buffer, OTHER);
    expect(after).to.have.lengthOf(2);
    const latest = after[after.length - 1];
    expect(latest.value_name).to.equal('Ariel II');
    expect(latest.value_mmsi).to.equal('244813000');
    expect(latest.value_aisShipTypeId).to.equal(36);
    expect(latest.value_callsignVhf).to.equal('PD1234');
    expect(latest.signalk_timestamp).to.equal('2024-06-01T10:08:00.000Z');
  });

  it('writes one row for a report that changes two known fields', async () => {
    await emit(
      host,
      OTHER,
      [
        { path: '', value: { name: 'Ariel' } },
        { path: 'communication.callsignVhf', value: 'PD1234' },
      ],
      T0,
      'ais.1'
    );
    expect(rows(buffer, OTHER)).to.have.lengthOf(1);

    // Renamed and re-registered in one report. Written per value, the first
    // value could not extend the stored row (its name had changed), so it
    // inserted {name: new, callsign: old} — an identity the vessel never had
    // — and the second inserted again.
    await emit(
      host,
      OTHER,
      [
        { path: '', value: { name: 'Ariel II' } },
        { path: 'communication.callsignVhf', value: 'PD9999' },
      ],
      '2024-06-01T11:00:00.000Z',
      'ais.1'
    );
    const got = rows(buffer, OTHER);
    expect(got.map(r => [r.value_name, r.value_callsignVhf])).to.deep.equal([
      ['Ariel', 'PD1234'],
      ['Ariel II', 'PD9999'],
    ]);
  });

  it('keeps two reports arriving in one pass as two rows', async () => {
    // Two deltas for one vessel, differently stamped, pushed before either
    // pass's microtasks run: each is still its own row.
    host.emitBus('', {
      context: OTHER,
      path: '',
      value: { name: 'Ariel' },
      timestamp: T0,
      $source: 'ais.1',
      isMeta: false,
    });
    host.emitBus('', {
      context: OTHER,
      path: '',
      value: { name: 'Ariel II' },
      timestamp: '2024-06-01T11:00:00.000Z',
      $source: 'ais.1',
      isMeta: false,
    });
    await Promise.resolve();
    expect(rows(buffer, OTHER).map(r => r.value_name)).to.deep.equal([
      'Ariel',
      'Ariel II',
    ]);
  });

  it('records the identity of the own vessel too', async () => {
    await emit(host, SELF, [{ path: '', value: { name: 'Zennora' } }]);
    expect(rows(buffer, SELF).map(r => r.value_name)).to.deep.equal([
      'Zennora',
    ]);
  });

  it('does not rewrite a vessel after a restart when nothing changed', async () => {
    await emit(host, OTHER, [{ path: '', value: { name: 'Ariel' } }]);
    service.stop();
    expect(
      await fs.pathExists(path.join(host.dataDir, 'identity-state.json'))
    ).to.equal(true);

    service = new VesselIdentityService(
      host.app,
      state,
      host.dataDir,
      () => {}
    );
    service.start();
    await emit(
      host,
      OTHER,
      [{ path: '', value: { name: 'Ariel' } }],
      '2024-06-02T10:00:00.000Z'
    );
    expect(rows(buffer, OTHER)).to.have.lengthOf(1);
  });

  it('ignores a one-path-per-message replay of a known identity after a restart', async () => {
    await emit(
      host,
      OTHER,
      [
        { path: '', value: { name: 'Ariel', mmsi: 244813000 } },
        { path: 'design.length', value: { overall: 12.5 } },
        { path: 'design.beam', value: 4.1 },
      ],
      T0,
      'ais.1'
    );
    expect(rows(buffer, OTHER)).to.have.lengthOf(1);
    service.stop();

    // An upstream Signal K websocket replays its cache on connect, one path
    // per message. Nothing here is new, so nothing is written.
    service = new VesselIdentityService(
      host.app,
      state,
      host.dataDir,
      () => {}
    );
    service.start();
    await emit(host, OTHER, [{ path: 'design.length', value: { overall: 12.5 } }]);
    await emit(host, OTHER, [{ path: 'design.beam', value: 4.1 }]);
    await emit(host, OTHER, [
      { path: '', value: { name: 'Ariel', mmsi: 244813000 } },
    ]);
    expect(rows(buffer, OTHER)).to.have.lengthOf(1);

    // A real change still writes one row carrying the whole identity.
    await emit(
      host,
      OTHER,
      [{ path: 'design.beam', value: 4.5 }],
      '2024-06-02T10:00:00.000Z'
    );
    const got = rows(buffer, OTHER);
    expect(got).to.have.lengthOf(2);
    expect(got[1].value_name).to.equal('Ariel');
    expect(got[1].value_lengthOverall).to.equal(12.5);
    expect(got[1].value_beam).to.equal(4.5);
  });

  it('folds a one-path-per-message first hearing into one row', async () => {
    // An upstream server replays its cache one path per message. A vessel
    // not yet on file arrives as class, then mmsi, then dimensions, then
    // name, each in its own delta.
    await emit(host, OTHER, [{ path: 'sensors.ais.class', value: 'A' }], T0, 'ais.1');
    await emit(host, OTHER, [{ path: '', value: { mmsi: 244813000 } }], T0, 'ais.1');
    await emit(
      host,
      OTHER,
      [{ path: 'design.length', value: { overall: 40 } }],
      '2024-06-01T10:00:01.000Z',
      'ais.1'
    );
    await emit(
      host,
      OTHER,
      [{ path: '', value: { name: 'Ariel' } }],
      '2024-06-01T10:00:01.000Z',
      'ais.1'
    );
    const got = rows(buffer, OTHER);
    expect(got).to.have.lengthOf(1);
    expect(got[0].value_aisClass).to.equal('A');
    expect(got[0].value_mmsi).to.equal('244813000');
    expect(got[0].value_lengthOverall).to.equal(40);
    expect(got[0].value_name).to.equal('Ariel');
    expect(got[0].signalk_timestamp).to.equal('2024-06-01T10:00:01.000Z');
    expect(buffer.getStats().totalRecords).to.equal(1);
  });

  it('does not extend a row that has already been exported', async () => {
    await emit(host, OTHER, [{ path: 'sensors.ais.class', value: 'A' }], T0, 'ais.1');
    // The daily export took the row to Parquet; it is immutable now.
    buffer.markDateExported(OTHER, 'identity', new Date(), 'batch-1');
    expect(rows(buffer, OTHER)).to.have.lengthOf(0);

    await emit(host, OTHER, [{ path: '', value: { mmsi: 244813000 } }], T0, 'ais.1');
    const got = rows(buffer, OTHER);
    expect(got).to.have.lengthOf(1);
    expect(got[0].value_aisClass).to.equal('A');
    expect(got[0].value_mmsi).to.equal('244813000');
    expect(buffer.getStats().totalRecords).to.equal(2);
  });

  it('trusts the buffer over a stale state file after a kill', async () => {
    await emit(host, OTHER, [{ path: '', value: { name: 'Ariel' } }], T0, 'ais.1');
    service.stop(); // state file now says: name only
    service = new VesselIdentityService(
      host.app,
      state,
      host.dataDir,
      () => {}
    );
    service.start();
    await emit(
      host,
      OTHER,
      [{ path: 'design.beam', value: 4.1 }],
      '2024-06-01T10:01:00.000Z',
      'ais.1'
    );
    expect(rows(buffer, OTHER)).to.have.lengthOf(1);
    expect(rows(buffer, OTHER)[0].value_beam).to.equal(4.1);

    // The server is killed: neither stop() nor the periodic flush runs, so
    // the state file still says "name only" while the buffer has the beam.
    const killed = service;
    host.dropBusHandlers();
    service = new VesselIdentityService(
      host.app,
      state,
      host.dataDir,
      () => {}
    );
    service.start();

    // The upstream replay repeats everything, one path per message.
    await emit(
      host,
      OTHER,
      [{ path: 'design.beam', value: 4.1 }],
      '2024-06-02T10:00:00.000Z'
    );
    await emit(
      host,
      OTHER,
      [{ path: '', value: { name: 'Ariel' } }],
      '2024-06-02T10:00:00.000Z'
    );
    expect(rows(buffer, OTHER)).to.have.lengthOf(1);
    expect(buffer.getStats().totalRecords).to.equal(1);

    // Still one row after a further completion across the restart.
    await emit(
      host,
      OTHER,
      [{ path: 'sensors.ais.class', value: 'B' }],
      '2024-06-02T10:00:00.000Z'
    );
    expect(buffer.getStats().totalRecords).to.equal(1);
    expect(rows(buffer, OTHER)[0].value_aisClass).to.equal('B');
    killed.stop();
  });

  describe('seeding from the full model at start', () => {
    // The server emits defaults (self name, design.*) once before plugins
    // load, so the service must pick them up from the model, not the bus.
    const model = {
      [SELF_ID]: {
        name: 'Zennora',
        design: {
          length: {
            value: { overall: 15 },
            $source: 'defaults',
            timestamp: '2024-06-01T09:00:00.000Z',
          },
          beam: { value: 5, $source: 'defaults', timestamp: T0 },
        },
      },
      'urn:mrn:imo:mmsi:244813000': {
        name: 'Ariel',
        mmsi: '244813000',
        sensors: { ais: { class: { value: 'B', $source: 'ais.1' } } },
      },
      'urn:mrn:imo:mmsi:999999999': { navigation: {} },
    };

    beforeEach(async () => {
      service.stop();
      buffer.close();
      await host.cleanup();
      // Rebuild the host with a populated model, as after a server start.
      host = createFakeSignalK({ selfId: SELF_ID, vessels: model });
      buffer = new SQLiteBuffer({
        dbPath: path.join(host.dataDir, 'buffer.db'),
      });
      state = { sqliteBuffer: buffer } as unknown as PluginState;
      service = new VesselIdentityService(
        host.app,
        state,
        host.dataDir,
        () => {}
      );
      service.start();
    });

    it('writes a seeded self identity at start, one row for all parts', async () => {
      const got = rows(buffer, SELF);
      expect(got).to.have.lengthOf(1);
      expect(got[0].value_name).to.equal('Zennora');
      expect(got[0].value_lengthOverall).to.equal(15);
      expect(got[0].value_beam).to.equal(5);
      expect(got[0].source_label).to.equal('defaults');
      expect(got[0].signalk_timestamp).to.equal(T0);
    });

    it('seeds other vessels and skips vessels without identity', async () => {
      const got = rows(buffer, OTHER);
      expect(got).to.have.lengthOf(1);
      expect(got[0].value_mmsi).to.equal('244813000');
      expect(got[0].value_aisClass).to.equal('B');
      expect(
        rows(buffer, 'vessels.urn:mrn:imo:mmsi:999999999')
      ).to.have.lengthOf(0);
    });

    it('does not rewrite a seeded identity after a restart', async () => {
      service.stop();
      service = new VesselIdentityService(
        host.app,
        state,
        host.dataDir,
        () => {}
      );
      service.start();
      expect(rows(buffer, SELF)).to.have.lengthOf(1);
    });
  });

  describe('a bounded memory that never writes an identity twice (#137)', () => {
    const A = 'vessels.urn:mrn:imo:mmsi:111111111';
    const B = 'vessels.urn:mrn:imo:mmsi:222222222';
    const C = 'vessels.urn:mrn:imo:mmsi:333333333';
    const stateFile = () => path.join(host.dataDir, 'identity-state.json');
    const remembered = async () =>
      Object.keys((await fs.readJson(stateFile())).lastWritten);

    /** A service that remembers two vessels, on the given buffer. */
    function small(buf: SQLiteBuffer): VesselIdentityService {
      const s = new VesselIdentityService(
        host.app,
        { sqliteBuffer: buf } as unknown as PluginState,
        host.dataDir,
        () => {},
        { maxRemembered: 2 }
      );
      s.start();
      return s;
    }

    beforeEach(async () => {
      await service.stop();
    });

    it('remembers at most the limit, the most recently written', async () => {
      service = small(buffer);
      for (const [ctx, name] of [[A, 'Alpha'], [B, 'Bravo'], [C, 'Charlie']]) {
        await emit(host, ctx, [{ path: '', value: { name } }]);
      }
      await service.stop();
      expect(await remembered()).to.deep.equal([B, C]);
    });

    it('does not write a vessel it no longer remembers again, from the buffer', async () => {
      service = small(buffer);
      await emit(host, A, [{ path: '', value: { name: 'Alpha' } }]);
      await emit(host, B, [{ path: '', value: { name: 'Bravo' } }]);
      await emit(host, C, [{ path: '', value: { name: 'Charlie' } }]);
      // A is no longer remembered; heard again unchanged, nothing is written.
      await emit(host, A, [{ path: '', value: { name: 'Alpha' } }], '2024-06-02T10:00:00.000Z');
      expect(rows(buffer, A)).to.have.lengthOf(1);
      // A real change is still written.
      await emit(host, A, [{ path: '', value: { name: 'Alpha II' } }], '2024-06-03T10:00:00.000Z');
      expect(rows(buffer, A).map(r => r.value_name)).to.deep.equal(['Alpha', 'Alpha II']);
    });

    describe('when the identity is only in parquet', () => {
      let fresh: SQLiteBuffer;

      beforeEach(async () => {
        await DuckDBPool.initialize();
        // A, then B and C, so A is not remembered; all exported to parquet.
        service = small(buffer);
        await emit(host, A, [
          { path: '', value: { name: 'Alpha', mmsi: '111111111' } },
          { path: 'design.beam', value: 4.2 },
        ]);
        await emit(host, B, [{ path: '', value: { name: 'Bravo' } }]);
        await emit(host, C, [{ path: '', value: { name: 'Charlie' } }]);
        await service.stop();
        await new ParquetExportService(
          buffer,
          new ParquetWriter({ format: 'parquet', app: host.app }),
          {
            outputDirectory: host.dataDir,
            filenamePrefix: 'signalk_data',
            useHivePartitioning: true,
            dailyExportHour: 4,
          },
          host.app
        ).exportDayToParquet(new Date());
        // A buffer past its retention: nothing of A left in it.
        fresh = new SQLiteBuffer({
          dbPath: path.join(host.dataDir, 'later', 'buffer.db'),
        });
        service = small(fresh);
      });

      afterEach(async () => {
        await service.stop();
        if (fresh.isOpen()) fresh.close();
        await DuckDBPool.shutdown();
      });

      it('does not write it again when heard unchanged', async () => {
        await emit(host, A, [
          { path: '', value: { name: 'Alpha', mmsi: '111111111' } },
          { path: 'design.beam', value: 4.2 },
        ], '2024-06-02T10:00:00.000Z');
        await service.stop();
        expect(rows(fresh, A)).to.have.lengthOf(0);
      });

      it('writes it, whole, when it has changed', async () => {
        await emit(host, A, [{ path: 'design.beam', value: 4.5 }], '2024-06-02T10:00:00.000Z');
        await service.stop();
        const got = rows(fresh, A);
        expect(got).to.have.lengthOf(1);
        expect(got[0].value_name).to.equal('Alpha');
        expect(got[0].value_mmsi).to.equal('111111111');
        expect(got[0].value_beam).to.equal(4.5);
      });
    });

    it('cuts a state file saved before the limit, keeping the newest', async () => {
      const lastWritten: Record<string, string> = {};
      for (const [ctx, name] of [[A, 'Alpha'], [B, 'Bravo'], [C, 'Charlie']]) {
        lastWritten[ctx] = JSON.stringify({ name });
      }
      await fs.writeJson(stateFile(), { lastWritten });
      service = small(buffer);
      await service.stop();
      expect(await remembered()).to.deep.equal([B, C]);
    });
  });

  it('detaches every bus listener on stop, and does not accumulate them', async () => {
    // The bus hands back an unsubscribe function, not an object carrying one,
    // so a stop() that calls `handle.unsubscribe?.()` detaches nothing and
    // silently leaves a set of live listeners behind on every restart. They
    // write no rows, because a restarted service is a new instance whose
    // predecessor's `running` flag stays false — so nothing downstream shows
    // it, and only the handler count does.
    const attached = host.activeBusHandlers();
    expect(attached, 'the service should have subscribed').to.be.greaterThan(0);

    service.stop();
    expect(
      host.activeBusHandlers(),
      'stop() must leave no listener attached'
    ).to.equal(0);

    // A restart attaches the same number again, not another set on top.
    service = new VesselIdentityService(
      host.app,
      state,
      host.dataDir,
      () => {}
    );
    service.start();
    expect(host.activeBusHandlers()).to.equal(attached);
    service.stop();
    expect(host.activeBusHandlers()).to.equal(0);
  });

  it('starts on a host whose app has no signalk emitter at all', async () => {
    // The Signal K plugin registry's activation check starts the plugin
    // against a stand-in app that has streambundle but no `signalk`
    // emitter; 1.0.0 threw "this.emitter.on is not a function" there.
    const bare = createFakeSignalK({ selfId: SELF_ID });
    delete (bare.app as unknown as { signalk?: unknown }).signalk;
    const svc = new VesselIdentityService(
      bare.app,
      state,
      bare.dataDir,
      () => {}
    );
    expect(() => svc.start()).to.not.throw();
    await emit(bare, OTHER, [{ path: '', value: { name: 'Ariel' } }], T0, 'ais.1');
    expect(rows(buffer, OTHER).map(r => r.value_name)).to.deep.equal(['Ariel']);
    svc.stop();
  });

  it('ignores deltas for non-vessel contexts and meta deltas', async () => {
    await emit(host, 'atons.urn:mrn:imo:mmsi:992471234', [
      { path: '', value: { name: 'Buoy' } },
    ]);
    // A meta update arrives on the same path bus as a value; the service's
    // isMeta filter must drop it.
    host.emitBus('design.beam', {
      context: OTHER,
      path: 'design.beam',
      value: { units: 'm' },
      timestamp: T0,
      $source: 'ais.1',
      isMeta: true,
    });
    expect(rows(buffer, 'atons.urn:mrn:imo:mmsi:992471234')).to.have.lengthOf(
      0
    );
    expect(rows(buffer, OTHER)).to.have.lengthOf(0);
  });

  it('is readable through the v2 History API provider as an object path', async () => {
    await DuckDBPool.initialize();
    try {
      await emit(
        host,
        SELF,
        [
          { path: '', value: { name: 'Zennora', mmsi: 368396230 } },
          { path: 'design.beam', value: 3.9 },
        ],
        T0,
        'ais.self'
      );

      const provider = new HistoryProvider(
        SELF_ID,
        host.dataDir,
        {
          debug: () => {},
          error: () => {},
          getMetadata: () => undefined,
          getSelfPath: () => undefined,
          selfId: SELF_ID,
          selfContext: SELF,
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
        } as any,
        () => {}
      );
      provider.setSqliteBuffer(buffer);
      DuckDBPool.initializeSQLiteBuffer(path.join(host.dataDir, 'buffer.db'));

      const res = await provider.getValues({
        from: '2024-06-01T00:00:00Z',
        to: '2024-06-01T23:59:59Z',
        context: 'vessels.self',
        resolution: 60,
        pathSpecs: [{ path: 'identity', aggregate: 'first', parameter: [] }],
      } as unknown as ValuesRequest);
      expect(res.data).to.have.lengthOf(1);
      expect(res.data[0][1]).to.deep.equal({
        name: 'Zennora',
        mmsi: '368396230',
        beam: 3.9,
      });
    } finally {
      await DuckDBPool.shutdown();
    }
  });
});
