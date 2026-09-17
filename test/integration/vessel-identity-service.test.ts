/**
 * Vessel identity capture end to end: whole delta messages on app.signalk
 * are folded into one `identity` row per vessel in the SQLite buffer,
 * written for every vessel heard, once per message that changes something,
 * and not rewritten after a restart. The rows are readable through the v2
 * History API provider.
 */
import { expect } from 'chai';
import * as path from 'path';
import * as fs from 'fs-extra';
import { EventEmitter } from 'events';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { HistoryProvider } from '../../src/history-provider';
import { VesselIdentityService } from '../../src/services/vessel-identity-service';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import type { PluginState } from '../../src/types';
import type { ValuesRequest } from '@signalk/server-api/dist/history';

const SELF_ID = 'identityself';
const SELF = `vessels.${SELF_ID}`;
const OTHER = 'vessels.urn:mrn:imo:mmsi:244813000';
const T0 = '2024-06-01T10:00:00.000Z';

/** A whole delta message with one update, as the server emits it. */
function delta(
  context: string,
  values: Array<{ path: string; value: unknown }>,
  timestamp = T0,
  $source?: string
): Record<string, unknown> {
  return { context, updates: [{ timestamp, $source, values }] };
}

function rows(buffer: SQLiteBuffer, context: string) {
  return buffer.getRowsForFederation(
    'identity',
    context,
    '2024-01-01T00:00:00.000Z',
    '2030-01-01T00:00:00.000Z',
    0,
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
    service = new VesselIdentityService(host.app, state, host.dataDir, () => {});
    service.start();
  });

  afterEach(async () => {
    service.stop();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  it('folds one AIS static report into one row, then writes only on change', () => {
    // One AIS type 5 report: the server emits it as a single delta carrying
    // several identity values.
    const report = [
      { path: '', value: { name: 'Ariel', mmsi: 244813000 } },
      { path: 'design.length', value: { overall: 12.5 } },
      { path: 'design.beam', value: 4.1 },
      { path: 'communication.callsignVhf', value: 'PD1234' },
      { path: 'sensors.ais.class', value: 'B' },
    ];
    host.emitDelta(delta(OTHER, report, T0, 'ais.1'));
    const first = rows(buffer, OTHER);
    expect(first).to.have.lengthOf(1);
    expect(first[0].value_name).to.equal('Ariel');
    expect(first[0].value_lengthOverall).to.equal(12.5);
    expect(first[0].value_beam).to.equal(4.1);
    expect(first[0].value_callsignVhf).to.equal('PD1234');
    expect(first[0].value_aisClass).to.equal('B');

    // The static report repeats: no new row.
    host.emitDelta(delta(OTHER, report, '2024-06-01T10:06:00.000Z', 'ais.1'));
    expect(rows(buffer, OTHER)).to.have.lengthOf(1);

    // Ship type arrives: it only completes the identity, so the row still in
    // the buffer is extended rather than joined by a second one.
    host.emitDelta(
      delta(
        OTHER,
        [{ path: 'design.aisShipType', value: { id: 36, name: 'Sailing' } }],
        '2024-06-01T10:07:00.000Z',
        'ais.1'
      )
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
    host.emitDelta(
      delta(
        OTHER,
        [{ path: '', value: { name: 'Ariel II' } }],
        '2024-06-01T10:08:00.000Z',
        'ais.1'
      )
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

  it('records the identity of the own vessel too', () => {
    host.emitDelta(delta(SELF, [{ path: '', value: { name: 'Zennora' } }]));
    expect(rows(buffer, SELF).map(r => r.value_name)).to.deep.equal(['Zennora']);
  });

  it('does not rewrite a vessel after a restart when nothing changed', async () => {
    host.emitDelta(delta(OTHER, [{ path: '', value: { name: 'Ariel' } }]));
    service.stop();
    expect(await fs.pathExists(path.join(host.dataDir, 'identity-state.json'))).to.equal(true);

    service = new VesselIdentityService(host.app, state, host.dataDir, () => {});
    service.start();
    host.emitDelta(
      delta(OTHER, [{ path: '', value: { name: 'Ariel' } }], '2024-06-02T10:00:00.000Z')
    );
    expect(rows(buffer, OTHER)).to.have.lengthOf(1);
  });

  it('ignores a one-path-per-message replay of a known identity after a restart', () => {
    host.emitDelta(
      delta(
        OTHER,
        [
          { path: '', value: { name: 'Ariel', mmsi: 244813000 } },
          { path: 'design.length', value: { overall: 12.5 } },
          { path: 'design.beam', value: 4.1 },
        ],
        T0,
        'ais.1'
      )
    );
    expect(rows(buffer, OTHER)).to.have.lengthOf(1);
    service.stop();

    // An upstream Signal K websocket replays its cache on connect, one path
    // per message. Nothing here is new, so nothing is written.
    service = new VesselIdentityService(host.app, state, host.dataDir, () => {});
    service.start();
    host.emitDelta(delta(OTHER, [{ path: 'design.length', value: { overall: 12.5 } }]));
    host.emitDelta(delta(OTHER, [{ path: 'design.beam', value: 4.1 }]));
    host.emitDelta(delta(OTHER, [{ path: '', value: { name: 'Ariel', mmsi: 244813000 } }]));
    expect(rows(buffer, OTHER)).to.have.lengthOf(1);

    // A real change still writes one row carrying the whole identity.
    host.emitDelta(
      delta(OTHER, [{ path: 'design.beam', value: 4.5 }], '2024-06-02T10:00:00.000Z')
    );
    const got = rows(buffer, OTHER);
    expect(got).to.have.lengthOf(2);
    expect(got[1].value_name).to.equal('Ariel');
    expect(got[1].value_lengthOverall).to.equal(12.5);
    expect(got[1].value_beam).to.equal(4.5);
  });

  it('folds a one-path-per-message first hearing into one row', () => {
    // An upstream server replays its cache one path per message. A vessel
    // not yet on file arrives as class, then mmsi, then dimensions, then
    // name, each in its own delta.
    host.emitDelta(delta(OTHER, [{ path: 'sensors.ais.class', value: 'A' }], T0, 'ais.1'));
    host.emitDelta(delta(OTHER, [{ path: '', value: { mmsi: 244813000 } }], T0, 'ais.1'));
    host.emitDelta(
      delta(OTHER, [{ path: 'design.length', value: { overall: 40 } }], '2024-06-01T10:00:01.000Z', 'ais.1')
    );
    host.emitDelta(
      delta(OTHER, [{ path: '', value: { name: 'Ariel' } }], '2024-06-01T10:00:01.000Z', 'ais.1')
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

  it('does not extend a row that has already been exported', () => {
    host.emitDelta(delta(OTHER, [{ path: 'sensors.ais.class', value: 'A' }], T0, 'ais.1'));
    // The daily export took the row to Parquet; it is immutable now.
    buffer.markDateExported(OTHER, 'identity', new Date(), 'batch-1');
    expect(rows(buffer, OTHER)).to.have.lengthOf(0);

    host.emitDelta(delta(OTHER, [{ path: '', value: { mmsi: 244813000 } }], T0, 'ais.1'));
    const got = rows(buffer, OTHER);
    expect(got).to.have.lengthOf(1);
    expect(got[0].value_aisClass).to.equal('A');
    expect(got[0].value_mmsi).to.equal('244813000');
    expect(buffer.getStats().totalRecords).to.equal(2);
  });

  it('trusts the buffer over a stale state file after a kill', () => {
    host.emitDelta(delta(OTHER, [{ path: '', value: { name: 'Ariel' } }], T0, 'ais.1'));
    service.stop(); // state file now says: name only
    service = new VesselIdentityService(host.app, state, host.dataDir, () => {});
    service.start();
    host.emitDelta(
      delta(OTHER, [{ path: 'design.beam', value: 4.1 }], '2024-06-01T10:01:00.000Z', 'ais.1')
    );
    expect(rows(buffer, OTHER)).to.have.lengthOf(1);
    expect(rows(buffer, OTHER)[0].value_beam).to.equal(4.1);

    // The server is killed: neither stop() nor the periodic flush runs, so
    // the state file still says "name only" while the buffer has the beam.
    const killed = service;
    (host.app as unknown as { signalk: EventEmitter }).signalk.removeAllListeners('delta');
    service = new VesselIdentityService(host.app, state, host.dataDir, () => {});
    service.start();

    // The upstream replay repeats everything, one path per message.
    host.emitDelta(delta(OTHER, [{ path: 'design.beam', value: 4.1 }], '2024-06-02T10:00:00.000Z'));
    host.emitDelta(delta(OTHER, [{ path: '', value: { name: 'Ariel' } }], '2024-06-02T10:00:00.000Z'));
    expect(rows(buffer, OTHER)).to.have.lengthOf(1);
    expect(buffer.getStats().totalRecords).to.equal(1);

    // Still one row after a further completion across the restart.
    host.emitDelta(delta(OTHER, [{ path: 'sensors.ais.class', value: 'B' }], '2024-06-02T10:00:00.000Z'));
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
      buffer = new SQLiteBuffer({ dbPath: path.join(host.dataDir, 'buffer.db') });
      state = { sqliteBuffer: buffer } as unknown as PluginState;
      service = new VesselIdentityService(host.app, state, host.dataDir, () => {});
      service.start();
    });

    it('writes a seeded self identity at start, one row for all parts', () => {
      const got = rows(buffer, SELF);
      expect(got).to.have.lengthOf(1);
      expect(got[0].value_name).to.equal('Zennora');
      expect(got[0].value_lengthOverall).to.equal(15);
      expect(got[0].value_beam).to.equal(5);
      expect(got[0].source_label).to.equal('defaults');
      expect(got[0].signalk_timestamp).to.equal(T0);
    });

    it('seeds other vessels and skips vessels without identity', () => {
      const got = rows(buffer, OTHER);
      expect(got).to.have.lengthOf(1);
      expect(got[0].value_mmsi).to.equal('244813000');
      expect(got[0].value_aisClass).to.equal('B');
      expect(rows(buffer, 'vessels.urn:mrn:imo:mmsi:999999999')).to.have.lengthOf(0);
    });

    it('does not rewrite a seeded identity after a restart', () => {
      service.stop();
      service = new VesselIdentityService(host.app, state, host.dataDir, () => {});
      service.start();
      expect(rows(buffer, SELF)).to.have.lengthOf(1);
    });
  });

  it('ignores deltas for non-vessel contexts and meta deltas', () => {
    host.emitDelta(
      delta('atons.urn:mrn:imo:mmsi:992471234', [{ path: '', value: { name: 'Buoy' } }])
    );
    // A meta-only update carries no values.
    host.emitDelta({
      context: OTHER,
      updates: [{ timestamp: T0, meta: [{ path: 'name', value: { units: '' } }] }],
    });
    expect(rows(buffer, 'atons.urn:mrn:imo:mmsi:992471234')).to.have.lengthOf(0);
    expect(rows(buffer, OTHER)).to.have.lengthOf(0);
  });

  it('is readable through the v2 History API provider as an object path', async () => {
    await DuckDBPool.initialize();
    try {
      host.emitDelta(
        delta(
          SELF,
          [
            { path: '', value: { name: 'Zennora', mmsi: 368396230 } },
            { path: 'design.beam', value: 3.9 },
          ],
          T0,
          'ais.self'
        )
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
