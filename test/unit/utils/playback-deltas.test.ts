import { expect } from 'chai';
import {
  decodeScalar,
  decodeJson,
  groupRowsIntoDeltas,
  scalarKindFromColumnType,
  unfoldIdentity,
  PlaybackRow,
} from '../../../src/utils/playback-deltas';

describe('playback deltas', () => {
  describe('scalarKindFromColumnType', () => {
    it('maps DuckDB and parquet type names to a scalar kind', () => {
      expect(scalarKindFromColumnType('DOUBLE')).to.equal('number');
      expect(scalarKindFromColumnType('INT64')).to.equal('number');
      expect(scalarKindFromColumnType('BOOLEAN')).to.equal('boolean');
      expect(scalarKindFromColumnType('VARCHAR')).to.equal('string');
      expect(scalarKindFromColumnType('BYTE_ARRAY')).to.equal('string');
    });
  });

  describe('decodeScalar', () => {
    it('honours an exact kind from parquet', () => {
      expect(decodeScalar('2.5', 'number')).to.equal(2.5);
      expect(decodeScalar('true', 'boolean')).to.equal(true);
      expect(decodeScalar('123', 'string')).to.equal('123');
      expect(decodeScalar('nan', 'number')).to.equal(null);
      expect(decodeScalar(null, 'number')).to.equal(null);
    });

    it('recognises numbers and booleans by shape when the kind is unknown', () => {
      expect(decodeScalar('2.829445161237219', 'unknown')).to.equal(
        2.829445161237219
      );
      expect(decodeScalar('-1e3', 'unknown')).to.equal(-1000);
      expect(decodeScalar('true', 'unknown')).to.equal(true);
      expect(decodeScalar('false', 'unknown')).to.equal(false);
      expect(decodeScalar('motoring', 'unknown')).to.equal('motoring');
      expect(decodeScalar('', 'unknown')).to.equal('');
    });
  });

  describe('decodeJson', () => {
    it('parses stored JSON and tolerates junk', () => {
      expect(decodeJson('{"latitude":1,"longitude":2}')).to.deep.equal({
        latitude: 1,
        longitude: 2,
      });
      expect(decodeJson('{not json')).to.equal(null);
      expect(decodeJson(null)).to.equal(null);
    });
  });

  describe('unfoldIdentity', () => {
    it('reverses the fold into the paths the live feed uses', () => {
      expect(
        unfoldIdentity({
          name: 'Ariel',
          mmsi: '244813000',
          aisShipTypeId: 36,
          aisShipTypeName: 'Sailing',
          lengthOverall: 12.5,
          beam: 4.1,
          callsignVhf: 'PD1234',
          aisClass: 'B',
        })
      ).to.deep.equal([
        { path: '', value: { name: 'Ariel', mmsi: '244813000' } },
        { path: 'design.aisShipType', value: { id: 36, name: 'Sailing' } },
        { path: 'design.length', value: { overall: 12.5 } },
        { path: 'design.beam', value: 4.1 },
        { path: 'communication.callsignVhf', value: 'PD1234' },
        { path: 'sensors.ais.class', value: 'B' },
      ]);
    });

    it('emits only the parts that are present', () => {
      expect(unfoldIdentity({ mmsi: '1' })).to.deep.equal([
        { path: '', value: { mmsi: '1' } },
      ]);
      expect(unfoldIdentity({})).to.deep.equal([]);
    });
  });

  describe('groupRowsIntoDeltas', () => {
    const row = (
      timestamp: string,
      context: string,
      path: string,
      value: unknown,
      source: string | null = 'gps.1'
    ): PlaybackRow => ({ timestamp, context, path, value, source });

    it('groups rows by instant, vessel and source in time order', () => {
      const deltas = groupRowsIntoDeltas([
        row('2024-06-01T10:00:01.000Z', 'vessels.a', 'navigation.speedOverGround', 3),
        row('2024-06-01T10:00:00.000Z', 'vessels.b', 'navigation.position', { latitude: 1, longitude: 2 }, 'ais.1'),
        row('2024-06-01T10:00:00.000Z', 'vessels.a', 'navigation.position', { latitude: 3, longitude: 4 }),
        row('2024-06-01T10:00:00.000Z', 'vessels.a', 'navigation.courseOverGroundTrue', 1.5),
        row('2024-06-01T10:00:00.000Z', 'vessels.a', 'navigation.headingTrue', 1.4, 'compass.1'),
        row('2024-06-01T10:00:00.000Z', 'vessels.a', 'environment.depth.belowKeel', 9, null),
      ]);
      expect(deltas.map(d => [d.context, d.updates[0].timestamp, d.updates[0].$source])).to.deep.equal([
        ['vessels.a', '2024-06-01T10:00:00.000Z', undefined],
        ['vessels.a', '2024-06-01T10:00:00.000Z', 'compass.1'],
        ['vessels.a', '2024-06-01T10:00:00.000Z', 'gps.1'],
        ['vessels.b', '2024-06-01T10:00:00.000Z', 'ais.1'],
        ['vessels.a', '2024-06-01T10:00:01.000Z', 'gps.1'],
      ]);
      expect(deltas[2].updates[0].values).to.deep.equal([
        { path: 'navigation.courseOverGroundTrue', value: 1.5 },
        { path: 'navigation.position', value: { latitude: 3, longitude: 4 } },
      ]);
      expect(deltas[0].updates[0]).to.not.have.property('$source');
    });

    it('replays an identity row as the paths it stands for, never as identity', () => {
      const deltas = groupRowsIntoDeltas([
        row('2024-06-01T10:00:00.000Z', 'vessels.a', 'identity', { name: 'Ariel', beam: 4 }, 'ais.1'),
      ]);
      expect(deltas).to.have.lengthOf(1);
      expect(deltas[0].updates[0].values).to.deep.equal([
        { path: '', value: { name: 'Ariel' } },
        { path: 'design.beam', value: 4 },
      ]);
    });

    it('drops an identity row whose value is unreadable', () => {
      expect(
        groupRowsIntoDeltas([row('2024-06-01T10:00:00.000Z', 'vessels.a', 'identity', null)])
      ).to.deep.equal([]);
    });
  });
});
