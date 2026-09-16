/**
 * Unit tests for the identity components extracted from deltas and merged
 * per vessel (utils/vessel-identity.ts).
 */
import { expect } from 'chai';
import {
  IDENTITY_PATH,
  identityFromDelta,
  isIdentityPath,
  mergeIdentity,
} from '../../../src/utils/vessel-identity';

describe('vessel identity', () => {
  it('names the identity path', () => {
    expect(IDENTITY_PATH).to.equal('identity');
    expect(isIdentityPath('identity')).to.equal(true);
    expect(isIdentityPath('navigation.position')).to.equal(false);
    expect(isIdentityPath(undefined)).to.equal(false);
  });

  it('reads name and mmsi from a root-bus object', () => {
    expect(
      identityFromDelta('', { name: 'Ariel', mmsi: 244813000, other: 1 })
    ).to.deep.equal({ name: 'Ariel', mmsi: '244813000' });
    expect(identityFromDelta('', { name: '   ' })).to.deep.equal({});
    expect(
      identityFromDelta('', { name: '  Ariel ', mmsi: ' 1 ' })
    ).to.deep.equal({ name: 'Ariel', mmsi: '1' });
    expect(identityFromDelta('', 'not an object')).to.deep.equal({});
  });

  it('reads the ship type as an object or a bare code', () => {
    expect(
      identityFromDelta('design.aisShipType', { id: 36, name: 'Sailing' })
    ).to.deep.equal({ aisShipTypeId: 36, aisShipTypeName: 'Sailing' });
    expect(identityFromDelta('design.aisShipType', 70)).to.deep.equal({
      aisShipTypeId: 70,
    });
  });

  it('reads length overall, beam, callsign and AIS class', () => {
    expect(
      identityFromDelta('design.length', { overall: 12.4, hull: 11 })
    ).to.deep.equal({ lengthOverall: 12.4 });
    expect(identityFromDelta('design.beam', 3.9)).to.deep.equal({ beam: 3.9 });
    expect(
      identityFromDelta('communication.callsignVhf', 'PD1234')
    ).to.deep.equal({ callsignVhf: 'PD1234' });
    expect(identityFromDelta('sensors.ais.class', 'B')).to.deep.equal({
      aisClass: 'B',
    });
  });

  it('ignores paths that carry no identity and non-finite numbers', () => {
    expect(identityFromDelta('navigation.speedOverGround', 5)).to.deep.equal(
      {}
    );
    expect(identityFromDelta('design.beam', NaN)).to.deep.equal({});
  });

  it('merges only changes and reports whether anything changed', () => {
    const known = { name: 'Ariel' };
    expect(mergeIdentity(known, { name: 'Ariel' })).to.equal(false);
    expect(mergeIdentity(known, { mmsi: '1' })).to.equal(true);
    expect(mergeIdentity(known, { name: 'Ariel II' })).to.equal(true);
    expect(known).to.deep.equal({ name: 'Ariel II', mmsi: '1' });
  });
});
