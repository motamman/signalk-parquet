/**
 * Unit tests for the SignalK context/path boundary validators. These reject
 * untrusted values before they reach a read_parquet() glob or the filesystem,
 * so the key cases are the injection/traversal attempts that must throw.
 */
import { expect } from 'chai';
import * as path from 'path';
import {
  validateContext,
  validateSignalKPath,
  assertWithinDataDir,
} from '../../../src/utils/signalk-validation';

describe('validateContext', () => {
  it('accepts well-formed vessel contexts', () => {
    expect(validateContext('vessels.self')).to.equal('vessels.self');
    expect(validateContext('vessels.urn:mrn:imo:mmsi:230099999')).to.equal(
      'vessels.urn:mrn:imo:mmsi:230099999'
    );
    expect(
      validateContext('vessels.urn:mrn:signalk:uuid:1234-5678-90ab')
    ).to.equal('vessels.urn:mrn:signalk:uuid:1234-5678-90ab');
  });

  it('rejects a SQL-quote breakout attempt', () => {
    expect(() => validateContext("vessels.x')+(SELECT 1")).to.throw();
  });

  it('rejects glob wildcards that would widen the read_parquet glob', () => {
    expect(() => validateContext('vessels.*')).to.throw();
    expect(() => validateContext('vessels.a?b')).to.throw();
  });

  it('rejects path separators and traversal', () => {
    expect(() => validateContext('vessels.a/b')).to.throw();
    expect(() => validateContext('vessels.../etc')).to.throw();
    expect(() => validateContext('../../etc')).to.throw();
  });

  it('rejects empty and structurally-incomplete contexts', () => {
    expect(() => validateContext('')).to.throw();
    expect(() => validateContext('vessels.')).to.throw();
    expect(() => validateContext('vessels')).to.throw();
  });

  it('rejects dot-only and consecutive-dot context ids', () => {
    expect(() => validateContext('vessels..')).to.throw();
    expect(() => validateContext('vessels...etc')).to.throw();
    expect(() => validateContext('vessels.a..b')).to.throw();
  });
});

describe('validateSignalKPath', () => {
  it('accepts well-formed SignalK paths', () => {
    expect(validateSignalKPath('navigation.position')).to.equal(
      'navigation.position'
    );
    expect(validateSignalKPath('environment.wind.speedApparent')).to.equal(
      'environment.wind.speedApparent'
    );
    expect(validateSignalKPath('electrical.batteries.1.voltage')).to.equal(
      'electrical.batteries.1.voltage'
    );
  });

  it('rejects traversal, separators, and quotes', () => {
    expect(() => validateSignalKPath('..')).to.throw();
    expect(() => validateSignalKPath('a..b')).to.throw();
    expect(() => validateSignalKPath('a/b')).to.throw();
    expect(() => validateSignalKPath("a'b")).to.throw();
    expect(() => validateSignalKPath('navigation.position*')).to.throw();
  });

  it('rejects empty, leading-dot, and over-long paths', () => {
    expect(() => validateSignalKPath('')).to.throw();
    expect(() => validateSignalKPath('.navigation')).to.throw();
    expect(() => validateSignalKPath('a.' + 'b'.repeat(300))).to.throw();
  });
});

describe('assertWithinDataDir', () => {
  const dataDir = path.resolve('var', 'signalk', 'data');

  it('returns the resolved path for files inside the data dir', () => {
    const inside = path.join(dataDir, 'vessels', 'self', 'x.parquet');
    expect(assertWithinDataDir(dataDir, inside)).to.equal(path.resolve(inside));
  });

  it('allows the data dir itself', () => {
    expect(assertWithinDataDir(dataDir, dataDir)).to.equal(dataDir);
  });

  it('throws on a ../ traversal that escapes the data dir', () => {
    const escape = path.join(dataDir, '..', '..', 'etc', 'passwd');
    expect(() => assertWithinDataDir(dataDir, escape)).to.throw();
  });

  it('throws on a sibling dir sharing the data-dir name as a prefix', () => {
    const sibling = path.resolve('var', 'signalk', 'data-evil', 'f');
    expect(() => assertWithinDataDir(dataDir, sibling)).to.throw();
  });
});
