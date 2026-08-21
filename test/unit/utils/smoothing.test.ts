/**
 * Unit tests for the History API sma/ema smoothing math. These functions are
 * the moving-window post-processing the v2 provider applies to a bucketed
 * series, so the edge cases that matter are: parameter parsing with spec
 * defaults, windows longer/shorter than the series, EMA alpha bounds, and the
 * circular variants staying sane across the 0/2π (and ±180°) wrap where a
 * naive linear mean would produce garbage.
 */
import { expect } from 'chai';
import {
  parseSmaWindow,
  parseEmaAlpha,
  sma,
  ema,
  smaCircularRad,
  emaCircularRad,
  smoothCircularDeg,
} from '../../../src/utils/smoothing';

const near = (a: number, b: number, eps = 1e-9) => Math.abs(a - b) <= eps;

describe('smoothing: parameter parsing', () => {
  it('defaults SMA window to 5 on empty/undefined/garbage', () => {
    expect(parseSmaWindow(undefined)).to.equal(5);
    expect(parseSmaWindow([])).to.equal(5);
    expect(parseSmaWindow(['x'])).to.equal(5);
    expect(parseSmaWindow(['0'])).to.equal(5); // must be >= 1
    expect(parseSmaWindow(['-3'])).to.equal(5);
    expect(parseSmaWindow(['2.5'])).to.equal(5); // must be integer
  });

  it('accepts a valid SMA window', () => {
    expect(parseSmaWindow(['3'])).to.equal(3);
    expect(parseSmaWindow(['1'])).to.equal(1);
  });

  it('defaults EMA alpha to 0.2 on empty/undefined/out-of-range', () => {
    expect(parseEmaAlpha(undefined)).to.equal(0.2);
    expect(parseEmaAlpha([])).to.equal(0.2);
    expect(parseEmaAlpha(['x'])).to.equal(0.2);
    expect(parseEmaAlpha(['0'])).to.equal(0.2); // must be > 0
    expect(parseEmaAlpha(['-0.5'])).to.equal(0.2);
    expect(parseEmaAlpha(['1.5'])).to.equal(0.2); // must be <= 1
  });

  it('accepts a valid EMA alpha including the bounds', () => {
    expect(parseEmaAlpha(['0.5'])).to.equal(0.5);
    expect(parseEmaAlpha(['1'])).to.equal(1);
  });
});

describe('smoothing: SMA (linear)', () => {
  it('is a trailing mean that grows to the window size', () => {
    expect(sma([1, 2, 3, 4, 5], 3)).to.deep.equal([1, 1.5, 2, 3, 4]);
  });

  it('handles a window longer than the series (mean of all so far)', () => {
    expect(sma([1, 2], 5)).to.deep.equal([1, 1.5]);
  });

  it('window of 1 is the identity', () => {
    expect(sma([3, 1, 4, 1, 5], 1)).to.deep.equal([3, 1, 4, 1, 5]);
  });

  it('empty series yields empty', () => {
    expect(sma([], 5)).to.deep.equal([]);
  });
});

describe('smoothing: EMA (linear)', () => {
  it('seeds with the first value and applies the recursion', () => {
    // alpha 0.5: [1, 0.5*2+0.5*1=1.5, 0.5*3+0.5*1.5=2.25]
    const out = ema([1, 2, 3], 0.5);
    expect(near(out[0], 1)).to.equal(true);
    expect(near(out[1], 1.5)).to.equal(true);
    expect(near(out[2], 2.25)).to.equal(true);
  });

  it('alpha of 1 is the identity', () => {
    expect(ema([3, 1, 4], 1)).to.deep.equal([3, 1, 4]);
  });

  it('single-element series returns that element', () => {
    expect(ema([7], 0.2)).to.deep.equal([7]);
  });
});

describe('smoothing: circular (radians)', () => {
  it('SMA across the 0/2π wrap stays near 0, not near π', () => {
    // Two angles just either side of 0. Linear mean would swing toward π.
    const out = smaCircularRad([-0.1, 0.1], 2);
    expect(Math.abs(out[1])).to.be.lessThan(1e-6); // ~0
  });

  it('EMA across the wrap tracks toward the true bearing, not the midpoint', () => {
    // Series hovering around 0 with a wrap-crossing sample expressed as ~2π.
    const twoPiMinus = 2 * Math.PI - 0.1;
    const out = emaCircularRad([0.1, twoPiMinus], 0.5);
    // Result must be a small-magnitude angle near 0 (equivalently ~2π), not ~π.
    const wrapped = Math.atan2(Math.sin(out[1]), Math.cos(out[1]));
    expect(Math.abs(wrapped)).to.be.lessThan(0.2);
  });
});

describe('smoothing: circular degrees (longitude / antimeridian)', () => {
  it('SMA of 179° and −179° lands near ±180°, not 0°', () => {
    const out = smoothCircularDeg([179, -179], 'sma', ['2']);
    expect(Math.abs(out[1])).to.be.greaterThan(179.9);
    expect(Math.abs(out[1])).to.be.at.most(180 + 1e-6);
  });

  it('stays in the canonical (−180, 180] range', () => {
    const out = smoothCircularDeg([170, -170, 175], 'sma', ['3']);
    for (const v of out) {
      expect(v).to.be.greaterThan(-180 - 1e-9);
      expect(v).to.be.at.most(180 + 1e-9);
    }
  });

  it('away from the wrap it matches ordinary averaging', () => {
    // 10° and 20°, window 2 -> ~15°, same as a linear mean here.
    const out = smoothCircularDeg([10, 20], 'sma', ['2']);
    expect(Math.abs(out[1] - 15)).to.be.lessThan(1e-6);
  });
});

describe('smoothing: non-finite window/alpha degrade to identity', () => {
  // The parse* helpers never emit non-finite parameters, but sma/ema are
  // exported directly — a NaN window/alpha must not smear NaN over the output.
  it('sma with a NaN or Infinite window returns the input unchanged', () => {
    expect(sma([1, 2, 3], NaN)).to.deep.equal([1, 2, 3]);
    expect(sma([1, 2, 3], Infinity)).to.deep.equal([1, 2, 3]);
  });

  it('ema with a NaN alpha returns the input unchanged', () => {
    expect(ema([1, 2, 3], NaN)).to.deep.equal([1, 2, 3]);
  });
});
