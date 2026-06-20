/**
 * Unit tests for angle conversion and wrap-around arithmetic. The threshold
 * automation compares vessel headings against configured windows, so the
 * shortest-difference and range checks must be exact at the wrap boundaries.
 */
import { expect } from 'chai';
import {
  degreesToRadians,
  radiansToDegrees,
  normalizeDegrees,
  normalizeRadians,
  angularDifferenceDegrees,
  angularDifferenceRadians,
  isAngleInRangeDegrees,
  isAngleInRangeRadians,
} from '../../../src/utils/angle-converter';

const EPSILON = 1e-12;

describe('degreesToRadians / radiansToDegrees', () => {
  it('converts 180 degrees to pi radians', () => {
    expect(degreesToRadians(180)).to.be.closeTo(Math.PI, EPSILON);
  });

  it('converts 0 degrees to 0 radians', () => {
    expect(degreesToRadians(0)).to.equal(0);
  });

  it('converts negative degrees', () => {
    expect(degreesToRadians(-90)).to.be.closeTo(-Math.PI / 2, EPSILON);
  });

  it('converts pi radians to 180 degrees', () => {
    expect(radiansToDegrees(Math.PI)).to.be.closeTo(180, EPSILON);
  });

  it('round-trips arbitrary values', () => {
    expect(radiansToDegrees(degreesToRadians(123.456))).to.be.closeTo(
      123.456,
      EPSILON
    );
  });
});

describe('normalizeDegrees', () => {
  const cases: Array<[number, number]> = [
    [0, 0],
    [359.5, 359.5],
    [360, 0],
    [450, 90],
    [720, 0],
    [-90, 270],
    [-360, 0],
    [-450, 270],
  ];
  cases.forEach(([input, expected]) => {
    it(`normalizes ${input} to ${expected}`, () => {
      expect(normalizeDegrees(input)).to.be.closeTo(expected, EPSILON);
    });
  });
});

describe('normalizeRadians', () => {
  it('keeps values inside [0, 2pi) unchanged', () => {
    expect(normalizeRadians(Math.PI)).to.be.closeTo(Math.PI, EPSILON);
  });

  it('wraps 2pi to 0', () => {
    expect(normalizeRadians(2 * Math.PI)).to.be.closeTo(0, EPSILON);
  });

  it('wraps values above 2pi', () => {
    expect(normalizeRadians(2.5 * Math.PI)).to.be.closeTo(
      0.5 * Math.PI,
      EPSILON
    );
  });

  it('wraps negative values into the positive range', () => {
    expect(normalizeRadians(-Math.PI / 2)).to.be.closeTo(
      1.5 * Math.PI,
      EPSILON
    );
  });
});

describe('angularDifferenceDegrees', () => {
  it('is zero for identical angles', () => {
    expect(angularDifferenceDegrees(42, 42)).to.equal(0);
  });

  it('takes the short way across north: 350 to 10 is +20', () => {
    expect(angularDifferenceDegrees(350, 10)).to.equal(20);
  });

  it('is signed: 10 to 350 is -20', () => {
    expect(angularDifferenceDegrees(10, 350)).to.equal(-20);
  });

  it('keeps the half-turn at +180', () => {
    expect(angularDifferenceDegrees(0, 180)).to.equal(180);
  });

  it('keeps -180 rather than wrapping it to +180', () => {
    expect(angularDifferenceDegrees(0, -180)).to.equal(-180);
  });

  it('wraps just past the half-turn: 0 to 181 is -179', () => {
    expect(angularDifferenceDegrees(0, 181)).to.equal(-179);
  });

  it('handles inputs beyond a full turn', () => {
    expect(angularDifferenceDegrees(0, 540)).to.equal(180);
  });
});

describe('angularDifferenceRadians', () => {
  it('takes the short way across the wrap', () => {
    expect(
      angularDifferenceRadians(1.9 * Math.PI, 0.1 * Math.PI)
    ).to.be.closeTo(0.2 * Math.PI, EPSILON);
  });

  it('is signed in the opposite direction', () => {
    expect(
      angularDifferenceRadians(0.1 * Math.PI, 1.9 * Math.PI)
    ).to.be.closeTo(-0.2 * Math.PI, EPSILON);
  });

  it('keeps the half-turn at +pi', () => {
    expect(angularDifferenceRadians(0, Math.PI)).to.be.closeTo(
      Math.PI,
      EPSILON
    );
  });
});

describe('isAngleInRangeDegrees', () => {
  it('accepts an angle inside the window', () => {
    expect(isAngleInRangeDegrees(5, 0, 10)).to.equal(true);
  });

  it('accepts the exact window edge', () => {
    expect(isAngleInRangeDegrees(10, 0, 10)).to.equal(true);
  });

  it('rejects an angle just outside the window', () => {
    expect(isAngleInRangeDegrees(10.001, 0, 10)).to.equal(false);
  });

  it('accepts across the 0/360 wrap', () => {
    // Use case: alarm window centred on north (heading 355 with a 10 degree
    // tolerance around 0 must trigger).
    expect(isAngleInRangeDegrees(355, 0, 10)).to.equal(true);
  });

  it('rejects with a negative tolerance', () => {
    expect(isAngleInRangeDegrees(0, 0, -1)).to.equal(false);
  });
});

describe('isAngleInRangeRadians', () => {
  it('accepts across the wrap', () => {
    expect(isAngleInRangeRadians(1.95 * Math.PI, 0, 0.2 * Math.PI)).to.equal(
      true
    );
  });

  it('rejects outside the window', () => {
    expect(isAngleInRangeRadians(Math.PI, 0, 0.5 * Math.PI)).to.equal(false);
  });
});
