/**
 * Unit tests for the geographic math used by geofencing thresholds and
 * spatial queries: Haversine distance, bearings, destination points, and
 * bounding-box construction (including the home-port anchor variants).
 */
import { expect } from 'chai';
import {
  calculateDistance,
  isPointInBoundingBox,
  createBoundingBoxFromRadius,
  calculateBearing,
  calculateDestinationPoint,
  calculateBoundingBoxFromHomePort,
} from '../../../src/utils/geo-calculator';

// One degree of latitude on the Haversine sphere (R = 6371000 m).
const METERS_PER_DEGREE = (Math.PI / 180) * 6371000; // ~111195 m

describe('calculateDistance', () => {
  it('is zero for identical points', () => {
    expect(calculateDistance(47.5, 8.7, 47.5, 8.7)).to.equal(0);
  });

  it('measures one degree of longitude at the equator', () => {
    expect(calculateDistance(0, 0, 0, 1)).to.be.closeTo(METERS_PER_DEGREE, 1);
  });

  it('measures one degree of latitude', () => {
    expect(calculateDistance(0, 0, 1, 0)).to.be.closeTo(METERS_PER_DEGREE, 1);
  });

  it('is symmetric', () => {
    const there = calculateDistance(47.3779, 8.5403, 46.948, 7.4474);
    const back = calculateDistance(46.948, 7.4474, 47.3779, 8.5403);
    expect(there).to.be.closeTo(back, 1e-6);
  });

  it('measures antipodal points as half the circumference', () => {
    expect(calculateDistance(0, 0, 0, 180)).to.be.closeTo(
      Math.PI * 6371000,
      10
    );
  });

  it('matches the known Zurich to Bern distance', () => {
    // Use case: cross-checking against the real-world straight-line distance
    // between Zurich HB and Bern station, roughly 95 km.
    const distance = calculateDistance(47.3779, 8.5403, 46.948, 7.4474);
    expect(distance).to.be.closeTo(95_000, 2_000);
  });
});

describe('isPointInBoundingBox', () => {
  const box = { west: 8, south: 47, east: 9, north: 48 };

  it('accepts a point inside', () => {
    expect(isPointInBoundingBox(47.5, 8.5, box)).to.equal(true);
  });

  it('accepts points exactly on the edges', () => {
    expect(isPointInBoundingBox(47, 8, box)).to.equal(true);
    expect(isPointInBoundingBox(48, 9, box)).to.equal(true);
  });

  it('rejects a point north of the box', () => {
    expect(isPointInBoundingBox(48.1, 8.5, box)).to.equal(false);
  });

  it('rejects a point west of the box', () => {
    expect(isPointInBoundingBox(47.5, 7.9, box)).to.equal(false);
  });

  describe('box crossing the 180 degree meridian', () => {
    // Use case: a geofence around Fiji, which straddles the antimeridian.
    const wrapped = { west: 170, south: -20, east: -170, north: -10 };

    it('accepts a point on the eastern-hemisphere side', () => {
      expect(isPointInBoundingBox(-15, 175, wrapped)).to.equal(true);
    });

    it('accepts a point on the western-hemisphere side', () => {
      expect(isPointInBoundingBox(-15, -175, wrapped)).to.equal(true);
    });

    it('accepts the boundary longitudes', () => {
      expect(isPointInBoundingBox(-15, 170, wrapped)).to.equal(true);
      expect(isPointInBoundingBox(-15, -170, wrapped)).to.equal(true);
    });

    it('rejects a longitude outside the wrapped band', () => {
      expect(isPointInBoundingBox(-15, 0, wrapped)).to.equal(false);
    });
  });
});

describe('createBoundingBoxFromRadius', () => {
  it('spans one degree per 111139 meters at the equator', () => {
    const box = createBoundingBoxFromRadius(0, 0, 111_139);
    expect(box.north).to.be.closeTo(1, 1e-9);
    expect(box.south).to.be.closeTo(-1, 1e-9);
    expect(box.east).to.be.closeTo(1, 1e-9);
    expect(box.west).to.be.closeTo(-1, 1e-9);
  });

  it('widens the longitude span at high latitude', () => {
    // cos(60 deg) = 0.5, so the longitude half-span doubles.
    const box = createBoundingBoxFromRadius(60, 0, 111_139);
    expect(box.north - box.south).to.be.closeTo(2, 1e-9);
    expect(box.east - box.west).to.be.closeTo(4, 1e-6);
  });

  it('collapses to the center for radius zero', () => {
    const box = createBoundingBoxFromRadius(47.5, 8.7, 0);
    expect(box.north).to.equal(47.5);
    expect(box.south).to.equal(47.5);
    expect(box.east).to.equal(8.7);
    expect(box.west).to.equal(8.7);
  });
});

describe('calculateBearing', () => {
  it('is 0 due north', () => {
    expect(calculateBearing(0, 0, 1, 0)).to.be.closeTo(0, 1e-9);
  });

  it('is 90 due east', () => {
    expect(calculateBearing(0, 0, 0, 1)).to.be.closeTo(90, 1e-9);
  });

  it('is 180 due south', () => {
    expect(calculateBearing(0, 0, -1, 0)).to.be.closeTo(180, 1e-9);
  });

  it('is 270 due west', () => {
    expect(calculateBearing(0, 0, 0, -1)).to.be.closeTo(270, 1e-9);
  });

  it('stays within [0, 360)', () => {
    const bearing = calculateBearing(10, 10, 9, 9);
    expect(bearing).to.be.at.least(0);
    expect(bearing).to.be.below(360);
  });
});

describe('calculateDestinationPoint', () => {
  it('travels north along a meridian', () => {
    const dest = calculateDestinationPoint(0, 0, 0, METERS_PER_DEGREE);
    expect(dest.latitude).to.be.closeTo(1, 1e-6);
    expect(dest.longitude).to.be.closeTo(0, 1e-6);
  });

  it('travels east along the equator', () => {
    const dest = calculateDestinationPoint(0, 0, 90, METERS_PER_DEGREE);
    expect(dest.latitude).to.be.closeTo(0, 1e-6);
    expect(dest.longitude).to.be.closeTo(1, 1e-6);
  });

  it('round-trips with calculateDistance and calculateBearing', () => {
    const dest = calculateDestinationPoint(47.5, 8.7, 222, 5_000);
    const distance = calculateDistance(
      47.5,
      8.7,
      dest.latitude,
      dest.longitude
    );
    const bearing = calculateBearing(47.5, 8.7, dest.latitude, dest.longitude);
    expect(distance).to.be.closeTo(5_000, 1);
    expect(bearing).to.be.closeTo(222, 0.1);
  });
});

describe('calculateBoundingBoxFromHomePort', () => {
  const SIZE = 1_000;
  const LAT = 47.5;
  const LON = 8.7;
  // Degrees corresponding to SIZE at the bbox conversion constant.
  const DELTA_LAT = SIZE / 111_139;

  it("treats anchor 'center' exactly like a radius box", () => {
    const fromHome = calculateBoundingBoxFromHomePort(LAT, LON, SIZE, 'center');
    const fromRadius = createBoundingBoxFromRadius(LAT, LON, SIZE);
    expect(fromHome).to.deep.equal(fromRadius);
  });

  it("puts an 'n'-anchored home port on the northern edge", () => {
    const box = calculateBoundingBoxFromHomePort(LAT, LON, SIZE, 'n');
    expect(box.north).to.be.closeTo(LAT, 1e-3);
    expect(box.south).to.be.closeTo(LAT - 2 * DELTA_LAT, 1e-3);
  });

  it("puts an 's'-anchored home port on the southern edge", () => {
    const box = calculateBoundingBoxFromHomePort(LAT, LON, SIZE, 's');
    expect(box.south).to.be.closeTo(LAT, 1e-3);
    expect(box.north).to.be.closeTo(LAT + 2 * DELTA_LAT, 1e-3);
  });

  it("extends a 'w'-anchored box west of the home port", () => {
    const box = calculateBoundingBoxFromHomePort(LAT, LON, SIZE, 'w');
    expect(box.east).to.be.closeTo(LON, 1e-3);
    expect(box.west).to.be.below(LON - 1e-4);
  });

  it("extends an 'e'-anchored box east of the home port", () => {
    const box = calculateBoundingBoxFromHomePort(LAT, LON, SIZE, 'e');
    expect(box.west).to.be.closeTo(LON, 1e-3);
    expect(box.east).to.be.above(LON + 1e-4);
  });

  it("combines both offsets for an 'nw' anchor", () => {
    const box = calculateBoundingBoxFromHomePort(LAT, LON, SIZE, 'nw');
    expect(box.north).to.be.closeTo(LAT, 1e-3);
    expect(box.east).to.be.closeTo(LON, 1e-3);
  });

  it("puts an 'ne'-anchored home port on the north-east corner", () => {
    const box = calculateBoundingBoxFromHomePort(LAT, LON, SIZE, 'ne');
    expect(box.north).to.be.closeTo(LAT, 1e-3);
    expect(box.west).to.be.closeTo(LON, 1e-3);
  });

  it("puts an 'sw'-anchored home port on the south-west corner", () => {
    const box = calculateBoundingBoxFromHomePort(LAT, LON, SIZE, 'sw');
    expect(box.south).to.be.closeTo(LAT, 1e-3);
    expect(box.east).to.be.closeTo(LON, 1e-3);
  });

  it("puts an 'se'-anchored home port on the south-east corner", () => {
    const box = calculateBoundingBoxFromHomePort(LAT, LON, SIZE, 'se');
    expect(box.south).to.be.closeTo(LAT, 1e-3);
    expect(box.west).to.be.closeTo(LON, 1e-3);
  });

  it('treats an unrecognized anchor like center', () => {
    const box = calculateBoundingBoxFromHomePort(LAT, LON, SIZE, 'middle');
    expect(box).to.deep.equal(createBoundingBoxFromRadius(LAT, LON, SIZE));
  });

  it('keeps the box size independent of the anchor', () => {
    const anchors = ['center', 'n', 's', 'e', 'w', 'nw', 'ne', 'sw', 'se'];
    for (const anchor of anchors) {
      const box = calculateBoundingBoxFromHomePort(LAT, LON, SIZE, anchor);
      expect(box.north - box.south, `anchor ${anchor}`).to.be.closeTo(
        2 * DELTA_LAT,
        1e-6
      );
    }
  });
});
