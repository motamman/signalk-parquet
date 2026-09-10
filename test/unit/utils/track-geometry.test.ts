import { expect } from 'chai';
import {
  boundingBoxOf,
  millisToIsoDuration,
  perpendicularDistanceMetres,
  simplifyIndices,
  splitIntoSegments,
} from '../../../src/utils/track-geometry';

describe('track-geometry', () => {
  describe('splitIntoSegments', () => {
    it('returns no segments for no points', () => {
      expect(splitIntoSegments([], 1000)).to.deep.equal([]);
    });

    it('keeps points closer than the gap in one segment', () => {
      const points = [0, 60, 120].map(s => ({ tMs: s * 1000 }));
      expect(splitIntoSegments(points, 5 * 60 * 1000)).to.deep.equal([points]);
    });

    it('starts a new segment across a gap and drops nothing', () => {
      const points = [0, 60, 120, 7200, 7260].map(s => ({ tMs: s * 1000 }));
      const segments = splitIntoSegments(points, 5 * 60 * 1000);
      expect(segments).to.have.lengthOf(2);
      expect(segments[0]).to.deep.equal(points.slice(0, 3));
      expect(segments[1]).to.deep.equal(points.slice(3));
    });

    it('treats a gap exactly equal to the threshold as continuous', () => {
      const points = [0, 300].map(s => ({ tMs: s * 1000 }));
      expect(splitIntoSegments(points, 300 * 1000)).to.have.lengthOf(1);
    });
  });

  describe('perpendicularDistanceMetres', () => {
    // At the equator one degree is ~111.2 km, so 0.0009 degrees of latitude
    // is ~100 m.
    const a = { lon: 0, lat: 0 };
    const b = { lon: 0.01, lat: 0 };

    it('measures the offset of a point beside the segment', () => {
      const d = perpendicularDistanceMetres({ lon: 0.005, lat: 0.0009 }, a, b);
      expect(d).to.be.closeTo(100, 1);
    });

    it('is zero for a point on the segment', () => {
      expect(
        perpendicularDistanceMetres({ lon: 0.005, lat: 0 }, a, b)
      ).to.equal(0);
    });

    it('measures to the nearer end for a point beyond the segment', () => {
      const d = perpendicularDistanceMetres({ lon: 0.02, lat: 0 }, a, b);
      // 0.01 degrees of longitude at the equator, ~1112 m
      expect(d).to.be.closeTo(1112, 2);
    });

    it('measures to the point when the segment has zero length', () => {
      const d = perpendicularDistanceMetres({ lon: 0, lat: 0.0009 }, a, a);
      expect(d).to.be.closeTo(100, 1);
    });
  });

  describe('simplifyIndices', () => {
    const line = [0, 1, 2, 3, 4].map(i => ({ lon: i * 0.001, lat: 47.5 }));

    it('keeps every point for two points or fewer', () => {
      expect(simplifyIndices(line.slice(0, 2), 10)).to.deep.equal([0, 1]);
      expect(simplifyIndices([], 10)).to.deep.equal([]);
    });

    it('keeps every point when epsilon is not positive', () => {
      expect(simplifyIndices(line, 0)).to.deep.equal([0, 1, 2, 3, 4]);
      expect(simplifyIndices(line, -1)).to.deep.equal([0, 1, 2, 3, 4]);
    });

    it('collapses a straight line to its ends', () => {
      expect(simplifyIndices(line, 1)).to.deep.equal([0, 4]);
    });

    it('keeps a point that deviates by more than epsilon', () => {
      const bent = line.map((p, i) => (i === 2 ? { ...p, lat: 47.501 } : p));
      // The bend is ~111 m off the straight line, so it is kept at 50 m and
      // dropped at 200 m. Once it is kept, its neighbours sit ~45 m off the
      // two sub-segments, which is why the tolerance here is 50 m and not
      // something smaller: a tighter epsilon rightly keeps them too.
      expect(simplifyIndices(bent, 50)).to.deep.equal([0, 2, 4]);
      expect(simplifyIndices(bent, 200)).to.deep.equal([0, 4]);
      expect(simplifyIndices(bent, 10)).to.deep.equal([0, 1, 2, 3, 4]);
    });
  });

  describe('boundingBoxOf', () => {
    it('is undefined for no points', () => {
      expect(boundingBoxOf([])).to.equal(undefined);
    });

    it('returns [west, south, east, north]', () => {
      expect(
        boundingBoxOf([
          { lon: 9.4, lat: 47.5 },
          { lon: 9.6, lat: 47.4 },
          { lon: 9.5, lat: 47.6 },
        ])
      ).to.deep.equal([9.4, 47.4, 9.6, 47.6]);
    });
  });

  describe('millisToIsoDuration', () => {
    it('renders whole and fractional seconds', () => {
      expect(millisToIsoDuration(1000)).to.equal('PT1S');
      expect(millisToIsoDuration(720)).to.equal('PT0.72S');
      expect(millisToIsoDuration(121000)).to.equal('PT121S');
      expect(millisToIsoDuration(1)).to.equal('PT0.001S');
    });

    it('uses minutes and hours when they are exact', () => {
      expect(millisToIsoDuration(120000)).to.equal('PT2M');
      expect(millisToIsoDuration(3600000)).to.equal('PT1H');
      expect(millisToIsoDuration(9600 * 3600000)).to.equal('PT9600H');
      expect(millisToIsoDuration(90000)).to.equal('PT90S');
    });
  });
});
