/**
 * Request decoding for the Track API provider: the window rules mirror the
 * server's (`duration` measures back from `to`; with `from` as well, the
 * later start wins), and durations arrive as Temporal.Duration objects, ISO
 * strings or seconds.
 */
import { expect } from 'chai';
import { Temporal } from '@js-temporal/polyfill';
import {
  durationToMillis,
  instantToMillis,
  nearestPropertyValue,
  resolveWindow,
} from '../../src/track-provider';

const NOW = Date.parse('2024-06-01T12:00:00.000Z');
const HOUR = 3_600_000;

describe('TrackProvider request decoding', () => {
  describe('instantToMillis', () => {
    it('accepts a Temporal.Instant, an ISO string and epoch millis', () => {
      const iso = '2024-06-01T10:00:00.000Z';
      const ms = Date.parse(iso);
      expect(instantToMillis(Temporal.Instant.from(iso))).to.equal(ms);
      expect(instantToMillis(iso)).to.equal(ms);
      expect(instantToMillis(ms)).to.equal(ms);
    });

    it('rejects an unparseable string', () => {
      expect(() => instantToMillis('yesterday-ish')).to.throw(
        /Invalid timestamp/
      );
    });
  });

  describe('durationToMillis', () => {
    it('reads a Temporal.Duration by its unit fields, days included', () => {
      // Temporal.Duration.total() refuses day units without a reference
      // date; the field sum does not need one.
      expect(durationToMillis(Temporal.Duration.from('P7D'))).to.equal(
        7 * 24 * HOUR
      );
      expect(durationToMillis(Temporal.Duration.from('PT1H30M'))).to.equal(
        1.5 * HOUR
      );
      expect(durationToMillis(Temporal.Duration.from('P1W'))).to.equal(
        7 * 24 * HOUR
      );
    });

    it('accepts ISO strings and a number of seconds', () => {
      expect(durationToMillis('PT1H')).to.equal(HOUR);
      expect(durationToMillis('P1D')).to.equal(24 * HOUR);
      expect(durationToMillis(3600)).to.equal(HOUR);
    });

    it('refuses calendar units', () => {
      expect(() => durationToMillis(Temporal.Duration.from('P1M'))).to.throw(
        /months or years/
      );
    });
  });

  describe('nearestPropertyValue', () => {
    // Buckets of 1 s starting at 0, 1000, 5000, 20000.
    const series = { bucketsMs: [0, 1000, 5000, 20000], values: [1, 2, 3, 4] };

    it('returns the value of the bucket containing the fix', () => {
      expect(nearestPropertyValue(series, 1200, 1000, 5000)).to.equal(2);
      expect(nearestPropertyValue(series, 0, 1000, 5000)).to.equal(1);
    });

    it('falls back to the nearest bucket start within the tolerance', () => {
      // 4200 is 800 ms before the 5000 bucket and 3200 after the 1000 one.
      expect(nearestPropertyValue(series, 4200, 1000, 5000)).to.equal(3);
      // 7000 is 2000 ms after the 5000 bucket ends; still inside 5 s.
      expect(nearestPropertyValue(series, 7000, 1000, 5000)).to.equal(3);
    });

    it('returns null when nothing is within the tolerance', () => {
      expect(nearestPropertyValue(series, 12000, 1000, 5000)).to.equal(null);
      expect(nearestPropertyValue(series, 40000, 1000, 5000)).to.equal(null);
      expect(
        nearestPropertyValue({ bucketsMs: [], values: [] }, 0, 1, 5000)
      ).to.equal(null);
    });

    it('matches a fix that precedes the first bucket', () => {
      expect(nearestPropertyValue(series, -300, 1000, 5000)).to.equal(1);
    });
  });

  describe('resolveWindow', () => {
    it('measures duration back from now when only duration is given', () => {
      const w = resolveWindow({ duration: 'PT1H' }, NOW);
      expect(w.bounded).to.equal(true);
      expect(w.toMs).to.equal(NOW);
      expect(w.fromMs).to.equal(NOW - HOUR);
    });

    it('measures duration back from an explicit to', () => {
      const w = resolveWindow(
        {
          to: '2024-06-01T06:00:00Z',
          duration: Temporal.Duration.from('PT2H'),
        },
        NOW
      );
      expect(w.fromIso).to.equal('2024-06-01T04:00:00.000Z');
      expect(w.toIso).to.equal('2024-06-01T06:00:00.000Z');
    });

    it('lets the later start win when from and duration disagree', () => {
      const later = resolveWindow(
        { from: '2024-06-01T11:30:00Z', duration: 'PT1H' },
        NOW
      );
      expect(later.fromIso).to.equal('2024-06-01T11:30:00.000Z');

      const earlier = resolveWindow(
        { from: '2024-06-01T09:00:00Z', duration: 'PT1H' },
        NOW
      );
      expect(earlier.fromIso).to.equal('2024-06-01T11:00:00.000Z');
    });

    it('defaults to to=now with from only', () => {
      const w = resolveWindow({ from: '2024-06-01T09:00:00Z' }, NOW);
      expect(w.fromIso).to.equal('2024-06-01T09:00:00.000Z');
      expect(w.toMs).to.equal(NOW);
    });

    it('is unbounded with neither from nor duration', () => {
      const w = resolveWindow({}, NOW);
      expect(w.bounded).to.equal(false);
      expect(w.fromMs).to.equal(0);
      expect(w.toMs).to.equal(NOW);
    });
  });
});
