/**
 * Unit tests for the History API duration/resolution parsers. These guard the
 * three accepted duration syntaxes (ISO 8601, plain seconds, shorthand) and
 * the stricter resolution grammar, including the rejection paths that protect
 * downstream SQL bucket arithmetic from zero or negative intervals.
 */
import { expect } from 'chai';
import {
  parseDurationToMillis,
  parseResolutionToMillis,
  InvalidResolutionError,
} from '../../../src/utils/duration-parser';

describe('parseDurationToMillis', () => {
  describe('ISO 8601 durations', () => {
    it('parses PT1H as one hour', () => {
      expect(parseDurationToMillis('PT1H')).to.equal(3_600_000);
    });

    it('parses PT30M as thirty minutes', () => {
      expect(parseDurationToMillis('PT30M')).to.equal(1_800_000);
    });

    it('parses P1D as one day', () => {
      expect(parseDurationToMillis('P1D')).to.equal(86_400_000);
    });

    it('parses combined components like PT1H30M', () => {
      expect(parseDurationToMillis('PT1H30M')).to.equal(5_400_000);
    });

    it('accepts lowercase ISO input', () => {
      expect(parseDurationToMillis('pt1h')).to.equal(3_600_000);
    });

    it('throws on a bare P with no components', () => {
      expect(() => parseDurationToMillis('P')).to.throw();
    });
  });

  describe('integer seconds', () => {
    it('parses plain digits as seconds', () => {
      expect(parseDurationToMillis('3600')).to.equal(3_600_000);
    });

    it('parses 0 as zero milliseconds', () => {
      expect(parseDurationToMillis('0')).to.equal(0);
    });

    it('trims surrounding whitespace', () => {
      expect(parseDurationToMillis('  60  ')).to.equal(60_000);
    });
  });

  describe('shorthand', () => {
    it('parses seconds shorthand 5s', () => {
      expect(parseDurationToMillis('5s')).to.equal(5_000);
    });

    it('parses minutes shorthand 30m', () => {
      expect(parseDurationToMillis('30m')).to.equal(1_800_000);
    });

    it('parses hours shorthand 1h', () => {
      expect(parseDurationToMillis('1h')).to.equal(3_600_000);
    });

    it('parses days shorthand 2d', () => {
      expect(parseDurationToMillis('2d')).to.equal(172_800_000);
    });

    it('accepts uppercase units', () => {
      expect(parseDurationToMillis('1H')).to.equal(3_600_000);
    });
  });

  describe('rejection', () => {
    // 'x5h' (leading junk) and '5hx' (trailing junk) guard the regex anchors:
    // without ^ or $ the shorthand pattern would match the embedded '5h'.
    const invalid = ['bogus', '', '1w', '-5s', '1.5h', 'h1', 'x5h', '5hx'];
    invalid.forEach(input => {
      it(`throws the guidance message for '${input}'`, () => {
        expect(() => parseDurationToMillis(input)).to.throw(
          `Invalid duration: ${input}. Use PT1H, 3600, or 1h`
        );
      });
    });
  });
});

describe('parseResolutionToMillis', () => {
  describe('numeric input (seconds)', () => {
    it('converts whole seconds to milliseconds', () => {
      expect(parseResolutionToMillis(60)).to.equal(60_000);
    });

    it('converts fractional seconds to milliseconds', () => {
      expect(parseResolutionToMillis(0.5)).to.equal(500);
    });

    const rejected: Array<[string, number]> = [
      ['zero', 0],
      ['negative', -1],
      ['NaN', NaN],
      ['Infinity', Infinity],
    ];
    rejected.forEach(([label, value]) => {
      it(`rejects ${label}`, () => {
        expect(() => parseResolutionToMillis(value)).to.throw(
          InvalidResolutionError
        );
      });
    });
  });

  describe('time expression strings', () => {
    it("parses '1s'", () => {
      expect(parseResolutionToMillis('1s')).to.equal(1_000);
    });

    it("parses '5m'", () => {
      expect(parseResolutionToMillis('5m')).to.equal(300_000);
    });

    it("parses '2h'", () => {
      expect(parseResolutionToMillis('2h')).to.equal(7_200_000);
    });

    it("parses '1d'", () => {
      expect(parseResolutionToMillis('1d')).to.equal(86_400_000);
    });

    it('accepts uppercase units', () => {
      expect(parseResolutionToMillis('1S')).to.equal(1_000);
    });

    it('trims surrounding whitespace', () => {
      expect(parseResolutionToMillis(' 1h ')).to.equal(3_600_000);
    });

    it("rejects a zero-valued expression like '0m'", () => {
      expect(() => parseResolutionToMillis('0m')).to.throw(
        InvalidResolutionError
      );
    });
  });

  describe('plain numeric strings (seconds)', () => {
    it("parses '30' as thirty seconds", () => {
      expect(parseResolutionToMillis('30')).to.equal(30_000);
    });

    it("parses decimal '2.5' as 2500 ms", () => {
      expect(parseResolutionToMillis('2.5')).to.equal(2_500);
    });

    // 'x5m'/'5mx' guard the ^...$ anchors on the time-expression regex.
    const rejected = ['0', '-1', '', '   ', 'abc', '-1h', '1.5s', 'x5m', '5mx'];
    rejected.forEach(input => {
      it(`rejects '${input}'`, () => {
        expect(() => parseResolutionToMillis(input)).to.throw(
          InvalidResolutionError
        );
      });
    });
  });

  describe('non-string non-number input', () => {
    it('rejects an array smuggled through the route layer cast', () => {
      // Express yields string[] for repeated query params
      // (?resolution=1s&resolution=2s); the route layer casts it to string.
      expect(() =>
        parseResolutionToMillis(['1s'] as unknown as string)
      ).to.throw(InvalidResolutionError);
    });
  });

  it('exposes a stable error name for API error mapping', () => {
    try {
      parseResolutionToMillis(0);
      expect.fail('expected InvalidResolutionError');
    } catch (err) {
      expect(err).to.be.instanceOf(InvalidResolutionError);
      expect((err as Error).name).to.equal('InvalidResolutionError');
    }
  });
});
