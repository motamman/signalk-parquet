/**
 * Unit tests for HivePathBuilder, which translates SignalK contexts/paths and
 * timestamps into Hive-style partition directories
 * (tier=.../context=.../path=.../year=.../day=...), parses such paths back,
 * and produces glob patterns for DuckDB and S3 queries. Pure methods are
 * tested directly with UTC-constructed dates so the results are identical in
 * every timezone; filesystem-backed methods (buildFilePath, flatToHive,
 * findEarliestDate) run against a throwaway temp directory.
 */
import { expect } from 'chai';
import * as fs from 'fs';
import * as fse from 'fs-extra';
import * as os from 'os';
import * as path from 'path';
import { HivePathBuilder } from '../../../src/utils/hive-path-builder';

describe('HivePathBuilder', () => {
  const builder = new HivePathBuilder();

  describe('buildPath', () => {
    it('sanitizes context and path into hive partition directories', () => {
      // context: 'vessels.urn:mrn:signalk:uuid:xxx'
      //   -> 'vessels__urn-mrn-signalk-uuid-xxx' ('.' -> '__', ':' -> '-')
      // path: 'navigation.speedOverGround' -> 'navigation__speedOverGround'
      const result = builder.buildPath(
        'data',
        'raw',
        'vessels.urn:mrn:signalk:uuid:xxx',
        'navigation.speedOverGround',
        new Date(Date.UTC(2024, 5, 15, 12, 0, 0))
      );
      expect(result).to.equal(
        path.join(
          'data',
          'tier=raw',
          'context=vessels__urn-mrn-signalk-uuid-xxx',
          'path=navigation__speedOverGround',
          'year=2024',
          'day=167'
        )
      );
    });

    it('zero-pads the day of year to three digits', () => {
      const result = builder.buildPath(
        'data',
        '5s',
        'vessels.self',
        'navigation.position',
        new Date(Date.UTC(2023, 0, 7))
      );
      expect(result).to.equal(
        path.join(
          'data',
          'tier=5s',
          'context=vessels__self',
          'path=navigation__position',
          'year=2023',
          'day=007'
        )
      );
    });

    it('partitions by the UTC calendar date regardless of local time', () => {
      // 2024-01-01T00:30Z is still Dec 31 in zones west of UTC; the builder
      // must use UTC accessors so the partition is day 001 everywhere.
      const result = builder.buildPath(
        'data',
        'raw',
        'vessels.self',
        'navigation.position',
        new Date(Date.UTC(2024, 0, 1, 0, 30, 0))
      );
      expect(result).to.contain(path.join('year=2024', 'day=001'));
    });
  });

  describe('sanitizeContext / unsanitizeContext', () => {
    it('replaces dots with double underscores and colons with dashes', () => {
      expect(
        builder.sanitizeContext('vessels.urn:mrn:signalk:uuid:xxx')
      ).to.equal('vessels__urn-mrn-signalk-uuid-xxx');
    });

    it('round-trips contexts that contain no literal dashes', () => {
      const original = 'vessels.urn:mrn:signalk:uuid:xxx';
      expect(
        builder.unsanitizeContext(builder.sanitizeContext(original))
      ).to.equal(original);
    });

    it('is lossy for contexts containing literal dashes (real UUIDs)', () => {
      // unsanitizeContext maps every '-' back to ':', so the dashes inside a
      // real UUID are corrupted:
      // Input:      'vessels.urn:mrn:signalk:uuid:c0d79334-4e25'
      // Sanitized:  'vessels__urn-mrn-signalk-uuid-c0d79334-4e25'
      // Round-trip: 'vessels.urn:mrn:signalk:uuid:c0d79334:4e25' (changed!)
      // Pinned so an intentional fix shows up as a test change.
      const original = 'vessels.urn:mrn:signalk:uuid:c0d79334-4e25';
      const sanitized = builder.sanitizeContext(original);
      expect(sanitized).to.equal('vessels__urn-mrn-signalk-uuid-c0d79334-4e25');
      expect(builder.unsanitizeContext(sanitized)).to.equal(
        'vessels.urn:mrn:signalk:uuid:c0d79334:4e25'
      );
    });
  });

  describe('sanitizePath / unsanitizePath', () => {
    it('replaces dots with double underscores', () => {
      expect(builder.sanitizePath('navigation.speedOverGround')).to.equal(
        'navigation__speedOverGround'
      );
    });

    it('round-trips multi-segment paths', () => {
      const original = 'environment.depth.belowTransducer';
      expect(builder.unsanitizePath(builder.sanitizePath(original))).to.equal(
        original
      );
    });
  });

  describe('getDayOfYear', () => {
    it('returns 1 for January 1st', () => {
      expect(builder.getDayOfYear(new Date(Date.UTC(2023, 0, 1)))).to.equal(1);
    });

    it('returns 366 for December 31st of a leap year', () => {
      expect(builder.getDayOfYear(new Date(Date.UTC(2024, 11, 31)))).to.equal(
        366
      );
    });

    it('returns 365 for December 31st of a non-leap year', () => {
      expect(builder.getDayOfYear(new Date(Date.UTC(2023, 11, 31)))).to.equal(
        365
      );
    });

    it('returns 60 for February 29th of a leap year', () => {
      expect(builder.getDayOfYear(new Date(Date.UTC(2024, 1, 29)))).to.equal(
        60
      );
    });

    it('ignores the time of day', () => {
      expect(
        builder.getDayOfYear(new Date(Date.UTC(2024, 11, 31, 23, 59, 59, 999)))
      ).to.equal(366);
      expect(
        builder.getDayOfYear(new Date(Date.UTC(2023, 0, 1, 23, 59, 59)))
      ).to.equal(1);
    });
  });

  describe('dateFromDayOfYear', () => {
    it('builds the UTC date for day 1', () => {
      expect(builder.dateFromDayOfYear(2023, 1).getTime()).to.equal(
        Date.UTC(2023, 0, 1)
      );
    });

    it('builds December 31st for day 366 of a leap year', () => {
      expect(builder.dateFromDayOfYear(2024, 366).getTime()).to.equal(
        Date.UTC(2024, 11, 31)
      );
    });

    it('round-trips with getDayOfYear', () => {
      const samples: Array<[number, number]> = [
        [2023, 1],
        [2023, 365],
        [2024, 60],
        [2024, 366],
      ];
      for (const [year, day] of samples) {
        const date = builder.dateFromDayOfYear(year, day);
        expect(date.getUTCFullYear(), `year for ${year}/${day}`).to.equal(year);
        expect(builder.getDayOfYear(date), `day for ${year}/${day}`).to.equal(
          day
        );
      }
    });
  });

  describe('getGlobPattern', () => {
    it('uses wildcards for every omitted argument', () => {
      expect(builder.getGlobPattern('base', 'raw')).to.equal(
        path.join(
          'base',
          'tier=raw',
          'context=*',
          'path=*',
          'year=*',
          'day=*',
          '*.parquet'
        )
      );
    });

    it('sanitizes explicit context and path and pads the day', () => {
      expect(
        builder.getGlobPattern(
          'base',
          '60s',
          'vessels.urn:mrn:signalk:uuid:xxx',
          'navigation.speedOverGround',
          2023,
          7
        )
      ).to.equal(
        path.join(
          'base',
          'tier=60s',
          'context=vessels__urn-mrn-signalk-uuid-xxx',
          'path=navigation__speedOverGround',
          'year=2023',
          'day=007',
          '*.parquet'
        )
      );
    });

    it('mixes explicit and wildcard segments', () => {
      expect(
        builder.getGlobPattern('base', 'raw', undefined, undefined, 2024, 167)
      ).to.equal(
        path.join(
          'base',
          'tier=raw',
          'context=*',
          'path=*',
          'year=2024',
          'day=167',
          '*.parquet'
        )
      );
    });
  });

  describe('getDaysInRange', () => {
    it('returns a single entry for a single-day range', () => {
      const day = new Date(Date.UTC(2024, 5, 15, 10, 0, 0));
      expect(builder.getDaysInRange(day, day)).to.deep.equal([
        { year: 2024, dayOfYear: 167 },
      ]);
    });

    it('spans a year boundary', () => {
      const from = new Date(Date.UTC(2023, 11, 30));
      const to = new Date(Date.UTC(2024, 0, 2));
      expect(builder.getDaysInRange(from, to)).to.deep.equal([
        { year: 2023, dayOfYear: 364 },
        { year: 2023, dayOfYear: 365 },
        { year: 2024, dayOfYear: 1 },
        { year: 2024, dayOfYear: 2 },
      ]);
    });

    it('returns an empty list when from is after to', () => {
      const from = new Date(Date.UTC(2024, 5, 16));
      const to = new Date(Date.UTC(2024, 5, 15));
      expect(builder.getDaysInRange(from, to)).to.deep.equal([]);
    });

    it('drops the final day when to has an earlier time of day', () => {
      // The loop advances by whole days while keeping from's time of day, so
      // a range ending at 06:00 on the next day never visits that day even
      // though six hours of it are inside the range. Pinned as current
      // behaviour; callers passing intraday bounds lose the last partition.
      const from = new Date(Date.UTC(2023, 11, 30, 12, 0, 0));
      const to = new Date(Date.UTC(2023, 11, 31, 6, 0, 0));
      expect(builder.getDaysInRange(from, to)).to.deep.equal([
        { year: 2023, dayOfYear: 364 },
      ]);
    });

    it('does not mutate the input dates', () => {
      const from = new Date(Date.UTC(2023, 11, 30));
      const to = new Date(Date.UTC(2024, 0, 2));
      const fromTime = from.getTime();
      const toTime = to.getTime();
      builder.getDaysInRange(from, to);
      expect(from.getTime()).to.equal(fromTime);
      expect(to.getTime()).to.equal(toTime);
    });
  });

  describe('buildDuckDBGlob', () => {
    const wildcard = path.join(
      'base',
      'tier=raw',
      'context=*',
      'path=*',
      'year=*',
      'day=*',
      '*.parquet'
    );

    it('lists explicit day patterns for short ranges', () => {
      const result = builder.buildDuckDBGlob(
        'base',
        'raw',
        'vessels.self',
        'navigation.position',
        new Date(Date.UTC(2024, 5, 15)),
        new Date(Date.UTC(2024, 5, 17))
      );
      const parts = result.split(',');
      expect(parts).to.have.length(3);
      expect(parts[0]).to.equal(
        path.join(
          'base',
          'tier=raw',
          'context=vessels__self',
          'path=navigation__position',
          'year=2024',
          'day=167',
          '*.parquet'
        )
      );
      expect(parts[2]).to.equal(
        path.join(
          'base',
          'tier=raw',
          'context=vessels__self',
          'path=navigation__position',
          'year=2024',
          'day=169',
          '*.parquet'
        )
      );
    });

    it('still lists explicit days for a range of exactly 7 days', () => {
      const result = builder.buildDuckDBGlob(
        'base',
        'raw',
        undefined,
        undefined,
        new Date(Date.UTC(2024, 5, 15)),
        new Date(Date.UTC(2024, 5, 21))
      );
      const parts = result.split(',');
      expect(parts).to.have.length(7);
      expect(parts[6]).to.contain(path.join('year=2024', 'day=173'));
    });

    it('falls back to wildcards for ranges longer than 7 days', () => {
      const result = builder.buildDuckDBGlob(
        'base',
        'raw',
        undefined,
        undefined,
        new Date(Date.UTC(2024, 5, 15)),
        new Date(Date.UTC(2024, 5, 22))
      );
      expect(result).to.equal(wildcard);
    });

    it('uses wildcards when no date range is given', () => {
      expect(builder.buildDuckDBGlob('base', 'raw')).to.equal(wildcard);
    });

    it('uses wildcards when only one bound is given', () => {
      expect(
        builder.buildDuckDBGlob(
          'base',
          'raw',
          undefined,
          undefined,
          new Date(Date.UTC(2024, 5, 15)),
          undefined
        )
      ).to.equal(wildcard);
    });
  });

  describe('detectPathStyle', () => {
    it('parses a hive-style path and unsanitizes its segments', () => {
      const result = builder.detectPathStyle(
        'data/tier=raw/context=vessels__urn-mrn-signalk-uuid-xxx/' +
          'path=navigation__speedOverGround/year=2024/day=167/file.parquet'
      );
      expect(result).to.deep.equal({
        isHive: true,
        isFlat: false,
        tier: 'raw',
        context: 'vessels.urn:mrn:signalk:uuid:xxx',
        signalkPath: 'navigation.speedOverGround',
        year: 2024,
        dayOfYear: 167,
      });
    });

    it('parses zero-padded day directories as numbers', () => {
      const result = builder.detectPathStyle(
        'data/tier=1h/context=vessels__self/path=navigation__position/' +
          'year=2023/day=007/file.parquet'
      );
      expect(result.dayOfYear).to.equal(7);
      expect(result.tier).to.equal('1h');
    });

    it('parses a legacy flat path and rebuilds context and path', () => {
      // 'vessels/urn_mrn_signalk_uuid_xxx/navigation/speedOverGround/f.parquet'
      //   context:     'vessels.urn:mrn:signalk:uuid:xxx' ('_' -> ':')
      //   signalkPath: 'navigation.speedOverGround'       ('/' -> '.')
      const result = builder.detectPathStyle(
        'vessels/urn_mrn_signalk_uuid_xxx/navigation/speedOverGround/' +
          'signalk_20240615.parquet'
      );
      expect(result.isHive).to.equal(false);
      expect(result.isFlat).to.equal(true);
      expect(result.context).to.equal('vessels.urn:mrn:signalk:uuid:xxx');
      expect(result.signalkPath).to.equal('navigation.speedOverGround');
      expect(result.tier).to.equal(undefined);
    });

    it('recognizes neither style for unrelated paths', () => {
      expect(builder.detectPathStyle('random/file.txt')).to.deep.equal({
        isHive: false,
        isFlat: false,
      });
    });

    it('requires at least one path segment for the flat style', () => {
      // Flat pattern is vessels/{context}/{path...}/file.parquet; with no
      // path segment between context and filename there is no match.
      expect(builder.detectPathStyle('vessels/x/file.parquet')).to.deep.equal({
        isHive: false,
        isFlat: false,
      });
    });

    it('does not match hive paths written with backslash separators', () => {
      // The regexes only accept '/' separators, so unnormalized Windows
      // paths are classified as neither style. Pinned as current behaviour.
      const result = builder.detectPathStyle(
        'data\\tier=raw\\context=vessels__self\\path=navigation__position\\' +
          'year=2024\\day=167\\file.parquet'
      );
      expect(result).to.deep.equal({ isHive: false, isFlat: false });
    });
  });

  describe('buildFilePath', () => {
    let tempDir: string;

    beforeEach(() => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hive-test-'));
    });

    afterEach(() => {
      fse.removeSync(tempDir);
    });

    const timestamp = new Date(Date.UTC(2024, 5, 15, 12, 0, 0));

    function expectedDir(base: string): string {
      return path.join(
        base,
        'tier=raw',
        'context=vessels__urn-mrn-signalk-uuid-xxx',
        'path=navigation__position',
        'year=2024',
        'day=167'
      );
    }

    function build(prefix?: string): string {
      return builder.buildFilePath(
        tempDir,
        'raw',
        'vessels.urn:mrn:signalk:uuid:xxx',
        'navigation.position',
        timestamp,
        prefix
      );
    }

    it('builds prefix_timestamp.parquet when no file exists yet', () => {
      // toISOString() with ':' and '.' stripped, cut to 17 chars:
      // '2024-06-15T12:00:00.000Z' -> '2024-06-15T120000'
      expect(build()).to.equal(
        path.join(expectedDir(tempDir), 'data_2024-06-15T120000.parquet')
      );
    });

    it('honours a custom filename prefix', () => {
      expect(build('signalk')).to.equal(
        path.join(expectedDir(tempDir), 'signalk_2024-06-15T120000.parquet')
      );
    });

    it('appends an incrementing suffix while the file exists', () => {
      const dir = expectedDir(tempDir);
      fse.mkdirpSync(dir);
      fs.writeFileSync(path.join(dir, 'data_2024-06-15T120000.parquet'), '');

      const second = build();
      expect(second).to.equal(
        path.join(dir, 'data_2024-06-15T120000_1.parquet')
      );

      fs.writeFileSync(second, '');
      expect(build()).to.equal(
        path.join(dir, 'data_2024-06-15T120000_2.parquet')
      );
    });
  });

  describe('flatToHive', () => {
    let tempDir: string;

    beforeEach(() => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hive-test-'));
    });

    afterEach(() => {
      fse.removeSync(tempDir);
    });

    it('rebuilds a hive file path from a legacy flat path', () => {
      // Filename timestamp '20240615T120000' parses to 2024-06-15T12:00:00Z
      // (day 167 of the leap year); the original basename becomes the new
      // filename prefix, so the timestamp appears twice in the result.
      const result = builder.flatToHive(
        'vessels/urn_mrn_signalk_uuid_xxx/navigation/position/' +
          'signalk_20240615T120000.parquet',
        tempDir
      );
      expect(result).to.equal(
        path.join(
          tempDir,
          'tier=raw',
          'context=vessels__urn-mrn-signalk-uuid-xxx',
          'path=navigation__position',
          'year=2024',
          'day=167',
          'signalk_20240615T120000_2024-06-15T120000.parquet'
        )
      );
    });

    it('falls back to the current time when the filename has none', () => {
      const result = builder.flatToHive(
        'vessels/abc/navigation/depth/snapshot.parquet',
        tempDir
      );
      expect(result).to.be.a('string');
      expect(
        (result as string).startsWith(
          path.join(
            tempDir,
            'tier=raw',
            'context=vessels__abc',
            'path=navigation__depth'
          )
        )
      ).to.equal(true);
      expect(result).to.match(/snapshot_.+\.parquet$/);
    });

    it('returns null for paths that are not flat style', () => {
      expect(builder.flatToHive('not/a/flat/path.txt', tempDir)).to.equal(null);
    });
  });

  describe('findEarliestDate', () => {
    let tempDir: string;

    beforeEach(() => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hive-test-'));
    });

    afterEach(() => {
      fse.removeSync(tempDir);
    });

    function makeDay(relative: string, file?: string): void {
      const dir = path.join(
        tempDir,
        'tier=raw',
        'context=ctx',
        'path=p',
        relative
      );
      fse.mkdirpSync(dir);
      if (file) {
        fs.writeFileSync(path.join(dir, file), '');
      }
    }

    it('returns the earliest year/day that contains parquet files', () => {
      makeDay(path.join('year=2023', 'day=200'), 'a.parquet');
      makeDay(path.join('year=2022', 'day=310'), 'b.parquet');
      makeDay(path.join('year=2022', 'day=300'), 'c.parquet');
      // Distractors that must all be ignored:
      makeDay(path.join('year=2021', 'day=100'), 'readme.txt'); // no parquet
      makeDay(path.join('year=2023', 'day=150')); // empty directory
      makeDay(path.join('year=abc', 'day=001'), 'x.parquet'); // NaN year
      makeDay(path.join('year=2022', 'day=xyz'), 'y.parquet'); // NaN day

      const result = builder.findEarliestDate(tempDir, 'raw', 'ctx', 'p');
      // Day 300 of 2022 is October 27th.
      expect(result?.getTime()).to.equal(Date.UTC(2022, 9, 27));
    });

    it('returns null when no day directory contains parquet files', () => {
      makeDay(path.join('year=2024', 'day=010'));
      makeDay(path.join('year=2024', 'day=020'), 'notes.txt');
      expect(builder.findEarliestDate(tempDir, 'raw', 'ctx', 'p')).to.equal(
        null
      );
    });

    it('returns null when the partition directory does not exist', () => {
      expect(builder.findEarliestDate(tempDir, 'raw', 'missing', 'p')).to.equal(
        null
      );
    });
  });

  describe('buildS3Glob', () => {
    const context = 'vessels.urn:mrn:signalk:uuid:xxx';
    const signalkPath = 'navigation.position';
    const prefix =
      'tier=raw/context=vessels__urn-mrn-signalk-uuid-xxx/' +
      'path=navigation__position';

    it('lists explicit days in a brace expansion for short ranges', () => {
      const result = builder.buildS3Glob(
        'bkt',
        'marine/',
        'raw',
        context,
        signalkPath,
        new Date(Date.UTC(2024, 5, 15)),
        new Date(Date.UTC(2024, 5, 16))
      );
      expect(result).to.equal(
        `s3://bkt/marine/${prefix}/` +
          '{year=2024/day=167,year=2024/day=168}/*.parquet'
      );
    });

    it('normalizes a key prefix without a trailing slash', () => {
      const result = builder.buildS3Glob(
        'bkt',
        'marine',
        'raw',
        context,
        signalkPath,
        new Date(Date.UTC(2024, 5, 15)),
        new Date(Date.UTC(2024, 5, 15))
      );
      expect(result).to.equal(
        `s3://bkt/marine/${prefix}/{year=2024/day=167}/*.parquet`
      );
    });

    it('omits the prefix segment when the key prefix is empty', () => {
      const result = builder.buildS3Glob(
        'bkt',
        '',
        'raw',
        context,
        signalkPath,
        new Date(Date.UTC(2024, 5, 15)),
        new Date(Date.UTC(2024, 5, 15))
      );
      expect(result).to.equal(
        `s3://bkt/${prefix}/{year=2024/day=167}/*.parquet`
      );
    });

    it('lists explicit days for a 6-day range but wildcards 7 days', () => {
      // The docstring promises explicit days for ranges <= 7 days, but the
      // implementation counts the 7th pushed day as "hit the cap" and falls
      // back to wildcards, so only ranges of up to 6 days stay explicit.
      // Pinned as current behaviour (off-by-one against the documentation,
      // and inconsistent with buildDuckDBGlob which keeps 7 days explicit).
      const sixDays = builder.buildS3Glob(
        'bkt',
        '',
        'raw',
        context,
        signalkPath,
        new Date(Date.UTC(2024, 5, 15)),
        new Date(Date.UTC(2024, 5, 20))
      );
      expect(sixDays.split(',')).to.have.length(6);

      const sevenDays = builder.buildS3Glob(
        'bkt',
        '',
        'raw',
        context,
        signalkPath,
        new Date(Date.UTC(2024, 5, 15)),
        new Date(Date.UTC(2024, 5, 21))
      );
      expect(sevenDays).to.equal(`s3://bkt/${prefix}/year=*/day=*/*.parquet`);
    });
  });

  describe('buildS3GlobsForRange', () => {
    const context = 'vessels.self';
    const signalkPath = 'navigation.position';

    it('routes the whole range to S3 when it ends before the cutoff', () => {
      const from = new Date(Date.UTC(2024, 5, 10));
      const to = new Date(Date.UTC(2024, 5, 12));
      const result = builder.buildS3GlobsForRange(
        'bkt',
        '',
        'raw',
        context,
        signalkPath,
        from,
        to,
        new Date(Date.UTC(2024, 5, 15))
      );
      expect(result.s3Pattern).to.equal(
        builder.buildS3Glob('bkt', '', 'raw', context, signalkPath, from, to)
      );
      expect(result.localPattern).to.equal(null);
    });

    it('returns no patterns when the range starts at or after the cutoff', () => {
      const cutoff = new Date(Date.UTC(2024, 5, 15));
      const result = builder.buildS3GlobsForRange(
        'bkt',
        '',
        'raw',
        context,
        signalkPath,
        cutoff,
        new Date(Date.UTC(2024, 5, 20)),
        cutoff
      );
      expect(result).to.deep.equal({ s3Pattern: null, localPattern: null });
    });

    it('ends the S3 part one day before the cutoff for hybrid ranges', () => {
      const from = new Date(Date.UTC(2024, 5, 10));
      const cutoff = new Date(Date.UTC(2024, 5, 15));
      const result = builder.buildS3GlobsForRange(
        'bkt',
        '',
        'raw',
        context,
        signalkPath,
        from,
        new Date(Date.UTC(2024, 5, 20)),
        cutoff
      );
      expect(result.s3Pattern).to.equal(
        builder.buildS3Glob(
          'bkt',
          '',
          'raw',
          context,
          signalkPath,
          from,
          new Date(Date.UTC(2024, 5, 14))
        )
      );
      // The local half is documented as "handled by existing logic" and is
      // never populated here.
      expect(result.localPattern).to.equal(null);
      // The cutoff date passed in must not be mutated by the internal
      // one-day subtraction.
      expect(cutoff.getTime()).to.equal(Date.UTC(2024, 5, 15));
    });
  });
});
