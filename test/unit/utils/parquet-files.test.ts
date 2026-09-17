/**
 * The one file lister every parquet read goes through: it must open exactly
 * the day partitions a window touches, never the special directories, and
 * never a glob.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'os';
import * as path from 'path';
import {
  filesFor,
  partitionDays,
  readParquetSql,
  readS3ParquetSql,
} from '../../../src/utils/parquet-files';

const CTX = 'vessels.urn:mrn:imo:mmsi:368076430';
const CTX_DIR = 'context=vessels__urn-mrn-imo-mmsi-368076430';
const PATH_DIR = 'path=navigation__position';

describe('parquet-files', () => {
  let base: string;

  beforeEach(async () => {
    base = await fs.mkdtemp(path.join(os.tmpdir(), 'parquet-files-'));
  });

  afterEach(async () => {
    await fs.remove(base);
  });

  /** A path relative to the fixture, with '/' separators on every platform. */
  function rel(file: string): string {
    return path.relative(base, file).split(path.sep).join('/');
  }

  async function touch(...segments: string[]): Promise<string> {
    const file = path.join(base, ...segments);
    await fs.ensureDir(path.dirname(file));
    await fs.writeFile(file, '');
    return file;
  }

  function day(year: number, doy: number, name = 'a.parquet'): Promise<string> {
    return touch(
      'tier=raw',
      CTX_DIR,
      PATH_DIR,
      `year=${year}`,
      `day=${String(doy).padStart(3, '0')}`,
      name
    );
  }

  describe('partitionDays', () => {
    it('covers every calendar day of a half-open window', () => {
      expect(
        partitionDays('2026-09-15T12:00:00Z', '2026-09-17T06:00:00Z')
      ).to.deep.equal([
        { year: 2026, dayOfYear: 258 },
        { year: 2026, dayOfYear: 259 },
        { year: 2026, dayOfYear: 260 },
      ]);
    });

    it('does not open the day a midnight `to` names', () => {
      expect(
        partitionDays('2026-09-16T00:00:00Z', '2026-09-17T00:00:00Z')
      ).to.deep.equal([{ year: 2026, dayOfYear: 259 }]);
    });

    it('is empty for empty, reversed and unparseable windows', () => {
      expect(partitionDays('2026-09-16T00:00:00Z', '2026-09-16T00:00:00Z')).to.deep.equal([]);
      expect(partitionDays('2026-09-17T00:00:00Z', '2026-09-16T00:00:00Z')).to.deep.equal([]);
      expect(partitionDays('nope', '2026-09-16T00:00:00Z')).to.deep.equal([]);
    });
  });

  describe('filesFor', () => {
    it('lists the files of the window days only, sorted', async () => {
      await day(2026, 257);
      await day(2026, 258, 'b.parquet');
      await day(2026, 258, 'a.parquet');
      await day(2026, 259);
      await day(2026, 260);
      const files = await filesFor({
        dataDir: base,
        contexts: [CTX],
        paths: ['navigation.position'],
        fromIso: '2026-09-15T10:00:00Z',
        toIso: '2026-09-16T10:00:00Z',
      });
      expect(files.map(rel)).to.deep.equal([
        `tier=raw/${CTX_DIR}/${PATH_DIR}/year=2026/day=258/a.parquet`,
        `tier=raw/${CTX_DIR}/${PATH_DIR}/year=2026/day=258/b.parquet`,
        `tier=raw/${CTX_DIR}/${PATH_DIR}/year=2026/day=259/a.parquet`,
      ]);
      expect(path.isAbsolute(files[0])).to.equal(true);
    });

    it('narrows across a year boundary', async () => {
      await day(2025, 363);
      await day(2025, 364);
      await day(2025, 365);
      await day(2026, 1);
      await day(2026, 2);
      await day(2026, 3);
      const files = await filesFor({
        dataDir: base,
        contexts: [CTX],
        paths: ['navigation.position'],
        fromIso: '2025-12-30T00:00:00Z',
        toIso: '2026-01-02T12:00:00Z',
      });
      expect(files.map(f => path.basename(path.dirname(f)))).to.deep.equal([
        'day=364',
        'day=365',
        'day=001',
        'day=002',
      ]);
    });

    it('never enters the special directories', async () => {
      await day(2026, 258);
      for (const special of ['processed', 'quarantine', 'failed', 'repaired']) {
        await touch('tier=raw', CTX_DIR, PATH_DIR, 'year=2026', 'day=258', special, 'x.parquet');
        await touch('tier=raw', CTX_DIR, PATH_DIR, 'year=2026', special, 'day=258', 'x.parquet');
        await touch('tier=raw', CTX_DIR, special, 'year=2026', 'day=258', 'x.parquet');
      }
      await touch('tier=raw', CTX_DIR, PATH_DIR, 'year=2026', '.compaction-trash-1', 'day=258', 'x.parquet');
      const files = await filesFor({
        dataDir: base,
        contexts: [CTX],
        paths: ['navigation.position'],
        fromIso: '2026-09-15T00:00:00Z',
        toIso: '2026-09-16T00:00:00Z',
      });
      expect(files.map(rel)).to.deep.equal([
        `tier=raw/${CTX_DIR}/${PATH_DIR}/year=2026/day=258/a.parquet`,
      ]);
    });

    it('is empty for a missing context, path or store, and for an empty window', async () => {
      await day(2026, 258);
      const window = { fromIso: '2026-09-15T00:00:00Z', toIso: '2026-09-16T00:00:00Z' };
      expect(
        await filesFor({ dataDir: base, contexts: ['vessels.other'], paths: ['navigation.position'], ...window })
      ).to.deep.equal([]);
      expect(
        await filesFor({ dataDir: base, contexts: [CTX], paths: ['navigation.speedOverGround'], ...window })
      ).to.deep.equal([]);
      expect(
        await filesFor({ dataDir: path.join(base, 'nope'), contexts: [CTX], paths: ['navigation.position'], ...window })
      ).to.deep.equal([]);
      expect(
        await filesFor({ dataDir: base, contexts: [CTX], paths: ['navigation.position'], fromIso: window.toIso, toIso: window.fromIso })
      ).to.deep.equal([]);
      expect(
        await filesFor({ dataDir: base, contexts: [], paths: ['navigation.position'], ...window })
      ).to.deep.equal([]);
    });

    it('falls back to whole years past 366 days, and the predicate does the rest', async () => {
      await day(2024, 10);
      await day(2024, 200);
      await day(2025, 300);
      await day(2026, 100);
      const files = await filesFor({
        dataDir: base,
        contexts: [CTX],
        paths: ['navigation.position'],
        fromIso: '2024-06-01T00:00:00Z',
        toIso: '2025-08-01T00:00:00Z',
      });
      // 2024/day=010 is outside the window but inside a touched year: the
      // documented superset. 2026 is not touched and stays closed.
      expect(files.map(f => rel(f).replace(/^.*\/(year=\d+\/day=\d+)\/.*$/, '$1'))).to.deep.equal([
        'year=2024/day=010',
        'year=2024/day=200',
        'year=2025/day=300',
      ]);
    });

    it('includes a compacted year file, with or without surviving day directories', async () => {
      // 2024 fully compacted: only the yearly file remains. 2025 partly:
      // a day file and a yearly file side by side. Temp outputs are not
      // parquet files and stay out.
      await touch('tier=raw', CTX_DIR, PATH_DIR, 'year=2024', 'year_compact_2024_20250101T0000.parquet');
      await touch('tier=raw', CTX_DIR, PATH_DIR, 'year=2025', 'year_compact_2025_20260101T0000.parquet');
      await touch('tier=raw', CTX_DIR, PATH_DIR, 'year=2025', 'year_compact_2025_20260102T0000.parquet.tmp');
      await day(2025, 300);
      const in2024 = await filesFor({
        dataDir: base,
        contexts: [CTX],
        paths: ['navigation.position'],
        fromIso: '2024-03-01T00:00:00Z',
        toIso: '2024-03-02T00:00:00Z',
      });
      expect(in2024.map(f => path.basename(f))).to.deep.equal(['year_compact_2024_20250101T0000.parquet']);
      const in2025 = await filesFor({
        dataDir: base,
        contexts: [CTX],
        paths: ['navigation.position'],
        fromIso: '2025-10-27T00:00:00Z',
        toIso: '2025-10-28T00:00:00Z',
      });
      expect(in2025.map(f => path.basename(f)).sort()).to.deep.equal(['a.parquet', 'year_compact_2025_20260101T0000.parquet']);
      const in2026 = await filesFor({
        dataDir: base,
        contexts: [CTX],
        paths: ['navigation.position'],
        fromIso: '2026-01-01T00:00:00Z',
        toIso: '2026-01-02T00:00:00Z',
      });
      expect(in2026).to.deep.equal([]);
    });

    it('lists the whole history only when no window is given', async () => {
      await day(2024, 10);
      await day(2026, 258);
      const files = await filesFor({
        dataDir: base,
        contexts: [CTX],
        paths: ['navigation.position'],
      });
      expect(files.map(f => path.basename(path.dirname(f)))).to.deep.equal([
        'day=010',
        'day=258',
      ]);
    });

    it('spans every context and path when asked, and takes sanitized directory names', async () => {
      await day(2026, 258);
      await touch('tier=raw', 'context=vessels__collide-abc', 'path=navigation__headingTrue', 'year=2026', 'day=258', 'h.parquet');
      await touch('tier=raw', 'context=vessels__collide-abc', 'path=navigation__position', 'year=2026', 'day=258', 'p.parquet');
      await touch('tier=5s', CTX_DIR, PATH_DIR, 'year=2026', 'day=258', 'agg.parquet');
      const window = { fromIso: '2026-09-15T00:00:00Z', toIso: '2026-09-16T00:00:00Z' };

      const everything = await filesFor({ dataDir: base, contexts: 'all', paths: 'all', ...window });
      expect(everything.map(rel)).to.deep.equal([
        'tier=raw/context=vessels__collide-abc/path=navigation__headingTrue/year=2026/day=258/h.parquet',
        'tier=raw/context=vessels__collide-abc/path=navigation__position/year=2026/day=258/p.parquet',
        `tier=raw/${CTX_DIR}/${PATH_DIR}/year=2026/day=258/a.parquet`,
      ]);

      const positions = await filesFor({ dataDir: base, contexts: 'all', paths: ['navigation.position'], ...window });
      expect(positions.map(f => path.basename(f))).to.deep.equal(['p.parquet', 'a.parquet']);

      const byDir = await filesFor({ dataDir: base, contexts: [], contextDirs: ['vessels__collide-abc'], paths: 'all', ...window });
      expect(byDir.map(f => path.basename(f))).to.deep.equal(['h.parquet', 'p.parquet']);

      const tier = await filesFor({ dataDir: base, tier: '5s', contexts: [CTX], paths: ['navigation.position'], ...window });
      expect(tier.map(f => path.basename(f))).to.deep.equal(['agg.parquet']);
    });
  });

  describe('readParquetSql', () => {
    it('always passes an explicit list with the two flags', () => {
      const sql = readParquetSql(['/a/b.parquet', "/it's/c.parquet"]);
      expect(sql).to.equal(
        "read_parquet(['/a/b.parquet', '/it''s/c.parquet'], hive_partitioning=false, union_by_name=true)"
      );
    });

    it('adds the filename column only when asked', () => {
      expect(readParquetSql(['/a/b.parquet'], { filename: true })).to.equal(
        "read_parquet(['/a/b.parquet'], hive_partitioning=false, union_by_name=true, filename=true)"
      );
    });

    it('refuses an empty list rather than producing a query DuckDB rejects', () => {
      expect(() => readParquetSql([])).to.throw(/empty file list/);
    });

    it('gives the S3 glob the same flags, and only an S3 URL', () => {
      expect(
        readS3ParquetSql('s3://b/p/tier=raw/context=c/path=p/{year=2026/day=001}/*.parquet')
      ).to.equal(
        "read_parquet('s3://b/p/tier=raw/context=c/path=p/{year=2026/day=001}/*.parquet', hive_partitioning=false, union_by_name=true)"
      );
      expect(() => readS3ParquetSql('/local/x.parquet')).to.throw(/not an S3 URL/);
    });
  });
});
