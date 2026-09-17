/**
 * The layout-aware lister behind the startup sweeps and the cloud sync: it
 * walks tier/context/path/year/day by name and prunes at each level, so a
 * week of day directories costs one readdir per path per year and a `**`
 * glob over the store is never needed.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'os';
import * as path from 'path';
import {
  dayKey,
  listEntries,
  listHiveDirs,
  listParquetFiles,
} from '../../../src/utils/hive-walk';

describe('hive-walk', () => {
  let base: string;

  beforeEach(async () => {
    base = await fs.mkdtemp(path.join(os.tmpdir(), 'hive-walk-'));
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

  function seed(): Promise<unknown> {
    return Promise.all([
      touch('tier=raw', 'context=a', 'path=p', 'year=2025', 'day=364', 'a.parquet'),
      touch('tier=raw', 'context=a', 'path=p', 'year=2026', 'day=001', 'b.parquet'),
      touch('tier=raw', 'context=a', 'path=q', 'year=2026', 'day=001', 'c.parquet'),
      touch('tier=raw', 'context=b', 'path=p', 'year=2026', 'day=002', 'd.parquet'),
      touch('tier=5s', 'context=a', 'path=p', 'year=2026', 'day=001', 'e.parquet'),
      // Never entered: special directories and dot-directories at any level.
      touch('tier=raw', 'context=a', 'path=p', 'year=2026', 'quarantine', 'x.parquet'),
      touch('tier=raw', 'context=a', 'path=p', 'year=2026', '.compaction-trash-1', 'day=001', 'y.parquet'),
      touch('tier=raw', 'processed', 'path=p', 'year=2026', 'day=001', 'z.parquet'),
      // Wrong shape at a level: ignored.
      touch('tier=raw', 'context=a', 'notes.txt'),
      touch('tier=raw', 'context=a', 'path=p', 'year=2026', 'day=001', 'readme.md'),
    ]);
  }

  it('lists year directories across tiers, contexts and paths', async () => {
    await seed();
    const years = await listHiveDirs(base, { level: 'year' });
    expect(
      years.map(d => rel(d.yearDir)).sort()
    ).to.deep.equal([
      'tier=5s/context=a/path=p/year=2026',
      'tier=raw/context=a/path=p/year=2025',
      'tier=raw/context=a/path=p/year=2026',
      'tier=raw/context=a/path=q/year=2026',
      'tier=raw/context=b/path=p/year=2026',
    ]);
    expect(years.every(d => d.dayDir === undefined)).to.equal(true);
    expect(years.find(d => d.tier === '5s')?.year).to.equal(2026);
  });

  it('lists only the wanted days and carries year and day of year', async () => {
    await seed();
    const days = await listHiveDirs(base, {
      level: 'day',
      years: [2026],
      days: [dayKey(2026, 1)],
    });
    expect(
      days.map(d => rel(d.dayDir!)).sort()
    ).to.deep.equal([
      'tier=5s/context=a/path=p/year=2026/day=001',
      'tier=raw/context=a/path=p/year=2026/day=001',
      'tier=raw/context=a/path=q/year=2026/day=001',
    ]);
    expect(days[0].dayOfYear).to.equal(1);
    expect(days[0].year).to.equal(2026);
  });

  it('prunes by tier and never descends into a year that is not wanted', async () => {
    await seed();
    const days = await listHiveDirs(base, {
      level: 'day',
      tiers: ['raw'],
      years: [2025],
    });
    expect(days.map(d => rel(d.dayDir!))).to.deep.equal([
      'tier=raw/context=a/path=p/year=2025/day=364',
    ]);
  });

  it('emits year directories alongside days when asked', async () => {
    await seed();
    const dirs = await listHiveDirs(base, {
      level: 'day',
      includeYearDirs: true,
      tiers: ['raw'],
      contexts: ['a'],
      paths: ['p'],
    });
    expect(dirs.map(d => rel(d.dayDir ?? d.yearDir)).sort()).to.deep.equal([
      'tier=raw/context=a/path=p/year=2025',
      'tier=raw/context=a/path=p/year=2025/day=364',
      'tier=raw/context=a/path=p/year=2026',
      'tier=raw/context=a/path=p/year=2026/day=001',
    ]);
  });

  it('prunes by context and path directory values', async () => {
    await seed();
    const days = await listHiveDirs(base, {
      level: 'day',
      tiers: ['raw'],
      contexts: ['a'],
      paths: ['q'],
    });
    expect(days.map(d => rel(d.dayDir!))).to.deep.equal([
      'tier=raw/context=a/path=q/year=2026/day=001',
    ]);
    expect(
      await listHiveDirs(base, { level: 'day', contexts: ['nope'] })
    ).to.deep.equal([]);
  });

  it('ignores partition directories that are not canonical year/day values', async () => {
    await seed();
    await Promise.all([
      touch('tier=raw', 'context=a', 'path=p', 'year=2026-old', 'day=001', 'n1.parquet'),
      touch('tier=raw', 'context=a', 'path=p', 'year=2026', 'day=001-backup', 'n2.parquet'),
      touch('tier=raw', 'context=a', 'path=p', 'year=2026', 'day=1', 'n3.parquet'),
      touch('tier=raw', 'context=a', 'path=p', 'year=2026', 'day=400', 'n4.parquet'),
      touch('tier=raw', 'context=a', 'path=p', 'year=2026', 'day=000', 'n5.parquet'),
    ]);
    const years = await listHiveDirs(base, { level: 'year' });
    expect(years.some(d => d.yearDir.endsWith('year=2026-old'))).to.equal(false);
    const days = await listHiveDirs(base, { level: 'day', tiers: ['raw'] });
    expect(days.map(d => path.basename(d.dayDir!)).sort()).to.deep.equal([
      'day=001',
      'day=001',
      'day=002',
      'day=364',
    ]);
  });

  it('yields nothing for a missing store', async () => {
    expect(
      await listHiveDirs(path.join(base, 'nope'), { level: 'day' })
    ).to.deep.equal([]);
  });

  it('lists parquet files in one directory only', async () => {
    await seed();
    const dir = path.join(base, 'tier=raw', 'context=a', 'path=p', 'year=2026', 'day=001');
    await touch('tier=raw', 'context=a', 'path=p', 'year=2026', 'day=001', 'processed', 'old.parquet');
    expect(await listParquetFiles(dir)).to.deep.equal([path.join(dir, 'b.parquet')]);
  });

  it('lists entries matching a test in one directory', async () => {
    await seed();
    const yearDir = path.join(base, 'tier=raw', 'context=a', 'path=p', 'year=2026');
    expect(
      await listEntries(yearDir, e => e.isDirectory() && e.name.startsWith('.compaction-trash-'))
    ).to.deep.equal([path.join(yearDir, '.compaction-trash-1')]);
  });

  it('gives the event loop turns while walking', async () => {
    await seed();
    // Count the walk's own yields instead of racing a timer against it. A
    // setInterval(0) is clamped to 1ms and this walk finishes in well under
    // that, so the timer often never came due even though the walk yielded on
    // every readdir, and the test failed at random.
    const yields = async (yieldEvery: number): Promise<number> => {
      const real = global.setImmediate;
      let count = 0;
      global.setImmediate = ((
        fn: (...args: unknown[]) => void,
        ...args: unknown[]
      ) => {
        count += 1;
        return real(fn, ...args);
      }) as unknown as typeof setImmediate;
      try {
        await listHiveDirs(base, { level: 'day', yieldEvery });
      } finally {
        global.setImmediate = real;
      }
      return count;
    };
    // One yield per readdir when asked for one, and none at all when the
    // threshold is past every directory in the store.
    expect(await yields(1)).to.be.greaterThan(0);
    expect(await yields(1_000_000)).to.equal(0);
  });
});
