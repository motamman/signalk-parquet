/**
 * Startup end to end: the crash-recovery sweeps run together (in-process
 * here; the plugin forks them) and report what they did, and the cloud
 * sync's file listing for a window of days is exact without a `**` glob.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'os';
import * as path from 'path';
import { runStartupSweeps } from '../../src/services/startup-sweeps';
import { listParquetFilesForDays } from '../../src/data-handler';
import { HivePathBuilder } from '../../src/utils/hive-path-builder';

const hive = new HivePathBuilder();

function dayDir(d: Date): string[] {
  return [
    `year=${d.getUTCFullYear()}`,
    `day=${String(hive.getDayOfYear(d)).padStart(3, '0')}`,
  ];
}

describe('startup sweeps and sync listing', () => {
  let base: string;
  const logs: string[] = [];
  const log = {
    debug: (m: string) => logs.push(m),
    error: (m: string) => logs.push(`ERROR ${m}`),
  };

  beforeEach(async () => {
    base = await fs.mkdtemp(path.join(os.tmpdir(), 'startup-sweeps-'));
    logs.length = 0;
  });

  afterEach(async () => {
    await fs.remove(base);
  });

  async function write(content: string, ...segments: string[]): Promise<string> {
    const file = path.join(base, ...segments);
    await fs.ensureDir(path.dirname(file));
    await fs.writeFile(file, content);
    return file;
  }

  it('runs every sweep, quarantines a stub and reports counts', async () => {
    const stub = await write('', 'tier=raw', 'context=a', 'path=p', 'year=2026', 'day=100', 'stub.parquet');
    const good = await write('x'.repeat(200), 'tier=raw', 'context=a', 'path=p', 'year=2026', 'day=100', 'good.parquet');
    const temp = await write('x', 'tier=raw', 'context=a', 'path=p', 'year=2026', 'year_compact_2026.parquet.tmp');
    // A DuckDB spill file left by a killed worker.
    const spill = await write('x', '.duckdb', 'tmp', 'duckdb_temp_storage-0.tmp');

    const result = await runStartupSweeps(base, log);

    expect(result.complete).to.equal(true);
    expect(result.removed).to.equal(1);
    expect(result.duckdbTempRemoved).to.equal(1);
    expect(await fs.pathExists(spill)).to.equal(false);
    expect(await fs.pathExists(path.dirname(spill)), 'the tmp directory itself stays').to.equal(true);
    expect(result.quarantined).to.equal(1);
    expect(result.quarantineFailed).to.equal(0);
    expect(result.durationMs).to.be.at.least(0);
    expect(await fs.pathExists(stub)).to.equal(false);
    expect(await fs.pathExists(temp)).to.equal(false);
    expect(await fs.pathExists(good)).to.equal(true);
    expect(
      await fs.pathExists(path.join(path.dirname(stub), 'quarantine', 'stub.parquet'))
    ).to.equal(true);
    expect(logs.some(l => l.startsWith('ERROR'))).to.equal(false);
  });

  it('lists exactly the files recorded on the requested days, across tiers', async () => {
    const d0 = new Date('2026-03-01T12:00:00Z');
    const inWindow = [1, 3, 7].map(n => new Date(d0.getTime() - n * 86400000));
    const outside = [0, 8].map(n => new Date(d0.getTime() - n * 86400000));
    const expected: string[] = [];
    for (const d of inWindow) {
      expected.push(await write('x', 'tier=raw', 'context=a', 'path=p', ...dayDir(d), 'a.parquet'));
      expected.push(await write('x', 'tier=60s', 'context=b', 'path=q', ...dayDir(d), 'b.parquet'));
    }
    for (const d of outside) {
      await write('x', 'tier=raw', 'context=a', 'path=p', ...dayDir(d), 'no.parquet');
    }
    // Side directories inside a wanted day are not entered.
    await write('x', 'tier=raw', 'context=a', 'path=p', ...dayDir(inWindow[0]), 'processed', 'no.parquet');
    await write('x', 'tier=raw', 'context=a', 'path=p', ...dayDir(inWindow[0]), 'notes.txt');

    const window: Date[] = [];
    for (let n = 1; n <= 7; n++) window.push(new Date(d0.getTime() - n * 86400000));
    const files = await listParquetFilesForDays(base, window);

    expect(files.sort()).to.deep.equal(expected.sort());
  });

  it('lists nothing for a missing store', async () => {
    expect(await listParquetFilesForDays(path.join(base, 'nope'), [new Date()])).to.deep.equal([]);
  });
});
