/**
 * Unit tests for globIn, the directory-anchored glob behind all file
 * discovery. The directory is passed to glob as its cwd rather than embedded
 * in the pattern, so neither native separators nor glob syntax in a
 * directory name can change what matches.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'os';
import * as path from 'path';
import { globIn } from '../../../src/utils/glob-in';

describe('globIn', () => {
  let base: string;

  beforeEach(async () => {
    // An extglob group and a character class: embedded in a pattern, this
    // directory name would match names like 'store1a' but never itself.
    base = await fs.mkdtemp(path.join(os.tmpdir(), 'store+(1)[a]-'));
  });

  afterEach(async () => {
    await fs.remove(base);
  });

  /** Creates an empty file under the base directory and returns its path. */
  async function touch(...segments: string[]): Promise<string> {
    const file = path.join(base, ...segments);
    await fs.ensureDir(path.dirname(file));
    await fs.writeFile(file, '');
    return file;
  }

  it('finds files under a directory whose name is glob syntax', async () => {
    const file = await touch(
      'tier=raw',
      'context=a',
      'path=b',
      'year=2024',
      'day=153',
      'x.parquet'
    );
    expect(await globIn(base, 'tier=*/**/*.parquet')).to.deep.equal([file]);
  });

  it('returns absolute native paths for directory patterns', async () => {
    await touch('tier=raw', 'context=a', 'x.parquet');
    expect(
      await globIn(path.join(base, 'tier=raw'), 'context=*')
    ).to.deep.equal([path.join(base, 'tier=raw', 'context=a')]);
  });

  it('drops matches under ignored directories', async () => {
    const kept = await touch('day=153', 'x.parquet');
    await touch('day=153', 'processed', 'y.parquet');
    expect(
      await globIn(base, '**/*.parquet', { ignore: ['**/processed/**'] })
    ).to.deep.equal([kept]);
  });

  it('matches case-insensitively with nocase', async () => {
    const file = await touch('TRACK.GPX');
    expect(await globIn(base, '**/*.gpx', { nocase: true })).to.deep.equal([
      file,
    ]);
  });

  it('returns no matches for a directory that does not exist', async () => {
    expect(
      await globIn(path.join(base, 'missing'), '**/*.parquet')
    ).to.deep.equal([]);
  });
});
