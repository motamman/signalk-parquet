/**
 * Unit tests for the cached recursive directory scanner. They run against
 * real temp directories: a small tree is created per test (including a
 * 'processed' directory that the default exclusion list must skip) and
 * removed afterwards. Cache expiry is exercised by stubbing Date.now rather
 * than sleeping, since validity is computed as now - scannedAt < cacheTTL.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'os';
import * as path from 'path';
import { DirectoryScanner } from '../../../src/utils/directory-scanner';

describe('DirectoryScanner', () => {
  const realDateNow = Date.now;
  let tmpDirs: string[];
  let tmpRoot: string;

  function makeTmpDir(): string {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'scanner-test-'));
    tmpDirs.push(dir);
    return dir;
  }

  /** Creates a file under tmpRoot, creating parent directories as needed. */
  function writeFile(relative: string[], content: string): string {
    const target = path.join(tmpRoot, ...relative);
    fs.outputFileSync(target, content);
    return target;
  }

  beforeEach(() => {
    tmpDirs = [];
    tmpRoot = makeTmpDir();
    // Layout mirrors a signalk-parquet data folder:
    //   <root>/signalk_data_2025-11-02.parquet
    //   <root>/notes.txt
    //   <root>/navigation/signalk_data_2025-11-02_consolidated.parquet
    //   <root>/navigation/speedOverGround/signalk_data_2025-11-03.parquet
    //   <root>/processed/leftover.parquet   (special dir, skipped by default)
    writeFile(['signalk_data_2025-11-02.parquet'], 'day-one-data');
    writeFile(['notes.txt'], 'not parquet');
    writeFile(
      ['navigation', 'signalk_data_2025-11-02_consolidated.parquet'],
      'consolidated-day-one'
    );
    writeFile(
      ['navigation', 'speedOverGround', 'signalk_data_2025-11-03.parquet'],
      'day-two-data'
    );
    writeFile(['processed', 'leftover.parquet'], 'must never appear');
  });

  afterEach(async () => {
    Date.now = realDateNow;
    for (const dir of tmpDirs) {
      await fs.remove(dir);
    }
  });

  // Every file outside the special 'processed' directory, sorted by name.
  const visibleNames = [
    'notes.txt',
    'signalk_data_2025-11-02.parquet',
    'signalk_data_2025-11-02_consolidated.parquet',
    'signalk_data_2025-11-03.parquet',
  ];

  describe('scanDirectory', () => {
    it('lists files recursively and skips special directories', async () => {
      const scanner = new DirectoryScanner();
      const files = await scanner.scanDirectory(tmpRoot);
      expect(files.map(f => f.name).sort()).to.deep.equal(visibleNames);
      const processedDir = path.join(tmpRoot, 'processed');
      expect(files.filter(f => f.directory === processedDir)).to.deep.equal([]);
    });

    it('populates the FileInfo metadata fields', async () => {
      const scanner = new DirectoryScanner();
      const files = await scanner.scanDirectory(tmpRoot);

      const rootFile = files.find(
        f => f.name === 'signalk_data_2025-11-02.parquet'
      );
      expect(rootFile).to.not.equal(undefined);
      expect(rootFile!.path).to.equal(
        path.join(tmpRoot, 'signalk_data_2025-11-02.parquet')
      );
      expect(rootFile!.directory).to.equal(tmpRoot);
      expect(rootFile!.size).to.equal(Buffer.byteLength('day-one-data'));
      expect(rootFile!.modifiedTime).to.be.greaterThan(0);

      const nestedFile = files.find(
        f => f.name === 'signalk_data_2025-11-03.parquet'
      );
      expect(nestedFile!.directory).to.equal(
        path.join(tmpRoot, 'navigation', 'speedOverGround')
      );
      expect(nestedFile!.path).to.equal(
        path.join(
          tmpRoot,
          'navigation',
          'speedOverGround',
          'signalk_data_2025-11-03.parquet'
        )
      );
    });

    it('filters by file name pattern but caches the full listing', async () => {
      const scanner = new DirectoryScanner();
      const parquetOnly = await scanner.scanDirectory(tmpRoot, /\.parquet$/);
      expect(parquetOnly.map(f => f.name).sort()).to.deep.equal(
        visibleNames.filter(n => n.endsWith('.parquet'))
      );
      // The cache stores the unfiltered listing; the pattern is applied on
      // the way out.
      expect(scanner.getCacheStats().totalFiles).to.equal(visibleNames.length);
    });

    it('returns an empty list for a missing directory', async () => {
      const scanner = new DirectoryScanner();
      const files = await scanner.scanDirectory(
        path.join(tmpRoot, 'does-not-exist')
      );
      expect(files).to.deep.equal([]);
      // Pinned: the empty result is cached like any other scan.
      expect(scanner.getCacheStats().entries).to.equal(1);
    });

    it('honours a custom exclusion list that replaces the defaults', async () => {
      const scanner = new DirectoryScanner(60000, ['navigation']);
      const files = await scanner.scanDirectory(tmpRoot);
      // 'processed' becomes visible because the override replaces
      // SPECIAL_DIRECTORIES rather than extending it.
      expect(files.map(f => f.name).sort()).to.deep.equal([
        'leftover.parquet',
        'notes.txt',
        'signalk_data_2025-11-02.parquet',
      ]);
    });
  });

  describe('caching', () => {
    it('serves cached results even after the directory changes', async () => {
      const scanner = new DirectoryScanner();
      await scanner.scanDirectory(tmpRoot);
      writeFile(['behind-the-cache.parquet'], 'sneaky');
      const files = await scanner.scanDirectory(tmpRoot);
      expect(files.map(f => f.name).sort()).to.deep.equal(visibleNames);
    });

    it('applies a pattern to the cached listing without rescanning', async () => {
      const scanner = new DirectoryScanner();
      await scanner.scanDirectory(tmpRoot);
      writeFile(['behind-the-cache.txt'], 'sneaky');
      const txtFiles = await scanner.scanDirectory(tmpRoot, /\.txt$/);
      // Only the .txt file known at scan time, not the new one.
      expect(txtFiles.map(f => f.name)).to.deep.equal(['notes.txt']);
    });

    it('forceRefresh bypasses a valid cache entry', async () => {
      const scanner = new DirectoryScanner();
      await scanner.scanDirectory(tmpRoot);
      writeFile(['behind-the-cache.parquet'], 'sneaky');
      const files = await scanner.scanDirectory(tmpRoot, null, true);
      expect(files.map(f => f.name)).to.include('behind-the-cache.parquet');
      expect(files).to.have.length(visibleNames.length + 1);
    });

    it('invalidateCache forces the next scan to hit the filesystem', async () => {
      const scanner = new DirectoryScanner();
      await scanner.scanDirectory(tmpRoot);
      writeFile(['behind-the-cache.parquet'], 'sneaky');
      scanner.invalidateCache(tmpRoot);
      const files = await scanner.scanDirectory(tmpRoot);
      expect(files).to.have.length(visibleNames.length + 1);
    });

    it('invalidateCache on an unknown directory is a no-op', async () => {
      const scanner = new DirectoryScanner();
      await scanner.scanDirectory(tmpRoot);
      scanner.invalidateCache(path.join(tmpRoot, 'never-scanned'));
      expect(scanner.getCacheStats().entries).to.equal(1);
    });

    it('clearCache drops every entry', async () => {
      const scanner = new DirectoryScanner();
      await scanner.scanDirectory(tmpRoot);
      scanner.clearCache();
      expect(scanner.getCacheStats()).to.deep.equal({
        entries: 0,
        totalFiles: 0,
        totalDirectories: 0,
      });
    });

    it('expires entries once the TTL has elapsed', async () => {
      const ttl = 1000;
      const scanner = new DirectoryScanner(ttl);
      // Freeze time so expiry is driven by this test, not the wall clock.
      let now = realDateNow();
      Date.now = () => now;

      await scanner.scanDirectory(tmpRoot);
      writeFile(['late-arrival.parquet'], 'late');

      // One millisecond before expiry the cache still answers.
      now += ttl - 1;
      expect(await scanner.scanDirectory(tmpRoot)).to.have.length(
        visibleNames.length
      );

      // At exactly the TTL the entry is stale and the scan repeats.
      now += 1;
      expect(await scanner.scanDirectory(tmpRoot)).to.have.length(
        visibleNames.length + 1
      );
    });

    it('a zero TTL disables caching', async () => {
      const scanner = new DirectoryScanner(0);
      await scanner.scanDirectory(tmpRoot);
      writeFile(['fresh.parquet'], 'fresh');
      expect(await scanner.scanDirectory(tmpRoot)).to.have.length(
        visibleNames.length + 1
      );
    });
  });

  describe('cache statistics', () => {
    it('counts entries, files and directories across cached roots', async () => {
      const scanner = new DirectoryScanner();
      await scanner.scanDirectory(tmpRoot);
      expect(scanner.getCacheStats()).to.deep.equal({
        entries: 1,
        totalFiles: visibleNames.length,
        // navigation/ and navigation/speedOverGround/; processed/ is
        // excluded before it is recorded.
        totalDirectories: 2,
      });

      const secondRoot = makeTmpDir();
      fs.outputFileSync(path.join(secondRoot, 'single.parquet'), 'tiny');
      await scanner.scanDirectory(secondRoot);
      expect(scanner.getCacheStats()).to.deep.equal({
        entries: 2,
        totalFiles: visibleNames.length + 1,
        totalDirectories: 2,
      });
    });

    it('getCachedDirectories returns the scanned roots', async () => {
      const scanner = new DirectoryScanner();
      await scanner.scanDirectory(tmpRoot);
      expect(scanner.getCachedDirectories()).to.deep.equal([tmpRoot]);
    });
  });

  describe('findFilesByDate', () => {
    it('matches files containing the date and skips consolidated ones', async () => {
      const scanner = new DirectoryScanner();
      const files = await scanner.findFilesByDate(tmpRoot, '2025-11-02');
      expect(files.map(f => f.name)).to.deep.equal([
        'signalk_data_2025-11-02.parquet',
      ]);
    });

    it('includes consolidated files when asked', async () => {
      const scanner = new DirectoryScanner();
      const files = await scanner.findFilesByDate(tmpRoot, '2025-11-02', false);
      expect(files.map(f => f.name).sort()).to.deep.equal([
        'signalk_data_2025-11-02.parquet',
        'signalk_data_2025-11-02_consolidated.parquet',
      ]);
    });

    it('returns an empty list when no file carries the date', async () => {
      const scanner = new DirectoryScanner();
      const files = await scanner.findFilesByDate(tmpRoot, '2025-12-25');
      expect(files).to.deep.equal([]);
    });
  });
});
