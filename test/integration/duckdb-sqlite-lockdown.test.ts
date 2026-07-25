/**
 * Pins the DuckDB-side half of the buffer isolation defence.
 *
 * DuckDB's bundled SQLite opening the live write buffer truncates
 * buffer.db-shm under node:sqlite's active mmap and kills the server with
 * SIGBUS (see src/utils/buffer-staging.ts). DuckDBPool.initialize() therefore
 * removes any cached sqlite extension, disables extension autoloading, and
 * locks the configuration so a query cannot switch it back on.
 *
 * The removal step is the one that actually stops `ATTACH ... (TYPE SQLITE)`:
 * a cached extension loads on ATTACH regardless of the autoload setting, and
 * every installation that ran the old federation code has one cached. That
 * step never executed in CI before this suite, because the other integration
 * suites call initialize() with no home directory.
 *
 * DuckDBPool is a static singleton shared with the other suites, so the pool
 * is shut down before and after each case to keep this order-independent.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'os';
import * as path from 'path';
import { DuckDBPool } from '../../src/utils/duckdb-pool';

/** Where DuckDB caches downloaded extensions under a plugin home directory. */
const CACHED_EXTENSION = path.join(
  '.duckdb',
  'extensions',
  'v1.5.3',
  'test_platform',
  'sqlite_scanner.duckdb_extension'
);

describe('DuckDB SQLite extension lockdown', function () {
  this.timeout(30000);

  let homeDir: string;
  let warnings: string[];

  beforeEach(async () => {
    await DuckDBPool.shutdown();
    homeDir = await fs.mkdtemp(path.join(os.tmpdir(), 'duckdb-lockdown-'));
    warnings = [];
  });

  afterEach(async () => {
    await DuckDBPool.shutdown();
    try {
      await fs.remove(homeDir);
    } catch {
      // Windows keeps a loaded extension DLL mapped for the life of the
      // process, so the temp directory may not be removable here. It is under
      // the OS temp dir; leaving it must not fail the suite.
    }
  });

  const initialize = () =>
    DuckDBPool.initialize(homeDir, message => warnings.push(message));

  /** Warnings unrelated to the lockdown (e.g. offline spatial download). */
  const lockdownWarnings = () =>
    warnings.filter(w => /lock down|cached DuckDB sqlite/i.test(w));

  it('removes a cached sqlite extension', async () => {
    const cached = path.join(homeDir, CACHED_EXTENSION);
    await fs.outputFile(cached, 'not a real extension');

    await initialize();

    expect(await fs.pathExists(cached)).to.equal(false);
    expect(DuckDBPool.getLockdownSummary()).to.include(
      'removed 1 cached sqlite extension file(s)'
    );
    expect(lockdownWarnings()).to.deep.equal([]);
  });

  it('reports the lockdown as applied', async () => {
    await initialize();

    expect(DuckDBPool.isSqliteLockedDown()).to.equal(true);
    expect(DuckDBPool.getLockdownSummary()).to.include(
      'autoload disabled: true'
    );
  });

  it('leaves extension autoloading disabled', async () => {
    await initialize();

    const connection = await DuckDBPool.getConnection();
    try {
      const reader = await connection.runAndReadAll(
        `SELECT value FROM duckdb_settings() ` +
          `WHERE name = 'autoload_known_extensions'`
      );
      expect(String(reader.getRowObjects()[0].value).toLowerCase()).to.equal(
        'false'
      );
    } finally {
      connection.disconnectSync();
    }
  });

  it('refuses to re-enable autoloading from a query', async () => {
    await initialize();

    const connection = await DuckDBPool.getConnection();
    try {
      let message = '';
      try {
        await connection.runAndReadAll(
          'SET GLOBAL autoload_known_extensions = true;'
        );
      } catch (err) {
        message = (err as Error).message;
      }
      expect(message).to.match(/configuration has been locked/);
    } finally {
      connection.disconnectSync();
    }
  });

  it('cannot ATTACH a SQLite database once the cache is cleared', async () => {
    await fs.outputFile(path.join(homeDir, CACHED_EXTENSION), 'stale');
    await initialize();

    const connection = await DuckDBPool.getConnection();
    try {
      const target = path.join(homeDir, 'buffer.db').replace(/\\/g, '/');
      let message = '';
      try {
        await connection.runAndReadAll(
          `ATTACH '${target}' AS b (TYPE SQLITE, READ_ONLY);`
        );
      } catch (err) {
        message = (err as Error).message;
      }
      // Fails at extension load, before any file is opened — which is the
      // whole point: no SQLite library touches the buffer.
      expect(message).to.match(/sqlite_scanner|not found|autoload/i);
    } finally {
      connection.disconnectSync();
    }
  });

  it('applies the lockdown when no home directory is configured', async () => {
    await DuckDBPool.initialize(undefined, message => warnings.push(message));

    expect(DuckDBPool.isSqliteLockedDown()).to.equal(true);
    expect(lockdownWarnings()).to.deep.equal([]);
  });
});
