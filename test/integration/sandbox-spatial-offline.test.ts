/**
 * Pins that the sandboxed DuckDB instance survives an unavailable spatial
 * extension, as the main pool already does. The Signal K plugin registry
 * runs this suite with no network and an empty extension cache; before this
 * the sandbox threw on INSTALL and every raw SQL test failed there.
 *
 * The no-network case is made deterministic by pointing DuckDB's extension
 * repository at a closed local port and its extension directory at an empty
 * temp dir, so neither a cached copy nor a download can exist.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'os';
import * as path from 'path';
import { DuckDBPool } from '../../src/utils/duckdb-pool';

describe('sandbox instance without the spatial extension', function () {
  this.timeout(60000);

  let home: string;
  let warnings: string[];

  beforeEach(async () => {
    await DuckDBPool.shutdown();
    home = await fs.mkdtemp(path.join(os.tmpdir(), 'sk-nospatial-'));
    warnings = [];
    await DuckDBPool.initialize(home, message => warnings.push(message), {
      extensionRepository: 'http://127.0.0.1:9/',
    });
  });

  afterEach(async () => {
    await DuckDBPool.shutdown();
    await fs.remove(home);
  });

  it('main pool reports spatial unavailable rather than failing to initialize', () => {
    expect(DuckDBPool.isInitialized()).to.equal(true);
    expect(DuckDBPool.isSpatialAvailable()).to.equal(false);
    expect(
      warnings.some(w => /spatial extension unavailable/.test(w))
    ).to.equal(true);
  });

  it('sandbox connection still opens and runs plain SQL', async () => {
    const connection = await DuckDBPool.getSandboxConnection(home);
    try {
      const reader = await connection.runAndReadAll('SELECT 42 AS answer;');
      expect(
        Number((reader.getRowObjects()[0] as { answer: number }).answer)
      ).to.equal(42);
    } finally {
      connection.disconnectSync();
    }
    expect(
      warnings.some(w => /unavailable on the sandbox instance/.test(w))
    ).to.equal(true);
  });

  it('sandbox stays locked down after the failed extension load', async () => {
    const connection = await DuckDBPool.getSandboxConnection(home);
    try {
      let error: Error | undefined;
      try {
        await connection.runAndReadAll("SET memory_limit = '4GB';");
      } catch (err) {
        error = err as Error;
      }
      expect(error, 'configuration must be locked').to.be.an('error');
      const outside = path.join(os.tmpdir(), 'sk-nospatial-outside.csv');
      let readError: Error | undefined;
      try {
        await connection.runAndReadAll(
          `SELECT * FROM read_csv('${outside.split(path.sep).join('/')}');`
        );
      } catch (err) {
        readError = err as Error;
      }
      expect(readError, 'reads outside the data dir must be refused').to.be.an(
        'error'
      );
    } finally {
      connection.disconnectSync();
    }
  });
});
