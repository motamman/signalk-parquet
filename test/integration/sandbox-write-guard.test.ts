/**
 * Establishes what the hardened sandbox instance does and does not prevent,
 * and that the SQL guard covers the gap.
 *
 * `getSandboxConnection` confines file access to the plugin data directory and
 * blocks extension loading, which stops untrusted SQL from reading host files,
 * reaching the network, or opening an external database. It does not make the
 * SQL read-only: inside the allowed directory the engine still writes, and the
 * plugin's recorded parquet files live in exactly that directory.
 *
 * The first test is a characterization test of DuckDB's behaviour rather than
 * of our code. It is here deliberately: if a future DuckDB makes the sandbox
 * read-only on its own, this test fails and the guard's justification should
 * be revisited rather than silently kept.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'os';
import * as path from 'path';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { findUnsafeSqlReason } from '../../src/utils/sql-guard';

/** DuckDB takes forward slashes on every platform, including Windows. */
function toSqlPath(filePath: string): string {
  return filePath.split(path.sep).join('/').replace(/'/g, "''");
}

describe('sandbox write exposure', function () {
  this.timeout(60000);

  let dataDir: string;
  let recorded: string;

  beforeEach(async () => {
    // The sandbox instance is scoped to the data directory of the first
    // caller and reused thereafter, so drop any instance an earlier suite
    // built before pointing it at this fixture directory.
    await DuckDBPool.shutdown();

    dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'sandbox-write-'));
    recorded = path.join(dataDir, 'navigation_position.parquet');

    // Stand in for a recorded data file the plugin has already written.
    const connection = await DuckDBPool.getSandboxConnection(dataDir);
    try {
      await connection.runAndReadAll(
        `COPY (SELECT 1 AS id, 'recorded' AS v) TO '${toSqlPath(recorded)}';`
      );
    } finally {
      connection.disconnectSync();
    }
  });

  afterEach(async () => {
    await DuckDBPool.shutdown();
    await fs.remove(dataDir);
  });

  it('the sandbox alone lets SQL overwrite a recorded parquet file', async () => {
    const connection = await DuckDBPool.getSandboxConnection(dataDir);
    try {
      await connection.runAndReadAll(
        `COPY (SELECT 99 AS id, 'clobbered' AS v) TO '${toSqlPath(recorded)}';`
      );
      const reader = await connection.runAndReadAll(
        `SELECT v FROM read_parquet('${toSqlPath(recorded)}');`
      );
      expect(
        reader.getRowObjects().map(row => row.v),
        'the sandbox is not read-only; the SQL guard is what makes it so'
      ).to.deep.equal(['clobbered']);
    } finally {
      connection.disconnectSync();
    }
  });

  it('the guard rejects the overwrite before it reaches the sandbox', () => {
    const reason = findUnsafeSqlReason(
      `COPY (SELECT 99 AS id, 'clobbered' AS v) TO '${toSqlPath(recorded)}';`
    );
    expect(reason).to.match(/'COPY' is not allowed/);
  });

  it('the guard rejects EXPORT DATABASE into the data directory', () => {
    const reason = findUnsafeSqlReason(
      `EXPORT DATABASE '${toSqlPath(path.join(dataDir, 'dump'))}';`
    );
    expect(reason).to.match(/'EXPORT' is not allowed/);
  });

  it('the guard still allows reading recorded data', () => {
    expect(
      findUnsafeSqlReason(
        `SELECT v FROM read_parquet('${toSqlPath(recorded)}')`
      )
    ).to.equal(null);
  });
});
