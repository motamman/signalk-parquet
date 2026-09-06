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

  describe('overwrites that an earlier revision of the guard let through', () => {
    // Same shape as the characterization test above, one level down: each of
    // these is a real overwrite that DuckDB performs, reached through a
    // construct the guard once mis-lexed. Running both halves keeps the
    // payloads honest — a payload that stopped executing would no longer
    // prove anything about the rule that rejects it.
    const overwrites: [string, (target: string) => string][] = [
      [
        'a COPY hidden behind a comment inside a quoted identifier',
        target =>
          `SELECT 1 AS "a--b"; COPY (SELECT 99 AS id, 'clobbered' AS v) TO '${target}'`,
      ],
      [
        'a COPY hidden behind an escaped quote in an escape string',
        target =>
          `SELECT E'a\\'b' AS x; COPY (SELECT 99 AS id, 'clobbered' AS v) TO '${target}'`,
      ],
      [
        'a COPY executed by EXPLAIN ANALYZE',
        target =>
          `EXPLAIN ANALYZE COPY (SELECT 99 AS id, 'clobbered' AS v) TO '${target}'`,
      ],
    ];

    for (const [label, build] of overwrites) {
      it(`the sandbox executes ${label}`, async () => {
        const connection = await DuckDBPool.getSandboxConnection(dataDir);
        try {
          await connection.runAndReadAll(build(toSqlPath(recorded)));
          const reader = await connection.runAndReadAll(
            `SELECT v FROM read_parquet('${toSqlPath(recorded)}');`
          );
          expect(reader.getRowObjects().map(row => row.v)).to.deep.equal([
            'clobbered',
          ]);
        } finally {
          connection.disconnectSync();
        }
      });

      it(`the guard rejects ${label}`, () => {
        expect(findUnsafeSqlReason(build(toSqlPath(recorded)))).to.match(
          /'COPY' is not allowed/
        );
      });
    }
  });

  describe('engine settings the sandbox cannot defend on its own', () => {
    const raiseMemoryLimit = "EXPLAIN ANALYZE SET memory_limit='4GB'";

    it('the sandbox lets SQL raise its own memory limit', async () => {
      // The sandbox sets memory_limit to 512MB precisely so untrusted SQL
      // cannot exhaust the Node process; EXPLAIN ANALYZE runs the SET that
      // lifts it.
      const connection = await DuckDBPool.getSandboxConnection(dataDir);
      try {
        await connection.runAndReadAll(raiseMemoryLimit);
        const reader = await connection.runAndReadAll(
          `SELECT current_setting('memory_limit') AS limit_setting;`
        );
        expect(reader.getRowObjects()[0].limit_setting).to.match(/GiB/);
      } finally {
        connection.disconnectSync();
      }
    });

    it('the guard rejects raising the memory limit', () => {
      expect(findUnsafeSqlReason(raiseMemoryLimit)).to.match(
        /'SET' is not allowed/
      );
    });
  });
});
