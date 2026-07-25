/**
 * Unit tests for the SQL guard protecting the two paths that execute
 * externally-supplied SQL (raw /api/query and Claude-generated analysis
 * queries). The guard exists because DuckDB's bundled SQLite opening the live
 * write buffer truncates buffer.db-shm under the writer's mmap and kills the
 * server with SIGBUS, so every statement must be read-only and no
 * database-opening table function may appear.
 *
 * The bypass cases below were all confirmed to execute on DuckDB 1.5.3 before
 * the rule that rejects them was added.
 */
import { expect } from 'chai';
import {
  maskSqlLiteralsAndComments,
  findUnsafeSqlReason,
} from '../../../src/utils/sql-guard';

describe('maskSqlLiteralsAndComments', () => {
  it('preserves length so offsets stay aligned', () => {
    const sql = "SELECT 'abc' FROM t -- note";
    expect(maskSqlLiteralsAndComments(sql)).to.have.lengthOf(sql.length);
  });

  it('masks single-quoted literals', () => {
    expect(
      maskSqlLiteralsAndComments("SELECT * FROM t WHERE p = 'engine.load'")
    ).to.not.include('load');
  });

  it('handles doubled-quote escapes inside literals', () => {
    const masked = maskSqlLiteralsAndComments(
      "SELECT 'it''s a load' AS note FROM t"
    );
    expect(masked).to.not.include('load');
    expect(masked).to.include('AS note');
  });

  it('masks dollar-quoted strings', () => {
    const masked = maskSqlLiteralsAndComments('SELECT $$a load$$ AS x');
    expect(masked).to.not.include('load');
    expect(masked).to.include('AS x');
  });

  it('masks tagged dollar-quoted strings', () => {
    expect(
      maskSqlLiteralsAndComments('SELECT $tag$ attach $tag$ AS x')
    ).to.not.include('attach');
  });

  it('masks an apostrophe inside a dollar-quoted string', () => {
    // The apostrophe in don't must not start a literal: doing so shifts every
    // later quote boundary and hides the following statement.
    const masked = maskSqlLiteralsAndComments(
      "SELECT $$don't$$ AS a; ATTACH '/d/buffer.db' AS b"
    );
    expect(masked).to.include('ATTACH');
  });

  it('masks quoted identifiers', () => {
    expect(maskSqlLiteralsAndComments('SELECT "load" FROM t')).to.not.include(
      'load'
    );
  });

  it('masks line and block comments', () => {
    expect(
      maskSqlLiteralsAndComments('SELECT 1 -- attach the buffer\nFROM t')
    ).to.not.include('attach');
    expect(
      maskSqlLiteralsAndComments('SELECT /* load bearing */ 1')
    ).to.not.include('load');
  });

  it('keeps newlines for line alignment', () => {
    expect(maskSqlLiteralsAndComments('SELECT 1 -- c\nFROM t')).to.include(
      '\nFROM t'
    );
  });

  describe('unterminated constructs blank the rest of the input', () => {
    // Each of these is a DuckDB parse error ("unterminated quoted string",
    // "unterminated /* comment", and the equivalents for quoted identifiers
    // and dollar-quoted strings), so blanking to end cannot hide an
    // executable statement.
    const cases: [string, string][] = [
      ['single quote', "SELECT 'oops; LOAD sqlite"],
      ['block comment', 'SELECT 1 /* oops; LOAD sqlite'],
      ['quoted identifier', 'SELECT "oops; LOAD sqlite'],
      ['dollar quote', 'SELECT $tag$ oops; LOAD sqlite'],
    ];

    for (const [label, sql] of cases) {
      it(`blanks after an unterminated ${label}`, () => {
        expect(maskSqlLiteralsAndComments(sql)).to.not.include('LOAD');
      });
    }
  });
});

describe('findUnsafeSqlReason', () => {
  describe('allows legitimate read-only queries', () => {
    it('allows a plain SELECT over parquet', () => {
      expect(
        findUnsafeSqlReason(
          "SELECT signalk_timestamp, value FROM read_parquet('/d/*.parquet') " +
            "WHERE context = 'vessels.self' ORDER BY signalk_timestamp"
        )
      ).to.equal(null);
    });

    it('allows a WITH/CTE query', () => {
      expect(
        findUnsafeSqlReason(
          "WITH b AS (SELECT * FROM read_parquet('/d/*.parquet')) " +
            'SELECT count(*) FROM b'
        )
      ).to.equal(null);
    });

    it('allows a trailing semicolon and surrounding whitespace', () => {
      expect(findUnsafeSqlReason('  SELECT 1;  ')).to.equal(null);
    });

    it('allows lowercase select', () => {
      expect(findUnsafeSqlReason('select 1')).to.equal(null);
    });

    it('allows a leading comment before the statement', () => {
      expect(findUnsafeSqlReason('-- daily average\nSELECT 1')).to.equal(null);
    });

    it('allows identifiers that contain blocked keywords', () => {
      expect(
        findUnsafeSqlReason(
          'SELECT payload, engine_load, created_at, updated_at FROM t'
        )
      ).to.equal(null);
    });

    it('allows a bare alias named load', () => {
      // A model analyzing propulsion.main.load naturally picks this alias.
      expect(findUnsafeSqlReason('SELECT AVG(value) AS load FROM t')).to.equal(
        null
      );
    });

    it('allows file paths containing blocked words inside literals', () => {
      expect(
        findUnsafeSqlReason(
          "SELECT * FROM read_parquet('/d/path=propulsion.main.load/*.parquet')"
        )
      ).to.equal(null);
    });

    it('allows spatial function queries', () => {
      expect(
        findUnsafeSqlReason(
          'SELECT ST_Distance_Sphere(ST_Point(lon, lat), ST_Point(0, 0)) FROM t'
        )
      ).to.equal(null);
    });
  });

  describe('allows read-only DuckDB syntax beyond plain SELECT', () => {
    // All verified to execute on DuckDB 1.5.3. These are idiomatic in a raw
    // SQL console, so rejecting them would be an unannounced regression.
    const readOnly: [string, string][] = [
      ['FROM-first', "FROM read_parquet('/d/*.parquet') SELECT *"],
      ['parenthesised union', '(SELECT 1) UNION (SELECT 2)'],
      ['DESCRIBE', 'DESCRIBE SELECT 1'],
      ['SUMMARIZE', 'SUMMARIZE SELECT 1'],
      ['EXPLAIN', 'EXPLAIN SELECT 1'],
      ['VALUES', 'VALUES (1), (2)'],
      ['PIVOT', 'PIVOT (SELECT 1 AS a, 2 AS b) ON a USING sum(b)'],
      ['SHOW', 'SHOW TABLES'],
    ];

    for (const [label, sql] of readOnly) {
      it(`allows ${label}`, () => {
        expect(findUnsafeSqlReason(sql)).to.equal(null);
      });
    }
  });

  describe('rejects statements that can reach the live buffer', () => {
    it('rejects ATTACH of a SQLite file', () => {
      expect(
        findUnsafeSqlReason("ATTACH '/d/buffer.db' AS b (TYPE SQLITE)")
      ).to.match(/'ATTACH' is not allowed/);
    });

    it('rejects a statement chained after a benign SELECT', () => {
      expect(
        findUnsafeSqlReason("SELECT 1; ATTACH '/d/buffer.db' AS b")
      ).to.match(/'ATTACH' is not allowed/);
    });

    it('rejects re-enabling extension autoloading', () => {
      // This would undo the engine-level lockdown in duckdb-pool.
      expect(
        findUnsafeSqlReason(
          'SELECT 1; SET GLOBAL autoload_known_extensions = true'
        )
      ).to.match(/'SET' is not allowed/);
    });

    it('rejects INSTALL and FORCE INSTALL', () => {
      expect(findUnsafeSqlReason('INSTALL sqlite')).to.match(
        /'INSTALL' is not allowed/
      );
      expect(findUnsafeSqlReason('FORCE INSTALL sqlite')).to.match(
        /'FORCE' is not allowed/
      );
    });

    it('rejects LOAD and PRAGMA', () => {
      expect(findUnsafeSqlReason('LOAD sqlite')).to.match(
        /'LOAD' is not allowed/
      );
      expect(findUnsafeSqlReason('PRAGMA database_list')).to.match(
        /'PRAGMA' is not allowed/
      );
    });

    it('rejects sqlite_scan, sqlite_attach and sqlite_query', () => {
      for (const fn of ['sqlite_scan', 'sqlite_attach', 'sqlite_query']) {
        expect(
          findUnsafeSqlReason(`SELECT * FROM ${fn}('/d/buffer.db', 't')`),
          fn
        ).to.match(/Table function/);
      }
    });

    it('rejects a quoted-identifier sqlite function call', () => {
      // DuckDB resolves "sqlite_scan"(...) exactly like the bare form; this
      // returned live buffer rows when the name check ran over SQL whose
      // quoted identifiers had already been masked.
      expect(
        findUnsafeSqlReason(`SELECT v FROM "sqlite_scan"('/d/buffer.db','t')`)
      ).to.match(/Table function/);
    });

    it('rejects nesting a SQLite scan inside query()', () => {
      // The nested SQL is a string literal, so the statement whitelist cannot
      // see into it; query() itself has to be rejected.
      expect(
        findUnsafeSqlReason(
          `SELECT v FROM query('SELECT v FROM sqlite_scan(''/d/buffer.db'',''t'')')`
        )
      ).to.match(/Table function 'query'/);
    });

    it('rejects query_table', () => {
      expect(findUnsafeSqlReason("SELECT * FROM query_table('t')")).to.match(
        /Table function/
      );
    });

    it('rejects CALL of a sqlite function', () => {
      expect(
        findUnsafeSqlReason("CALL sqlite_attach('/d/buffer.db')")
      ).to.match(/'CALL' is not allowed/);
    });

    it('rejects sqlite functions regardless of case and spacing', () => {
      expect(
        findUnsafeSqlReason("SELECT * FROM SQLITE_SCAN ('/d/buffer.db', 't')")
      ).to.match(/Table function/);
    });

    it('rejects a statement hidden behind a dollar-quoted apostrophe', () => {
      expect(
        findUnsafeSqlReason(
          "SELECT $$don't$$ AS a; ATTACH '/d/buffer.db' AS b (TYPE SQLITE)"
        )
      ).to.match(/'ATTACH' is not allowed/);
    });
  });

  describe('rejects raw file readers', () => {
    for (const fn of ['read_text', 'read_blob', 'glob']) {
      it(`rejects ${fn}`, () => {
        expect(
          findUnsafeSqlReason(`SELECT * FROM ${fn}('/etc/passwd')`)
        ).to.match(/Table function/);
      });
    }

    it('rejects duckdb_secrets', () => {
      expect(findUnsafeSqlReason('SELECT * FROM duckdb_secrets()')).to.match(
        /Table function/
      );
    });

    it('still allows read_parquet and read_csv', () => {
      expect(
        findUnsafeSqlReason("SELECT * FROM read_parquet('/d/x.parquet')")
      ).to.equal(null);
      expect(
        findUnsafeSqlReason("SELECT * FROM read_csv('/d/x.csv')")
      ).to.equal(null);
    });
  });

  describe('rejects filesystem writes', () => {
    it('rejects COPY ... TO', () => {
      expect(findUnsafeSqlReason("COPY (SELECT 1) TO '/d/buffer.db'")).to.match(
        /'COPY' is not allowed/
      );
    });

    it('rejects EXPORT DATABASE', () => {
      expect(
        findUnsafeSqlReason("SELECT 1; EXPORT DATABASE '/d/out'")
      ).to.match(/'EXPORT' is not allowed/);
    });
  });

  describe('rejects data modification', () => {
    for (const sql of [
      'DROP TABLE t',
      'DELETE FROM t',
      'UPDATE t SET a = 1',
      'INSERT INTO t VALUES (1)',
      'CREATE TABLE t (a INT)',
      'ALTER TABLE t ADD COLUMN b INT',
      'TRUNCATE t',
    ]) {
      it(`rejects ${sql.split(' ')[0]}`, () => {
        expect(findUnsafeSqlReason(sql)).to.match(/Only read-only queries/);
      });
    }

    it('rejects a WITH-prefixed INSERT', () => {
      // This parses and runs on DuckDB and starts with an allowed keyword, so
      // only the anywhere-scan for data-modifying keywords catches it.
      expect(
        findUnsafeSqlReason(
          'WITH x AS (SELECT 3 AS a) INSERT INTO t SELECT a FROM x'
        )
      ).to.match(/Data-modifying keyword 'INSERT'/);
    });
  });

  describe('edge cases', () => {
    it('rejects empty input', () => {
      expect(findUnsafeSqlReason('')).to.equal('Query is empty.');
    });

    it('rejects whitespace-only input', () => {
      expect(findUnsafeSqlReason('   \n  ')).to.equal('Query is empty.');
    });

    it('rejects a comment-only query', () => {
      expect(findUnsafeSqlReason('-- nothing here')).to.equal(
        'Query is empty.'
      );
    });

    it('ignores empty statements from repeated semicolons', () => {
      expect(findUnsafeSqlReason('SELECT 1;; ;')).to.equal(null);
    });

    it('names the offending statement so callers can correct it', () => {
      expect(findUnsafeSqlReason('VACUUM')).to.include("got 'VACUUM'");
    });
  });
});
