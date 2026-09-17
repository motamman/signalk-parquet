/**
 * Static invariant: there is one way to read parquet.
 *
 * Every read site that builds its own glob, and every metadata question asked
 * through DuckDB, opens more of the archive than the request needs, and the
 * memory DuckDB allocates for that work is retained by the process (measured
 * on brain, 2026-09-16/17: a 7-day contexts lookup retained ~1.4 GB through
 * DuckDB footers vs ~0 MB through parquetjs; a dashboard P1D query grew the
 * process by 366 MB through a `**` glob and by 1-3 MB once narrowed to the
 * window's days). Read paths must take their file lists from
 * utils/parquet-files.ts and their metadata from utils/parquet-footer.ts
 * (other.md, "Plan: one way to read parquet").
 *
 * A failure's output is the migration worklist for any read site that has
 * strayed, and it is what stops a private glob builder or DuckDB metadata
 * probe being written outside the allowlisted files.
 */
import { expect } from 'chai';
import { findAll, listSourceFiles } from './source-scan';

interface Rule {
  name: string;
  why: string;
  pattern: RegExp;
  /** Files (relative to src/, forward slashes) allowed to contain the pattern. */
  allowedIn: string[];
  /** A match on a source line matching this is not a violation. */
  except?: RegExp;
}

const RULES: Rule[] = [
  {
    name: 'a `**` parquet glob',
    why:
      '`**` descends the whole archive (every year, every day, and the ' +
      'processed/quarantine/failed/repaired directories that then have to be ' +
      'filtered back out). File lists come from parquet-files.ts, narrowed to ' +
      "the window's days.",
    pattern: /\*\*\/\*\.parquet|['"`]\*\*['"`]/,
    // migration-service walks a user-supplied pre-hive source directory to
    // convert it; there is no partition level to prune.
    allowedIn: ['services/migration-service.ts'],
    // The pre-hive flat layout (`vessels/<id>/<path parts>/*.parquet`) has
    // no partition levels for the walker to prune, and the two places that
    // scan it (the schema validation job and the schema-examiner tool) read
    // with parquetjs, not DuckDB. They are the one legitimate `**`.
    except:
      /['"`]vessels['"`],\s*['"`]\*\*['"`]|['"`]vessels\/\*\*\/\*\.parquet['"`]/,
  },
  {
    name: 'a `year=*` or `day=*` wildcard',
    why:
      'A partition wildcard is a glob over the whole history of a path. Only ' +
      'the hive builder and walker may spell partitions.',
    pattern: /\b(year|day)=\*/,
    allowedIn: ['utils/hive-path-builder.ts', 'utils/hive-walk.ts'],
  },
  {
    name: 'a read_parquet( call',
    why:
      'read_parquet is issued from one builder that always passes an explicit ' +
      'file list with hive_partitioning=false and union_by_name=true. The ' +
      'analyzer runs model-authored SQL in its own sandbox and is guarded ' +
      'separately by sql-guard.ts.',
    pattern: /\bread_parquet\s*\(/i,
    allowedIn: [
      'utils/parquet-files.ts',
      'claude-analyzer.ts',
      'utils/sql-guard.ts',
    ],
  },
  {
    name: 'a parquet_metadata( or parquet_schema( call',
    why:
      'Footer questions (context, timestamp span, column types) go through ' +
      'parquet-footer.ts, whose allocations live in the V8 heap and are ' +
      'collected; DuckDB retains them.',
    pattern: /\bparquet_(metadata|schema)\s*\(/i,
    allowedIn: ['utils/sql-guard.ts'],
  },
  {
    name: 'a DESCRIBE SELECT probe',
    why:
      'Column presence and types come from parquet-footer.ts, not from ' +
      'DuckDB opening the files to describe them.',
    pattern: /\bDESCRIBE\s+SELECT\b/i,
    allowedIn: ['utils/sql-guard.ts'],
  },
];

describe('parquet read invariant (one way to read parquet)', () => {
  const allFiles = listSourceFiles();

  it('finds the source tree', () => {
    expect(allFiles.length).to.be.greaterThan(10);
  });

  for (const rule of RULES) {
    it(`no source file outside [${rule.allowedIn.join(', ') || 'none'}] contains ${rule.name}`, () => {
      const files = listSourceFiles(rule.allowedIn);
      const violations = findAll(files, rule.pattern, rule.except);
      expect(
        violations,
        `${rule.why}\nFound ${rule.name} at:\n  ${violations.join('\n  ')}`
      ).to.deep.equal([]);
    });
  }
});
