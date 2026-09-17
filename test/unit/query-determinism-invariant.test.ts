/**
 * Static invariants for two defects the Phase 3a byte-compare surfaced on
 * 2026-09-17 (see other.md):
 *
 * 1. A time bound built with js-joda's Instant.toString() drops the
 *    milliseconds when they are zero, and stored timestamps carry them, so
 *    the string comparison in the SQL shifts every window by up to a second
 *    at each edge ('…00:00:00.000Z' < '…00:00:00Z'). Bounds are formatted by
 *    utils/iso-time.ts with fixed milliseconds, which sorts correctly against
 *    both millisecond and second-precision rows.
 *
 * 2. DuckDB's FIRST(x) and LAST(x) without ORDER BY are "some row of the
 *    group", and which row changes between runs of the same query (measured:
 *    11 of 281 buckets). Every FIRST/LAST is built by utils/aggregate-sql.ts
 *    with an ORDER BY on the row timestamp.
 */
import { expect } from 'chai';
import { findAll, listSourceFiles } from './source-scan';

const RULES: Array<{
  name: string;
  why: string;
  pattern: RegExp;
  allowedIn: string[];
}> = [
  {
    name: 'a toInstant().toString() time bound',
    why: 'Zero milliseconds are dropped and the SQL string comparison shifts the window by up to a second at each edge. Use isoBound() from utils/iso-time.ts.',
    pattern: /\.toInstant\(\)\.toString\(\)/,
    allowedIn: ['utils/iso-time.ts'],
  },
  {
    name: 'a bare FIRST( or LAST( aggregate',
    why: 'Without ORDER BY these pick an arbitrary row of the bucket and the answer changes between runs. Use firstSql()/lastSql() from utils/aggregate-sql.ts.',
    // Both the spelled-out call and the function-name literal that
    // getAggregateFunction() helpers return for interpolation.
    pattern: /\b(FIRST|LAST)\s*\(|'(FIRST|LAST)'/,
    allowedIn: ['utils/aggregate-sql.ts'],
  },
];

describe('query determinism invariant', () => {
  for (const rule of RULES) {
    it(`no source file outside [${rule.allowedIn.join(', ')}] contains ${rule.name}`, () => {
      const violations = findAll(listSourceFiles(rule.allowedIn), rule.pattern);
      expect(
        violations,
        `${rule.why}\nFound ${rule.name} at:\n  ${violations.join('\n  ')}`
      ).to.deep.equal([]);
    });
  }
});
