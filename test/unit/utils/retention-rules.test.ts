/**
 * Unit tests for per-path retention rules: glob compilation, specificity
 * resolution, retention-day fallbacks, aggregation skipping, and config
 * validation. These rules decide which raw data is deleted and which paths
 * never reach the aggregation tiers, so mismatches are destructive.
 */
import { expect } from 'chai';
import {
  PathRetentionRule,
  RetentionRuleSet,
  validatePathRetentionRules,
} from '../../../src/utils/retention-rules';

const rule = (
  pattern: string,
  days = 1,
  skipAggregation?: boolean
): PathRetentionRule => ({ pattern, days, skipAggregation });

describe('RetentionRuleSet.match', () => {
  it('matches a literal pattern exactly', () => {
    const rules = new RetentionRuleSet([rule('navigation.position')]);
    expect(rules.match('navigation.position')).to.not.equal(null);
    expect(rules.match('navigation.position2')).to.equal(null);
  });

  it('treats dots as literal characters', () => {
    const rules = new RetentionRuleSet([rule('a.b')]);
    expect(rules.match('aXb')).to.equal(null);
  });

  it('matches * across dots', () => {
    const rules = new RetentionRuleSet([rule('environment.wind.*')]);
    expect(rules.match('environment.wind.speedTrue')).to.not.equal(null);
    expect(rules.match('environment.wind.deep.nested')).to.not.equal(null);
    expect(rules.match('environment.water.temp')).to.equal(null);
  });

  it('supports wildcards in the middle of a pattern', () => {
    const rules = new RetentionRuleSet([rule('environment.*.depth')]);
    expect(rules.match('environment.water.depth')).to.not.equal(null);
    expect(rules.match('environment.depth')).to.equal(null);
  });

  it('matches everything with a bare *', () => {
    const rules = new RetentionRuleSet([rule('*')]);
    expect(rules.match('anything.at.all')).to.not.equal(null);
    expect(rules.match('')).to.not.equal(null);
  });

  it('returns null when no rule matches', () => {
    const rules = new RetentionRuleSet([rule('navigation.*')]);
    expect(rules.match('environment.depth')).to.equal(null);
  });

  it('returns null from an empty rule set', () => {
    const rules = new RetentionRuleSet();
    expect(rules.match('navigation.position')).to.equal(null);
    expect(rules.isEmpty()).to.equal(true);
  });

  it('collapses runs of * and still matches', () => {
    // Input pattern '*****wind*****' compiles to the same matcher as
    // '*wind*'; the collapse also prevents pathological backtracking.
    const rules = new RetentionRuleSet([rule('*****wind*****')]);
    expect(rules.match('environment.wind.speedTrue')).to.not.equal(null);
    expect(rules.match('environment.water.temp')).to.equal(null);
  });

  it('collapses a long run of consecutive * so matching stays linear', () => {
    // The realistic ReDoS vector is a hand-edited typo with a long run of
    // consecutive stars (e.g. `environment.****...****foo`). Collapsing the
    // run to a single `*` (a single `.*`) keeps matching against long
    // non-matching input fast. Note the collapse only merges *consecutive*
    // stars: alternating patterns like `*a*a*b` are NOT defended and remain
    // a latent ReDoS vector, but those do not occur in practice.
    // No wall-clock assertion: an un-collapsed pattern degrades to
    // catastrophic backtracking, which blocks the event loop and trips the
    // mocha timeout, so a regression still fails the test without a flaky
    // perf threshold.
    const rules = new RetentionRuleSet([rule('*'.repeat(50) + 'foo')]);
    const longInput = 'a'.repeat(100_000);
    expect(rules.match(longInput)).to.equal(null);
  });

  describe('specificity resolution', () => {
    it('prefers the rule with more literal characters', () => {
      const general = rule('environment.wind.*', 7);
      const specific = rule('environment.wind.speedApparent', 1);
      const rules = new RetentionRuleSet([general, specific]);
      expect(rules.match('environment.wind.speedApparent')).to.equal(specific);
      expect(rules.match('environment.wind.speedTrue')).to.equal(general);
    });

    it('breaks specificity ties by declaration order', () => {
      const first = rule('a.*', 1);
      const second = rule('*.b', 2);
      const rules = new RetentionRuleSet([first, second]);
      expect(rules.match('a.b')).to.equal(first);
    });
  });
});

describe('RetentionRuleSet.resolveRetentionDays', () => {
  const rules = new RetentionRuleSet([
    rule('environment.wind.*', 1),
    rule('navigation.position', 0),
  ]);

  it('returns the matched rule days', () => {
    expect(
      rules.resolveRetentionDays('environment.wind.speedTrue', 7)
    ).to.equal(1);
  });

  it('falls back to the global default when nothing matches', () => {
    expect(rules.resolveRetentionDays('environment.depth', 7)).to.equal(7);
  });

  it('maps a matched 0 to null (keep forever), overriding the global', () => {
    expect(rules.resolveRetentionDays('navigation.position', 7)).to.equal(null);
  });

  it('maps a global 0 to null when nothing matches', () => {
    expect(rules.resolveRetentionDays('environment.depth', 0)).to.equal(null);
  });
});

describe('RetentionRuleSet.shouldSkipAggregation', () => {
  it('is true only when the matched rule sets the flag', () => {
    const rules = new RetentionRuleSet([
      rule('environment.wind.*', 1, true),
      rule('navigation.*', 7),
    ]);
    expect(rules.shouldSkipAggregation('environment.wind.speedTrue')).to.equal(
      true
    );
    expect(rules.shouldSkipAggregation('navigation.position')).to.equal(false);
    expect(rules.shouldSkipAggregation('unmatched.path')).to.equal(false);
  });

  it('uses the winning rule when several match', () => {
    const rules = new RetentionRuleSet([
      rule('environment.*', 7, true),
      rule('environment.wind.speedTrue', 1),
    ]);
    expect(rules.shouldSkipAggregation('environment.wind.speedTrue')).to.equal(
      false
    );
  });
});

describe('validatePathRetentionRules', () => {
  it('accepts undefined and null as empty rule lists', () => {
    expect(validatePathRetentionRules(undefined)).to.deep.equal({
      rules: [],
      errors: [],
    });
    expect(validatePathRetentionRules(null)).to.deep.equal({
      rules: [],
      errors: [],
    });
  });

  it('rejects non-array input', () => {
    expect(validatePathRetentionRules('nope')).to.deep.equal({
      rules: [],
      errors: ['pathRetentionOverrides must be an array'],
    });
  });

  it('accepts a valid rule and normalizes skipAggregation false to undefined', () => {
    const result = validatePathRetentionRules([
      { pattern: 'a.*', days: 3, skipAggregation: false },
    ]);
    expect(result.errors).to.deep.equal([]);
    expect(result.rules).to.deep.equal([
      { pattern: 'a.*', days: 3, skipAggregation: undefined },
    ]);
  });

  it('keeps skipAggregation true', () => {
    const result = validatePathRetentionRules([
      { pattern: 'a.*', days: 3, skipAggregation: true },
    ]);
    expect(result.rules[0].skipAggregation).to.equal(true);
  });

  it('keeps valid entries while reporting invalid ones with their index', () => {
    const result = validatePathRetentionRules([
      { pattern: 'a.*', days: 1 },
      'garbage',
      { pattern: '', days: 1 },
      { pattern: 'b.*', days: -1 },
      { pattern: 'c.*', days: 1.5 },
      { pattern: 'd.*', days: NaN },
      { pattern: 'e.*', days: '7' },
      { pattern: 'f.*', days: 1, skipAggregation: 'yes' },
      { pattern: 'g.*', days: 2 },
    ]);
    expect(result.rules.map(r => r.pattern)).to.deep.equal(['a.*', 'g.*']);
    expect(result.errors).to.deep.equal([
      'pathRetentionOverrides[1] must be an object',
      'pathRetentionOverrides[2].pattern must be a non-empty string',
      'pathRetentionOverrides[3].days must be a non-negative integer',
      'pathRetentionOverrides[4].days must be a non-negative integer',
      'pathRetentionOverrides[5].days must be a non-negative integer',
      'pathRetentionOverrides[6].days must be a non-negative integer',
      'pathRetentionOverrides[7].skipAggregation must be a boolean',
    ]);
  });

  it('accepts days 0 as keep-forever', () => {
    const result = validatePathRetentionRules([{ pattern: 'a.*', days: 0 }]);
    expect(result.errors).to.deep.equal([]);
    expect(result.rules[0].days).to.equal(0);
  });
});
