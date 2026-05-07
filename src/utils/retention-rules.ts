/**
 * Per-path retention rules.
 *
 * The plugin keeps a single global `retentionDays` value (0 = keep
 * forever). On top of that, operators can declare a list of overrides:
 *
 *   [{ pattern: 'environment.wind.*', days: 1, skipAggregation: true }]
 *
 * meaning: any SignalK path matching `environment.wind.*` is kept for
 * one day in tier=raw and is NOT aggregated into 5s/60s/1h tiers.
 *
 * Pattern syntax: glob with `*` matching zero-or-more characters
 * (including dots — `environment.wind.*` covers `environment.wind.deep.x`
 * too). Other characters are literal.
 *
 * Resolution: when more than one rule matches a path, the rule with the
 * most non-wildcard characters wins (rough proxy for "most specific").
 * Ties are broken in declaration order.
 */
export interface PathRetentionRule {
  // Glob pattern over the SignalK path. `*` matches any chars, including
  // dots. Examples: `environment.wind.*`, `environment.*.depth`,
  // `navigation.position`.
  pattern: string;

  // Days to keep; 0 means keep forever (the same convention as the
  // global retentionDays setting).
  days: number;

  // When true, the live aggregation pipeline will not roll this path up
  // into 5s/60s/1h tiers — useful for high-volume paths where a short
  // retention is the only sensible policy.
  skipAggregation?: boolean;
}

interface CompiledRule {
  rule: PathRetentionRule;
  regex: RegExp;
  specificity: number;
  declarationOrder: number;
}

/**
 * Glob-to-regex with the simple `*` semantics described above.
 *
 * Multiple consecutive `*` characters are collapsed to a single `*`
 * before compilation. This both keeps the regex tidy and shuts the
 * door on ReDoS via patterns like `*****foo*****` — without the
 * collapse, runs of `.*` against a long non-matching input cause
 * catastrophic backtracking.
 */
function compilePattern(pattern: string): RegExp {
  const collapsed = pattern.replace(/\*+/g, '*');
  // Escape every regex special character except `*`, then turn `*` into
  // `.*`. Anchor full-string.
  const escaped = collapsed.replace(/[.+?^${}()|[\]\\]/g, '\\$&');
  const regex = escaped.replace(/\*/g, '.*');
  return new RegExp(`^${regex}$`);
}

/**
 * Specificity is the count of literal (non-`*`) characters in the
 * pattern. `environment.wind.speedApparent` (29 literal chars) beats
 * `environment.wind.*` (16 literal chars) which beats `*` (0 literal
 * chars). It's a rough heuristic, but it does the right thing for the
 * shapes operators actually write.
 */
function computeSpecificity(pattern: string): number {
  let n = 0;
  for (const ch of pattern) if (ch !== '*') n++;
  return n;
}

export class RetentionRuleSet {
  private readonly rules: CompiledRule[];

  /**
   * Construct with a list of rules. Per-rule compile failures (from a
   * malformed pattern in a hand-edited config) are not fatal — the bad
   * rule is dropped and `onCompileError` is invoked so the caller can
   * surface it. This keeps a single typo from poisoning plugin start.
   */
  constructor(
    rules: PathRetentionRule[] = [],
    onCompileError?: (rule: PathRetentionRule, error: Error) => void
  ) {
    const compiled: CompiledRule[] = [];
    rules.forEach((rule, i) => {
      try {
        compiled.push({
          rule,
          regex: compilePattern(rule.pattern),
          specificity: computeSpecificity(rule.pattern),
          declarationOrder: i,
        });
      } catch (err) {
        if (onCompileError) onCompileError(rule, err as Error);
      }
    });
    this.rules = compiled;
  }

  /**
   * Resolve the matching rule for a given SignalK path, or null if none
   * matches. The caller decides whether to fall back to the global
   * retention default.
   */
  match(signalkPath: string): PathRetentionRule | null {
    let best: CompiledRule | null = null;
    for (const c of this.rules) {
      if (!c.regex.test(signalkPath)) continue;
      if (
        best === null ||
        c.specificity > best.specificity ||
        (c.specificity === best.specificity &&
          c.declarationOrder < best.declarationOrder)
      ) {
        best = c;
      }
    }
    return best ? best.rule : null;
  }

  /**
   * Resolve effective retention for a path. Falls back to the global
   * default when no rule matches. `null` means "keep forever".
   */
  resolveRetentionDays(
    signalkPath: string,
    globalDefaultDays: number
  ): number | null {
    const matched = this.match(signalkPath);
    const days = matched ? matched.days : globalDefaultDays;
    return days > 0 ? days : null;
  }

  /**
   * True if aggregation should skip this path (because a matching rule
   * has skipAggregation set).
   */
  shouldSkipAggregation(signalkPath: string): boolean {
    return this.match(signalkPath)?.skipAggregation === true;
  }

  isEmpty(): boolean {
    return this.rules.length === 0;
  }
}

/**
 * Validate a list of rules. Always returns the valid rules plus any
 * per-entry errors so callers can drop only the bad entries instead of
 * all of them. Omitted input (undefined / null) is valid and yields an
 * empty rule set; a present-but-non-array input returns `rules: []` with
 * one error.
 */
export function validatePathRetentionRules(rules: unknown): {
  rules: PathRetentionRule[];
  errors: string[];
} {
  if (rules === undefined || rules === null) {
    return { rules: [], errors: [] };
  }
  if (!Array.isArray(rules)) {
    return { rules: [], errors: ['pathRetentionOverrides must be an array'] };
  }
  const errors: string[] = [];
  const out: PathRetentionRule[] = [];
  rules.forEach((entry, i) => {
    if (typeof entry !== 'object' || entry === null) {
      errors.push(`pathRetentionOverrides[${i}] must be an object`);
      return;
    }
    const r = entry as Record<string, unknown>;
    if (typeof r.pattern !== 'string' || r.pattern.length === 0) {
      errors.push(
        `pathRetentionOverrides[${i}].pattern must be a non-empty string`
      );
      return;
    }
    if (
      typeof r.days !== 'number' ||
      !Number.isFinite(r.days) ||
      !Number.isInteger(r.days) ||
      r.days < 0
    ) {
      errors.push(
        `pathRetentionOverrides[${i}].days must be a non-negative integer`
      );
      return;
    }
    if (
      r.skipAggregation !== undefined &&
      typeof r.skipAggregation !== 'boolean'
    ) {
      errors.push(
        `pathRetentionOverrides[${i}].skipAggregation must be a boolean`
      );
      return;
    }
    out.push({
      pattern: r.pattern,
      days: r.days,
      skipAggregation: r.skipAggregation === true ? true : undefined,
    });
  });
  return { rules: out, errors };
}
