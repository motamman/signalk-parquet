/**
 * SQL fragments for History API aggregate methods that the V1 routes
 * (HistoryAPI.ts) and the v2 provider (history-provider.ts) must compute
 * identically.
 */

/**
 * `middle_index`: the value of the chronologically middle sample in a bucket.
 * With n samples ordered by `timestampColumn` it takes the 1-based position
 * (n + 1) / 2 using integer division, so an even count yields the first of the
 * two middle samples.
 *
 * Example (one bucket, values listed in timestamp order):
 *   [10, 20, 30, 40, 50] -> 30 (position 3 of 5)
 *   [10, 20, 30, 40]     -> 20 (position 2 of 4)
 *
 * As for every other method, callers drop rows that carry no value before
 * aggregating, so the middle is taken among the recorded samples. Within the
 * rows that remain, DuckDB's list() keeps NULLs, so the position refers to the
 * same row for every column aggregated in one GROUP BY: the components of an
 * object path (latitude, longitude) come from the same sample. The explicit
 * ORDER BY makes the pick independent of the order files happen to be scanned
 * in.
 */
export function middleIndexSql(
  valueExpr: string,
  timestampColumn: string
): string {
  return `list(${valueExpr} ORDER BY ${timestampColumn})[(count(*) + 1) // 2]`;
}

/**
 * `first` / `last`: the value of the chronologically first (last) sample in a
 * bucket. DuckDB's FIRST(x) and LAST(x) without ORDER BY are "some row of the
 * group", and which row changes between runs of the same query under a
 * parallel scan (measured on a shore station, 2026-09-17: 11 of 281 buckets
 * changed between two runs). The ORDER BY makes the pick the earliest
 * (latest) sample, which is what the method names promise.
 */
export function firstSql(valueExpr: string, timestampColumn: string): string {
  return `FIRST(${valueExpr} ORDER BY ${timestampColumn})`;
}

export function lastSql(valueExpr: string, timestampColumn: string): string {
  return `LAST(${valueExpr} ORDER BY ${timestampColumn})`;
}
