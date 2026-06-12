/**
 * Helpers for safely embedding string literals in the SQL we build by
 * interpolation for DuckDB. The query builders in this plugin assemble SQL as
 * strings rather than using bound parameters, so any externally-supplied value
 * (context, source reference, timestamps) must be escaped before it is spliced
 * into a quoted literal.
 */

/**
 * Escape a string for inclusion inside a single-quoted SQL literal by doubling
 * embedded single quotes. This is the standard SQL escaping for string
 * literals and prevents the value from terminating the literal early.
 *
 * Input:  O'Brien        Output: O''Brien
 * Usage:  `WHERE label = '${escapeSqlString(value)}'`
 */
export function escapeSqlString(value: string): string {
  return value.replace(/'/g, "''");
}
