/**
 * Time bounds for the string comparisons the queries make against stored
 * `signalk_timestamp` values.
 *
 * Stored timestamps are ISO strings with milliseconds ('…T00:00:00.000Z').
 * js-joda's Instant.toString() drops the milliseconds when they are zero
 * ('…T00:00:00Z'), and as strings '…00.000Z' < '…00Z', so a bound built that
 * way shifted every window by up to a second at each edge: rows in the first
 * second of `from` fell out, rows in the first second of `to` fell in
 * (measured on a shore station, 2026-09-17: one row stamped exactly
 * 2026-09-16T00:00:00.000Z admitted into a window ending there).
 *
 * A bound with fixed milliseconds sorts correctly against both millisecond
 * and second-precision rows: for a legacy '…T00:00:00Z' row, 'Z' > '.', so
 * it sits above '…T00:00:00.000Z', which is where it belongs.
 */

import { Instant, ZonedDateTime } from '@js-joda/core';

/** The SQL/string-comparison form of an instant: ISO 8601 UTC, always `.sssZ`. */
export function isoBound(t: ZonedDateTime | Instant | Date): string {
  const ms =
    t instanceof Date
      ? t.getTime()
      : t instanceof ZonedDateTime
        ? t.toInstant().toEpochMilli()
        : t.toEpochMilli();
  return new Date(ms).toISOString();
}

/**
 * The display form js-joda gives an instant (no milliseconds when zero), for
 * response fields that have always carried it. Never use this as a bound.
 */
export function isoInstant(t: ZonedDateTime | Instant): string {
  return (t instanceof ZonedDateTime ? t.toInstant() : t).toString();
}
