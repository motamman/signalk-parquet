/**
 * Which retention stamp a row gets, resolved once per row at insert.
 *
 * A path is either **full** (buffered, then written to Parquet and the cloud,
 * the default and the behaviour of every existing config) or **buffer-only**
 * (kept in the SQLite buffer for the retention window, queryable through the
 * History API, never written to Parquet).
 *
 * `fullWhileRegimen` promotes a buffer-only path for as long as a regimen is
 * active, so a path can be buffered continuously and exported only during a
 * passage. This works because the stamp lives on the row rather than on the
 * path: the same path can write buffer-only rows in the morning and tracked
 * rows in the afternoon, from one subscription.
 *
 * Nothing re-stamps a row afterwards. Changing a path's mode affects only
 * rows written from that point on, so promoting a path starts its Parquet
 * history at the moment of promotion, with a gap before it.
 */

import { PathConfig } from '../types';
import { EXPORTED_BUFFER_ONLY, EXPORTED_PENDING } from './sqlite-buffer';

/** The retention modes a path may be configured with. */
export type RetentionMode = 'buffer' | 'full';

/** The mode a path is configured with; absent means `full`. */
export function retentionModeOf(
  pathConfig: Pick<PathConfig, 'retention'>
): RetentionMode {
  return pathConfig.retention === 'buffer' ? 'buffer' : 'full';
}

/**
 * The `exported` stamp for a row of this path, given the regimens active at
 * this instant. `EXPORTED_PENDING` means the row owes a Parquet write;
 * `EXPORTED_BUFFER_ONLY` means it never will be written and ages out on the
 * retention clock.
 */
export function resolveRetentionStamp(
  pathConfig: Pick<PathConfig, 'retention' | 'fullWhileRegimen'>,
  activeRegimens: ReadonlySet<string> | undefined
): number {
  if (retentionModeOf(pathConfig) === 'full') {
    return EXPORTED_PENDING;
  }
  const promoteWhile = pathConfig.fullWhileRegimen?.trim();
  if (promoteWhile && activeRegimens?.has(promoteWhile)) {
    return EXPORTED_PENDING;
  }
  return EXPORTED_BUFFER_ONLY;
}
