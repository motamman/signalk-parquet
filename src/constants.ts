/**
 * Central tunable constants.
 *
 * Single home for hardcoded numeric tunables so they're easy to find and
 * later migrate to plugin config. When adding a new magic number, put it
 * here with a short comment on what it controls.
 */

/**
 * Maximum plausible implied speed between consecutive GPS fixes, in m/s.
 * Used by position aggregation to reject single-point GPS glitches: if the
 * implied speed from a candidate's temporal neighbor exceeds this, the
 * candidate is treated as an outlier.
 *
 * 25 m/s ≈ 48.6 kn — well above any sailing/power vessel's realistic top speed.
 */
export const POSITION_MAX_SPEED_MPS = 25;
