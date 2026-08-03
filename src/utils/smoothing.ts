/**
 * Moving-average smoothing for the History API `sma` / `ema` aggregate
 * methods (SignalK v2 History API spec).
 *
 * These run as TypeScript post-processing over the already-bucketed,
 * time-ordered series a provider produces — EMA is recursive and doesn't map
 * to a SQL window function cleanly, and keeping both here keeps the math
 * unit-testable and independent of the query layer.
 *
 * Two flavours:
 *  - **linear** — plain arithmetic mean (SMA) / exponential average (EMA).
 *  - **circular** — the same, but computed in sin/cos space and recombined
 *    with `atan2`, so a series that wraps (compass angles, longitude) is not
 *    averaged across the discontinuity into garbage (e.g. 179° and −179°
 *    smooth toward ±180°, not 0°). `*Rad` treats inputs as radians (SignalK
 *    angular scalars are radians); `smoothCircularDeg` wraps that for degree
 *    inputs (longitude).
 *
 * Defaults follow the spec's doc comment: sma → 5 samples, ema → alpha 0.2.
 */

export type SmoothMethod = 'sma' | 'ema';

/** SMA sample count from the PathSpec `parameter` array; default 5. */
export function parseSmaWindow(parameter?: string[]): number {
  const n = Number(parameter?.[0]);
  return Number.isInteger(n) && n >= 1 ? n : 5;
}

/** EMA alpha from the PathSpec `parameter` array; default 0.2, bounded (0, 1]. */
export function parseEmaAlpha(parameter?: string[]): number {
  const a = Number(parameter?.[0]);
  return Number.isFinite(a) && a > 0 && a <= 1 ? a : 0.2;
}

/**
 * Trailing simple moving average: `out[i]` is the mean of up to the last
 * `window` values ending at `i` (fewer than `window` near the start). `window`
 * is clamped to an integer >= 1. Matches the v1 rolling-window behaviour.
 */
export function sma(values: number[], window: number): number[] {
  const n = Math.max(1, Math.trunc(window));
  const out: number[] = [];
  let sum = 0;
  for (let i = 0; i < values.length; i++) {
    sum += values[i];
    if (i >= n) sum -= values[i - n];
    out.push(sum / Math.min(i + 1, n));
  }
  return out;
}

/**
 * Exponential moving average: `out[0] = values[0]`,
 * `out[i] = alpha*values[i] + (1-alpha)*out[i-1]`. `alpha` is clamped to (0, 1].
 */
export function ema(values: number[], alpha: number): number[] {
  const a = Math.min(1, Math.max(Number.EPSILON, alpha));
  const out: number[] = [];
  let prev = 0;
  for (let i = 0; i < values.length; i++) {
    prev = i === 0 ? values[i] : a * values[i] + (1 - a) * prev;
    out.push(prev);
  }
  return out;
}

/** Circular SMA over radians, via sin/cos component means recombined with atan2. */
export function smaCircularRad(values: number[], window: number): number[] {
  const s = sma(values.map(Math.sin), window);
  const c = sma(values.map(Math.cos), window);
  return s.map((si, i) => Math.atan2(si, c[i]));
}

/** Circular EMA over radians, via sin/cos exponential averages recombined with atan2. */
export function emaCircularRad(values: number[], alpha: number): number[] {
  const s = ema(values.map(Math.sin), alpha);
  const c = ema(values.map(Math.cos), alpha);
  return s.map((si, i) => Math.atan2(si, c[i]));
}

/** Dispatch sma/ema over a linear series, parsing the parameter with defaults. */
export function smoothLinear(
  values: number[],
  method: SmoothMethod,
  parameter?: string[]
): number[] {
  return method === 'ema'
    ? ema(values, parseEmaAlpha(parameter))
    : sma(values, parseSmaWindow(parameter));
}

/** Dispatch sma/ema over a circular series in **radians**. */
export function smoothCircularRad(
  values: number[],
  method: SmoothMethod,
  parameter?: string[]
): number[] {
  return method === 'ema'
    ? emaCircularRad(values, parseEmaAlpha(parameter))
    : smaCircularRad(values, parseSmaWindow(parameter));
}

const DEG = Math.PI / 180;

/**
 * Dispatch sma/ema over a circular series in **degrees** (e.g. longitude).
 * Converts to radians, smooths on the circle, converts back — so the ±180°
 * antimeridian is handled instead of linear-averaging 179°/−179° to 0°. The
 * atan2 result lands in (−180°, 180°], the canonical longitude range.
 */
export function smoothCircularDeg(
  values: number[],
  method: SmoothMethod,
  parameter?: string[]
): number[] {
  const rad = smoothCircularRad(
    values.map(v => v * DEG),
    method,
    parameter
  );
  return rad.map(v => v / DEG);
}
