/**
 * How long this plugin's host server is blocked, measured rather than inferred.
 *
 * A plugin that reads SQLite and drives DuckDB on the server's only thread can
 * stop the server serving anything, and until now the only evidence available
 * was client-side: time a request and guess how much of it was blocking. That
 * conflates the work with the queueing, and it cannot tell a slow answer from a
 * frozen server. Two of the readings taken while building this were wrong for
 * exactly that reason.
 *
 * `perf_hooks.monitorEventLoopDelay` is the direct measurement: libuv records
 * how late each loop iteration actually was, in a histogram maintained outside
 * JavaScript, so nothing is sampled or missed and the cost is negligible. The
 * histogram covers the whole process — every plugin and the server itself — and
 * that is the right scope, because "the server stalled" is the complaint.
 *
 * Read it around an operation to attribute blocking to that operation: reset,
 * do the thing, snapshot. Both ends wait for the sampling timer, so the
 * sequence holds even when the thing is synchronous.
 */

import { monitorEventLoopDelay, IntervalHistogram } from 'perf_hooks';

/**
 * Loop delay in milliseconds, as libuv records it: each figure *includes* the
 * sampling interval, so an unblocked loop reads about `resolutionMs` and a real
 * block is roughly the figure minus that. Measured locally (2026-09-26), a
 * deliberate 150 ms block recorded 159 ms with a 10 ms resolution.
 *
 * `max` is therefore the longest single freeze, plus about one interval.
 */
export interface EventLoopDelay {
  /** Sampling resolution, so a reader knows the floor on these numbers. */
  resolutionMs: number;
  /** Seconds since the last reset, for rates. */
  sinceResetSec: number;
  min: number;
  mean: number;
  p50: number;
  p90: number;
  p99: number;
  max: number;
}

/**
 * 10 ms: the histogram's own timer fires this often, so it is the floor on what
 * can be seen. Finer costs a timer wake-up more often for detail that is below
 * the noise this server already makes (an idle client's request varies by more
 * than 10 ms), and these numbers exist to find blocks of tens to hundreds of
 * milliseconds.
 */
const RESOLUTION_MS = 10;

let histogram: IntervalHistogram | undefined;
let resetAt = Date.now();

/** Begin recording. Idempotent; a second call does not restart the histogram. */
export function startEventLoopMonitor(): void {
  if (histogram) return;
  histogram = monitorEventLoopDelay({ resolution: RESOLUTION_MS });
  histogram.enable();
  resetAt = Date.now();
}

/** Stop recording and discard. */
export function stopEventLoopMonitor(): void {
  if (!histogram) return;
  histogram.disable();
  histogram = undefined;
}

/** Whether recording is on, so a caller can say "unavailable" rather than zero. */
export function eventLoopMonitorRunning(): boolean {
  return histogram !== undefined;
}

/**
 * The delay seen since the last reset, or null when not recording.
 *
 * Resolves only after the histogram has taken one more sample, because a block
 * is recorded when the sampling timer next fires, not when the block ends: a
 * caller that resets, does synchronous work and reads in the same loop turn
 * would otherwise get a snapshot the work is not in yet. Waiting for the
 * sample is what makes the reset-work-read sequence mean what it says.
 *
 * libuv reports nanoseconds; these are milliseconds, which is the unit every
 * other latency in this plugin is quoted in.
 */
export async function eventLoopDelay(): Promise<EventLoopDelay | null> {
  const h = histogram;
  if (!h) return null;
  // The timer fires every RESOLUTION_MS while the histogram is enabled, so
  // this ends on the next sample; it also ends if the monitor is stopped.
  // A reset while waiting empties the histogram, which changes the count
  // without adding a sample: an empty histogram reports a min of 2^63 ns, so
  // the wait goes on until there is one.
  const before = h.count;
  while (histogram === h && (h.count === before || h.count === 0)) {
    await new Promise(resolve => setTimeout(resolve, RESOLUTION_MS));
  }
  if (histogram !== h) return null;
  const ms = (ns: number) => (Number.isFinite(ns) ? ns / 1e6 : 0);
  return {
    resolutionMs: RESOLUTION_MS,
    sinceResetSec: (Date.now() - resetAt) / 1000,
    min: ms(h.min),
    mean: ms(h.mean),
    p50: ms(h.percentile(50)),
    p90: ms(h.percentile(90)),
    p99: ms(h.percentile(99)),
    max: ms(h.max),
  };
}

/**
 * Forget everything recorded so far, so the next snapshot covers only what
 * happens next. This is what makes the monitor usable as a measurement around
 * one operation rather than only as a running health figure.
 *
 * Resolves after one sampling interval, and callers must await it, because a
 * block that begins in the same loop turn as the reset is not recorded:
 * measured locally (2026-09-26), a 150 ms block registered as 159 ms when it
 * followed no reset and as 12 ms when it followed one immediately, but as
 * 155 ms when one interval was allowed to pass in between. Awaiting here means
 * a caller cannot get that wrong; the alternative was to document the trap and
 * leave it in the API.
 */
export async function resetEventLoopDelay(): Promise<void> {
  if (!histogram) return;
  histogram.reset();
  resetAt = Date.now();
  // Settle *after* clearing, and do not clear again: a second reset here would
  // put the caller's work in the same loop turn as a reset, which is the case
  // that goes unrecorded.
  await new Promise(resolve => setTimeout(resolve, RESOLUTION_MS * 2));
}
