/**
 * Detaching a Signal K streambundle subscription.
 *
 * `app.streambundle.getBus(path).onValue(handler)` returns Bacon.js's
 * unsubscribe **function**, not an object with an `unsubscribe` method. Code
 * that stores the handle as `{ unsubscribe?: () => void }` and calls
 * `handle.unsubscribe?.()` therefore detaches nothing, silently: the optional
 * call sees `undefined` and does nothing, the listener stays attached, and each
 * plugin restart adds another set that runs on every matching delta for the
 * lifetime of the process.
 *
 * The server's own type for a bus is loose enough that TypeScript does not
 * catch it, so this exists to be the single place that knows the shape. Both
 * forms are accepted because the stream a bus returns has varied across server
 * versions and because tests supply their own doubles.
 */

/** A handle `onValue` may hand back: the unsubscribe function, or an object. */
export type StreamSubscription =
  | (() => void)
  | {
      unsubscribe?: () => void;
      dispose?: () => void;
      end?: () => void;
      off?: () => void;
    };

/**
 * Detach one subscription, whatever shape its handle takes. Never throws: a
 * handle that is already detached, or that is not a handle at all, is ignored,
 * because every caller is in a `stop()` path where the next teardown step
 * matters more than this one failing.
 */
export function disposeStreamSubscription(subscription: unknown): void {
  try {
    if (typeof subscription === 'function') {
      (subscription as () => void)();
      return;
    }
    if (!subscription || typeof subscription !== 'object') return;
    const candidate = subscription as {
      unsubscribe?: () => void;
      dispose?: () => void;
      end?: () => void;
      off?: () => void;
    };
    for (const method of ['unsubscribe', 'dispose', 'end', 'off'] as const) {
      if (typeof candidate[method] === 'function') {
        candidate[method]!();
        return;
      }
    }
  } catch {
    // Best effort: teardown continues regardless.
  }
}
