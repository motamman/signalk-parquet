/**
 * Generates a unique identifier for a background job.
 *
 * WHY a shared helper: job-id construction was duplicated across many routes
 * with inconsistent randomness (the deprecated String.prototype.substr vs
 * substring, and differing suffix lengths). Centralizing it guarantees one
 * format and removes the deprecated call.
 *
 * Format: `${prefix}_${epochMillis}_${base36-random}`. The timestamp keeps ids
 * roughly sortable; the random suffix avoids collisions within the same
 * millisecond.
 *
 * Input:  newJobId('val')  ->  "val_1718900000000_k3f9a1b2c"
 */
export function newJobId(prefix: string): string {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).substring(2, 11)}`;
}
