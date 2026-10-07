/**
 * Messages a background job reports (errors, files it wrote), kept for a
 * person reading its progress: the job counts every one, and keeps the text
 * of the first JOB_MESSAGES_KEPT. A job over a full disk or a multi-year
 * range otherwise grows its list by one entry per file or date, to tens of
 * MB, and every progress poll serialises all of it again (#143).
 */
export const JOB_MESSAGES_KEPT = 100;

/** Keep `message` if the list has room; the caller counts it either way. */
export function keepMessage(list: string[], message: string): void {
  if (list.length < JOB_MESSAGES_KEPT) list.push(message);
}
