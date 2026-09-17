/**
 * Profile a Signal K server's delta stream: which paths change regularly,
 * which arrive in bursts, and which barely change at all.
 *
 * The plugin records a configured subset of paths. Deciding what else is
 * worth recording needs three things this reports and the server does not:
 * how often a path actually arrives, whether its value changes when it does,
 * and how many vessels carry it. The last one is the cost driver, because the
 * hive layout writes one file per path per context per day.
 *
 * Usage:
 *   npx tsx tools/path-profile.ts ws://localhost:3000 300
 *   npx tsx tools/path-profile.ts wss://localhost:3443 300 --buffer=~/.signalk/data/buffer.db
 *
 * For a self-signed certificate, prefix with NODE_TLS_REJECT_UNAUTHORIZED=0.
 * The --buffer option marks paths the plugin already records, read from the
 * buffer's own table list; it is opened read-only and never written.
 */

import { DatabaseSync } from 'node:sqlite';

/** How a path behaves over the sample window. */
type Behaviour = 'periodic' | 'irregular' | 'episodic' | 'stable' | 'static' | 'single';

interface PerContext {
  lastSeenMs: number;
  lastValue: string;
  samples: number;
  changes: number;
  intervalSum: number;
  intervalSquares: number;
  intervals: number;
}

interface PathStats {
  path: string;
  contexts: Map<string, PerContext>;
  samples: number;
  changes: number;
  /** Updates after the first sighting per context, the denominator for chg%. */
  intervals: number;
  types: Set<string>;
  sampleValue: string;
}

const args = process.argv.slice(2);
// Options may appear anywhere, so the url and the duration are read from the
// positional arguments: `--buffer=…` in second place is an option, not a
// duration, and must not turn the sample window into NaN.
const positional = args.filter(a => !a.startsWith('--'));
const DEFAULT_SECONDS = 300;
const url = positional[0] ?? 'ws://localhost:3000';
const requestedSeconds = Number(positional[1]);
const seconds =
  Number.isFinite(requestedSeconds) && requestedSeconds > 0
    ? requestedSeconds
    : DEFAULT_SECONDS;
const bufferArg = args.find(a => a.startsWith('--buffer='))?.slice('--buffer='.length);

const stats = new Map<string, PathStats>();
let totalValues = 0;
let totalMessages = 0;

/** Paths the plugin already records, from the buffer's table registry. */
function recordedPaths(dbPath: string): Set<string> {
  const out = new Set<string>();
  try {
    const db = new DatabaseSync(dbPath.replace(/^~/, process.env.HOME ?? '~'), {
      readOnly: true,
    });
    for (const row of db.prepare('SELECT path FROM buffer_tables').all() as Array<{ path: string }>) {
      out.add(row.path);
    }
    db.close();
  } catch (err) {
    console.error(`could not read recorded paths from ${dbPath}: ${(err as Error).message}`);
  }
  return out;
}

function valueType(v: unknown): string {
  if (v === null) return 'null';
  if (Array.isArray(v)) return 'array';
  return typeof v === 'object' ? 'object' : typeof v;
}

function observe(path: string, context: string, value: unknown, atMs: number): void {
  let s = stats.get(path);
  if (!s) {
    s = {
      path,
      contexts: new Map(),
      samples: 0,
      changes: 0,
      intervals: 0,
      types: new Set(),
      sampleValue: '',
    };
    stats.set(path, s);
  }
  const encoded = JSON.stringify(value ?? null);
  s.samples += 1;
  s.types.add(valueType(value));
  if (s.sampleValue === '') s.sampleValue = encoded.slice(0, 40);

  let c = s.contexts.get(context);
  if (!c) {
    c = { lastSeenMs: atMs, lastValue: encoded, samples: 1, changes: 0, intervalSum: 0, intervalSquares: 0, intervals: 0 };
    s.contexts.set(context, c);
    return; // No interval or change to measure on the first sighting.
  }
  const gap = atMs - c.lastSeenMs;
  c.lastSeenMs = atMs;
  c.samples += 1;
  c.intervalSum += gap;
  c.intervalSquares += gap * gap;
  c.intervals += 1;
  s.intervals += 1;
  if (encoded !== c.lastValue) {
    c.changes += 1;
    s.changes += 1;
    c.lastValue = encoded;
  }
}

/**
 * Coefficient of variation of the gaps between updates: 0 is metronomic.
 *
 * Measured per context and then averaged, weighted by sample count. A path
 * carried by twenty vessels has twenty independent cadences, and pooling
 * their intervals would measure how the vessels interleave rather than how
 * regularly any one of them reports — which reads as bursty even when every
 * vessel is perfectly periodic.
 */
function intervalCV(s: PathStats): number {
  let weighted = 0;
  let weight = 0;
  for (const c of s.contexts.values()) {
    if (c.intervals < 3) continue;
    const mean = c.intervalSum / c.intervals;
    if (mean <= 0) continue;
    const variance = c.intervalSquares / c.intervals - mean * mean;
    const cv = variance <= 0 ? 0 : Math.sqrt(variance) / mean;
    weighted += cv * c.intervals;
    weight += c.intervals;
  }
  return weight === 0 ? NaN : weighted / weight;
}

function classify(s: PathStats, rate: number, changeRatio: number, cv: number): Behaviour {
  // No interval anywhere means no context was seen twice, so there is nothing
  // to say about cadence or change — including when several contexts each
  // reported once, which a sample count alone would read as 'static'.
  if (s.intervals === 0) return 'single';
  if (s.changes === 0) return 'static';
  if (changeRatio < 0.05) return 'stable';
  if (Number.isNaN(cv)) return 'irregular';
  if (cv < 0.5 && rate >= 0.2) return 'periodic';
  if (cv >= 1.0) return 'episodic';
  return 'irregular';
}

function pad(s: string, n: number): string {
  return s.length > n ? s.slice(0, n - 1) + '…' : s.padEnd(n);
}

function report(recorded: Set<string>, elapsedSeconds: number): void {
  // Rates are per second of connected time, which is not the requested window
  // when the server closed the stream early.
  const span = elapsedSeconds > 0 ? elapsedSeconds : seconds;
  const rows = [...stats.values()].map(s => {
    const rate = s.samples / span;
    const changeRatio = s.intervals > 0 ? s.changes / s.intervals : 0;
    const cv = intervalCV(s);
    return {
      s,
      rate,
      changeRatio,
      cv,
      behaviour: classify(s, rate, changeRatio, cv),
      contexts: s.contexts.size,
      rowsPerDay: rate * 86400,
      dedupPerDay: rate * 86400 * changeRatio,
      filesPerDay: s.contexts.size,
      isRecorded: recorded.has(s.path),
    };
  });

  const byClass = new Map<Behaviour, number>();
  for (const r of rows) byClass.set(r.behaviour, (byClass.get(r.behaviour) ?? 0) + 1);

  console.log(`\nsampled ${span.toFixed(0)}s from ${url}`);
  console.log(`  ${totalMessages.toLocaleString()} delta messages, ${totalValues.toLocaleString()} path values (${(totalValues / span).toFixed(1)}/s)`);
  console.log(`  ${rows.length} distinct paths, ${new Set(rows.flatMap(r => [...r.s.contexts.keys()])).size} contexts`);
  if (recorded.size) console.log(`  ${rows.filter(r => r.isRecorded).length} of them already recorded`);

  console.log('\nbehaviour:');
  for (const [b, n] of [...byClass.entries()].sort((a, b2) => b2[1] - a[1])) {
    console.log(`  ${pad(b, 10)} ${String(n).padStart(5)}  ${describe(b)}`);
  }

  const totals = rows.reduce(
    (a, r) => ({
      rows: a.rows + r.rowsPerDay,
      dedup: a.dedup + r.dedupPerDay,
      files: a.files + r.filesPerDay,
    }),
    { rows: 0, dedup: 0, files: 0 }
  );
  console.log('\nif every path were recorded, per day:');
  console.log(`  rows        : ${Math.round(totals.rows).toLocaleString()}`);
  console.log(`  rows, change-only: ${Math.round(totals.dedup).toLocaleString()}  (${(100 * totals.dedup / Math.max(totals.rows, 1)).toFixed(1)}% of the above)`);
  console.log(`  files       : ${Math.round(totals.files).toLocaleString()}  (one per path per context)`);

  const show = (title: string, list: typeof rows, n = 20) => {
    if (list.length === 0) return;
    console.log(`\n${title}`);
    console.log(`  ${pad('path', 46)} ${pad('class', 9)} ${'rate/s'.padStart(8)} ${'chg%'.padStart(6)} ${'CV'.padStart(6)} ${'ctx'.padStart(4)} ${'rows/day'.padStart(11)} ${'files/day'.padStart(9)}  rec`);
    for (const r of list.slice(0, n)) {
      console.log(
        `  ${pad(r.s.path || '(root)', 46)} ${pad(r.behaviour, 9)} ${r.rate.toFixed(2).padStart(8)} ${(100 * r.changeRatio).toFixed(0).padStart(6)} ${(Number.isNaN(r.cv) ? '-' : r.cv.toFixed(2)).padStart(6)} ${String(r.contexts).padStart(4)} ${Math.round(r.rowsPerDay).toLocaleString().padStart(11)} ${String(r.filesPerDay).padStart(9)}  ${r.isRecorded ? 'yes' : ''}`
      );
    }
  };

  const unrecorded = rows.filter(r => !r.isRecorded);
  show('heaviest paths by rows per day:', [...rows].sort((a, b) => b.rowsPerDay - a.rowsPerDay));
  show('not recorded, changing regularly (best candidates):',
    unrecorded.filter(r => r.behaviour === 'periodic' || r.behaviour === 'irregular').sort((a, b) => b.rowsPerDay - a.rowsPerDay));
  show('not recorded, episodic (cheap, event-like):',
    unrecorded.filter(r => r.behaviour === 'episodic').sort((a, b) => b.rowsPerDay - a.rowsPerDay));
  show('arriving often but barely changing (dedup wins most):',
    rows.filter(r => r.behaviour === 'stable' || r.behaviour === 'static').sort((a, b) => b.rowsPerDay - a.rowsPerDay));
}

function describe(b: Behaviour): string {
  switch (b) {
    case 'periodic': return 'steady cadence, value moves — time-series data';
    case 'irregular': return 'changes, uneven cadence';
    case 'episodic': return 'bursty, long gaps — events rather than a series';
    case 'stable': return 'arrives often, value rarely moves — dedup or fold';
    case 'static': return 'never changed in the window — metadata, not a series';
    case 'single': return 'no repeated observation in any context';
  }
}

const recorded = bufferArg ? recordedPaths(bufferArg) : new Set<string>();
const ws = new WebSocket(`${url.replace(/\/$/, '')}/signalk/v1/stream?subscribe=all`);

/** When the stream opened, and so when sampling actually began. */
let openedMs = 0;
let timer: ReturnType<typeof setTimeout> | undefined;
let reported = false;

/** Report the window that was actually sampled, once, and stop. */
function finish(code: number): void {
  if (reported) return;
  reported = true;
  if (timer) clearTimeout(timer);
  report(recorded, openedMs === 0 ? 0 : (Date.now() - openedMs) / 1000);
  try {
    ws.close();
  } catch {
    // already closing
  }
  process.exit(code);
}

ws.onopen = () => {
  // The clock starts with the stream, not with the process: counting the
  // connection handshake as sample time would understate every rate.
  openedMs = Date.now();
  timer = setTimeout(() => finish(0), seconds * 1000);
  console.error(`connected, sampling ${seconds}s…`);
};
ws.onerror = e => console.error('websocket error:', (e as ErrorEvent).message ?? e);
ws.onclose = e => {
  if (totalMessages === 0) {
    console.error(`closed before any data (code ${e.code} ${e.reason || ''})`);
    process.exit(1);
  }
  // Closed early: report the measured duration now rather than let the timer
  // fire later and present a short sample as a full window. Non-zero exit
  // marks the sample as incomplete.
  const elapsed = openedMs === 0 ? 0 : (Date.now() - openedMs) / 1000;
  console.error(
    `stream closed after ${elapsed.toFixed(0)}s of the requested ${seconds}s (code ${e.code} ${e.reason || ''})`
  );
  finish(1);
};
ws.onmessage = ev => {
  let d: { context?: string; updates?: Array<{ timestamp?: string; values?: Array<{ path?: string; value?: unknown }> }> };
  try {
    d = JSON.parse(String((ev as MessageEvent).data));
  } catch {
    return;
  }
  if (!d.updates) return;
  totalMessages += 1;
  const now = Date.now();
  for (const u of d.updates) {
    for (const v of u.values ?? []) {
      totalValues += 1;
      observe(v.path === '' || v.path === undefined ? '(root)' : v.path, d.context ?? '(none)', v.value, now);
    }
  }
};

