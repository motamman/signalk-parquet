/**
 * How long a Track API query freezes the Signal K server, measured from
 * outside it. Not shipped in the package; nothing in the plugin depends on it.
 *
 * Run it on the server's own host, so the network adds nothing to what it
 * measures (Node 22.6+ runs TypeScript directly):
 *
 *   node --experimental-strip-types track-freeze-bench.ts \
 *     --label worker --mode load --runs 3 --server-pid $(systemctl show signalk -p MainPID --value)
 *
 * Each run:
 *   1. resets the plugin's event-loop histogram (POST, admin: pass
 *      --token-file when the server has security enabled; a server on its
 *      self-signed https port also needs NODE_TLS_REJECT_UNAUTHORIZED=0);
 *   2. starts a probe that requests a cheap endpoint back to back, --probe-ms
 *      apart, timing each, which is what an unrelated client sees;
 *   3. makes the track request once (--mode single) or every --interval-ms for
 *      --load-seconds (--mode load), timing each;
 *   4. stops the probe and reads the histogram: `max` is the longest single
 *      freeze of the server's event loop, plus about one 10 ms sampling
 *      interval (see the plugin's utils/event-loop-monitor.ts);
 *   5. with --server-pid, reads the resident memory of the server and of its
 *      track-worker child from /proc before and after.
 *
 * One JSON line per run on stdout, then a summary line per label and mode.
 * Run it once against each build with the same arguments and compare.
 */

import { readFileSync } from 'node:fs';

interface Options {
  base: string;
  token?: string;
  label: string;
  mode: 'single' | 'load';
  runs: number;
  query: string;
  loadSeconds: number;
  intervalMs: number;
  probePath: string;
  probeMs: number;
  serverPid?: number;
}

function parseArgs(argv: string[]): Options {
  const args = new Map<string, string>();
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (!key.startsWith('--')) throw new Error(`unexpected argument ${key}`);
    const value = argv[i + 1];
    if (value === undefined || value.startsWith('--')) {
      throw new Error(`${key} needs a value`);
    }
    args.set(key.slice(2), value);
    i++;
  }
  const mode = args.get('mode') ?? 'single';
  if (mode !== 'single' && mode !== 'load') {
    throw new Error('--mode must be single or load');
  }
  const pid = args.get('server-pid');
  // A file rather than an argument, so the token is not in the process list.
  const tokenFile = args.get('token-file');
  return {
    base: args.get('base') ?? 'http://localhost:3000',
    token: tokenFile ? readFileSync(tokenFile, 'utf8').trim() : args.get('token'),
    label: args.get('label') ?? 'unlabelled',
    mode,
    runs: Number(args.get('runs') ?? 3),
    // The case that froze the server: the own vessel's track over a window
    // that reaches into today, so the buffer's unexported rows are staged.
    query: args.get('query') ?? 'contexts=self&duration=PT24H&times=true',
    loadSeconds: Number(args.get('load-seconds') ?? 60),
    intervalMs: Number(args.get('interval-ms') ?? 2000),
    probePath: args.get('probe-path') ?? '/signalk',
    probeMs: Number(args.get('probe-ms') ?? 50),
    serverPid: pid === undefined ? undefined : Number(pid),
  };
}

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

function headers(opts: Options): Record<string, string> {
  return opts.token ? { Authorization: `Bearer ${opts.token}` } : {};
}

/** Milliseconds a request took, and what it returned. */
async function timed(
  url: string,
  init: RequestInit
): Promise<{ ms: number; status: number; bytes: number }> {
  const t0 = performance.now();
  const res = await fetch(url, init);
  const body = await res.arrayBuffer();
  return { ms: performance.now() - t0, status: res.status, bytes: body.byteLength };
}

function quantile(sorted: number[], q: number): number {
  if (sorted.length === 0) return NaN;
  const i = Math.min(sorted.length - 1, Math.ceil(q * sorted.length) - 1);
  return sorted[Math.max(0, i)];
}

function summarise(values: number[]) {
  const sorted = [...values].sort((a, b) => a - b);
  const round = (n: number) => Math.round(n * 10) / 10;
  return {
    count: sorted.length,
    p50: round(quantile(sorted, 0.5)),
    p99: round(quantile(sorted, 0.99)),
    max: round(sorted[sorted.length - 1] ?? NaN),
  };
}

/** Resident memory in MB, or null when the process is not there. */
function rssMb(pid: number): number | null {
  try {
    const status = readFileSync(`/proc/${pid}/status`, 'utf8');
    const kb = /VmRSS:\s+(\d+)\s+kB/.exec(status);
    return kb ? Math.round(Number(kb[1]) / 1024) : null;
  } catch {
    return null;
  }
}

/** The server's track-worker child, if it has one. */
function trackWorkerPid(serverPid: number): number | undefined {
  try {
    const children = readFileSync(
      `/proc/${serverPid}/task/${serverPid}/children`,
      'utf8'
    )
      .trim()
      .split(/\s+/)
      .filter(Boolean)
      .map(Number);
    for (const child of children) {
      const cmd = readFileSync(`/proc/${child}/cmdline`, 'utf8');
      if (cmd.includes('track-worker')) return child;
    }
  } catch {
    // No such process, or no children file on this kernel.
  }
  return undefined;
}

function memory(opts: Options) {
  if (opts.serverPid === undefined) return null;
  const worker = trackWorkerPid(opts.serverPid);
  return {
    serverMb: rssMb(opts.serverPid),
    workerMb: worker === undefined ? null : rssMb(worker),
  };
}

/** The first object in a JSON body carrying the histogram's `max`. */
function findDelay(body: unknown): Record<string, number> | null {
  if (!body || typeof body !== 'object') return null;
  if ('max' in body && 'p99' in body) return body as Record<string, number>;
  for (const value of Object.values(body)) {
    const found = findDelay(value);
    if (found) return found;
  }
  return null;
}

async function run(opts: Options, index: number) {
  const eventLoop = `${opts.base}/plugins/signalk-parquet/api/event-loop`;
  const reset = await fetch(`${eventLoop}/reset`, {
    method: 'POST',
    headers: headers(opts),
  });
  if (!reset.ok) {
    throw new Error(
      `event-loop reset answered ${reset.status}; with security enabled, pass --token`
    );
  }
  const memBefore = memory(opts);

  const probe: number[] = [];
  let probing = true;
  const probeLoop = (async () => {
    while (probing) {
      const r = await timed(`${opts.base}${opts.probePath}`, { headers: headers(opts) });
      probe.push(r.ms);
      await sleep(opts.probeMs);
    }
  })();

  const track: number[] = [];
  let bytes = 0;
  const trackUrl = `${opts.base}/signalk/v2/api/tracks?${opts.query}`;
  const once = async () => {
    const r = await timed(trackUrl, { headers: headers(opts) });
    if (r.status !== 200) throw new Error(`track request answered ${r.status}`);
    track.push(r.ms);
    bytes = r.bytes;
  };
  if (opts.mode === 'single') {
    await once();
  } else {
    const end = performance.now() + opts.loadSeconds * 1000;
    while (performance.now() < end) {
      const started = performance.now();
      await once();
      await sleep(Math.max(0, opts.intervalMs - (performance.now() - started)));
    }
  }
  // One more probe interval, so a freeze at the very end is seen by it.
  await sleep(opts.probeMs * 2);
  probing = false;
  await probeLoop;

  const delayRes = await fetch(eventLoop, { headers: headers(opts) });
  const delay = findDelay(await delayRes.json());

  return {
    label: opts.label,
    mode: opts.mode,
    run: index + 1,
    trackMs: summarise(track),
    responseBytes: bytes,
    probeMs: summarise(probe),
    eventLoopMs: delay && {
      max: delay.max,
      p99: delay.p99,
      p50: delay.p50,
      mean: delay.mean,
    },
    memoryBefore: memBefore,
    memoryAfter: memory(opts),
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const results: Array<Awaited<ReturnType<typeof run>>> = [];
  for (let i = 0; i < opts.runs; i++) {
    const result = await run(opts, i);
    results.push(result);
    console.log(JSON.stringify(result));
    // Let the server settle between runs, so one run's tail is not the next
    // one's start.
    await sleep(5000);
  }
  const worst = (pick: (r: (typeof results)[number]) => number | null | undefined) =>
    Math.max(...results.map(r => pick(r) ?? NaN));
  console.log(
    JSON.stringify({
      summary: opts.label,
      mode: opts.mode,
      runs: results.length,
      worstEventLoopMaxMs: worst(r => r.eventLoopMs?.max),
      worstEventLoopP99Ms: worst(r => r.eventLoopMs?.p99),
      worstProbeMaxMs: worst(r => r.probeMs.max),
      worstProbeP99Ms: worst(r => r.probeMs.p99),
      trackP50Ms: results.map(r => r.trackMs.p50),
    })
  );
}

main().catch(err => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
