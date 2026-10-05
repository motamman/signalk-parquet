/**
 * Whether history queries make the Signal K server's memory climb and stay
 * up, measured from outside it. Not shipped in the package; nothing in the
 * plugin depends on it.
 *
 * Run it on the server's own host (Node 22.6+ runs TypeScript directly):
 *
 *   NODE_TLS_REJECT_UNAUTHORIZED=0 node --experimental-strip-types history-memory-bench.ts \
 *     --base https://localhost:3443 --token-file ~/.track-bench-token \
 *     --label cache-on --rounds 20 --server-pid $(systemctl show signalk -p MainPID --value)
 *
 * Each round makes the requests a map-explorer session makes, which open
 * many parquet files: the contexts listing and the path listing over
 * --list-days, and the values of --paths for the own vessel over
 * --values-days. After each round it reads the resident memory of the server
 * and of its children from /proc. A baseline before the first round and a
 * reading --settle-seconds after the last one separate memory that is kept
 * from memory that is only in use while a query runs.
 *
 * One JSON line per round on stdout, then a summary line. Run it against each
 * build with the same arguments and compare.
 */

import { readFileSync } from 'node:fs';

interface Options {
  base: string;
  token?: string;
  label: string;
  rounds: number;
  listDays: number;
  valuesDays: number;
  paths: string;
  settleSeconds: number;
  serverPid: number;
  only?: string;
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
  const pid = Number(args.get('server-pid'));
  if (!Number.isInteger(pid) || pid <= 0) {
    throw new Error('--server-pid is required');
  }
  // A file rather than an argument, so the token is not in the process list.
  const tokenFile = args.get('token-file');
  return {
    base: args.get('base') ?? 'http://localhost:3000',
    token: tokenFile ? readFileSync(tokenFile, 'utf8').trim() : undefined,
    label: args.get('label') ?? 'unlabelled',
    rounds: Number(args.get('rounds') ?? 20),
    listDays: Number(args.get('list-days') ?? 7),
    valuesDays: Number(args.get('values-days') ?? 30),
    paths: args.get('paths') ?? 'navigation.position,navigation.speedOverGround',
    settleSeconds: Number(args.get('settle-seconds') ?? 30),
    serverPid: pid,
    // One of contexts, paths or values: make only that request each round,
    // to tell which of them the memory follows.
    only: args.get('only'),
  };
}

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

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

/** The server's and each child's resident memory, children by command. */
function memory(serverPid: number) {
  const children: Record<string, number | null> = {};
  try {
    const pids = readFileSync(
      `/proc/${serverPid}/task/${serverPid}/children`,
      'utf8'
    )
      .trim()
      .split(/\s+/)
      .filter(Boolean)
      .map(Number);
    for (const child of pids) {
      const cmd = readFileSync(`/proc/${child}/cmdline`, 'utf8')
        .split('\0')
        .filter(Boolean)
        .pop();
      children[`${cmd ?? 'child'}#${child}`] = rssMb(child);
    }
  } catch {
    // No children file on this kernel, or the server is gone.
  }
  return { serverMb: rssMb(serverPid), children };
}

async function timed(url: string, token: string | undefined) {
  const t0 = performance.now();
  const res = await fetch(url, {
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  });
  const body = await res.arrayBuffer();
  return {
    status: res.status,
    ms: Math.round(performance.now() - t0),
    bytes: body.byteLength,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const iso = (msAgo: number) => new Date(Date.now() - msAgo).toISOString();
  const day = 86_400_000;

  const baseline = memory(opts.serverPid);
  console.log(JSON.stringify({ label: opts.label, baseline }));

  const servers: number[] = [];
  for (let round = 1; round <= opts.rounds; round++) {
    const listFrom = iso(opts.listDays * day);
    const valuesFrom = iso(opts.valuesDays * day);
    const now = iso(0);
    // Every parameter encoded: the timestamps carry ':' and --paths is free
    // text that may hold '&', '#' or '+'.
    const query = (params: Record<string, string>) =>
      new URLSearchParams(params).toString();
    const requests = {
      contexts: `${opts.base}/signalk/v1/history/contexts?${query({ from: listFrom, to: now })}`,
      paths: `${opts.base}/api/history/paths?${query({ context: '', from: listFrom, to: now })}`,
      values: `${opts.base}/signalk/v1/history/values?${query({ context: '', from: valuesFrom, to: now, paths: opts.paths })}`,
    };
    const results: Record<string, unknown> = {};
    if (opts.only && !(opts.only in requests)) {
      throw new Error(`--only must be one of ${Object.keys(requests).join(', ')}`);
    }
    for (const [name, url] of Object.entries(requests)) {
      if (opts.only && name !== opts.only) continue;
      const r = await timed(url, opts.token);
      if (r.status !== 200) throw new Error(`${name} answered ${r.status}`);
      results[name] = r;
    }
    const mem = memory(opts.serverPid);
    if (mem.serverMb !== null) servers.push(mem.serverMb);
    console.log(JSON.stringify({ label: opts.label, round, results, memory: mem }));
  }

  await sleep(opts.settleSeconds * 1000);
  const settled = memory(opts.serverPid);
  console.log(
    JSON.stringify({
      summary: opts.label,
      rounds: opts.rounds,
      baselineServerMb: baseline.serverMb,
      afterRound1Mb: servers[0],
      afterLastRoundMb: servers[servers.length - 1],
      peakServerMb: Math.max(...servers),
      settledServerMb: settled.serverMb,
      // Growth after the first round, per round: the first round fills
      // whatever caches exist; a steady climb after it is what the issue
      // describes.
      growthPerRoundAfterFirstMb:
        servers.length > 1
          ? Math.round(
              ((servers[servers.length - 1] - servers[0]) /
                (servers.length - 1)) *
                10
            ) / 10
          : null,
      settledChildren: settled.children,
    })
  );
}

main().catch(err => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
