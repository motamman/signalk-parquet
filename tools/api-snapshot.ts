/**
 * Whether two builds answer the same requests the same way. Not shipped in
 * the package; nothing in the plugin depends on it.
 *
 * Run it on the server's host, once per build, with the same requests file,
 * then compare the two directories (Node 22.6+ runs TypeScript directly):
 *
 *   NODE_TLS_REJECT_UNAUTHORIZED=0 node --experimental-strip-types api-snapshot.ts \
 *     snapshot --base https://localhost:3443 --token-file ~/.track-bench-token \
 *     --requests requests.json --out snap-main
 *   node --experimental-strip-types api-snapshot.ts compare snap-main snap-branch
 *
 * The requests file is a JSON array of
 *   { "name": "...", "path": "/signalk/v1/history/values?..." }   an HTTP GET
 *   { "name": "...", "ws": "/signalk/v1/playback?...", "count": 40 }
 *                                                   the first `count` messages
 * Use fixed time windows: a window that reaches the present differs between
 * runs for reasons that have nothing to do with the build. Only GET requests
 * belong here; the snapshot never writes to the server.
 *
 * compare matches each response as JSON, numbers within 1e-9 (DuckDB's
 * parallel sums differ in the last digits from run to run), and prints every
 * difference with its location. Exit status 1 when any response differs.
 */

import { mkdirSync, readFileSync, writeFileSync, existsSync } from 'node:fs';
import * as path from 'node:path';

type Request =
  | { name: string; path: string }
  | { name: string; ws: string; count: number };

function args(argv: string[]): Map<string, string> {
  const out = new Map<string, string>();
  for (let i = 0; i < argv.length; i += 2) {
    if (!argv[i].startsWith('--')) throw new Error(`unexpected ${argv[i]}`);
    out.set(argv[i].slice(2), argv[i + 1]);
  }
  return out;
}

async function getHttp(base: string, token: string | undefined, p: string) {
  const res = await fetch(base + p, {
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  });
  const text = await res.text();
  let body: unknown = text;
  try {
    body = JSON.parse(text);
  } catch {
    // not JSON: kept as text
  }
  return { status: res.status, body };
}

function getWs(base: string, token: string | undefined, p: string, count: number) {
  const url =
    base.replace(/^http/, 'ws') + p + (token ? `${p.includes('?') ? '&' : '?'}token=${token}` : '');
  return new Promise<{ status: string; body: unknown[] }>(resolve => {
    const messages: unknown[] = [];
    const ws = new WebSocket(url);
    const done = (status: string) => {
      clearTimeout(timer);
      try {
        ws.close();
      } catch {
        // already closed
      }
      resolve({ status, body: messages });
    };
    const timer = setTimeout(() => done(`timeout after ${messages.length}`), 60_000);
    ws.onmessage = e => {
      const m = JSON.parse(String(e.data));
      // The hello names the server and the time of connection: not data.
      if (m && typeof m === 'object' && 'roles' in m) return;
      messages.push(m);
      if (messages.length >= count) done('ok');
    };
    ws.onerror = () => done('error');
    ws.onclose = () => done(messages.length >= count ? 'ok' : `closed after ${messages.length}`);
  });
}

async function snapshot(opts: Map<string, string>) {
  const base = opts.get('base') ?? 'http://localhost:3000';
  const tokenFile = opts.get('token-file');
  const token = tokenFile ? readFileSync(tokenFile, 'utf8').trim() : undefined;
  const requests = JSON.parse(readFileSync(opts.get('requests')!, 'utf8')) as Request[];
  const out = opts.get('out')!;
  mkdirSync(out, { recursive: true });
  for (const r of requests) {
    const t0 = performance.now();
    const result =
      'path' in r ? await getHttp(base, token, r.path) : await getWs(base, token, r.ws, r.count);
    const ms = Math.round(performance.now() - t0);
    writeFileSync(path.join(out, `${r.name}.json`), JSON.stringify(result));
    const size = JSON.stringify(result.body).length;
    console.log(`${r.name}: ${result.status} ${size} bytes ${ms} ms`);
  }
}

/** Every difference between two JSON values, as "where: a vs b". */
function diff(a: unknown, b: unknown, where: string, out: string[], max = 20) {
  if (out.length >= max) return;
  if (typeof a === 'number' && typeof b === 'number') {
    const ok = a === b || Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(a), Math.abs(b));
    if (!ok) out.push(`${where}: ${a} vs ${b}`);
    return;
  }
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) out.push(`${where}: length ${a.length} vs ${b.length}`);
    for (let i = 0; i < Math.min(a.length, b.length); i++) diff(a[i], b[i], `${where}[${i}]`, out, max);
    return;
  }
  if (a && b && typeof a === 'object' && typeof b === 'object') {
    const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
    for (const k of keys) {
      if (!(k in (a as object))) out.push(`${where}.${k}: missing vs present`);
      else if (!(k in (b as object))) out.push(`${where}.${k}: present vs missing`);
      else diff((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k], `${where}.${k}`, out, max);
    }
    return;
  }
  if (a !== b) out.push(`${where}: ${JSON.stringify(a)?.slice(0, 120)} vs ${JSON.stringify(b)?.slice(0, 120)}`);
}

function compare(dirA: string, dirB: string, requestsFile?: string) {
  const names = requestsFile
    ? (JSON.parse(readFileSync(requestsFile, 'utf8')) as Request[]).map(r => r.name)
    : undefined;
  const files = names ?? [];
  let differing = 0;
  for (const name of files) {
    const fa = path.join(dirA, `${name}.json`);
    const fb = path.join(dirB, `${name}.json`);
    if (!existsSync(fa) || !existsSync(fb)) {
      console.log(`${name}: MISSING in ${existsSync(fa) ? dirB : dirA}`);
      differing++;
      continue;
    }
    const a = JSON.parse(readFileSync(fa, 'utf8'));
    const b = JSON.parse(readFileSync(fb, 'utf8'));
    const out: string[] = [];
    diff(a, b, name, out);
    if (out.length === 0) {
      console.log(`${name}: same`);
    } else {
      differing++;
      console.log(`${name}: DIFFERENT`);
      for (const line of out) console.log(`    ${line}`);
    }
  }
  console.log(`${files.length - differing} same, ${differing} different`);
  process.exitCode = differing > 0 ? 1 : 0;
}

const [command, ...rest] = process.argv.slice(2);
if (command === 'snapshot') {
  await snapshot(args(rest));
} else if (command === 'compare') {
  const [dirA, dirB, ...more] = rest;
  compare(dirA, dirB, args(more).get('requests'));
} else {
  console.error('usage: snapshot --requests F --out D [--base U --token-file T] | compare A B --requests F');
  process.exit(2);
}
