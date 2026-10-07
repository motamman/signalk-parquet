/**
 * Which code allocates the JavaScript memory a history values request
 * creates in a running server. Not shipped in the package; nothing in the
 * plugin depends on it.
 *
 * It uses V8's sampling heap profiler through the Node inspector, which a
 * running process opens on 127.0.0.1:9229 when it receives SIGUSR1. Samples
 * include objects already collected, so short-lived garbage counts. Run it on
 * the server's host:
 *
 *   kill -USR1 $(systemctl show signalk -p MainPID --value)
 *   NODE_TLS_REJECT_UNAUTHORIZED=0 node --experimental-strip-types server-alloc-profile.ts \
 *     --base https://localhost:3443 --token-file ~/.track-bench-token --requests 5
 *
 * With --requests 0 it samples for --seconds without making any request: the
 * server's background allocation over the same time, to subtract.
 *
 * Prints total sampled bytes, the functions that allocated most themselves,
 * and the plugin functions with the most allocation beneath them.
 */

import { readFileSync } from 'node:fs';

interface Options {
  base: string;
  token?: string;
  requests: number;
  seconds: number;
  days: number;
  paths: string;
  inspector: string;
  top: number;
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
  const tokenFile = args.get('token-file');
  return {
    base: args.get('base') ?? 'http://localhost:3000',
    token: tokenFile ? readFileSync(tokenFile, 'utf8').trim() : undefined,
    requests: Number(args.get('requests') ?? 5),
    seconds: Number(args.get('seconds') ?? 10),
    days: Number(args.get('days') ?? 30),
    paths: args.get('paths') ?? 'navigation.position,navigation.speedOverGround',
    inspector: args.get('inspector') ?? 'http://127.0.0.1:9229',
    top: Number(args.get('top') ?? 25),
  };
}

interface ProfileNode {
  callFrame: { functionName: string; url: string; lineNumber: number };
  selfSize: number;
  children: ProfileNode[];
}

const mb = (b: number) => Math.round((b / 1048576) * 10) / 10;

function frameName(n: ProfileNode) {
  const url = n.callFrame.url.replace(/^.*\/(node_modules|dist)\//, '$1/');
  return `${n.callFrame.functionName || '(anonymous)'} ${url}:${n.callFrame.lineNumber + 1}`;
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const targets = (await (await fetch(`${opts.inspector}/json/list`)).json()) as Array<{
    webSocketDebuggerUrl: string;
  }>;
  if (!targets.length) throw new Error('no inspector target');
  const ws = new WebSocket(targets[0].webSocketDebuggerUrl);
  await new Promise((resolve, reject) => {
    ws.onopen = resolve;
    ws.onerror = reject;
  });
  let nextId = 1;
  const pending = new Map<number, (msg: any) => void>();
  ws.onmessage = event => {
    const msg = JSON.parse(String(event.data));
    if (msg.id && pending.has(msg.id)) {
      pending.get(msg.id)!(msg);
      pending.delete(msg.id);
    }
  };
  const call = (method: string, params: object = {}) =>
    new Promise<any>((resolve, reject) => {
      const id = nextId++;
      pending.set(id, msg => (msg.error ? reject(new Error(msg.error.message)) : resolve(msg.result)));
      ws.send(JSON.stringify({ id, method, params }));
    });

  await call('HeapProfiler.enable');
  await call('HeapProfiler.startSampling', {
    samplingInterval: 16384,
    includeObjectsCollectedByMajorGC: true,
    includeObjectsCollectedByMinorGC: true,
  });

  const t0 = performance.now();
  if (opts.requests > 0) {
    for (let i = 0; i < opts.requests; i++) {
      const to = new Date().toISOString();
      const from = new Date(Date.now() - opts.days * 86_400_000).toISOString();
      const res = await fetch(
        `${opts.base}/signalk/v1/history/values?context=&from=${from}&to=${to}&paths=${opts.paths}`,
        { headers: opts.token ? { Authorization: `Bearer ${opts.token}` } : {} }
      );
      await res.arrayBuffer();
      if (res.status !== 200) throw new Error(`values answered ${res.status}`);
    }
  } else {
    await new Promise(resolve => setTimeout(resolve, opts.seconds * 1000));
  }
  const seconds = Math.round((performance.now() - t0) / 100) / 10;

  const { profile } = await call('HeapProfiler.stopSampling');
  await call('HeapProfiler.disable');
  ws.close();

  // Self bytes per function, and bytes beneath each plugin function (each
  // node counted once per distinct plugin frame on its stack).
  const self = new Map<string, number>();
  const pluginInclusive = new Map<string, number>();
  let total = 0;
  const walk = (node: ProfileNode, stack: string[]): number => {
    const name = frameName(node);
    self.set(name, (self.get(name) ?? 0) + node.selfSize);
    total += node.selfSize;
    const isPlugin = node.callFrame.url.includes('/signalk-parquet/dist/');
    const below = stack.includes(name) || !isPlugin ? stack : [...stack, name];
    let sum = node.selfSize;
    for (const child of node.children) sum += walk(child, below);
    if (isPlugin && !stack.includes(name)) {
      pluginInclusive.set(name, (pluginInclusive.get(name) ?? 0) + sum);
    }
    return sum;
  };
  walk(profile.head, []);

  const top = (m: Map<string, number>) =>
    [...m.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, opts.top)
      .map(([name, bytes]) => ({ mb: mb(bytes), name }));

  console.log(
    JSON.stringify(
      {
        requests: opts.requests,
        seconds,
        totalSampledMb: mb(total),
        perRequestMb: opts.requests > 0 ? mb(total / opts.requests) : null,
        topSelf: top(self),
        topPluginInclusive: top(pluginInclusive),
      },
      null,
      1
    )
  );
}

main().catch(err => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
