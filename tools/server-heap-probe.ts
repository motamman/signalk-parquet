/**
 * How much of a running Node server's memory is JavaScript heap, how much of
 * that heap is garbage, and how much lies outside the heap. Not shipped in the
 * package; nothing in the plugin depends on it.
 *
 * It reads the server through the Node inspector, which a running process
 * opens on 127.0.0.1:9229 when it receives SIGUSR1 (no restart). Run it on the
 * server's host:
 *
 *   kill -USR1 $(systemctl show signalk -p MainPID --value)
 *   node --experimental-strip-types server-heap-probe.ts --label fresh
 *
 * It takes one reading, forces a full garbage collection in the server (a
 * pause of the server's event loop, typically well under a second), and takes
 * a second reading, then reads DuckDB's own memory account for the plugin's
 * pool. The inspector stays open until the server restarts.
 * Prints one JSON line.
 */

interface Options {
  label: string;
  inspector: string;
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
  return {
    label: args.get('label') ?? 'unlabelled',
    inspector: args.get('inspector') ?? 'http://127.0.0.1:9229',
  };
}

// Evaluated in the server. Spaces by name so new-space garbage and old-space
// growth can be told apart.
const READING = `JSON.stringify((() => {
  const mb = b => Math.round(b / 1048576);
  const m = process.memoryUsage();
  const spaces = {};
  for (const s of process.getBuiltinModule('v8').getHeapSpaceStatistics()) {
    spaces[s.space_name] = mb(s.space_used_size);
  }
  return {
    rssMb: mb(m.rss),
    heapTotalMb: mb(m.heapTotal),
    heapUsedMb: mb(m.heapUsed),
    externalMb: mb(m.external),
    arrayBuffersMb: mb(m.arrayBuffers),
    spacesUsedMb: spaces,
  };
})())`;

// Evaluated in the server: DuckDB's own account of the plugin's main pool
// instance, read through the DuckDBPool the plugin already loaded (found in
// Node's module cache, so nothing is loaded or created). Reads only.
const DUCKDB_READING = `(async () => {
  const mb = b => Math.round(Number(b) / 1048576);
  const cache = process.getBuiltinModule('module')._cache;
  const key = Object.keys(cache).find(k =>
    k.endsWith('/signalk-parquet/dist/utils/duckdb-pool.js'));
  if (!key) return JSON.stringify({ error: 'duckdb-pool not loaded' });
  const { DuckDBPool } = cache[key].exports;
  const conn = await DuckDBPool.getConnection();
  try {
    const reader = await conn.runAndReadAll(
      'SELECT tag, memory_usage_bytes, temporary_storage_bytes FROM duckdb_memory()');
    const tags = {};
    let total = 0;
    let temp = 0;
    for (const row of reader.getRowObjects()) {
      total += Number(row.memory_usage_bytes);
      temp += Number(row.temporary_storage_bytes);
      if (Number(row.memory_usage_bytes) > 0) tags[row.tag] = mb(row.memory_usage_bytes);
    }
    return JSON.stringify({ module: key, totalMb: mb(total), tempStorageMb: mb(temp), tags });
  } finally {
    conn.disconnectSync();
  }
})()`;

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
  const evaluate = async (expression: string) => {
    const r = await call('Runtime.evaluate', {
      expression,
      returnByValue: true,
      awaitPromise: true,
    });
    if (r.exceptionDetails) {
      throw new Error(r.exceptionDetails.exception?.description ?? r.exceptionDetails.text);
    }
    return JSON.parse(r.result.value);
  };

  const before = await evaluate(READING);
  await call('HeapProfiler.collectGarbage');
  const afterGc = await evaluate(READING);
  const duckdb = await evaluate(DUCKDB_READING);
  ws.close();
  console.log(JSON.stringify({ label: opts.label, before, afterGc, duckdb }));
}

main().catch(err => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
