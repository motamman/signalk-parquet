/**
 * Where the memory goes when the same history values query runs again and
 * again: the JavaScript heap, buffers outside it, DuckDB's own accounting, or
 * none of these (memory the allocator holds on to). Not shipped in the
 * package; nothing in the plugin depends on it.
 *
 * It runs the plugin's own built HistoryAPI in this process, not in the
 * Signal K server, so the server is not touched and needs no restart. Run it
 * on the server's host against the installed build and the live data
 * directory (Node 22.6+ runs TypeScript directly):
 *
 *   node --expose-gc --experimental-strip-types history-values-memory.ts \
 *     --plugin-dir /opt/staging/signalk-parquet \
 *     --data-dir ~/.signalk/data \
 *     --self-id urn:mrn:imo:mmsi:368396230 \
 *     --duckdb-home ~/history-values-memory --rounds 60
 *
 * The buffer is opened read-only, which takes no write lock (see
 * SQLiteBufferConfig.readOnly). DuckDB gets its own home directory so its
 * temporary files cannot collide with the server's. The app passed to the
 * query has no getMetadata, so no path is treated as angular; that changes
 * which SQL aggregate is used, not how much is read.
 *
 * After each round, with --expose-gc, it collects garbage first so the heap
 * figure is what is still referenced. One JSON line per round, then a summary.
 */

import { createRequire } from 'node:module';
import { EventEmitter } from 'node:events';
import * as path from 'node:path';

interface Options {
  pluginDir: string;
  dataDir: string;
  selfId: string;
  duckdbHome: string;
  rounds: number;
  days: number;
  paths: string;
  buffer: boolean;
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
  const required = (name: string) => {
    const v = args.get(name);
    if (!v) throw new Error(`--${name} is required`);
    return v;
  };
  return {
    pluginDir: path.resolve(required('plugin-dir')),
    dataDir: path.resolve(required('data-dir')),
    selfId: required('self-id'),
    duckdbHome: path.resolve(required('duckdb-home')),
    rounds: Number(args.get('rounds') ?? 20),
    days: Number(args.get('days') ?? 30),
    paths: args.get('paths') ?? 'navigation.position,navigation.speedOverGround',
    // --buffer off leaves the SQLite buffer out, so no rows are staged into
    // DuckDB: the query reads parquet only.
    buffer: (args.get('buffer') ?? 'on') !== 'off',
  };
}

const mb = (bytes: number) => Math.round(bytes / 1048576);

/** Stands in for the Express response: counts what is written, keeps nothing. */
class DiscardingResponse extends EventEmitter {
  bytes = 0;
  headersSent = false;
  statusCode = 200;
  error: unknown = null;
  setHeader() {
    return this;
  }
  write(chunk: string | Uint8Array) {
    this.headersSent = true;
    this.bytes += typeof chunk === 'string' ? Buffer.byteLength(chunk) : chunk.byteLength;
    return true;
  }
  end() {
    this.headersSent = true;
  }
  status(code: number) {
    this.statusCode = code;
    return this;
  }
  json(body: unknown) {
    this.error = body;
  }
  destroy() {
    this.error = 'response destroyed mid-stream';
  }
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const load = createRequire(path.join(opts.pluginDir, 'package.json'));
  const { HistoryAPI } = load('./dist/HistoryAPI.js');
  const { DuckDBPool } = load('./dist/utils/duckdb-pool.js');
  const { SQLiteBuffer } = load('./dist/utils/sqlite-buffer.js');
  const { ZonedDateTime, ZoneOffset } = load('@js-joda/core');

  await DuckDBPool.initialize(opts.duckdbHome, (m: string) => console.error(m));
  const buffer = opts.buffer
    ? new SQLiteBuffer({ dbPath: path.join(opts.dataDir, 'buffer.db'), readOnly: true })
    : undefined;
  const api = new HistoryAPI(opts.selfId, opts.dataDir, buffer);
  const context = `vessels.${opts.selfId}`;
  const app = {};
  const debug = () => {};

  const gc = (globalThis as { gc?: () => void }).gc;

  async function duckdbMb() {
    const conn = await DuckDBPool.getConnection();
    try {
      const reader = await conn.runAndReadAll(
        'SELECT tag, memory_usage_bytes, temporary_storage_bytes FROM duckdb_memory()'
      );
      const tags: Record<string, number> = {};
      let total = 0;
      let temp = 0;
      for (const row of reader.getRowObjects()) {
        const used = Number(row.memory_usage_bytes);
        total += used;
        temp += Number(row.temporary_storage_bytes);
        if (used > 0) tags[String(row.tag)] = mb(used);
      }
      return { totalMb: mb(total), tempStorageMb: mb(temp), tags };
    } finally {
      conn.disconnectSync();
    }
  }

  async function reading() {
    gc?.();
    const m = process.memoryUsage();
    return {
      rssMb: mb(m.rss),
      heapUsedMb: mb(m.heapUsed),
      externalMb: mb(m.external),
      arrayBuffersMb: mb(m.arrayBuffers),
      duckdb: await duckdbMb(),
    };
  }

  const baseline = await reading();
  console.log(JSON.stringify({ opts: { ...opts }, gc: !!gc, baseline }));

  const rounds: Array<Awaited<ReturnType<typeof reading>>> = [];
  for (let round = 1; round <= opts.rounds; round++) {
    const to = ZonedDateTime.now(ZoneOffset.UTC);
    const from = to.minusDays(opts.days);
    const res = new DiscardingResponse();
    const t0 = performance.now();
    await api.getValues(context, from, to, null, app, debug, { query: { paths: opts.paths } }, res);
    const ms = Math.round(performance.now() - t0);
    if (res.error) throw new Error(`round ${round}: ${JSON.stringify(res.error)}`);
    const mem = await reading();
    rounds.push(mem);
    console.log(JSON.stringify({ round, ms, bytes: res.bytes, ...mem }));
  }

  const first = rounds[0];
  const last = rounds[rounds.length - 1];
  const growth = (pick: (r: typeof first) => number) => pick(last) - pick(first);
  console.log(
    JSON.stringify({
      summary: true,
      rounds: opts.rounds,
      buffer: opts.buffer,
      // Change from after round 1 to after the last round: the first round
      // fills whatever caches exist.
      growthAfterRound1Mb: {
        rss: growth(r => r.rssMb),
        heapUsed: growth(r => r.heapUsedMb),
        external: growth(r => r.externalMb),
        duckdb: growth(r => r.duckdb.totalMb),
      },
      peakRssMb: Math.max(...rounds.map(r => r.rssMb)),
    })
  );

  buffer?.close();
  await DuckDBPool.shutdown();
}

main().catch(err => {
  console.error(err instanceof Error ? err.stack : err);
  process.exit(1);
});
