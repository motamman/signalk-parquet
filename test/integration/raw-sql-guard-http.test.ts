/**
 * Pins that the raw SQL endpoint is actually guarded.
 *
 * The SQL guard itself is unit-tested; what this covers is the wiring. Without
 * it, deleting the guard call from the /api/query handler leaves the whole
 * suite green while re-opening the path that lets DuckDB's bundled SQLite open
 * the live write buffer and kill the server with SIGBUS (see buffer-staging).
 *
 * The real handler is obtained by running the plugin's own route registration
 * against a recording router, rather than by mounting an Express app: several
 * unrelated routes in this module use Express 4 path syntax that the Express 5
 * in devDependencies refuses to parse, and the Signal K server supplies the
 * router in production. Invoking the captured handler keeps the test on the
 * real code path while sidestepping route parsing entirely.
 *
 * The guard returns before any DuckDB connection is taken, so no pool or
 * fixture data is needed.
 */
import { expect } from 'chai';
import { Router } from 'express';
import { registerApiRoutes } from '../../src/api-routes';
import { PluginState } from '../../src/types';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';

const SELF_ID = 'rawsqlself';

type Handler = (req: unknown, res: unknown) => Promise<void> | void;

interface CapturedResponse {
  status: number;
  body: {
    success?: boolean;
    error?: string;
    rowCount?: number;
    truncated?: boolean;
    data?: unknown[];
  };
}

/** Minimal router double recording the handlers the plugin registers. */
function createRecordingRouter(): {
  router: Router;
  posts: Map<string, Handler>;
} {
  const posts = new Map<string, Handler>();
  const noop = () => router;
  const router = {
    use: noop,
    get: noop,
    put: noop,
    delete: noop,
    patch: noop,
    post: (routePath: string, ...handlers: Handler[]) => {
      // Some routes take middleware first; the last argument is the handler.
      posts.set(routePath, handlers[handlers.length - 1]);
      return router;
    },
  } as unknown as Router;
  return { router, posts };
}

/** Express-shaped response double capturing status and JSON body. */
function createResponse(): {
  res: unknown;
  captured: () => CapturedResponse;
} {
  let status = 200;
  let body: CapturedResponse['body'] = {};
  const res = {
    status(code: number) {
      status = code;
      return res;
    },
    json(payload: CapturedResponse['body']) {
      body = payload;
      return res;
    },
  };
  return { res, captured: () => ({ status, body }) };
}

describe('raw SQL endpoint guard', function () {
  this.timeout(30000);

  let host: FakeSignalK;
  let handler: Handler;
  let enableRawSql: boolean;

  const queryApp = {
    debug: () => {},
    error: () => {},
    getMetadata: () => undefined,
    getSelfPath: () => undefined,
    selfId: SELF_ID,
    selfContext: `vessels.${SELF_ID}`,
  };

  async function postQuery(query: string): Promise<CapturedResponse> {
    const { res, captured } = createResponse();
    await handler({ body: { query }, params: {}, query: {} }, res);
    return captured();
  }

  beforeEach(() => {
    host = createFakeSignalK({ selfId: SELF_ID });
    enableRawSql = true;

    // Only the slice of PluginState the /api/query handler reads.
    const state = {
      getDataDirPath: () => host.dataDir,
      get currentConfig() {
        return { enableRawSql, outputDirectory: host.dataDir };
      },
    } as unknown as PluginState;

    const { router, posts } = createRecordingRouter();
    registerApiRoutes(router, state, queryApp as never);

    const captured = posts.get('/api/query');
    expect(captured, 'the plugin registers a POST /api/query route').to.be.a(
      'function'
    );
    handler = captured as Handler;
  });

  afterEach(async () => {
    await host?.cleanup();
  });

  it('rejects ATTACH of a SQLite database with 400', async () => {
    const { status, body } = await postQuery(
      "ATTACH '/tmp/buffer.db' AS b (TYPE SQLITE)"
    );
    expect(status).to.equal(400);
    expect(body.success).to.equal(false);
    expect(body.error).to.match(/'ATTACH' is not allowed/);
  });

  it('rejects a quoted-identifier sqlite_scan with 400', async () => {
    const { status, body } = await postQuery(
      `SELECT v FROM "sqlite_scan"('/tmp/buffer.db','t')`
    );
    expect(status).to.equal(400);
    expect(body.error).to.match(/Table function/);
  });

  it('rejects a statement chained after a benign SELECT with 400', async () => {
    const { status, body } = await postQuery(
      'SELECT 1; SET GLOBAL autoload_known_extensions = true'
    );
    expect(status).to.equal(400);
    expect(body.error).to.match(/'SET' is not allowed/);
  });

  it('returns 403 before the guard when raw SQL is disabled', async () => {
    enableRawSql = false;
    const { status, body } = await postQuery("ATTACH '/tmp/buffer.db' AS b");
    expect(status).to.equal(403);
    expect(body.error).to.match(/Raw SQL queries are disabled/);
  });

  it('caps a large result and reports the truncation', async () => {
    // range() needs no fixture files, so this exercises the row cap without
    // depending on which data directory the sandbox instance was scoped to.
    const { body } = await postQuery('SELECT i FROM range(25000) t(i)');
    expect(body.success).to.equal(true);
    expect(body.rowCount).to.equal(10000);
    expect(body.truncated).to.equal(true);
    expect(body.data).to.have.lengthOf(10000);
  });

  it('does not report truncation for a result under the cap', async () => {
    const { body } = await postQuery('SELECT i FROM range(5) t(i)');
    expect(body.success).to.equal(true);
    expect(body.rowCount).to.equal(5);
    expect(body.truncated).to.equal(false);
  });

  it('lets a read-only query past the guard', async () => {
    // Reaches DuckDB and fails there (pool not initialized in this suite),
    // which proves the guard passed it rather than rejecting it.
    const { body } = await postQuery(
      "SELECT * FROM read_parquet('/nonexistent/x.parquet')"
    );
    expect(body.error ?? '').to.not.match(
      /is not allowed|Only read-only queries|Table function/
    );
  });
});
