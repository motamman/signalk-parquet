/**
 * End-to-end test of the History API over real HTTP.
 *
 * The plugin registers its History API routes on the Signal K server's
 * Express router. This suite mounts those same routes on a bare Express app
 * listening on an ephemeral loopback port, writes a Parquet fixture through
 * the real export pipeline, and queries it over HTTP exactly as a History API
 * client would — exercising request parsing, tier selection, the DuckDB query,
 * and the streamed JSON response together.
 *
 * Timestamps in the response are converted to the server's local time zone,
 * so assertions target the aggregated values and response shape rather than
 * exact timestamp strings, keeping the suite stable across time zones (local
 * dev vs. UTC CI).
 */
import { expect } from 'chai';
import express from 'express';
import type { Server } from 'http';
import type { AddressInfo } from 'net';
import * as path from 'path';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { registerHistoryApiRoute } from '../../src/HistoryAPI';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makeScalarRecord } from './helpers/records';

// No hyphen/colon: sanitizeContext maps ':' -> '-' and unsanitizeContext maps
// '-' -> ':', so a hyphenated id would not survive the contexts round-trip.
const SELF_ID = 'integrationself';
const STORED_CONTEXT = `vessels.${SELF_ID}`;
const DAY = new Date('2024-06-01T00:00:00.000Z');

// Thin wrapper binding this suite's context to the shared builder.
const scalarRecord = (signalkPath: string, value: number, isoTime: string) =>
  makeScalarRecord(STORED_CONTEXT, signalkPath, value, isoTime);

interface ValuesResponse {
  context: string;
  range: { from: string; to: string };
  values: Array<{ path: string; method: string }>;
  data: Array<[string, ...number[]]>;
}

describe('History API over HTTP', function () {
  this.timeout(30000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;
  let server: Server;
  let baseUrl: string;

  /** Lenient ServerAPI stand-in for the query handlers (no Proxy traps). */
  const queryApp = {
    debug: () => {},
    error: () => {},
    getMetadata: () => undefined,
    getSelfPath: () => undefined,
    selfId: SELF_ID,
    selfContext: STORED_CONTEXT,
  };

  beforeEach(async () => {
    host = createFakeSignalK({ selfId: SELF_ID });
    buffer = new SQLiteBuffer({ dbPath: path.join(host.dataDir, 'buffer.db') });
    await DuckDBPool.initialize();

    // Write a fixture for a fixed past day via the real export pipeline.
    const writer = new ParquetWriter({ format: 'parquet', app: host.app });
    const exportService = new ParquetExportService(
      buffer,
      writer,
      {
        outputDirectory: host.dataDir,
        filenamePrefix: 'signalk_data',
        useHivePartitioning: true,
        dailyExportHour: 4,
      },
      host.app
    );
    for (let i = 0; i < 5; i++) {
      buffer.insert(
        scalarRecord(
          'navigation.speedOverGround',
          5,
          `2024-06-01T10:0${i}:00.000Z`
        )
      );
    }
    buffer.insert(
      scalarRecord(
        'environment.depth.belowTransducer',
        12,
        '2024-06-01T10:00:00.000Z'
      )
    );
    await exportService.exportDayToParquet(DAY);

    // Mount the real routes on a bare Express app (no SQLite federation:
    // the fixture lives entirely in Parquet, so a plain parquet read suffices).
    const app = express();
    const router = express.Router();
    registerHistoryApiRoute(
      router,
      SELF_ID,
      host.dataDir,
      () => {},
      queryApp,
      undefined,
      undefined
    );
    app.use(router);
    await new Promise<void>(resolve => {
      server = app.listen(0, '127.0.0.1', () => resolve());
    });
    const addr = server.address() as AddressInfo;
    baseUrl = `http://127.0.0.1:${addr.port}`;
  });

  afterEach(async () => {
    // Guard each resource so a partial beforeEach failure doesn't mask the
    // real setup error with a teardown throw.
    if (server)
      await new Promise<void>(resolve => server.close(() => resolve()));
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  function valuesUrl(params: Record<string, string>): string {
    const qs = new URLSearchParams(params).toString();
    return `${baseUrl}/signalk/v1/history/values?${qs}`;
  }

  it('returns aggregated values for a recorded path', async () => {
    const res = await fetch(
      valuesUrl({
        from: '2024-06-01T00:00:00Z',
        to: '2024-06-01T23:59:59Z',
        paths: 'navigation.speedOverGround',
        context: 'vessels.self',
        resolution: '3600',
      })
    );
    expect(res.status).to.equal(200);

    const body = (await res.json()) as ValuesResponse;
    // self resolves to the configured vessel id.
    expect(body.context).to.equal(STORED_CONTEXT);
    expect(body.values).to.have.lengthOf(1);
    expect(body.values[0].path).to.equal('navigation.speedOverGround');
    // One hour bucket covering all five samples, each value 5 -> average 5.
    expect(body.data.length).to.be.greaterThan(0);
    const values = body.data.map(row => row[1]);
    for (const v of values) {
      expect(v).to.be.closeTo(5, 1e-9);
    }
  });

  it('reports the resolved context and time range in the response', async () => {
    const res = await fetch(
      valuesUrl({
        from: '2024-06-01T00:00:00Z',
        to: '2024-06-01T23:59:59Z',
        paths: 'navigation.speedOverGround',
        context: 'vessels.self',
        resolution: '3600',
      })
    );
    const body = (await res.json()) as ValuesResponse;
    expect(body).to.have.keys(['context', 'range', 'values', 'data']);
    expect(body.range).to.have.keys(['from', 'to']);
  });

  it('returns empty data for a path with no recorded values', async () => {
    const res = await fetch(
      valuesUrl({
        from: '2024-06-01T00:00:00Z',
        to: '2024-06-01T23:59:59Z',
        paths: 'propulsion.main.temperature',
        context: 'vessels.self',
        resolution: '3600',
      })
    );
    expect(res.status).to.equal(200);
    const body = (await res.json()) as ValuesResponse;
    // No rows carry a value for the unknown path.
    const hasValue = body.data.some(
      row => row[1] !== null && row[1] !== undefined
    );
    expect(hasValue).to.equal(false);
  });

  it('queries multiple paths in one request, value columns in path order', async () => {
    const res = await fetch(
      valuesUrl({
        from: '2024-06-01T00:00:00Z',
        to: '2024-06-01T23:59:59Z',
        paths: 'navigation.speedOverGround,environment.depth.belowTransducer',
        context: 'vessels.self',
        resolution: '3600',
      })
    );
    expect(res.status).to.equal(200);
    const body = (await res.json()) as ValuesResponse;
    expect(body.values.map(v => v.path)).to.deep.equal([
      'navigation.speedOverGround',
      'environment.depth.belowTransducer',
    ]);

    // Find a row that has both values populated and check them.
    const row = body.data.find(
      r =>
        r[1] !== null &&
        r[1] !== undefined &&
        r[2] !== null &&
        r[2] !== undefined
    );
    expect(row, 'expected a row with both path values').to.not.equal(undefined);
    expect(row![1]).to.be.closeTo(5, 1e-9); // speedOverGround average
    expect(row![2]).to.be.closeTo(12, 1e-9); // depth average
  });

  it('rejects a request with no time range parameters', async () => {
    const res = await fetch(
      `${baseUrl}/signalk/v1/history/values?paths=navigation.speedOverGround&context=vessels.self`
    );
    expect(res.status).to.be.greaterThanOrEqual(400);
  });

  it('serves the contexts endpoint for the recorded day', async () => {
    const res = await fetch(
      `${baseUrl}/signalk/v1/history/contexts?from=2024-06-01T00:00:00Z&to=2024-06-01T23:59:59Z`
    );
    expect(res.status).to.equal(200);
    const contexts = (await res.json()) as string[];
    expect(contexts).to.include(STORED_CONTEXT);
  });

  it('serves the paths endpoint for the recorded day', async () => {
    const res = await fetch(
      `${baseUrl}/signalk/v1/history/paths?from=2024-06-01T00:00:00Z&to=2024-06-01T23:59:59Z&context=vessels.self`
    );
    expect(res.status).to.equal(200);
    const paths = (await res.json()) as string[];
    expect(paths).to.include('navigation.speedOverGround');
  });
});
