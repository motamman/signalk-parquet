/**
 * Spatial correlation of non-position paths in the History API.
 *
 * When a request carries a spatial filter but does not ask for the position
 * path itself, the API runs a separate position query to work out when the
 * vessel was inside the area, then filters the other paths to those times.
 *
 * That correlation has three distinct outcomes and they must stay distinct:
 * positions inside the area (filter to them), no positions inside the area
 * (an authoritative empty result — clear the correlated paths), and the
 * position query failing (nothing is known, so the correlated paths must be
 * returned unfiltered). Collapsing the third case into the second silently
 * deletes real data from the response on a recoverable hiccup, which is what
 * these tests pin.
 */
import { expect } from 'chai';
import express from 'express';
import type { Server } from 'http';
import type { AddressInfo } from 'net';
import * as path from 'path';
import * as fs from 'fs-extra';
import { glob } from 'glob';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { registerHistoryApiRoute } from '../../src/HistoryAPI';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makeScalarRecord, makePositionRecord } from './helpers/records';

// No hyphen/colon: sanitizeContext maps ':' -> '-' and unsanitizeContext maps
// '-' -> ':', so a hyphenated id would not survive the contexts round-trip.
const SELF_ID = 'spatialself';
const STORED_CONTEXT = `vessels.${SELF_ID}`;
const DAY = new Date('2024-06-01T00:00:00.000Z');
const DEPTH_PATH = 'environment.depth.belowTransducer';

/** Where the fixture vessel sits, and a box comfortably around it. */
const LAT = 47.5;
const LON = 9.4;
const BOX_AROUND = '9.3,47.4,9.5,47.6'; // west,south,east,north
const BOX_ELSEWHERE = '1.0,1.0,2.0,2.0';

interface ValuesResponse {
  context: string;
  values: Array<{ path: string; method: string }>;
  data: Array<[string, ...number[]]>;
}

describe('History API spatial correlation', function () {
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

    // Depth samples plus a position fix at the same minute, so the correlation
    // buckets line up at any resolution coarser than a minute.
    for (let i = 0; i < 5; i++) {
      const iso = `2024-06-01T10:0${i}:00.000Z`;
      buffer.insert(makeScalarRecord(STORED_CONTEXT, DEPTH_PATH, 12 + i, iso));
      buffer.insert(makePositionRecord(STORED_CONTEXT, LAT, LON, iso));
    }
    await exportService.exportDayToParquet(DAY);

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
    baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });

  afterEach(async () => {
    if (server)
      await new Promise<void>(resolve => server.close(() => resolve()));
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  function valuesUrl(params: Record<string, string>): string {
    return `${baseUrl}/signalk/v1/history/values?${new URLSearchParams(params).toString()}`;
  }

  /** Request the depth path alone, optionally with a spatial filter. */
  async function queryDepth(bbox?: string): Promise<ValuesResponse> {
    const res = await fetch(
      valuesUrl({
        from: '2024-06-01T00:00:00Z',
        to: '2024-06-01T23:59:59Z',
        paths: DEPTH_PATH,
        context: 'vessels.self',
        resolution: '60',
        ...(bbox ? { bbox } : {}),
      })
    );
    expect(res.status).to.equal(200);
    return (await res.json()) as ValuesResponse;
  }

  /**
   * Replace every stored navigation.position parquet file with bytes DuckDB
   * cannot parse, so the correlation query fails the way a truncated or
   * half-written file would.
   */
  async function corruptPositionData(): Promise<void> {
    const pattern = path
      .join(host.dataDir, 'tier=raw', 'context=*', 'path=navigation*position*')
      .split(path.sep)
      .join('/');
    const dirs = await glob(pattern);
    expect(dirs, 'position fixture exists to corrupt').to.not.be.empty;

    const files = await glob(`${dirs[0]}/**/*.parquet`);
    expect(files, 'position parquet files exist').to.not.be.empty;
    for (const file of files) {
      await fs.writeFile(file, 'this is not a parquet file');
    }
  }

  it('returns depth data when no spatial filter is applied', async () => {
    const body = await queryDepth();

    expect(body.context).to.equal(STORED_CONTEXT);
    expect(body.data.length, 'baseline has rows').to.be.greaterThan(0);
  });

  it('keeps depth data when the vessel was inside the area', async () => {
    const body = await queryDepth(BOX_AROUND);

    expect(body.data.length).to.be.greaterThan(0);
  });

  // An authoritative empty result: the position query succeeded and found
  // nothing in the box, so correlated paths genuinely have no matching times.
  it('clears depth data when the vessel was never inside the area', async () => {
    const body = await queryDepth(BOX_ELSEWHERE);

    expect(body.data).to.be.empty;
  });

  // The regression: a failed position query used to yield an empty Set, which
  // the consumer could not distinguish from "no positions in area", so every
  // correlated path was silently cleared. Null means "skip correlation".
  it('returns depth data unfiltered when the position query fails', async () => {
    const baseline = await queryDepth();
    await corruptPositionData();

    const body = await queryDepth(BOX_AROUND);

    expect(body.data, 'a query error must not clear correlated paths').to.not.be
      .empty;
    expect(body.data.length).to.equal(baseline.data.length);
  });

  it('still returns depth unfiltered on a failed query with a box far away', async () => {
    const baseline = await queryDepth();
    await corruptPositionData();

    // The box is irrelevant once the position query fails: nothing is known
    // about where the vessel was, so nothing may be filtered out.
    const body = await queryDepth(BOX_ELSEWHERE);

    expect(body.data).to.not.be.empty;
    expect(body.data.length).to.equal(baseline.data.length);
  });
});
