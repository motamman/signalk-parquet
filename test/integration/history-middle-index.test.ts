/**
 * `middle_index` aggregation across the History API query paths.
 *
 * The method returns the value of the chronologically middle sample in each
 * time bucket: position (n + 1) / 2 of the n samples, so the first of the two
 * middle samples when n is even. Fixtures go through the real export pipeline
 * (or, for the aggregated tier, are written directly in that tier's schema) and
 * are queried through the V1 routes over HTTP and through the v2 provider.
 * Samples are inserted out of time order, so a pick that followed insertion or
 * file order instead of time order would fail.
 *
 * The V1 routes run without a SQLite buffer unless a test attaches one, so
 * most queries have a single source; that setup also pins object paths reading
 * correctly in that case.
 */
import { expect } from 'chai';
import express from 'express';
import type { Server } from 'http';
import type { AddressInfo } from 'net';
import * as path from 'path';
import * as fs from 'fs-extra';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { HivePathBuilder } from '../../src/utils/hive-path-builder';
import { middleIndexSql } from '../../src/utils/aggregate-sql';
import { registerHistoryApiRoute } from '../../src/HistoryAPI';
import { HistoryProvider } from '../../src/history-provider';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makeScalarRecord, makePositionRecord } from './helpers/records';
import type { ValuesRequest } from '@signalk/server-api/dist/history';

const SELF_ID = 'middleindexself';
const STORED_CONTEXT = `vessels.${SELF_ID}`;
const DAY = new Date('2024-06-01T00:00:00.000Z');
const TEMPERATURE = 'environment.water.temperature';
const POSITION = 'navigation.position';
const VOLTAGE = 'electrical.batteries.house.voltage';
const DEPTH = 'environment.depth.belowKeel';
// Starts a minute before the first sample so no sample sits on the boundary.
const FROM = '2024-06-01T09:59:00Z';
const TO = '2024-06-01T10:02:00Z';

// 10:00 bucket: five samples, the middle (3rd of 5) is 283.
// 10:01 bucket: four samples, the middle is the 2nd of 4, i.e. 2.
const TEMPERATURE_SAMPLES: Array<[string, number]> = [
  ['10:00:20', 283],
  ['10:00:00', 281],
  ['10:00:40', 285],
  ['10:00:10', 282],
  ['10:00:30', 284],
  ['10:01:45', 4],
  ['10:01:00', 1],
  ['10:01:30', 3],
  ['10:01:15', 2],
];
const EXPECTED_TEMPERATURE = [283, 2];

// One 10:00 bucket of positions; the middle sample is latitude 11, longitude 21.
const POSITION_SAMPLES: Array<[string, number, number]> = [
  ['10:00:40', 12, 22],
  ['10:00:00', 10, 20],
  ['10:00:20', 11, 21],
];

/** DuckDB needs forward slashes in string literals on every platform. */
function toSqlPath(p: string): string {
  return p.split(path.sep).join('/').replace(/'/g, "''");
}

interface ValuesResponse {
  values: Array<{ path: string; method: string }>;
  data: Array<[string, ...unknown[]]>;
}

describe('middle_index aggregation', function () {
  // Native DuckDB init may install the spatial extension on first run.
  this.timeout(30000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;
  let server: Server | undefined;
  let baseUrl: string;

  // Lenient ServerAPI stand-in: no metadata, so no path is treated as angular.
  const queryApp = {
    debug: () => {},
    error: () => {},
    getMetadata: () => undefined,
    getSelfPath: () => undefined,
    selfId: SELF_ID,
    selfContext: STORED_CONTEXT,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } as any;

  beforeEach(async () => {
    host = createFakeSignalK({ selfId: SELF_ID });
    buffer = new SQLiteBuffer({ dbPath: path.join(host.dataDir, 'buffer.db') });
    await DuckDBPool.initialize();

    for (const [time, value] of TEMPERATURE_SAMPLES) {
      buffer.insert(
        makeScalarRecord(
          STORED_CONTEXT,
          TEMPERATURE,
          value,
          `2024-06-01T${time}.000Z`
        )
      );
    }
    for (const [time, latitude, longitude] of POSITION_SAMPLES) {
      buffer.insert(
        makePositionRecord(
          STORED_CONTEXT,
          latitude,
          longitude,
          `2024-06-01T${time}.000Z`
        )
      );
    }
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
    await exportService.exportDayToParquet(DAY);

    // The fixtures live entirely in Parquet, so the routes run without the
    // SQLite buffer unless a test attaches it.
    await startServer();
  });

  afterEach(async () => {
    // Guard each resource so a partial beforeEach failure doesn't mask the
    // real setup error with a teardown throw.
    await stopServer();
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  /** Mounts the real V1 routes on a bare Express app listening on loopback. */
  async function startServer(sqliteBuffer?: SQLiteBuffer): Promise<void> {
    const app = express();
    const router = express.Router();
    registerHistoryApiRoute(
      router,
      SELF_ID,
      host.dataDir,
      () => {},
      queryApp,
      sqliteBuffer,
      undefined
    );
    app.use(router);
    const listening = await new Promise<Server>(resolve => {
      const s = app.listen(0, '127.0.0.1', () => resolve(s));
    });
    server = listening;
    baseUrl = `http://127.0.0.1:${(listening.address() as AddressInfo).port}`;
  }

  /** Closes the server started by startServer, if any. */
  async function stopServer(): Promise<void> {
    const s = server;
    server = undefined;
    if (s) await new Promise<void>(resolve => s.close(() => resolve()));
  }

  /** V1 values request at 60s resolution over the two fixture minutes. */
  async function v1Values(paths: string): Promise<ValuesResponse> {
    const qs = new URLSearchParams({
      from: FROM,
      to: TO,
      paths,
      context: 'vessels.self',
      resolution: '60',
    });
    const res = await fetch(`${baseUrl}/signalk/v1/history/values?${qs}`);
    expect(res.status).to.equal(200);
    return (await res.json()) as ValuesResponse;
  }

  /** Non-empty values of one response column, in timestamp order. */
  function column(body: ValuesResponse, index: number): unknown[] {
    return body.data
      .map(row => row[index])
      .filter(v => v !== null && v !== undefined);
  }

  /** v2 values request for one path's middle_index over the fixture window. */
  function v2Request(signalkPath: string): ValuesRequest {
    return {
      from: FROM,
      to: TO,
      context: 'vessels.self',
      resolution: 60,
      pathSpecs: [
        { path: signalkPath, aggregate: 'middle_index', parameter: [] },
      ],
    } as unknown as ValuesRequest;
  }

  /** A v2 provider without a SQLite buffer; the fixtures live in Parquet. */
  function newProvider(): HistoryProvider {
    return new HistoryProvider(SELF_ID, host.dataDir, queryApp, () => {});
  }

  describe('SQL helper', () => {
    it('takes every column from the same middle row, NULLs included', async () => {
      // The middle row (t=2) has no latitude. Longitude must still come from
      // t=2, not shift to a neighbour where both columns are set.
      const conn = await DuckDBPool.getConnection();
      try {
        const result = await conn.runAndReadAll(`
          SELECT ${middleIndexSql('lat', 't')} AS lat,
                 ${middleIndexSql('lon', 't')} AS lon
          FROM (VALUES (3, 12, 22), (1, 10, 20), (2, NULL, 21)) v(t, lat, lon)`);
        expect(result.getRowObjects()).to.deep.equal([{ lat: null, lon: 21 }]);
      } finally {
        conn.disconnectSync();
      }
    });
  });

  describe('V1 routes', () => {
    it('returns the middle sample of each bucket for a scalar path', async () => {
      const body = await v1Values(`${TEMPERATURE}:middle_index`);
      expect(body.values[0].method).to.equal('middle_index');
      expect(column(body, 1)).to.deep.equal(EXPECTED_TEMPERATURE);
    });

    it('takes every object component from the middle sample', async () => {
      const body = await v1Values(`${POSITION}:middle_index`);
      expect(column(body, 1)).to.deep.equal([{ latitude: 11, longitude: 21 }]);
    });

    // Use case: an install running without the SQLite buffer queries a
    // position track. With parquet as the only source, the object-path query
    // once selected the raw value_* columns from a subquery that exposes the
    // component names, failed to bind, and came back as empty data for every
    // aggregate method.
    it('returns object paths when parquet is the only source', async () => {
      const body = await v1Values(POSITION);
      // Average of the three fixture positions.
      expect(column(body, 1)).to.deep.equal([{ latitude: 11, longitude: 21 }]);
    });

    it('picks the middle bucket of an aggregated tier', async () => {
      // Once a tier=5s directory exists, a 60s-resolution query reads the 5s
      // tier, whose rows are pre-aggregated buckets keyed by bucket_time.
      const dir = new HivePathBuilder().buildPath(
        host.dataDir,
        '5s',
        STORED_CONTEXT,
        VOLTAGE,
        DAY
      );
      await fs.ensureDir(dir);
      const rows = [12.1, 12.2, 12.3, 12.4, 12.5].map((avg, i) => {
        const ts = `2024-06-01 10:00:${String(i * 5).padStart(2, '0')}`;
        return `(TIMESTAMP '${ts}', '${STORED_CONTEXT}', '${VOLTAGE}', ${avg}::DOUBLE, ${avg}::DOUBLE, ${avg}::DOUBLE, 5::BIGINT, TIMESTAMP '${ts}', TIMESTAMP '${ts}')`;
      });
      const conn = await DuckDBPool.getConnection();
      try {
        await conn.runAndReadAll(
          `COPY (SELECT * FROM (VALUES ${rows.join(',')}) AS t(
             bucket_time, context, path, value_avg, value_min, value_max,
             sample_count, first_timestamp, last_timestamp
           )) TO '${toSqlPath(path.join(dir, 'signalk_data_5s.parquet'))}' (FORMAT PARQUET);`
        );
      } finally {
        conn.disconnectSync();
      }

      const body = await v1Values(`${VOLTAGE}:middle_index`);
      expect(column(body, 1)).to.deep.equal([12.3]);
    });

    // Use case: a parquet file for the path is unreadable (truncated or
    // half-written) while the SQLite buffer still holds the samples. The
    // buffer-only fallback must apply the requested method, not a fixed
    // average.
    it('applies middle_index in the buffer-only fallback', async () => {
      // One 10:00 bucket; in time order the samples are [10, 50, 20, 40, 30],
      // so the middle is 20 while their average is 30.
      [10, 50, 20, 40, 30].forEach((value, i) => {
        buffer.insert(
          makeScalarRecord(
            STORED_CONTEXT,
            DEPTH,
            value,
            `2024-06-01T10:00:${String(i * 10).padStart(2, '0')}.000Z`
          )
        );
      });
      const dayDir = new HivePathBuilder().buildPath(
        host.dataDir,
        'raw',
        STORED_CONTEXT,
        DEPTH,
        DAY
      );
      await fs.ensureDir(dayDir);
      await fs.writeFile(
        path.join(dayDir, 'signalk_data_corrupt.parquet'),
        'this is not a parquet file'
      );
      DuckDBPool.initializeSQLiteBuffer(path.join(host.dataDir, 'buffer.db'));
      await stopServer();
      await startServer(buffer);

      const body = await v1Values(`${DEPTH}:middle_index`);
      expect(column(body, 1)).to.deep.equal([20]);
    });
  });

  describe('v2 provider', () => {
    it('returns the middle sample of each bucket for a scalar path', async () => {
      const res = await newProvider().getValues(v2Request(TEMPERATURE));
      expect(res.data.map(row => row[1])).to.deep.equal(EXPECTED_TEMPERATURE);
    });

    it('takes both position coordinates from the middle sample', async () => {
      const res = await newProvider().getValues(v2Request(POSITION));
      // The provider returns position as [longitude, latitude].
      expect(res.data.map(row => row[1])).to.deep.equal([[21, 11]]);
    });
  });
});
