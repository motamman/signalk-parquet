/**
 * Two defects the Phase 3a byte-compare surfaced on 2026-09-17, pinned on
 * both routes:
 *
 * - `first` and `last` must be the chronologically first and last sample of
 *   the bucket. DuckDB's bare FIRST()/LAST() pick an arbitrary row and the
 *   answer changed between runs (11 of 281 buckets on a shore station).
 * - The window is [from, to) on the stored millisecond timestamps. A bound
 *   spelled without milliseconds ('…00Z') sorted above '…00.000Z' as a
 *   string, so a row half a second after `from` fell out and a row exactly
 *   at `to` fell in.
 *
 * One 60 s bucket holds four samples out of time order in the file, plus one
 * sample exactly at `to`. Expected: first 0.5, last 3, average 1.625, one
 * row, on both the v1 route and the v2 provider.
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
import { HistoryProvider } from '../../src/history-provider';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makeScalarRecord } from './helpers/records';
import type { ValuesRequest } from '@signalk/server-api/dist/history';

const SELF_ID = 'firstlastself';
const STORED_CONTEXT = `vessels.${SELF_ID}`;
const DAY = new Date('2024-06-01T00:00:00.000Z');
const PATH = 'navigation.speedOverGround';
const FROM = '2024-06-01T10:00:00Z';
const TO = '2024-06-01T10:05:00Z';

describe('History API first/last ordering and window bounds', function () {
  this.timeout(30000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;
  let server: Server;
  let baseUrl: string;
  let provider: HistoryProvider;

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

    // Inserted out of time order so a "physically first" pick differs from
    // the chronologically first one.
    buffer.insert(makeScalarRecord(STORED_CONTEXT, PATH, 2, '2024-06-01T10:00:40.000Z'));
    buffer.insert(makeScalarRecord(STORED_CONTEXT, PATH, 3, '2024-06-01T10:00:55.000Z'));
    buffer.insert(makeScalarRecord(STORED_CONTEXT, PATH, 1, '2024-06-01T10:00:10.000Z'));
    // Half a second after `from`: inside the window.
    buffer.insert(makeScalarRecord(STORED_CONTEXT, PATH, 0.5, '2024-06-01T10:00:00.500Z'));
    // Exactly at `to`: outside the half-open window.
    buffer.insert(makeScalarRecord(STORED_CONTEXT, PATH, 9, '2024-06-01T10:05:00.000Z'));
    await exportService.exportDayToParquet(DAY);

    const app = express();
    const router = express.Router();
    registerHistoryApiRoute(router, SELF_ID, host.dataDir, () => {}, queryApp, undefined, undefined);
    app.use(router);
    await new Promise<void>(resolve => {
      server = app.listen(0, '127.0.0.1', () => resolve());
    });
    baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

    provider = new HistoryProvider(SELF_ID, host.dataDir, queryApp, () => {});
  });

  afterEach(async () => {
    if (server) await new Promise<void>(resolve => server.close(() => resolve()));
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  it('v1 route: first and last are chronological, and the window is [from, to)', async () => {
    const params = new URLSearchParams({
      from: FROM,
      to: TO,
      context: 'vessels.self',
      resolution: '60',
      paths: `${PATH}:first,${PATH}:last,${PATH}`,
    });
    const res = await fetch(`${baseUrl}/signalk/v1/history/values?${params}`);
    expect(res.status).to.equal(200);
    const body = (await res.json()) as { data: Array<[string, ...number[]]> };
    expect(body.data, 'one bucket; the row at `to` is excluded').to.have.lengthOf(1);
    const [, first, last, average] = body.data[0];
    expect(first).to.equal(0.5);
    expect(last).to.equal(3);
    expect(average).to.equal(1.625);
  });

  it('v2 provider: first and last are chronological, and the window is [from, to)', async () => {
    const res = await provider.getValues({
      from: FROM,
      to: TO,
      context: 'vessels.self',
      resolution: 60,
      pathSpecs: [
        { path: PATH, aggregate: 'first', parameter: [] },
        { path: PATH, aggregate: 'last', parameter: [] },
        { path: PATH, aggregate: 'average', parameter: [] },
      ],
    } as unknown as ValuesRequest);
    expect(res.data, 'one bucket; the row at `to` is excluded').to.have.lengthOf(1);
    const [ts, first, last, average] = res.data[0];
    expect(ts).to.equal('2024-06-01T10:00:00Z');
    expect(first).to.equal(0.5);
    expect(last).to.equal(3);
    expect(average).to.equal(1.625);
    // The echoed range keeps js-joda's spelling; only the SQL bounds changed.
    expect(res.range.from).to.equal('2024-06-01T10:00:00Z');
  });
});
