/**
 * Regression tests for issue #71: context ids containing literal dashes
 * (SignalK's default UUID vessel ids) must survive the contexts round-trip.
 *
 * The hive directory encoding is lossy — sanitizeContext maps both ':' and
 * (implicitly) literal '-' onto '-', so unsanitizeContext turned a UUID's
 * dashes into colons. The fix stops reconstructing identity from directory
 * names: the contexts listing resolves the true string from the parquet
 * `context` data column, and the spatial endpoint reads that column directly.
 */
import { expect } from 'chai';
import express from 'express';
import { Server } from 'http';
import { AddressInfo } from 'net';
import * as path from 'path';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { registerHistoryApiRoute } from '../../src/HistoryAPI';
import { clearFileListCache } from '../../src/utils/context-discovery';
import { clearAllCaches } from '../../src/utils/path-cache';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makeScalarRecord, makePositionRecord } from './helpers/records';

const SELF_ID = 'urn:mrn:signalk:uuid:c0d79334-4e25-4149-a2b8-1f7a3210ab42';
// The regression case: a default (no-MMSI) SignalK install's UUID vessel id.
const UUID_CONTEXT = `vessels.${SELF_ID}`;
// The control: MMSI ids carry no literal dashes and always round-tripped fine.
const MMSI_CONTEXT = 'vessels.urn:mrn:imo:mmsi:368076430';
const DAY = new Date('2024-06-01T00:00:00.000Z');
const RANGE = 'from=2024-06-01T00:00:00Z&to=2024-06-01T23:59:59Z';
// Matches the position fixtures below (New York harbor-ish).
const BBOX = 'bbox=-74.5,40.2,-73.8,40.9';

describe('Context discovery with dash-bearing ids (issue #71)', function () {
  this.timeout(30000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;
  let server: Server;
  let baseUrl: string;

  const queryApp = {
    debug: () => {},
    error: () => {},
    getMetadata: () => undefined,
    getSelfPath: () => undefined,
    selfId: SELF_ID,
    selfContext: UUID_CONTEXT,
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

    // Scalar + position data for the UUID vessel, position for the MMSI one.
    buffer.insert(
      makeScalarRecord(
        UUID_CONTEXT,
        'navigation.speedOverGround',
        5,
        '2024-06-01T10:00:00.000Z'
      )
    );
    buffer.insert(
      makePositionRecord(UUID_CONTEXT, 40.6, -73.98, '2024-06-01T10:00:00.000Z')
    );
    buffer.insert(
      makePositionRecord(MMSI_CONTEXT, 40.65, -74.0, '2024-06-01T10:00:00.000Z')
    );
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
    const addr = server.address() as AddressInfo;
    baseUrl = `http://127.0.0.1:${addr.port}`;
  });

  afterEach(async () => {
    clearFileListCache();
    clearAllCaches();
    if (server)
      await new Promise<void>(resolve => server.close(() => resolve()));
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  it('contexts listing returns the exact UUID context string', async () => {
    const res = await fetch(`${baseUrl}/signalk/v1/history/contexts?${RANGE}`);
    expect(res.status).to.equal(200);
    const contexts = (await res.json()) as string[];
    // Before the fix this came back with the UUID's dashes turned into
    // colons: vessels.urn:mrn:signalk:uuid:c0d79334:4e25:4149:...
    expect(contexts).to.include(UUID_CONTEXT);
    expect(contexts).to.include(MMSI_CONTEXT);
    for (const c of contexts) {
      expect(c).to.not.match(/uuid:[0-9a-f]+:/i);
    }
  });

  it('spatial contexts endpoint returns the exact UUID context string', async () => {
    const res = await fetch(
      `${baseUrl}/api/history/contexts/spatial?${RANGE}&${BBOX}`
    );
    expect(res.status).to.equal(200);
    const contexts = (await res.json()) as string[];
    expect(contexts).to.include(UUID_CONTEXT);
    // MMSI contexts were also corrupted here pre-fix only via the double
    // unsanitize when they contained dashes post-sanitize; assert exactness.
    expect(contexts).to.include(MMSI_CONTEXT);
  });
});
