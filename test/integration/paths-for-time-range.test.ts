/**
 * The v1 paths endpoint with a time range: which paths of a context have a
 * row in [from, to). Answered from the window's files' footers (Phase 3b of
 * the plan in other.md), with DuckDB kept only for files whose footers cannot
 * say. Pins: day narrowing, the half-open bound at `to`, an unreadable file
 * excluding its path rather than failing the request, and an empty window.
 */
import { expect } from 'chai';
import express from 'express';
import type { Server } from 'http';
import type { AddressInfo } from 'net';
import * as fs from 'fs-extra';
import * as path from 'path';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { registerHistoryApiRoute } from '../../src/HistoryAPI';
import { filesFor } from '../../src/utils/parquet-files';
import { clearAllCaches } from '../../src/utils/path-cache';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makeScalarRecord } from './helpers/records';

const SELF_ID = 'pathsrangeself';
const STORED_CONTEXT = `vessels.${SELF_ID}`;
const SPEED = 'navigation.speedOverGround';
const HEADING = 'navigation.headingTrue';
const DEPTH = 'environment.depth.belowTransducer';

describe('History API v1 paths for a time range', function () {
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
    selfContext: STORED_CONTEXT,
  };

  beforeEach(async () => {
    host = createFakeSignalK({ selfId: SELF_ID });
    buffer = new SQLiteBuffer({ dbPath: path.join(host.dataDir, 'buffer.db') });
    await DuckDBPool.initialize();
    clearAllCaches();

    const writer = new ParquetWriter({ format: 'parquet', app: host.app });
    const exportService = new ParquetExportService(
      buffer,
      writer,
      { outputDirectory: host.dataDir, filenamePrefix: 'signalk_data', useHivePartitioning: true, dailyExportHour: 4 },
      host.app
    );
    // Speed on June 1, heading on June 2 only, depth as one row exactly at
    // noon on June 1.
    buffer.insert(makeScalarRecord(STORED_CONTEXT, SPEED, 5, '2024-06-01T10:00:00.000Z'));
    buffer.insert(makeScalarRecord(STORED_CONTEXT, HEADING, 1.5, '2024-06-02T10:00:00.000Z'));
    buffer.insert(makeScalarRecord(STORED_CONTEXT, DEPTH, 12, '2024-06-01T12:00:00.000Z'));
    await exportService.exportDayToParquet(new Date('2024-06-01T00:00:00.000Z'));
    await exportService.exportDayToParquet(new Date('2024-06-02T00:00:00.000Z'));

    const app = express();
    const router = express.Router();
    registerHistoryApiRoute(router, SELF_ID, host.dataDir, () => {}, queryApp, undefined, undefined);
    app.use(router);
    await new Promise<void>(resolve => {
      server = app.listen(0, '127.0.0.1', () => resolve());
    });
    baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });

  afterEach(async () => {
    clearAllCaches();
    if (server) await new Promise<void>(resolve => server.close(() => resolve()));
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  async function pathsFor(from: string, to: string): Promise<string[]> {
    const res = await fetch(
      `${baseUrl}/signalk/v1/history/paths?context=vessels.self&from=${from}&to=${to}`
    );
    expect(res.status).to.equal(200);
    return (await res.json()) as string[];
  }

  it('lists only the paths with a row in the window, sorted', async () => {
    expect(await pathsFor('2024-06-01T00:00:00Z', '2024-06-02T00:00:00Z')).to.deep.equal([DEPTH, SPEED]);
    expect(await pathsFor('2024-06-01T00:00:00Z', '2024-06-03T00:00:00Z')).to.deep.equal([DEPTH, HEADING, SPEED]);
    expect(await pathsFor('2024-06-02T00:00:00Z', '2024-06-03T00:00:00Z')).to.deep.equal([HEADING]);
  });

  it('treats the window as [from, to): a row exactly at `to` does not count', async () => {
    expect(await pathsFor('2024-06-01T00:00:00Z', '2024-06-01T12:00:00Z')).to.deep.equal([SPEED]);
    expect(await pathsFor('2024-06-01T12:00:00Z', '2024-06-02T00:00:00Z')).to.deep.equal([DEPTH]);
  });

  it('is empty for a window with no files', async () => {
    expect(await pathsFor('2024-06-05T00:00:00Z', '2024-06-06T00:00:00Z')).to.deep.equal([]);
  });

  it('still lists a path whose year has been compacted into one file', async () => {
    // Move the speed day file up into its year directory under the
    // compaction output name and remove the day directory, as a compaction
    // leaves things.
    const [speedFile] = await filesFor({ dataDir: host.dataDir, contexts: [STORED_CONTEXT], paths: [SPEED] });
    const yearDir = path.dirname(path.dirname(speedFile));
    await fs.move(speedFile, path.join(yearDir, 'year_compact_2024_20250101T0000_test.parquet'));
    await fs.remove(path.dirname(speedFile));
    expect(await pathsFor('2024-06-01T00:00:00Z', '2024-06-02T00:00:00Z')).to.deep.equal([DEPTH, SPEED]);
  });

  it('excludes a path whose file cannot be read, rather than failing the request', async () => {
    const speedFiles = await filesFor({ dataDir: host.dataDir, contexts: [STORED_CONTEXT], paths: [SPEED] });
    expect(speedFiles).to.not.be.empty;
    for (const file of speedFiles) await fs.writeFile(file, 'not a parquet file');
    expect(await pathsFor('2024-06-01T00:00:00Z', '2024-06-02T00:00:00Z')).to.deep.equal([DEPTH]);
  });
});
