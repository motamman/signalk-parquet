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
import {
  SQLiteBuffer,
  EXPORTED_BUFFER_ONLY,
} from '../../src/utils/sqlite-buffer';
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
const WIND = 'environment.wind.speedApparent';

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
      {
        outputDirectory: host.dataDir,
        filenamePrefix: 'signalk_data',
        useHivePartitioning: true,
        dailyExportHour: 4,
      },
      host.app
    );
    // Speed on June 1, heading on June 2 only, depth as one row exactly at
    // noon on June 1.
    buffer.insert(
      makeScalarRecord(STORED_CONTEXT, SPEED, 5, '2024-06-01T10:00:00.000Z')
    );
    buffer.insert(
      makeScalarRecord(STORED_CONTEXT, HEADING, 1.5, '2024-06-02T10:00:00.000Z')
    );
    buffer.insert(
      makeScalarRecord(STORED_CONTEXT, DEPTH, 12, '2024-06-01T12:00:00.000Z')
    );
    await exportService.exportDayToParquet(
      new Date('2024-06-01T00:00:00.000Z')
    );
    await exportService.exportDayToParquet(
      new Date('2024-06-02T00:00:00.000Z')
    );

    const app = express();
    const router = express.Router();
    registerHistoryApiRoute(
      router,
      SELF_ID,
      host.dataDir,
      () => {},
      queryApp,
      buffer,
      undefined
    );
    app.use(router);
    await new Promise<void>(resolve => {
      server = app.listen(0, '127.0.0.1', () => resolve());
    });
    baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });

  afterEach(async () => {
    clearAllCaches();
    if (server)
      await new Promise<void>(resolve => server.close(() => resolve()));
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
    expect(
      await pathsFor('2024-06-01T00:00:00Z', '2024-06-02T00:00:00Z')
    ).to.deep.equal([DEPTH, SPEED]);
    expect(
      await pathsFor('2024-06-01T00:00:00Z', '2024-06-03T00:00:00Z')
    ).to.deep.equal([DEPTH, HEADING, SPEED]);
    expect(
      await pathsFor('2024-06-02T00:00:00Z', '2024-06-03T00:00:00Z')
    ).to.deep.equal([HEADING]);
  });

  it('treats the window as [from, to): a row exactly at `to` does not count', async () => {
    expect(
      await pathsFor('2024-06-01T00:00:00Z', '2024-06-01T12:00:00Z')
    ).to.deep.equal([SPEED]);
    expect(
      await pathsFor('2024-06-01T12:00:00Z', '2024-06-02T00:00:00Z')
    ).to.deep.equal([DEPTH]);
  });

  it('is empty for a window with no files', async () => {
    expect(
      await pathsFor('2024-06-05T00:00:00Z', '2024-06-06T00:00:00Z')
    ).to.deep.equal([]);
  });

  it('lists a path recorded since the last export, from the buffer, in its window only', async () => {
    // Wind on June 3 has no file yet: it is only in the buffer, as every path
    // is between its first row and its first daily export.
    buffer.insert(
      makeScalarRecord(STORED_CONTEXT, WIND, 7, '2024-06-03T10:00:00.000Z')
    );
    expect(
      await pathsFor('2024-06-03T00:00:00Z', '2024-06-04T00:00:00Z')
    ).to.deep.equal([WIND]);
    expect(
      await pathsFor('2024-06-01T00:00:00Z', '2024-06-02T00:00:00Z')
    ).to.deep.equal([DEPTH, SPEED]);
    // A path in files and in the buffer is listed once.
    buffer.insert(
      makeScalarRecord(STORED_CONTEXT, SPEED, 6, '2024-06-03T11:00:00.000Z')
    );
    clearAllCaches();
    expect(
      await pathsFor('2024-06-01T00:00:00Z', '2024-06-04T00:00:00Z')
    ).to.deep.equal([DEPTH, WIND, HEADING, SPEED]);
  });

  it('lists a path recorded after an identical request was cached', async () => {
    // The listing is cached per (directory, context, window). Parquet only
    // changes at export, so caching it is free — but the buffer gains a path
    // the moment one is recorded, and a combined listing in the cache would
    // hide that path until the entry expired. No cache clearing here on
    // purpose: the second request must see the new path by itself.
    const window = ['2024-06-03T00:00:00Z', '2024-06-04T00:00:00Z'] as const;
    buffer.insert(
      makeScalarRecord(STORED_CONTEXT, WIND, 7, '2024-06-03T10:00:00.000Z')
    );
    expect(await pathsFor(...window)).to.deep.equal([WIND]);

    const TEMP = 'environment.outside.temperature';
    buffer.insert(
      makeScalarRecord(STORED_CONTEXT, TEMP, 291, '2024-06-03T10:30:00.000Z')
    );
    expect(await pathsFor(...window)).to.deep.equal([TEMP, WIND]);
  });

  it('still lists a path after its buffer rows are exported behind a cached listing', async () => {
    // Wind on June 3 is only in the buffer when the window's file listing is
    // cached. The export then writes its file and marks its rows exported,
    // so the per-request buffer probe stops listing it; the export must
    // invalidate the cached listing or the path vanishes until the entry
    // expires. No cache clearing here on purpose.
    const window = ['2024-06-03T00:00:00Z', '2024-06-04T00:00:00Z'] as const;
    buffer.insert(
      makeScalarRecord(STORED_CONTEXT, WIND, 7, '2024-06-03T10:00:00.000Z')
    );
    expect(await pathsFor(...window)).to.deep.equal([WIND]);

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
    const result = await exportService.exportDayToParquet(
      new Date('2024-06-03T00:00:00.000Z')
    );
    expect(result.filesCreated).to.have.lengthOf(1);
    expect(
      buffer.hasRowsInWindow(
        WIND,
        STORED_CONTEXT,
        '2024-06-03T00:00:00.000Z',
        '2024-06-04T00:00:00.000Z'
      )
    ).to.equal(false);

    expect(await pathsFor(...window)).to.deep.equal([WIND]);
  });

  it('does not drop an exported path while the rest of the export runs', async () => {
    // The export loops over paths: each writes its file, then has its buffer
    // rows marked exported. From that moment the buffer probe stops listing
    // the path, so a listing cached before its file existed shows it from
    // neither source — and that lasts until the whole export finishes, which
    // across many paths is not brief. Observed from inside the export by
    // hooking markDateExported and making the request there.
    const window = ['2026-05-01T00:00:00Z', '2026-05-02T00:00:00Z'] as const;
    const A = 'environment.outside.pressure';
    const B = 'environment.outside.humidity';
    buffer.insert(
      makeScalarRecord(STORED_CONTEXT, A, 101300, '2026-05-01T10:00:00.000Z')
    );
    buffer.insert(
      makeScalarRecord(STORED_CONTEXT, B, 0.6, '2026-05-01T10:30:00.000Z')
    );
    // Cache the listing while both paths are buffer-only.
    expect(await pathsFor(...window)).to.deep.equal([B, A]);

    // Proxy the buffer so each markDateExported can be observed mid-export.
    const seenDuringExport: string[][] = [];
    let pending: Promise<void> = Promise.resolve();
    const hooked = new Proxy(buffer, {
      get(target, prop, receiver) {
        if (prop !== 'markDateExported') {
          return Reflect.get(target, prop, receiver);
        }
        return (...args: Parameters<SQLiteBuffer['markDateExported']>) => {
          const out = target.markDateExported(...args);
          // The export is synchronous here on out; queue the observation.
          pending = pending.then(async () => {
            seenDuringExport.push(await pathsFor(...window));
          });
          return out;
        };
      },
    }) as SQLiteBuffer;

    const writer = new ParquetWriter({ format: 'parquet', app: host.app });
    const exportService = new ParquetExportService(
      hooked,
      writer,
      {
        outputDirectory: host.dataDir,
        filenamePrefix: 'signalk_data',
        useHivePartitioning: true,
        dailyExportHour: 4,
      },
      host.app
    );
    const result = await exportService.exportDayToParquet(
      new Date('2026-05-01T00:00:00.000Z')
    );
    expect(result.filesCreated).to.have.lengthOf(2);
    await pending;

    // Every observation taken during the export must still list both paths:
    // the one already exported (now only in files) and the one still pending
    // (still only in the buffer).
    expect(seenDuringExport).to.have.lengthOf(2);
    for (const listed of seenDuringExport) {
      expect(listed, 'a path vanished mid-export').to.include.members([A, B]);
    }
    expect(await pathsFor(...window)).to.deep.equal([B, A]);
  });

  it('lists a buffer-only path for as long as its rows are in the buffer', async () => {
    const record = makeScalarRecord(
      STORED_CONTEXT,
      WIND,
      7,
      '2024-06-03T10:00:00.000Z'
    );
    record.exported = EXPORTED_BUFFER_ONLY;
    buffer.insert(record);
    expect(
      await pathsFor('2024-06-03T00:00:00Z', '2024-06-04T00:00:00Z')
    ).to.deep.equal([WIND]);
  });

  it('lists a path only in the buffer without a window too, for the context it was recorded for', async () => {
    buffer.insert(
      makeScalarRecord(STORED_CONTEXT, WIND, 7, '2024-06-03T10:00:00.000Z')
    );
    buffer.insert(
      makeScalarRecord(
        'vessels.urn:mrn:signalk:uuid:other',
        DEPTH,
        3,
        '2024-06-03T10:00:00.000Z'
      )
    );
    const res = await fetch(
      `${baseUrl}/signalk/v1/history/paths?context=vessels.self`
    );
    expect(res.status).to.equal(200);
    expect((await res.json()) as string[]).to.deep.equal([
      DEPTH,
      WIND,
      HEADING,
      SPEED,
    ]);
    const other = await fetch(
      `${baseUrl}/signalk/v1/history/paths?context=vessels.urn:mrn:signalk:uuid:other`
    );
    expect((await other.json()) as string[]).to.deep.equal([DEPTH]);
  });

  it('still lists a path whose year has been compacted into one file', async () => {
    // Move the speed day file up into its year directory under the
    // compaction output name and remove the day directory, as a compaction
    // leaves things.
    const [speedFile] = await filesFor({
      dataDir: host.dataDir,
      contexts: [STORED_CONTEXT],
      paths: [SPEED],
    });
    const yearDir = path.dirname(path.dirname(speedFile));
    await fs.move(
      speedFile,
      path.join(yearDir, 'year_compact_2024_20250101T0000_test.parquet')
    );
    await fs.remove(path.dirname(speedFile));
    expect(
      await pathsFor('2024-06-01T00:00:00Z', '2024-06-02T00:00:00Z')
    ).to.deep.equal([DEPTH, SPEED]);
  });

  it('excludes a path whose file cannot be read, rather than failing the request', async () => {
    const speedFiles = await filesFor({
      dataDir: host.dataDir,
      contexts: [STORED_CONTEXT],
      paths: [SPEED],
    });
    expect(speedFiles).to.not.be.empty;
    for (const file of speedFiles)
      await fs.writeFile(file, 'not a parquet file');
    expect(
      await pathsFor('2024-06-01T00:00:00Z', '2024-06-02T00:00:00Z')
    ).to.deep.equal([DEPTH]);
  });
});
