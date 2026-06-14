/**
 * End-to-end integration of the storage pipeline with no mocks on the data
 * path: records are inserted into a real SQLite buffer, exported to real
 * Hive-partitioned Parquet files by the ParquetExportService (which drives
 * the real ParquetWriter and @dsnp/parquetjs), and then read back through a
 * real DuckDB instance — both directly from the Parquet files and through the
 * SQLite buffer federation used for live (not-yet-exported) data.
 *
 * This is the plugin's core promise: "data you write can be queried back",
 * exercised against the real native dependencies rather than test doubles.
 */
import { expect } from 'chai';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs-extra';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { HivePathBuilder } from '../../src/utils/hive-path-builder';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makeScalarRecord, makePositionRecord } from './helpers/records';

// A fixed historical day so export (which excludes "today") always includes it
// and the assertions never depend on the wall clock.
const DAY = new Date('2024-06-01T00:00:00.000Z');
const CONTEXT = 'vessels.test-self';

// Thin wrappers binding this suite's context to the shared builders.
const scalarRecord = (
  signalkPath: string,
  value: number,
  isoTime: string,
  sourceLabel?: string
) => makeScalarRecord(CONTEXT, signalkPath, value, isoTime, sourceLabel);

const positionRecord = (latitude: number, longitude: number, isoTime: string) =>
  makePositionRecord(CONTEXT, latitude, longitude, isoTime);

/** DuckDB accepts forward slashes on every platform; Windows paths use \. */
function toGlob(p: string): string {
  return p.replace(/\\/g, '/');
}

describe('storage pipeline (SQLite buffer -> Parquet -> DuckDB)', function () {
  // Native DuckDB init may install the spatial extension on first run.
  this.timeout(30000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;
  let exportService: ParquetExportService;
  const hive = new HivePathBuilder();

  beforeEach(async () => {
    host = createFakeSignalK();
    buffer = new SQLiteBuffer({
      dbPath: path.join(host.dataDir, 'buffer.db'),
    });
    const writer = new ParquetWriter({ format: 'parquet', app: host.app });
    exportService = new ParquetExportService(
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
    await DuckDBPool.initialize();
  });

  afterEach(async () => {
    // Guard each resource so a partial beforeEach failure doesn't mask the
    // real setup error with a teardown throw.
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  it('exports buffered scalar records to a Hive-partitioned parquet tree', async () => {
    for (let i = 0; i < 5; i++) {
      buffer.insert(
        scalarRecord(
          'navigation.speedOverGround',
          5,
          `2024-06-01T10:0${i}:00.000Z`
        )
      );
    }

    const result = await exportService.exportDayToParquet(DAY);

    expect(result.errors).to.deep.equal([]);
    expect(result.recordsExported).to.equal(5);
    expect(result.filesCreated).to.have.lengthOf(1);

    // The file lands under the documented Hive layout.
    const created = result.filesCreated[0];
    expect(created).to.match(/tier=raw/);
    expect(created).to.match(/context=vessels__test-self/);
    expect(created).to.match(/path=navigation__speedOverGround/);
    expect(created).to.match(/year=2024/);
    expect(created).to.match(/day=153/); // 2024-06-01 is day 153 of a leap year
    expect(await fs.pathExists(created)).to.equal(true);
  });

  it('reads exported scalar values back through DuckDB', async () => {
    for (let i = 0; i < 5; i++) {
      buffer.insert(
        scalarRecord(
          'navigation.speedOverGround',
          4 + i, // 4,5,6,7,8 -> avg 6
          `2024-06-01T10:0${i}:00.000Z`
        )
      );
    }
    await exportService.exportDayToParquet(DAY);

    const glob = toGlob(
      path.join(
        host.dataDir,
        'tier=raw',
        'context=vessels__test-self',
        'path=navigation__speedOverGround',
        'year=2024',
        'day=153',
        '*.parquet'
      )
    );
    const conn = await DuckDBPool.getConnection();
    try {
      const res = await conn.runAndReadAll(
        `SELECT COUNT(*) AS n, AVG(TRY_CAST(value AS DOUBLE)) AS avg_value
         FROM read_parquet('${glob}')`
      );
      const row = res.getRowObjects()[0] as { n: bigint; avg_value: number };
      expect(Number(row.n)).to.equal(5);
      expect(Number(row.avg_value)).to.equal(6);
    } finally {
      conn.disconnectSync();
    }
  });

  it('round-trips object (position) records with component columns', async () => {
    buffer.insert(positionRecord(47.5, 8.7, '2024-06-01T10:00:00.000Z'));
    buffer.insert(positionRecord(47.6, 8.8, '2024-06-01T10:01:00.000Z'));

    const result = await exportService.exportDayToParquet(DAY);
    expect(result.recordsExported).to.equal(2);

    const glob = toGlob(
      path.join(
        host.dataDir,
        'tier=raw',
        'context=vessels__test-self',
        'path=navigation__position',
        '**',
        '*.parquet'
      )
    );
    const conn = await DuckDBPool.getConnection();
    try {
      const res = await conn.runAndReadAll(
        `SELECT
           COUNT(*) AS n,
           MIN(TRY_CAST(value_latitude AS DOUBLE)) AS min_lat,
           MAX(TRY_CAST(value_longitude AS DOUBLE)) AS max_lon
         FROM read_parquet('${glob}')`
      );
      const row = res.getRowObjects()[0] as {
        n: bigint;
        min_lat: number;
        max_lon: number;
      };
      expect(Number(row.n)).to.equal(2);
      expect(Number(row.min_lat)).to.be.closeTo(47.5, 1e-9);
      expect(Number(row.max_lon)).to.be.closeTo(8.8, 1e-9);
    } finally {
      conn.disconnectSync();
    }
  });

  it('separates distinct paths into distinct parquet files', async () => {
    buffer.insert(
      scalarRecord('navigation.speedOverGround', 3, '2024-06-01T10:00:00.000Z')
    );
    buffer.insert(
      scalarRecord(
        'environment.depth.belowTransducer',
        12,
        '2024-06-01T10:00:00.000Z'
      )
    );

    const result = await exportService.exportDayToParquet(DAY);

    expect(result.recordsExported).to.equal(2);
    expect(result.filesCreated).to.have.lengthOf(2);
    expect(
      result.filesCreated.some(f => /path=navigation__speedOverGround/.test(f))
    ).to.equal(true);
    expect(
      result.filesCreated.some(f =>
        /path=environment__depth__belowTransducer/.test(f)
      )
    ).to.equal(true);
  });

  it('marks exported records so a second export is a no-op', async () => {
    buffer.insert(
      scalarRecord('navigation.speedOverGround', 5, '2024-06-01T10:00:00.000Z')
    );

    const first = await exportService.exportDayToParquet(DAY);
    expect(first.recordsExported).to.equal(1);

    const second = await exportService.exportDayToParquet(DAY);
    expect(second.recordsExported).to.equal(0);
    expect(second.filesCreated).to.deep.equal([]);
  });

  it('queries not-yet-exported records through the SQLite buffer federation', async () => {
    // Live data still in the buffer must be queryable before any export
    // runs; this is the path the History API uses for "today".
    buffer.insert(
      scalarRecord('navigation.speedOverGround', 9, '2024-06-01T10:00:00.000Z')
    );
    buffer.insert(
      scalarRecord('navigation.speedOverGround', 11, '2024-06-01T10:01:00.000Z')
    );
    buffer.checkpoint(); // flush WAL so the read-only ATTACH sees the rows

    DuckDBPool.initializeSQLiteBuffer(buffer.getDbPath());
    const conn = await DuckDBPool.getConnectionWithBuffer();
    try {
      const res = await conn.runAndReadAll(
        `SELECT AVG(TRY_CAST(value AS DOUBLE)) AS avg_value
         FROM buffer.buffer_navigation_speedOverGround
         WHERE exported = 0`
      );
      const row = res.getRowObjects()[0] as { avg_value: number };
      expect(Number(row.avg_value)).to.equal(10);
    } finally {
      conn.disconnectSync();
    }
  });

  it('builds a DuckDB glob for the day that DuckDB can read', async () => {
    buffer.insert(
      scalarRecord('navigation.speedOverGround', 7, '2024-06-01T10:00:00.000Z')
    );
    await exportService.exportDayToParquet(DAY);

    // Mirror how the read path locates files for a single day.
    const glob = hive.buildDuckDBGlob(
      host.dataDir,
      'raw',
      CONTEXT,
      'navigation.speedOverGround',
      DAY,
      DAY
    );
    const globs = Array.isArray(glob) ? glob : [glob];
    const conn = await DuckDBPool.getConnection();
    try {
      let total = 0;
      for (const g of globs) {
        const res = await conn.runAndReadAll(
          `SELECT COUNT(*) AS n FROM read_parquet('${toGlob(g)}')`
        );
        total += Number((res.getRowObjects()[0] as { n: bigint }).n);
      }
      expect(total).to.equal(1);
    } finally {
      conn.disconnectSync();
    }
  });
});
