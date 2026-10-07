/**
 * The aggregation worker writes the same tiers as aggregating in-process.
 * The startup catch-up moved from the second to the first (#142), so what it
 * produces must not change: the same day, exported once and copied to two
 * data directories, is aggregated in this process into one and through the
 * forked worker into the other, and every tier's rows are compared.
 *
 * The worker is forked as TypeScript, under the tsx loader the suite runs on.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as path from 'path';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import {
  AggregationConfig,
  AggregationService,
} from '../../src/services/aggregation-service';
import { runAggregationInWorker } from '../../src/services/aggregation-runner';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { filesFor, readParquetSql } from '../../src/utils/parquet-files';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makePositionRecord, makeScalarRecord } from './helpers/records';

const SELF = 'vessels.runnerself';
const DAY = new Date('2024-06-01T00:00:00.000Z');
const HEADING = 'navigation.headingTrue';

describe('aggregation in the worker', function () {
  this.timeout(120000);

  let host: FakeSignalK;
  let inProcessDir: string;
  let workerDir: string;

  const config = (dir: string): AggregationConfig => ({
    outputDirectory: dir,
    filenamePrefix: 'signalk_data',
    retentionDays: { raw: 0, '5s': 0, '60s': 0, '1h': 0 },
  });

  before(async () => {
    host = createFakeSignalK({
      // Heading is angular: both sides must average it on the circle.
      metadata: { [`${SELF}.${HEADING}`]: { units: 'rad' } },
    });
    inProcessDir = path.join(host.dataDir, 'in-process');
    workerDir = path.join(host.dataDir, 'worker');
    const buffer = new SQLiteBuffer({
      dbPath: path.join(host.dataDir, 'buffer.db'),
    });
    const exporter = new ParquetExportService(
      buffer,
      new ParquetWriter({ format: 'parquet', app: host.app }),
      {
        outputDirectory: inProcessDir,
        filenamePrefix: 'signalk_data',
        useHivePartitioning: true,
        dailyExportHour: 4,
      },
      host.app
    );
    // Three hours, a sample every 7 s: several buckets in every tier, and a
    // heading that crosses north, where a linear average would be wrong.
    const start = DAY.getTime() + 10 * 3600_000;
    for (let i = 0; i < (3 * 3600) / 7; i++) {
      const iso = new Date(start + i * 7000).toISOString();
      buffer.insert(
        makeScalarRecord(SELF, 'navigation.speedOverGround', 5 + Math.sin(i / 50), iso)
      );
      buffer.insert(
        makeScalarRecord(SELF, HEADING, (2 * Math.PI + (i % 40) / 100 - 0.2) % (2 * Math.PI), iso)
      );
      buffer.insert(makePositionRecord(SELF, 40.6 + i / 1e5, -73.9 - i / 1e5, iso));
    }
    await exporter.exportDayToParquet(DAY);
    buffer.close();
    await fs.copy(inProcessDir, workerDir);
    await DuckDBPool.initialize();
  });

  after(async () => {
    await DuckDBPool.shutdown();
    await host.cleanup();
  });

  /** Every row of one tier, as JSON text, in a stable order. */
  async function tierRows(dataDir: string, tier: string): Promise<string[]> {
    const files = await filesFor({ dataDir, tier, contexts: 'all', paths: 'all' });
    if (files.length === 0) return [];
    const connection = await DuckDBPool.getConnection();
    try {
      const reader = await connection.runAndReadAll(
        `SELECT * FROM ${readParquetSql(files, { filename: true })}`
      );
      return reader
        .getRowObjects()
        .map(row => {
          // The file a row is in: its place in the tree, not its name, which
          // carries the time it was written.
          const { filename, ...rest } = row;
          const dir = path.relative(dataDir, path.dirname(String(filename)));
          return JSON.stringify({ dir, ...rest }, (_k, v) =>
            typeof v === 'bigint' ? v.toString() : v
          );
        })
        .sort();
    } finally {
      connection.disconnectSync();
    }
  }

  it('writes the same rows to every tier as aggregating in-process', async () => {
    const inProcess = await new AggregationService(
      config(inProcessDir),
      host.app
    ).aggregateDate(DAY);
    expect(inProcess.flatMap(r => r.errors)).to.deep.equal([]);

    const viaWorker = await runAggregationInWorker(
      {
        config: config(workerDir),
        dataDir: workerDir,
        dateISO: DAY.toISOString(),
        angularPathNames: [HEADING],
      },
      host.app,
      undefined,
      {
        workerPath: path.join(__dirname, '..', '..', 'src', 'aggregation-worker.ts'),
        execArgv: ['-r', 'tsx/cjs'],
      }
    );
    expect(viaWorker.flatMap(r => r.errors)).to.deep.equal([]);

    for (const tier of ['5s', '60s', '1h']) {
      const a = await tierRows(inProcessDir, tier);
      const b = await tierRows(workerDir, tier);
      expect(a.length, `${tier} has rows`).to.be.greaterThan(0);
      expect(b, `${tier} rows`).to.deep.equal(a);
    }
  });
});
