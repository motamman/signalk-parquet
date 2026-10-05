/**
 * Small leaks on rare actions and error paths (#143): what a failed write
 * leaves behind, the sandbox DuckDB instance made once, bulk aggregations
 * stopped with the plugin, job message lists that stay short, and the
 * aggregation routes following the plugin's current configuration.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as path from 'path';
import { Router } from 'express';
import { DuckDBInstance } from '@duckdb/node-api';
import { ParquetWriter } from '../../src/parquet-writer';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import {
  AggregationService,
  cancelAllBulkAggregations,
} from '../../src/services/aggregation-service';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { JOB_MESSAGES_KEPT, keepMessage } from '../../src/utils/job-messages';
import { registerApiRoutes } from '../../src/api-routes';
import { filesFor } from '../../src/utils/parquet-files';
import type { PluginState } from '../../src/types';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makeScalarRecord } from './helpers/records';

const SELF = 'vessels.releaseself';
const SOG = 'navigation.speedOverGround';

/** Export one record per day for `days`, from 2024-06-01, into `dataDir`. */
async function exportDays(
  host: FakeSignalK,
  dataDir: string,
  days: number
): Promise<void> {
  const buffer = new SQLiteBuffer({ dbPath: path.join(dataDir, 'buffer.db') });
  const exporter = new ParquetExportService(
    buffer,
    new ParquetWriter({ format: 'parquet', app: host.app }),
    {
      outputDirectory: dataDir,
      filenamePrefix: 'signalk_data',
      useHivePartitioning: true,
      dailyExportHour: 4,
    },
    host.app
  );
  for (let d = 0; d < days; d++) {
    const day = new Date(Date.UTC(2024, 5, 1 + d));
    for (let i = 0; i < 10; i++) {
      buffer.insert(
        makeScalarRecord(SELF, SOG, i, new Date(day.getTime() + 36e5 + i * 1000).toISOString())
      );
    }
    await exporter.exportDayToParquet(day);
  }
  buffer.close();
}

describe('resources released on rare actions and error paths (#143)', function () {
  this.timeout(60000);

  let host: FakeSignalK;

  beforeEach(() => {
    host = createFakeSignalK();
  });

  afterEach(async () => {
    await DuckDBPool.shutdown();
    await host.cleanup();
  });

  it('leaves no partial parquet file when a row fails to write', async () => {
    const writer = new ParquetWriter({ format: 'parquet', app: host.app });
    const original = writer.prepareRecordForParquet.bind(writer);
    let calls = 0;
    writer.prepareRecordForParquet = (record, schema) => {
      if (++calls === 2) throw new Error('row two cannot be written');
      return original(record, schema);
    };
    const file = path.join(host.dataDir, 'out', 'speed.parquet');
    await fs.ensureDir(path.dirname(file));
    const records = [0, 1, 2].map(i =>
      makeScalarRecord(SELF, SOG, i, `2024-06-01T10:00:0${i}.000Z`)
    );

    let failed = false;
    try {
      await writer.writeRecords(file, records);
    } catch {
      failed = true;
    }
    expect(failed).to.equal(true);
    expect(await fs.pathExists(file), 'no partial file at the final name').to.equal(false);
    // The records are kept as JSON, as before.
    expect(
      await fs.pathExists(path.join(path.dirname(file), 'failed', 'speed_FAILED.json'))
    ).to.equal(true);
  });

  describe('the sandbox DuckDB instance', () => {
    it('is created once for callers that arrive together', async () => {
      await DuckDBPool.initialize();
      // Count the instances made: two first callers each made one before,
      // and only one of them was ever kept, and closed.
      const create = DuckDBInstance.create;
      let created = 0;
      DuckDBInstance.create = ((...args: Parameters<typeof create>) => {
        created++;
        return create.apply(DuckDBInstance, args);
      }) as typeof create;
      try {
        const connections = await Promise.all([
          DuckDBPool.getSandboxConnection(host.dataDir),
          DuckDBPool.getSandboxConnection(host.dataDir),
          DuckDBPool.getSandboxConnection(host.dataDir),
        ]);
        for (const c of connections) c.disconnectSync();
      } finally {
        DuckDBInstance.create = create;
      }
      expect(created).to.equal(1);
    });

    it('is not made once the pool has been shut down', async () => {
      await DuckDBPool.initialize();
      await DuckDBPool.shutdown();
      let error: Error | undefined;
      try {
        await DuckDBPool.getSandboxConnection(host.dataDir);
      } catch (err) {
        error = err as Error;
      }
      expect(error?.message).to.match(/not initialized/);
    });
  });

  it('stops a running bulk aggregation when the plugin stops', async () => {
    await exportDays(host, host.dataDir, 3);
    await DuckDBPool.initialize();
    const service = new AggregationService(
      {
        outputDirectory: host.dataDir,
        filenamePrefix: 'signalk_data',
        retentionDays: { raw: 0, '5s': 0, '60s': 0, '1h': 0 },
      },
      host.app
    );
    const jobId = service.startBulkAggregation();
    await cancelAllBulkAggregations();
    const progress = service.getBulkProgress(jobId)!;
    expect(progress.status).to.equal('cancelled');
    expect(progress.datesProcessed).to.be.below(3);
  });

  it('keeps the first messages of a job and no more', () => {
    const kept: string[] = [];
    for (let i = 0; i < JOB_MESSAGES_KEPT + 50; i++) keepMessage(kept, `error ${i}`);
    expect(kept).to.have.lengthOf(JOB_MESSAGES_KEPT);
    expect(kept[0]).to.equal('error 0');
  });

  it('aggregates into the directory the plugin is configured with now', async () => {
    const first = path.join(host.dataDir, 'first');
    const second = path.join(host.dataDir, 'second');
    await exportDays(host, first, 1);
    await exportDays(host, second, 1);
    await DuckDBPool.initialize();

    // The routes are registered once; the configuration changes under them.
    let dataDir = first;
    const state = {
      getDataDirPath: () => dataDir,
      get currentConfig() {
        return { outputDirectory: dataDir, retentionDays: 0 };
      },
    } as unknown as PluginState;
    const posts = new Map<string, (req: unknown, res: unknown) => Promise<void>>();
    const noop = () => router;
    const router = {
      use: noop,
      get: noop,
      put: noop,
      delete: noop,
      patch: noop,
      post: (p: string, ...handlers: Array<(req: unknown, res: unknown) => Promise<void>>) => {
        posts.set(p, handlers[handlers.length - 1]);
        return router;
      },
    } as unknown as Router;
    registerApiRoutes(router, state, host.app);
    const aggregate = async () => {
      let body: { success?: boolean } = {};
      const res = {
        status: () => res,
        json: (b: { success?: boolean }) => {
          body = b;
          return res;
        },
      };
      await posts.get('/api/aggregate')!({ body: { date: '2024-06-01' } }, res);
      expect(body.success).to.equal(true);
    };
    const fiveSecondFiles = (dir: string) =>
      filesFor({ dataDir: dir, tier: '5s', contexts: 'all', paths: 'all' });

    await aggregate();
    expect(await fiveSecondFiles(first)).to.have.lengthOf(1);

    dataDir = second;
    await aggregate();
    expect(await fiveSecondFiles(second), 'into the new directory').to.have.lengthOf(1);
  });
});
