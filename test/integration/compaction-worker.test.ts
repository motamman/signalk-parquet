/**
 * Compaction runs in a forked worker (compaction-worker.ts) so the DuckDB
 * memory a merge takes leaves with the child. The parent mirrors the
 * worker's progress into its own job table, so callers see the same
 * CompactionProgress as an in-process run. The worker is forked as
 * TypeScript here under the tsx loader the suite already runs on.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as path from 'path';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { CompactionService } from '../../src/services/compaction-service';
import { AggregationService } from '../../src/services/aggregation-service';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { filesFor } from '../../src/utils/parquet-files';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makeScalarRecord } from './helpers/records';

const SELF_ID = 'compactworkerself';
const STORED_CONTEXT = `vessels.${SELF_ID}`;
const SOG = 'navigation.speedOverGround';

describe('compaction in a forked worker', function () {
  this.timeout(60000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;

  beforeEach(async () => {
    host = createFakeSignalK({ selfId: SELF_ID });
    buffer = new SQLiteBuffer({ dbPath: path.join(host.dataDir, 'buffer.db') });
    const writer = new ParquetWriter({ format: 'parquet', app: host.app });
    const exportService = new ParquetExportService(
      buffer,
      writer,
      { outputDirectory: host.dataDir, filenamePrefix: 'signalk_data', useHivePartitioning: true, dailyExportHour: 4 },
      host.app
    );
    buffer.insert(makeScalarRecord(STORED_CONTEXT, SOG, 5, '2024-06-01T10:00:00.000Z'));
    buffer.insert(makeScalarRecord(STORED_CONTEXT, SOG, 6, '2024-06-02T10:00:00.000Z'));
    await exportService.exportDayToParquet(new Date('2024-06-01T00:00:00.000Z'));
    await exportService.exportDayToParquet(new Date('2024-06-02T00:00:00.000Z'));
  });

  afterEach(async () => {
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  it('aggregates a day out of a compacted year exactly as from its day file', async () => {
    await DuckDBPool.initialize();
    const aggregation = new AggregationService(
      {
        outputDirectory: host.dataDir,
        filenamePrefix: 'signalk_data',
        retentionDays: { raw: 0, '5s': 0, '60s': 0, '1h': 0 },
      },
      host.app
    );
    const day = new Date('2024-06-01T00:00:00.000Z');
    const rows = async (): Promise<unknown[]> => {
      const [out] = await filesFor({ dataDir: host.dataDir, tier: '5s', contexts: [STORED_CONTEXT], paths: [SOG] });
      const conn = await DuckDBPool.getConnection();
      try {
        return (await conn.runAndReadAll(`SELECT * FROM read_parquet('${out.replace(/'/g, "''")}') ORDER BY bucket_time`)).getRowObjects();
      } finally {
        conn.disconnectSync();
      }
    };
    // From the day file.
    let result = await aggregation.aggregateTier('raw', '5s', day);
    expect(result.errors).to.deep.equal([]);
    expect(result.filesCreated).to.equal(1);
    const fromDayFile = await rows();
    expect(fromDayFile).to.have.lengthOf(1);
    // Compact the raw year (both days into one file), then aggregate the
    // same day again from the year file.
    const compaction = new CompactionService(host.app, { inProcess: true });
    const jobId = await compaction.compact({ baseDirectory: host.dataDir, tier: 'raw', beforeYear: 2025 });
    let progress = compaction.getProgress(jobId);
    const deadline = Date.now() + 30000;
    while (progress && (progress.status === 'scanning' || progress.status === 'running') && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 100));
      progress = compaction.getProgress(jobId);
    }
    expect(progress?.status).to.equal('completed');
    result = await aggregation.aggregateTier('raw', '5s', day);
    expect(result.errors).to.deep.equal([]);
    expect(result.filesProcessed, 'the year file is the one source').to.equal(1);
    expect(result.filesCreated).to.equal(1);
    const fromYearFile = await rows();
    expect(JSON.stringify(fromYearFile, (_k, v) => (typeof v === 'bigint' ? Number(v) : v))).to.equal(
      JSON.stringify(fromDayFile, (_k, v) => (typeof v === 'bigint' ? Number(v) : v))
    );
  });

  it('compacts an aggregated tier, ordering by its bucket_time column', async () => {
    // Before 2026-09-17 every aggregated-tier group failed with
    // "signalk_timestamp not found": the COPY ordered by the raw column.
    await DuckDBPool.initialize();
    const aggregation = new AggregationService(
      {
        outputDirectory: host.dataDir,
        filenamePrefix: 'signalk_data',
        retentionDays: { raw: 0, '5s': 0, '60s': 0, '1h': 0 },
      },
      host.app
    );
    for (const day of ['2024-06-01', '2024-06-02']) {
      const result = await aggregation.aggregateTier('raw', '5s', new Date(`${day}T00:00:00.000Z`));
      expect(result.errors).to.deep.equal([]);
      expect(result.filesCreated).to.equal(1);
    }
    expect(await filesFor({ dataDir: host.dataDir, tier: '5s', contexts: [STORED_CONTEXT], paths: [SOG] })).to.have.lengthOf(2);

    const service = new CompactionService(host.app, { inProcess: true });
    const jobId = await service.compact({ baseDirectory: host.dataDir, tier: '5s', beforeYear: 2025 });
    let progress = service.getProgress(jobId);
    const deadline = Date.now() + 30000;
    while (progress && (progress.status === 'scanning' || progress.status === 'running') && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 100));
      progress = service.getProgress(jobId);
    }
    expect(progress?.errors).to.deep.equal([]);
    expect(progress?.status).to.equal('completed');
    expect(progress?.groupsCompacted).to.equal(1);
    const after = await filesFor({ dataDir: host.dataDir, tier: '5s', contexts: [STORED_CONTEXT], paths: [SOG] });
    expect(after).to.have.lengthOf(1);
    expect(path.basename(after[0])).to.match(/^year_compact_2024_/);
  });

  it('merges a year in the child and reports completion to the parent', async () => {
    const before = await filesFor({ dataDir: host.dataDir, contexts: [STORED_CONTEXT], paths: [SOG] });
    expect(before).to.have.lengthOf(2);

    const service = new CompactionService(host.app, {
      workerPath: path.join(__dirname, '..', '..', 'src', 'compaction-worker.ts'),
      workerExecArgv: ['-r', 'tsx/cjs'],
    });
    const jobId = await service.compact({ baseDirectory: host.dataDir, tier: 'raw', beforeYear: 2025 });

    let progress = service.getProgress(jobId);
    const deadline = Date.now() + 50000;
    while (progress && (progress.status === 'scanning' || progress.status === 'running') && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 200));
      progress = service.getProgress(jobId);
    }
    expect(progress?.status, progress?.error).to.equal('completed');
    expect(progress?.groupsCompacted).to.equal(1);
    expect(progress?.filesRemoved).to.equal(2);
    expect(progress?.errors).to.deep.equal([]);

    const after = await filesFor({ dataDir: host.dataDir, contexts: [STORED_CONTEXT], paths: [SOG] });
    expect(after).to.have.lengthOf(1);
    expect(path.basename(after[0])).to.match(/^year_compact_2024_/);
    expect(path.basename(path.dirname(after[0]))).to.equal('year=2024');
    expect(await fs.pathExists(path.dirname(before[0]))).to.equal(false);
  });
});
