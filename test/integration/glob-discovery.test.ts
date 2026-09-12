/**
 * File discovery under a data directory whose name is glob syntax.
 *
 * Every scan used to glob a pattern built with path.join(dataDir, ...). That
 * breaks on Windows, where glob reads the backslashes as escape characters,
 * and on any OS when a directory name is itself glob syntax. The data
 * directory here contains `+(1)`, an extglob group, so these tests reproduce
 * the failure on every platform: before the fix each scan found nothing and
 * each service silently did no work. DuckDB treats `+(1)` literally, so the
 * aggregation step still reads the files it is given.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'os';
import * as path from 'path';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { HivePathBuilder } from '../../src/utils/hive-path-builder';
import { AggregationService } from '../../src/services/aggregation-service';
import {
  CompactionService,
  cleanupStrandedCompactionTempFiles,
  recoverStrandedCompactionTrash,
} from '../../src/services/compaction-service';
import { MigrationService } from '../../src/services/migration-service';
import { GpxImportService } from '../../src/services/gpx-import-service';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makeScalarRecord } from './helpers/records';

const SELF_ID = 'globself';
const STORED_CONTEXT = `vessels.${SELF_ID}`;
const SOG = 'navigation.speedOverGround';
const DAY_ONE = new Date('2024-06-01T00:00:00.000Z');
const DAY_TWO = new Date('2024-06-02T00:00:00.000Z');

describe('file discovery under a data directory named with glob syntax', function () {
  // Native DuckDB init may install the spatial extension on first run.
  this.timeout(30000);

  const builder = new HivePathBuilder();
  let host: FakeSignalK;
  let dataDir: string;

  beforeEach(async () => {
    host = createFakeSignalK({ selfId: SELF_ID });
    // Embedded in a pattern, '+(1)' would match 'sk-glob1-...' but never
    // this directory's own name.
    dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'sk-glob+(1)-'));
    await DuckDBPool.initialize();
  });

  afterEach(async () => {
    await DuckDBPool.shutdown();
    await fs.remove(dataDir);
    await host?.cleanup();
  });

  /** Writes one raw-tier parquet file per day through the export pipeline. */
  async function exportRawDays(days: Date[]): Promise<void> {
    const buffer = new SQLiteBuffer({
      dbPath: path.join(host.dataDir, 'buffer.db'),
    });
    try {
      const exportService = new ParquetExportService(
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
      for (const day of days) {
        const date = day.toISOString().slice(0, 10);
        for (let i = 0; i < 3; i++) {
          buffer.insert(
            makeScalarRecord(
              STORED_CONTEXT,
              SOG,
              5 + i,
              `${date}T10:00:0${i}.000Z`
            )
          );
        }
        await exportService.exportDayToParquet(day);
      }
    } finally {
      buffer.close();
    }
  }

  /** An aggregation service over the glob-syntax data directory. */
  function newAggregationService(rawRetentionDays = 0): AggregationService {
    return new AggregationService(
      {
        outputDirectory: dataDir,
        filenamePrefix: 'signalk_data',
        retentionDays: { raw: rawRetentionDays, '5s': 0, '60s': 0, '1h': 0 },
      },
      host.app
    );
  }

  describe('aggregation', () => {
    it('discovers the recorded raw dates', async () => {
      await exportRawDays([DAY_ONE]);
      const dates = await newAggregationService().discoverRawDates();
      expect(dates.map(d => d.getTime())).to.deep.equal([DAY_ONE.getTime()]);
    });

    it('rolls a raw day up into the 5s tier', async () => {
      await exportRawDays([DAY_ONE]);
      const result = await newAggregationService().aggregateTier(
        'raw',
        '5s',
        DAY_ONE
      );
      expect(result.errors).to.deep.equal([]);
      expect(result.filesProcessed).to.equal(1);
      expect(result.filesCreated).to.equal(1);
      expect(
        await fs.pathExists(
          builder.buildPath(dataDir, '5s', STORED_CONTEXT, SOG, DAY_ONE)
        )
      ).to.equal(true);
    });

    it('deletes raw files past their retention', async () => {
      await exportRawDays([DAY_ONE]);
      const result = await newAggregationService(1).cleanupOldData();
      expect(result.deletedFiles).to.equal(1);
      expect(result.failedFiles).to.equal(0);
    });
  });

  describe('compaction', () => {
    it('finds a year of day partitions to compact', async () => {
      await exportRawDays([DAY_ONE, DAY_TWO]);
      const plan = await new CompactionService(host.app).scan({
        baseDirectory: dataDir,
        tier: 'raw',
        beforeYear: 2025,
      });
      expect(plan.totalGroups).to.equal(1);
      expect(plan.totalSourceFiles).to.equal(2);
    });

    it('removes a temp file stranded by an interrupted compaction', async () => {
      const yearDir = path.dirname(
        builder.buildPath(dataDir, 'raw', STORED_CONTEXT, SOG, DAY_ONE)
      );
      const stranded = path.join(yearDir, 'year_compact_2024.parquet.tmp');
      await fs.ensureDir(yearDir);
      await fs.writeFile(stranded, '');

      const result = await cleanupStrandedCompactionTempFiles(
        host.app,
        dataDir
      );
      expect(result.removed).to.equal(1);
      expect(await fs.pathExists(stranded)).to.equal(false);
    });

    it('restores sources from the trash of an unpublished compaction', async () => {
      const dayDir = builder.buildPath(
        dataDir,
        'raw',
        STORED_CONTEXT,
        SOG,
        DAY_ONE
      );
      const yearDir = path.dirname(dayDir);
      const trashed = path.join(
        yearDir,
        '.compaction-trash-1',
        path.basename(dayDir),
        'a.parquet'
      );
      await fs.ensureDir(path.dirname(trashed));
      await fs.writeFile(trashed, '');

      const result = await recoverStrandedCompactionTrash(host.app, dataDir);
      expect(result).to.deep.equal({ restored: 1, cleaned: 0, failed: 0 });
      expect(await fs.pathExists(path.join(dayDir, 'a.parquet'))).to.equal(
        true
      );
    });
  });

  describe('migration', () => {
    it('counts flat-layout files and skips processed directories', async () => {
      const flatDir = path.join(
        dataDir,
        'vessels',
        'urn_mrn_imo_mmsi_1',
        'navigation',
        'speedOverGround'
      );
      await fs.ensureDir(path.join(flatDir, 'processed'));
      await fs.writeFile(
        path.join(flatDir, 'signalk_data_20240601T100000.parquet'),
        ''
      );
      await fs.writeFile(
        path.join(flatDir, 'processed', 'signalk_data_20240601T090000.parquet'),
        ''
      );

      const result = await new MigrationService(host.app).scan(dataDir);
      expect(result.totalFiles).to.equal(1);
      expect(result.sourceStyle).to.equal('flat');
    });
  });

  describe('GPX import', () => {
    it('finds GPX files regardless of extension case', async () => {
      const gpx = path.join(dataDir, 'tracks', 'TRACK.GPX');
      await fs.ensureDir(path.dirname(gpx));
      await fs.writeFile(gpx, '<gpx/>');

      const service = new GpxImportService(
        host.app,
        new ParquetWriter({ format: 'parquet', app: host.app })
      );
      const result = await service.scan(dataDir);
      expect(result.files.map(f => f.path)).to.deep.equal([gpx]);
    });
  });
});
