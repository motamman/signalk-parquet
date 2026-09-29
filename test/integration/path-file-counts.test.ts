/**
 * The path listing with file counts (the webapp's /api/paths, the analyzer's
 * schema prompt) counts the files a query reads, found by the asynchronous
 * hive walker: exported day files of the context asked about, and nothing in
 * quarantine/ or another vessel's directories.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as path from 'path';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { filesFor } from '../../src/utils/parquet-files';
import {
  getAvailablePaths,
  getAvailablePathsWithFileCounts,
} from '../../src/utils/path-discovery';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makeScalarRecord } from './helpers/records';

const SELF_ID = 'countself';
const SELF = `vessels.${SELF_ID}`;
const OTHER = 'vessels.countother';
const SOG = 'navigation.speedOverGround';
const DEPTH = 'environment.depth.belowTransducer';
const DAYS = ['2024-06-01', '2024-06-02'];

describe('path listing with file counts', function () {
  this.timeout(30000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;

  before(async () => {
    host = createFakeSignalK({ selfId: SELF_ID });
    buffer = new SQLiteBuffer({ dbPath: path.join(host.dataDir, 'buffer.db') });
    const exportService = new ParquetExportService(
      buffer,
      new ParquetWriter({ format: 'parquet', app: host.app }),
      {
        outputDirectory: host.dataDir,
        filenamePrefix: 'signalk_data',
        useHivePartitioning: true,
        dailyExportHour: 4,
      },
      host.app
    );
    for (const day of DAYS) {
      buffer.insert(makeScalarRecord(SELF, SOG, 5, `${day}T10:00:00.000Z`));
      buffer.insert(makeScalarRecord(SELF, DEPTH, 12, `${day}T10:00:00.000Z`));
      buffer.insert(makeScalarRecord(OTHER, SOG, 7, `${day}T10:00:00.000Z`));
      await exportService.exportDayToParquet(new Date(`${day}T00:00:00.000Z`));
    }
  });

  after(async () => {
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  const byPath = async () =>
    new Map(
      (await getAvailablePathsWithFileCounts(host.dataDir, host.app)).map(p => [
        p.path,
        p,
      ])
    );

  it('counts exactly the files a query over the path would read', async () => {
    const listed = await byPath();
    expect([...listed.keys()].sort()).to.deep.equal([DEPTH, SOG].sort());
    for (const signalkPath of [SOG, DEPTH]) {
      const files = await filesFor({
        dataDir: host.dataDir,
        contexts: [SELF],
        paths: [signalkPath],
      });
      expect(files, signalkPath).to.have.lengthOf(DAYS.length);
      expect(listed.get(signalkPath)!.fileCount, signalkPath).to.equal(
        files.length
      );
    }
  });

  it('does not count a quarantined file, nor another vessel\'s files', async () => {
    const [dayFile] = await filesFor({
      dataDir: host.dataDir,
      contexts: [SELF],
      paths: [SOG],
    });
    const quarantine = path.join(path.dirname(dayFile), 'quarantine');
    await fs.ensureDir(quarantine);
    await fs.copy(dayFile, path.join(quarantine, path.basename(dayFile)));

    const listed = await byPath();
    expect(listed.get(SOG)!.fileCount).to.equal(DAYS.length);
  });

  it('lists the same paths as the synchronous listing', async () => {
    const counted = [...(await byPath()).keys()].sort();
    const listed = getAvailablePaths(host.dataDir, host.app)
      .map(p => p.path)
      .sort();
    expect(counted).to.deep.equal(listed);
  });
});
