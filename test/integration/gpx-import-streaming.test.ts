/**
 * Pins the streaming import path (#54): points are written as they are
 * parsed through a capped pool of parquet writers. With the cap forced to
 * one writer and a file whose days are interleaved, a partition is closed,
 * evicted and reopened as a second file, and every record still lands.
 * The written files are read back through DuckDB.
 */
import { expect } from 'chai';
import * as path from 'path';
import * as fs from 'fs-extra';
import { ParquetWriter } from '../../src/parquet-writer';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import {
  GpxImportService,
  GpxImportProgress,
} from '../../src/services/gpx-import-service';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';

const SELF_ID = 'gpxstream';
const POINTS_PER_RUN = 120;

/** `count` one-second points starting at `startIso`, with a speed. */
function run(startIso: string, count: number, lat: number): string {
  const start = new Date(startIso).getTime();
  let out = '';
  for (let i = 0; i < count; i++) {
    const t = new Date(start + i * 1000).toISOString();
    out += `<trkpt lat="${lat + i * 1e-5}" lon="8.5"><time>${t}</time><speed>${(i % 10) / 2}</speed></trkpt>\n`;
  }
  return out;
}

async function waitForJob(
  service: GpxImportService,
  jobId: string
): Promise<GpxImportProgress> {
  for (let i = 0; i < 600; i++) {
    const progress = service.getProgress(jobId);
    if (
      progress &&
      (progress.status === 'completed' ||
        progress.status === 'error' ||
        progress.status === 'cancelled')
    ) {
      return progress;
    }
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  throw new Error('import job did not finish');
}

async function countRows(glob: string, where = ''): Promise<number> {
  const conn = await DuckDBPool.getConnection();
  try {
    const res = await conn.runAndReadAll(
      `SELECT COUNT(*) AS n FROM read_parquet('${glob.replace(/\\/g, '/')}') ${where}`
    );
    return Number((res.getRowObjects()[0] as { n: bigint }).n);
  } finally {
    conn.disconnectSync();
  }
}

describe('GPX import streams points through a capped writer pool (#54)', function () {
  this.timeout(60000);

  let host: FakeSignalK;
  let source: string;

  beforeEach(async () => {
    host = createFakeSignalK({ selfId: SELF_ID });
    source = path.join(host.dataDir, 'incoming');
    await fs.ensureDir(source);
    await DuckDBPool.initialize();
  });

  afterEach(async () => {
    await DuckDBPool.shutdown();
    await host?.cleanup();
  });

  function positionGlob(): string {
    return path.join(
      host.dataDir,
      'tier=raw',
      `context=vessels__${SELF_ID}`,
      'path=navigation__position',
      '**',
      '*.parquet'
    );
  }

  it('writes every record when days are interleaved and only one writer may be open', async () => {
    // Day 153 (2024-06-01), day 154, day 153 again, day 155: the second
    // visit to day 153 arrives after its writer was evicted.
    const gpx =
      '<gpx><trk><trkseg>\n' +
      run('2024-06-01T10:00:00Z', POINTS_PER_RUN, 47.0) +
      run('2024-06-02T10:00:00Z', POINTS_PER_RUN, 48.0) +
      run('2024-06-01T12:00:00Z', POINTS_PER_RUN, 47.5) +
      run('2024-06-03T10:00:00Z', POINTS_PER_RUN, 49.0) +
      '</trkseg></trk></gpx>';
    const file = path.join(source, 'interleaved.gpx');
    await fs.writeFile(file, gpx);

    const service = new GpxImportService(
      host.app,
      new ParquetWriter({ format: 'parquet', app: host.app }),
      undefined,
      { maxOpenWriters: 1, openAfterRecords: 30 }
    );
    const jobId = await service.import({
      sourceFiles: [file],
      targetDirectory: host.dataDir,
      context: 'vessels.self',
      paths: ['navigation.position', 'navigation.speedOverGround'],
      filenamePrefix: 'test_gpx',
      deleteSourceAfterImport: false,
      sourceLabel: 'gpx-import',
    });
    const progress = await waitForJob(service, jobId);

    expect(progress.status).to.equal('completed');
    expect(progress.errors).to.deep.equal([]);
    expect(progress.pointsParsed).to.equal(4 * POINTS_PER_RUN);
    expect(progress.recordsWritten).to.equal(4 * POINTS_PER_RUN * 2);
    // 3 days x 2 paths = 6 partitions, and day 153 was reopened for both
    // paths, so at least two partitions hold two files.
    expect(progress.filesCreated.length).to.be.at.least(8);
    const day153 = progress.filesCreated.filter(
      f => f.includes('day=153') && f.includes('navigation__position')
    );
    expect(day153.length).to.be.at.least(2);
    for (const created of progress.filesCreated) {
      expect(created.endsWith('.parquet')).to.equal(true);
      expect(await fs.pathExists(created)).to.equal(true);
    }
    // No temp files left behind.
    const leftovers = progress.filesCreated.filter(f =>
      fs.existsSync(f + '.tmp')
    );
    expect(leftovers).to.deep.equal([]);

    expect(await countRows(positionGlob())).to.equal(4 * POINTS_PER_RUN);
    expect(
      await countRows(
        positionGlob(),
        "WHERE signalk_timestamp >= '2024-06-01' AND signalk_timestamp < '2024-06-02'"
      )
    ).to.equal(2 * POINTS_PER_RUN);
    expect(
      await countRows(positionGlob(), 'WHERE value_latitude IS NULL')
    ).to.equal(0);
  });

  it('skips a file whose points carry no timestamps and writes nothing', async () => {
    const file = path.join(source, 'untimed.gpx');
    await fs.writeFile(
      file,
      '<gpx><trk><trkseg><trkpt lat="1" lon="2"/><trkpt lat="3" lon="4"/></trkseg></trk></gpx>'
    );
    const service = new GpxImportService(
      host.app,
      new ParquetWriter({ format: 'parquet', app: host.app })
    );
    const jobId = await service.import({
      sourceFiles: [file],
      targetDirectory: host.dataDir,
      context: 'vessels.self',
      paths: ['navigation.position'],
      filenamePrefix: 'test_gpx',
      deleteSourceAfterImport: false,
      sourceLabel: 'gpx-import',
    });
    const progress = await waitForJob(service, jobId);

    expect(progress.status).to.equal('completed');
    expect(progress.pointsParsed).to.equal(2);
    expect(progress.filesSkipped).to.equal(1);
    expect(progress.filesImported).to.equal(0);
    expect(progress.filesCreated).to.deep.equal([]);
  });
});
