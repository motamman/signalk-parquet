/**
 * Pins the original-filename plumbing added for #68.
 *
 * Uploaded files are staged under de-duplicated names so two originals that
 * sanitise to the same stem cannot overwrite each other. The name the user
 * gave the file still has to be what progress shows and what the records'
 * source.file carries, so the route passes a staging-path -> original-name
 * map to the service. This suite drives the service directly with such a
 * map and reads the written parquet back through DuckDB.
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

const SELF_ID = 'gpxnames';

function gpx(lat: number, lon: number, iso: string): string {
  return `<?xml version="1.0"?><gpx version="1.1"><trk><trkseg>
    <trkpt lat="${lat}" lon="${lon}"><time>${iso}</time></trkpt>
  </trkseg></trk></gpx>`;
}

async function waitForJob(
  service: GpxImportService,
  jobId: string
): Promise<GpxImportProgress> {
  for (let i = 0; i < 200; i++) {
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

describe('GPX import with original filenames (#68)', function () {
  this.timeout(30000);

  let host: FakeSignalK;
  let staging: string;

  beforeEach(async () => {
    host = createFakeSignalK({ selfId: SELF_ID });
    staging = path.join(host.dataDir, 'gpx-uploads', 'upload_1');
    await fs.ensureDir(staging);
    await DuckDBPool.initialize();
  });

  afterEach(async () => {
    await DuckDBPool.shutdown();
    await host?.cleanup();
  });

  it('imports both colliding uploads and records each original name', async () => {
    // Use case: 'nav 1.gpx' and 'nav@1.gpx' both sanitise to 'nav_1'; they
    // are staged under distinct names and must both be imported.
    const stagedA = path.join(staging, 'nav_1_aaaaaa.gpx');
    const stagedB = path.join(staging, 'nav_1_bbbbbb.gpx');
    await fs.writeFile(stagedA, gpx(47.5, 8.7, '2024-06-01T10:00:00Z'));
    await fs.writeFile(stagedB, gpx(47.6, 8.8, '2024-06-01T11:00:00Z'));

    const service = new GpxImportService(
      host.app,
      new ParquetWriter({ format: 'parquet', app: host.app })
    );
    const jobId = await service.import({
      sourceFiles: [stagedA, stagedB],
      sourceNames: { [stagedA]: 'nav 1.gpx', [stagedB]: 'nav@1.gpx' },
      targetDirectory: host.dataDir,
      context: 'vessels.self',
      paths: ['navigation.position'],
      filenamePrefix: 'test_gpx',
      deleteSourceAfterImport: false,
      sourceLabel: 'gpx-import',
    });
    const progress = await waitForJob(service, jobId);

    expect(progress.status).to.equal('completed');
    expect(progress.errors).to.deep.equal([]);
    expect(progress.filesImported).to.equal(2);
    expect(progress.recordsWritten).to.equal(2);
    // Progress shows the user's name, not the staged one.
    expect(progress.currentFile).to.equal('nav@1.gpx');

    const glob = path
      .join(
        host.dataDir,
        'tier=raw',
        `context=vessels__${SELF_ID}`,
        'path=navigation__position',
        '**',
        '*.parquet'
      )
      .replace(/\\/g, '/');
    const conn = await DuckDBPool.getConnection();
    try {
      const res = await conn.runAndReadAll(
        `SELECT source FROM read_parquet('${glob}') ORDER BY signalk_timestamp`
      );
      const sources = res
        .getRowObjects()
        .map(row => JSON.parse(String(row.source)) as { file: string });
      expect(sources.map(s => s.file)).to.deep.equal(['nav 1.gpx', 'nav@1.gpx']);
    } finally {
      conn.disconnectSync();
    }
  });

  it('falls back to the staged basename when no original name is given', async () => {
    const staged = path.join(staging, 'archive.gpx');
    await fs.writeFile(staged, gpx(47.5, 8.7, '2024-06-01T10:00:00Z'));

    const service = new GpxImportService(
      host.app,
      new ParquetWriter({ format: 'parquet', app: host.app })
    );
    const jobId = await service.import({
      sourceFiles: [staged],
      targetDirectory: host.dataDir,
      context: 'vessels.self',
      paths: ['navigation.position'],
      filenamePrefix: 'test_gpx',
      deleteSourceAfterImport: false,
      sourceLabel: 'gpx-import',
    });
    const progress = await waitForJob(service, jobId);

    expect(progress.status).to.equal('completed');
    expect(progress.currentFile).to.equal('archive.gpx');
  });
});
