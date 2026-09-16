/**
 * The compaction crash-recovery sweeps that run at plugin start: stranded
 * temp files are removed, pre-publish trash is restored and post-publish
 * trash is discarded. They now find their targets by walking the known
 * hive levels rather than a `**` glob; the results must be unchanged.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'os';
import * as path from 'path';
import {
  cleanupStrandedCompactionTempFiles,
  recoverStrandedCompactionTrash,
} from '../../../src/services/compaction-service';
import type { ServerAPI } from '@signalk/server-api';

describe('compaction startup sweeps', () => {
  let base: string;
  const logs: string[] = [];
  const app = {
    debug: (m: string) => logs.push(m),
    error: (m: string) => logs.push(`ERROR ${m}`),
  } as unknown as ServerAPI;

  beforeEach(async () => {
    base = await fs.mkdtemp(path.join(os.tmpdir(), 'compaction-startup-'));
    logs.length = 0;
  });

  afterEach(async () => {
    await fs.remove(base);
  });

  async function touch(...segments: string[]): Promise<string> {
    const file = path.join(base, ...segments);
    await fs.ensureDir(path.dirname(file));
    await fs.writeFile(file, 'x');
    return file;
  }

  it('removes stranded compaction and aggregation temp files and nothing else', async () => {
    const compactionTemp = await touch('tier=raw', 'context=a', 'path=p', 'year=2025', 'year_compact_2025.parquet.tmp');
    const aggregationTemp = await touch('tier=5s', 'context=a', 'path=p', 'year=2025', 'day=010', 'signalk_data_aggregated.parquet.tmp');
    const keep1 = await touch('tier=raw', 'context=a', 'path=p', 'year=2025', 'year_compact_2025.parquet');
    const keep2 = await touch('tier=raw', 'context=a', 'path=p', 'year=2025', 'day=010', 'signalk_data.parquet');
    const keep3 = await touch('tier=raw', 'context=a', 'path=p', 'year=2025', 'day=010', 'other.tmp');

    const result = await cleanupStrandedCompactionTempFiles(app, base);

    expect(result.removed).to.equal(2);
    expect(await fs.pathExists(compactionTemp)).to.equal(false);
    expect(await fs.pathExists(aggregationTemp)).to.equal(false);
    for (const kept of [keep1, keep2, keep3]) {
      expect(await fs.pathExists(kept)).to.equal(true);
    }
  });

  it('restores pre-publish trash and discards post-publish trash', async () => {
    // Pre-publish: no compacted output beside the trash, so the files go back.
    const trashed = await touch('tier=raw', 'context=a', 'path=p', 'year=2024', '.compaction-trash-1', 'day=005', 'x.parquet');
    // Post-publish: compacted output exists, so the trash is residue.
    await touch('tier=raw', 'context=b', 'path=p', 'year=2024', '.compaction-trash-2', 'day=001', 'y.parquet');
    await touch('tier=raw', 'context=b', 'path=p', 'year=2024', 'year_compact_2024.parquet');

    const result = await recoverStrandedCompactionTrash(app, base);

    expect(result).to.deep.equal({ restored: 1, cleaned: 1, failed: 0 });
    expect(await fs.pathExists(trashed)).to.equal(false);
    expect(
      await fs.pathExists(path.join(base, 'tier=raw', 'context=a', 'path=p', 'year=2024', 'day=005', 'x.parquet'))
    ).to.equal(true);
    expect(
      await fs.pathExists(path.join(base, 'tier=raw', 'context=a', 'path=p', 'year=2024', '.compaction-trash-1'))
    ).to.equal(false);
    expect(
      await fs.pathExists(path.join(base, 'tier=raw', 'context=b', 'path=p', 'year=2024', '.compaction-trash-2'))
    ).to.equal(false);
  });

  it('does nothing on a missing or empty store', async () => {
    expect(await cleanupStrandedCompactionTempFiles(app, path.join(base, 'nope'))).to.deep.equal({ removed: 0 });
    expect(await recoverStrandedCompactionTrash(app, base)).to.deep.equal({ restored: 0, cleaned: 0, failed: 0 });
  });
});
