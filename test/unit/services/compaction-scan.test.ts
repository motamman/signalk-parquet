/**
 * The compaction scan on a store with more compactable groups than the
 * scan's concurrency limit. The previous scan stat'ed each group's files
 * through the same limiter its per-group tasks ran on, so once every slot
 * was held by a task waiting for a slot it never returned (found on a
 * 190-context store, 2026-09-17). It also must skip a year that already
 * holds a compaction output, a group of one file, and years at or past the
 * cutoff, and never look inside repaired/ or quarantine/.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'os';
import * as path from 'path';
import type { ServerAPI } from '@signalk/server-api';
import { CompactionService } from '../../../src/services/compaction-service';

describe('compaction scan', function () {
  this.timeout(10000);
  let base: string;
  const app = {
    debug: () => {},
    error: () => {},
    setPluginStatus: () => {},
  } as unknown as ServerAPI;

  beforeEach(async () => {
    base = await fs.mkdtemp(path.join(os.tmpdir(), 'compaction-scan-'));
  });
  afterEach(async () => {
    await fs.remove(base);
  });

  async function file(...segments: string[]): Promise<void> {
    const f = path.join(base, 'tier=raw', ...segments);
    await fs.ensureDir(path.dirname(f));
    await fs.writeFile(f, 'x'.repeat(10));
  }

  it('returns every multi-file group past the limiter width, and nothing it must not', async () => {
    // 20 contexts, one path each, two day files in 2024: 20 groups > 8 slots.
    for (let i = 0; i < 20; i++) {
      await file(`context=vessels__c${i}`, 'path=navigation__position', 'year=2024', 'day=001', 'a.parquet');
      await file(`context=vessels__c${i}`, 'path=navigation__position', 'year=2024', 'day=002', 'b.parquet');
    }
    // Already compacted: skipped.
    await file('context=vessels__done', 'path=navigation__position', 'year=2024', 'year_compact_2024_x.parquet');
    await file('context=vessels__done', 'path=navigation__position', 'year=2024', 'day=003', 'left.parquet');
    await file('context=vessels__done', 'path=navigation__position', 'year=2024', 'day=004', 'over.parquet');
    // One file: already compact.
    await file('context=vessels__single', 'path=navigation__position', 'year=2024', 'day=001', 'only.parquet');
    // At the cutoff: untouched.
    await file('context=vessels__c0', 'path=navigation__position', 'year=2025', 'day=001', 'a.parquet');
    await file('context=vessels__c0', 'path=navigation__position', 'year=2025', 'day=002', 'b.parquet');
    // Sidecars never counted.
    await file('context=vessels__c0', 'path=navigation__position', 'year=2024', 'day=001', 'quarantine', 'q.parquet');
    await file('context=vessels__c0', 'path=navigation__position', 'year=2024', 'repaired', 'r.parquet');

    const plan = await new CompactionService(app).scan({
      baseDirectory: base,
      tier: 'raw',
      beforeYear: 2025,
    });
    expect(plan.totalGroups).to.equal(20);
    expect(plan.totalSourceFiles).to.equal(40);
    expect(plan.totalSourceBytes).to.equal(400);
    expect(plan.groups.every(g => g.year === 2024)).to.equal(true);
    expect(plan.groups.map(g => g.context)).to.include('vessels.c7');
    expect(plan.groups.map(g => g.context)).to.not.include('vessels.done');
    expect(plan.groups.map(g => g.context)).to.not.include('vessels.single');
  });

  it('honours the path filter', async () => {
    await file('context=vessels__a', 'path=navigation__position', 'year=2024', 'day=001', 'a.parquet');
    await file('context=vessels__a', 'path=navigation__position', 'year=2024', 'day=002', 'b.parquet');
    await file('context=vessels__a', 'path=environment__depth', 'year=2024', 'day=001', 'a.parquet');
    await file('context=vessels__a', 'path=environment__depth', 'year=2024', 'day=002', 'b.parquet');
    const plan = await new CompactionService(app).scan({
      baseDirectory: base,
      tier: 'raw',
      beforeYear: 2025,
      pathFilter: 'environment.',
    });
    expect(plan.groups.map(g => g.path)).to.deep.equal(['environment.depth']);
  });
});
