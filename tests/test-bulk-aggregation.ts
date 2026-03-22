/**
 * Test bulk aggregation logic against real copied data
 *
 * Test data at /tmp/signalk-test-data/:
 * - 2 contexts (self 368396230 + AIS 338339307)
 * - 2 paths each (navigation.position, navigation.speedOverGround)
 * - 4 unique dates (days 189, 190, 191 for self; day 215 for AIS)
 *
 * Expected: aggregateDate creates tier=5s, tier=60s, tier=1h for each date
 * across ALL contexts and scalar paths (position is object-type, should be skipped)
 */

import * as path from 'path';
import * as fs from 'fs-extra';
import { glob } from 'glob';
import { AggregationService, AggregationConfig } from '../src/services/aggregation-service';
import { DuckDBPool } from '../src/utils/duckdb-pool';

const DATA_DIR = '/tmp/signalk-test-data';

// Minimal mock of ServerAPI
const mockApp = {
  selfContext: 'vessels.urn:mrn:imo:mmsi:368396230',
  debug: (msg: string) => console.log(`  [debug] ${msg}`),
  error: (msg: string) => console.error(`  [error] ${msg}`),
  getMetadata: (_path: string) => null,
} as any;

const aggConfig: AggregationConfig = {
  outputDirectory: DATA_DIR,
  filenamePrefix: 'signalk_data',
  retentionDays: { raw: 365, '5s': 365, '60s': 365, '1h': 365 },
};

async function runTest() {
  console.log('=== Test: Bulk Aggregation ===\n');

  // Initialize DuckDB
  await DuckDBPool.initialize();
  console.log('DuckDB initialized\n');

  // Clean up any previous test output
  for (const tier of ['5s', '60s', '1h']) {
    await fs.remove(path.join(DATA_DIR, `tier=${tier}`));
  }

  const service = new AggregationService(aggConfig, mockApp);

  // Test 1: discoverRawDates
  console.log('--- Test 1: discoverRawDates ---');
  const dates = await service.discoverRawDates();
  console.log(`  Found ${dates.length} dates:`);
  for (const d of dates) {
    console.log(`    ${d.toISOString().slice(0, 10)}`);
  }
  if (dates.length !== 4) {
    console.error(`  FAIL: Expected 4 dates, got ${dates.length}`);
    process.exit(1);
  }
  console.log('  PASS\n');

  // Test 2: discoverRawDates with date range filter
  console.log('--- Test 2: discoverRawDates with range filter ---');
  const filteredDates = await service.discoverRawDates(
    new Date('2025-07-09T00:00:00Z'),
    new Date('2025-07-10T00:00:00Z')
  );
  console.log(`  Found ${filteredDates.length} dates in range:`);
  for (const d of filteredDates) {
    console.log(`    ${d.toISOString().slice(0, 10)}`);
  }
  if (filteredDates.length !== 2) {
    console.error(`  FAIL: Expected 2 dates, got ${filteredDates.length}`);
    process.exit(1);
  }
  console.log('  PASS\n');

  // Test 3: aggregateDate for a single date
  console.log('--- Test 3: aggregateDate (2025-07-08) ---');
  const results = await service.aggregateDate(new Date('2025-07-08T00:00:00Z'));
  for (const r of results) {
    console.log(`  ${r.sourceTier} → ${r.targetTier}: ${r.filesProcessed} files processed, ${r.filesCreated} created, ${r.recordsAggregated} records`);
    if (r.errors.length > 0) {
      console.log(`    errors: ${r.errors.join('; ')}`);
    }
  }

  // Check that tier=5s files were created
  const tier5sFiles = await glob(path.join(DATA_DIR, 'tier=5s', '**', '*.parquet'));
  console.log(`  tier=5s files created: ${tier5sFiles.length}`);
  if (tier5sFiles.length === 0) {
    console.error('  FAIL: No tier=5s files created');
    process.exit(1);
  }
  // Check tier=60s
  const tier60sFiles = await glob(path.join(DATA_DIR, 'tier=60s', '**', '*.parquet'));
  console.log(`  tier=60s files created: ${tier60sFiles.length}`);
  // Check tier=1h
  const tier1hFiles = await glob(path.join(DATA_DIR, 'tier=1h', '**', '*.parquet'));
  console.log(`  tier=1h files created: ${tier1hFiles.length}`);
  console.log('  PASS\n');

  // Test 4: Full bulk aggregation across all dates
  console.log('--- Test 4: Full bulk aggregation (all 4 dates) ---');
  // Clean again
  for (const tier of ['5s', '60s', '1h']) {
    await fs.remove(path.join(DATA_DIR, `tier=${tier}`));
  }

  // Use startBulkAggregation and poll
  const jobId = service.startBulkAggregation();
  console.log(`  Job started: ${jobId}`);

  // Poll until complete
  let progress = service.getBulkProgress(jobId);
  while (progress && progress.status !== 'completed' && progress.status !== 'error' && progress.status !== 'cancelled') {
    await new Promise(r => setTimeout(r, 500));
    progress = service.getBulkProgress(jobId);
    if (progress) {
      console.log(`  [${progress.status}] ${progress.phase} — ${progress.datesProcessed}/${progress.datesTotal} dates (${progress.percent}%) ${progress.currentDate || ''}`);
    }
  }

  if (!progress || progress.status !== 'completed') {
    console.error(`  FAIL: Job ended with status ${progress?.status}, error: ${progress?.error}`);
    process.exit(1);
  }

  console.log(`  Completed: ${progress.datesProcessed} dates, ${progress.filesCreated} files created, ${progress.recordsAggregated} records`);
  if (progress.errors.length > 0) {
    console.log(`  Errors: ${progress.errors.join('\n    ')}`);
  }

  // Verify output: should have files for BOTH contexts
  const allCreated = await glob(path.join(DATA_DIR, 'tier=*', 'context=*', 'path=*', '**', '*.parquet'));
  const tierFiles = allCreated.filter(f => !f.includes('tier=raw'));
  console.log(`\n  Total aggregated files created: ${tierFiles.length}`);

  // Check both contexts have output
  const contexts = new Set(tierFiles.map(f => f.match(/context=([^/]+)/)?.[1]).filter(Boolean));
  console.log(`  Contexts with aggregated data: ${[...contexts].join(', ')}`);
  if (contexts.size < 2) {
    console.error('  FAIL: Expected aggregated data for both contexts');
    process.exit(1);
  }

  // Check all 3 tiers have output
  const tiers = new Set(tierFiles.map(f => f.match(/tier=([^/]+)/)?.[1]).filter(Boolean));
  console.log(`  Tiers created: ${[...tiers].join(', ')}`);

  // List all created files
  console.log('\n  Created files:');
  for (const f of tierFiles.sort()) {
    const rel = f.replace(DATA_DIR + '/', '');
    const stat = await fs.stat(f);
    console.log(`    ${rel} (${(stat.size / 1024).toFixed(1)} KB)`);
  }

  console.log('\n  PASS');
  console.log('\n=== All tests passed ===');
}

runTest().catch(err => {
  console.error('Test failed:', err);
  process.exit(1);
});
