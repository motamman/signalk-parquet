/**
 * Test position aggregation: middle-point selection with GPS glitch rejection.
 *
 * Synthesizes raw position parquet files in a temp hive directory, runs
 * aggregation, and asserts the selected point per bucket.
 *
 * Cases:
 *   1. Clean straight track → middle-by-timestamp wins
 *   2. GPS glitch at midpoint → glitch rejected, walk-forward neighbor wins
 *   3. Peninsula curve (no glitch) → middle wins despite track shape
 *   4. All-invalid bucket → no output row
 *   5. Single valid record → that record wins
 *
 * Run: npx tsx tests/test-position-aggregation.ts
 */

import * as path from 'path';
import * as fs from 'fs-extra';
import { AggregationService, AggregationConfig } from '../src/services/aggregation-service';
import { DuckDBPool } from '../src/utils/duckdb-pool';

const DATA_DIR = '/tmp/signalk-position-test';
const CONTEXT = 'vessels.urn:mrn:imo:mmsi:368396230';
const SIGNALK_PATH = 'navigation.position';
const TEST_DATE = new Date('2026-04-12T00:00:00Z');

const mockApp = {
  selfContext: CONTEXT,
  debug: (msg: string) => console.log(`  [debug] ${msg}`),
  error: (msg: string) => console.error(`  [error] ${msg}`),
  getMetadata: (_path: string) => null,
} as any;

const aggConfig: AggregationConfig = {
  outputDirectory: DATA_DIR,
  filenamePrefix: 'signalk_data',
  retentionDays: { raw: 365, '5s': 365, '60s': 365, '1h': 365 },
};

interface RawPoint {
  ts: string; // ISO
  lat: number | null;
  lon: number | null;
}

function dayOfYear(d: Date): number {
  const start = Date.UTC(d.getUTCFullYear(), 0, 0);
  return Math.floor((d.getTime() - start) / 86400000);
}

async function writeRawPositionParquet(points: RawPoint[]) {
  const year = TEST_DATE.getUTCFullYear();
  const day = String(dayOfYear(TEST_DATE)).padStart(3, '0');
  const dir = path.join(
    DATA_DIR,
    'tier=raw',
    `context=${CONTEXT.replace(/\./g, '__')}`,
    `path=${SIGNALK_PATH.replace(/\./g, '__')}`,
    `year=${year}`,
    `day=${day}`
  );
  await fs.ensureDir(dir);
  const file = path.join(dir, `signalk_data_${TEST_DATE.toISOString().slice(0, 10)}.parquet`);

  const conn = await DuckDBPool.getConnection();
  try {
    const valuesSql = points
      .map(
        p =>
          `('${p.ts}'::TIMESTAMP, '${CONTEXT}', '${SIGNALK_PATH}', ${
            p.lat === null ? 'NULL' : p.lat
          }, ${p.lon === null ? 'NULL' : p.lon})`
      )
      .join(',');
    await conn.runAndReadAll(`
      COPY (
        SELECT * FROM (VALUES ${valuesSql})
        AS t(received_timestamp, context, path, value_latitude, value_longitude)
      ) TO '${file}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
    `);
  } finally {
    conn.disconnectSync();
  }
  return file;
}

interface AggRow {
  bucket_time: string;
  value_latitude: number | null;
  value_longitude: number | null;
  sample_count: number;
}

async function readAggregated(tier: '5s' | '60s' | '1h'): Promise<AggRow[]> {
  const pattern = path.join(
    DATA_DIR,
    `tier=${tier}`,
    '**',
    '*.parquet'
  );
  const { glob } = await import('glob');
  const files = await glob(pattern);
  if (files.length === 0) return [];
  const conn = await DuckDBPool.getConnection();
  try {
    const fileList = files.map(f => `'${f}'`).join(',');
    const result = await conn.runAndReadAll(`
      SELECT bucket_time::VARCHAR AS bucket_time, value_latitude, value_longitude, sample_count
      FROM read_parquet([${fileList}], union_by_name=true)
      ORDER BY bucket_time
    `);
    return result.getRowObjects().map((r: any) => ({
      bucket_time: r.bucket_time,
      value_latitude: r.value_latitude === null ? null : Number(r.value_latitude),
      value_longitude: r.value_longitude === null ? null : Number(r.value_longitude),
      sample_count: Number(r.sample_count),
    }));
  } finally {
    conn.disconnectSync();
  }
}

let testsPassed = 0;
let testsFailed = 0;

function assertEqual<T>(actual: T, expected: T, label: string) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (ok) {
    console.log(`    ✓ ${label}`);
    testsPassed++;
  } else {
    console.error(`    ✗ ${label}`);
    console.error(`      expected: ${JSON.stringify(expected)}`);
    console.error(`      actual:   ${JSON.stringify(actual)}`);
    testsFailed++;
  }
}

function assertClose(actual: number, expected: number, label: string, eps = 1e-6) {
  const ok = Math.abs(actual - expected) < eps;
  if (ok) {
    console.log(`    ✓ ${label} (${actual})`);
    testsPassed++;
  } else {
    console.error(`    ✗ ${label}: expected ${expected}, got ${actual}`);
    testsFailed++;
  }
}

async function resetDataDir() {
  await fs.remove(DATA_DIR);
  await fs.ensureDir(DATA_DIR);
}

async function runAggregation() {
  const service = new AggregationService(aggConfig, mockApp);
  await service.aggregateDate(TEST_DATE);
}

async function testCleanTrack() {
  console.log('\n--- Test 1: Clean straight track, midpoint wins ---');
  await resetDataDir();

  // 10 points in a single 5s bucket [00:00:00, 00:00:05), every 0.5s.
  // lat step = 0.00001° ≈ 1.1 m → ~2.2 m/s, well under the 25 m/s threshold.
  // Bucket midpoint = 2500ms; tiebreak "at-or-after midpoint, closest" picks i=5.
  const points: RawPoint[] = [];
  for (let i = 0; i < 10; i++) {
    const ts = new Date(TEST_DATE.getTime() + i * 500).toISOString().replace('Z', '');
    points.push({ ts, lat: 40.0 + i * 0.00001, lon: -70.0 });
  }
  await writeRawPositionParquet(points);
  await runAggregation();

  const rows = await readAggregated('5s');
  assertEqual(rows.length, 1, 'one bucket produced');
  if (rows.length === 1) {
    assertClose(rows[0].value_latitude!, 40.0 + 5 * 0.00001, 'middle latitude');
    assertEqual(rows[0].sample_count, 10, 'sample_count = 10');
  }
}

async function testGpsGlitch() {
  console.log('\n--- Test 2: GPS glitch at midpoint, walk forward ---');
  await resetDataDir();

  // 10 clean points in bucket, but the one nearest the midpoint is replaced
  // with a wild jump (lat 0, lon 0 → ~7800 km from cluster). Should be rejected.
  const points: RawPoint[] = [];
  for (let i = 0; i < 10; i++) {
    const ts = new Date(TEST_DATE.getTime() + i * 500).toISOString().replace('Z', '');
    if (i === 5) {
      // Glitch: massive jump, neighbors will see implausible implied speed
      points.push({ ts, lat: 0.0, lon: 0.0 });
    } else {
      points.push({ ts, lat: 40.0 + i * 0.0001, lon: -70.0 + i * 0.0001 });
    }
  }
  await writeRawPositionParquet(points);
  await runAggregation();

  const rows = await readAggregated('5s');
  assertEqual(rows.length, 1, 'one bucket produced');
  if (rows.length === 1) {
    // Glitch at i=5 rejected. Walk forward to i=6 (ts=00:00:03.000, also >= midpoint)
    // lat = 40.0006
    const lat = rows[0].value_latitude!;
    const ok = lat > 39.9 && lat < 40.1 && lat !== 0.0;
    if (ok) {
      console.log(`    ✓ glitch rejected, picked clean neighbor (lat=${lat})`);
      testsPassed++;
    } else {
      console.error(`    ✗ glitch was NOT rejected, lat=${lat}`);
      testsFailed++;
    }
  }
}

async function testPeninsulaCurve() {
  console.log('\n--- Test 3: Curved track (no glitch) ---');
  await resetDataDir();

  // 10 points tracing a quarter circle around a "peninsula" — first and last
  // points are far apart in a bounding box sense, but each step is plausible.
  // Radius 0.0001° ≈ 11 m; arc ≈ 17.5 m / 9 steps = ~1.9 m per 0.5 s = 3.9 m/s.
  const radius = 0.0001;
  const points: RawPoint[] = [];
  for (let i = 0; i < 10; i++) {
    const ts = new Date(TEST_DATE.getTime() + i * 500).toISOString().replace('Z', '');
    const angle = (i / 9) * (Math.PI / 2);
    const lat = 40.0 + radius * Math.sin(angle);
    const lon = -70.0 + radius * Math.cos(angle);
    points.push({ ts, lat, lon });
  }
  await writeRawPositionParquet(points);
  await runAggregation();

  const rows = await readAggregated('5s');
  assertEqual(rows.length, 1, 'one bucket produced');
  if (rows.length === 1) {
    // Should pick i=5 (closest at-or-after midpoint), no glitch rejection
    const expectedAngle = (5 / 9) * (Math.PI / 2);
    const expectedLat = 40.0 + 0.0001 * Math.sin(expectedAngle);
    assertClose(rows[0].value_latitude!, expectedLat, 'curved-track midpoint');
    assertEqual(rows[0].sample_count, 10, 'sample_count = 10');
  }
}

async function testAllInvalid() {
  console.log('\n--- Test 4: All-invalid bucket produces no row ---');
  await resetDataDir();

  // All points have null lat/lon → filtered out at source CTE → no bucket
  const points: RawPoint[] = [];
  for (let i = 0; i < 5; i++) {
    const ts = new Date(TEST_DATE.getTime() + i * 1000).toISOString().replace('Z', '');
    points.push({ ts, lat: null, lon: null });
  }
  await writeRawPositionParquet(points);
  await runAggregation();

  const rows = await readAggregated('5s');
  assertEqual(rows.length, 0, 'no buckets produced (all invalid filtered)');
}

async function testSingleValid() {
  console.log('\n--- Test 5: Single valid record ---');
  await resetDataDir();

  const ts = new Date(TEST_DATE.getTime() + 1500).toISOString().replace('Z', '');
  await writeRawPositionParquet([{ ts, lat: 41.5, lon: -71.5 }]);
  await runAggregation();

  const rows = await readAggregated('5s');
  assertEqual(rows.length, 1, 'one bucket produced');
  if (rows.length === 1) {
    assertClose(rows[0].value_latitude!, 41.5, 'single record lat');
    assertClose(rows[0].value_longitude!, -71.5, 'single record lon');
    assertEqual(rows[0].sample_count, 1, 'sample_count = 1');
  }
}

async function testReAggregationCascade() {
  console.log('\n--- Test 6: Re-aggregation 5s → 60s → 1h ---');
  await resetDataDir();

  // 120 points over 60 seconds, every 0.5s. lat step 0.00001° ≈ 2.2 m/s.
  // Should produce 12 buckets at 5s, 1 at 60s, 1 at 1h.
  const points: RawPoint[] = [];
  for (let i = 0; i < 120; i++) {
    const ts = new Date(TEST_DATE.getTime() + i * 500).toISOString().replace('Z', '');
    points.push({ ts, lat: 40.0 + i * 0.00001, lon: -70.0 });
  }
  await writeRawPositionParquet(points);
  await runAggregation();

  const rows5s = await readAggregated('5s');
  const rows60s = await readAggregated('60s');
  const rows1h = await readAggregated('1h');
  assertEqual(rows5s.length, 12, '12 buckets at 5s');
  assertEqual(rows60s.length, 1, '1 bucket at 60s');
  assertEqual(rows1h.length, 1, '1 bucket at 1h');
  if (rows60s.length === 1) {
    // 60s midpoint = 30s. At-or-after rule prefers ts≥30s. Expected near the
    // [30s,35s) 5s-bucket pick (i≈65, lat≈40.00065).
    const lat = rows60s[0].value_latitude!;
    const ok = lat > 40.0004 && lat < 40.0008;
    if (ok) {
      console.log(`    ✓ 60s tier picked plausible point (lat=${lat})`);
      testsPassed++;
    } else {
      console.error(`    ✗ 60s tier lat out of range: ${lat}`);
      testsFailed++;
    }
  }
}

async function main() {
  console.log('=== Position Aggregation Tests ===');
  await DuckDBPool.initialize();
  console.log('DuckDB initialized');

  try {
    await testCleanTrack();
    await testGpsGlitch();
    await testPeninsulaCurve();
    await testAllInvalid();
    await testSingleValid();
    await testReAggregationCascade();
  } finally {
    await fs.remove(DATA_DIR);
  }

  console.log(`\n=== ${testsPassed} passed, ${testsFailed} failed ===`);
  if (testsFailed > 0) process.exit(1);
}

main().catch(err => {
  console.error('Test failed:', err);
  process.exit(1);
});
