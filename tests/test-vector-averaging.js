#!/usr/bin/env node
/**
 * Vector Averaging for Angular Data — Tests
 *
 * Part 1: Math validation (offline)
 * Part 2: Live API tests against running SignalK server
 * Part 3: Parquet file validation via DuckDB
 *
 * Usage: node tests/test-vector-averaging.js
 */

const path = require('path');
const fs = require('fs');

// Load .env from tests directory
const envPath = path.join(__dirname, '.env');
if (fs.existsSync(envPath)) {
  const envContent = fs.readFileSync(envPath, 'utf8');
  envContent.split('\n').forEach(line => {
    const match = line.match(/^([^#=]+)=(.*)$/);
    if (match) process.env[match[1].trim()] = match[2].trim();
  });
}

const BASE_URL = process.env.BASE_URL || 'http://localhost:3000';
const SK_USERNAME = process.env.SK_USERNAME;
const SK_PASSWORD = process.env.SK_PASSWORD;
const DATA_DIR = process.env.SIGNALK_DATA_DIR || '/Users/mauricetamman/.signalk/data';

const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  cyan: '\x1b[36m',
  dim: '\x1b[2m',
};

function log(msg, color = '') {
  console.log(color + msg + colors.reset);
}

// ─── Math helpers ───────────────────────────────────────────────

function degToRad(deg) {
  return (deg * Math.PI) / 180;
}

function radToDeg(rad) {
  return (rad * 180) / Math.PI;
}

function vectorAverageDeg(anglesInDegrees) {
  const radians = anglesInDegrees.map(degToRad);
  const avgSin =
    radians.reduce((sum, a) => sum + Math.sin(a), 0) / radians.length;
  const avgCos =
    radians.reduce((sum, a) => sum + Math.cos(a), 0) / radians.length;

  if (Math.abs(avgSin) < 1e-10 && Math.abs(avgCos) < 1e-10) {
    return null;
  }

  let result = Math.atan2(avgSin, avgCos);
  if (result < 0) result += 2 * Math.PI;
  return radToDeg(result);
}

function arithmeticAverageDeg(anglesInDegrees) {
  return anglesInDegrees.reduce((s, a) => s + a, 0) / anglesInDegrees.length;
}

function reAggregateFromSinCos(buckets) {
  let sinSum = 0;
  let cosSum = 0;
  let totalSamples = 0;

  for (const { sinAvg, cosAvg, sampleCount } of buckets) {
    sinSum += sinAvg * sampleCount;
    cosSum += cosAvg * sampleCount;
    totalSamples += sampleCount;
  }

  const avgSin = sinSum / totalSamples;
  const avgCos = cosSum / totalSamples;

  if (Math.abs(avgSin) < 1e-10 && Math.abs(avgCos) < 1e-10) {
    return null;
  }

  let result = Math.atan2(avgSin, avgCos);
  if (result < 0) result += 2 * Math.PI;
  return radToDeg(result);
}

// ─── Test runner ────────────────────────────────────────────────

let passed = 0;
let failed = 0;

function assertClose(actual, expected, tolerance, label) {
  if (expected === null) {
    if (actual === null) {
      passed++;
      log(`  ✓ ${label}`, colors.green);
    } else {
      failed++;
      log(`  ✗ ${label}: expected null, got ${actual}`, colors.red);
    }
    return;
  }
  if (actual === null) {
    failed++;
    log(`  ✗ ${label}: expected ${expected}, got null`, colors.red);
    return;
  }
  const diff = Math.min(
    Math.abs(actual - expected),
    Math.abs(actual - expected + 360),
    Math.abs(actual - expected - 360)
  );
  if (diff <= tolerance) {
    passed++;
    log(`  ✓ ${label} (got ${actual.toFixed(4)}°)`, colors.green);
  } else {
    failed++;
    log(
      `  ✗ ${label}: expected ≈${expected}°, got ${actual.toFixed(4)}° (diff=${diff.toFixed(4)})`,
      colors.red
    );
  }
}

function assertTrue(value, label) {
  if (value) {
    passed++;
    log(`  ✓ ${label}`, colors.green);
  } else {
    failed++;
    log(`  ✗ ${label}`, colors.red);
  }
}

// ─── API helpers ────────────────────────────────────────────────

async function getToken() {
  const res = await fetch(`${BASE_URL}/signalk/v1/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username: SK_USERNAME, password: SK_PASSWORD }),
  });
  const data = await res.json();
  return data.token;
}

async function apiGet(path, token) {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.json();
}

async function apiPost(path, body, token) {
  const res = await fetch(`${BASE_URL}${path}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify(body),
  });
  return res.json();
}

// ─── Part 1: Math Tests ─────────────────────────────────────────

function runMathTests() {
  log('\n═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  PART 1: MATH VALIDATION', colors.cyan);
  log('═══════════════════════════════════════════════════════════════\n', colors.cyan);

  log('Test 1: Angles near 0°/360° boundary', colors.yellow);
  assertClose(vectorAverageDeg([10, 350]), 0, 0.01, 'AVG(10°, 350°) = 0°');
  assertClose(vectorAverageDeg([20, 340]), 0, 0.01, 'AVG(20°, 340°) = 0°');
  assertClose(vectorAverageDeg([5, 355]), 0, 0.01, 'AVG(5°, 355°) = 0°');
  assertClose(vectorAverageDeg([1, 359]), 0, 0.01, 'AVG(1°, 359°) = 0°');
  log('  (Arithmetic would give: ' + arithmeticAverageDeg([10, 350]) + '°)\n');

  log('Test 2: Same angles', colors.yellow);
  assertClose(vectorAverageDeg([45, 45, 45]), 45, 0.01, 'AVG(45°, 45°, 45°) = 45°');
  assertClose(vectorAverageDeg([180, 180]), 180, 0.01, 'AVG(180°, 180°) = 180°');
  assertClose(vectorAverageDeg([0, 0, 0, 0]), 0, 0.01, 'AVG(0°, 0°, 0°, 0°) = 0°');
  console.log();

  log('Test 3: Opposite angles (should be null)', colors.yellow);
  assertClose(vectorAverageDeg([0, 180]), null, 0, 'AVG(0°, 180°) = null');
  assertClose(vectorAverageDeg([90, 270]), null, 0, 'AVG(90°, 270°) = null');
  assertClose(vectorAverageDeg([0, 90, 180, 270]), null, 0, 'AVG(0°, 90°, 180°, 270°) = null');
  console.log();

  log('Test 4: Simple averages', colors.yellow);
  assertClose(vectorAverageDeg([0, 90]), 45, 0.01, 'AVG(0°, 90°) = 45°');
  assertClose(vectorAverageDeg([270, 90]), null, 0, 'AVG(270°, 90°) = null (opposite)');
  assertClose(vectorAverageDeg([90, 180]), 135, 0.01, 'AVG(90°, 180°) = 135°');
  console.log();

  log('Test 5: Re-aggregation from stored sin/cos averages', colors.yellow);
  const bucket1Angles = [10, 350].map(degToRad);
  const bucket1SinAvg = bucket1Angles.reduce((s, a) => s + Math.sin(a), 0) / bucket1Angles.length;
  const bucket1CosAvg = bucket1Angles.reduce((s, a) => s + Math.cos(a), 0) / bucket1Angles.length;
  const bucket2Angles = [5, 355].map(degToRad);
  const bucket2SinAvg = bucket2Angles.reduce((s, a) => s + Math.sin(a), 0) / bucket2Angles.length;
  const bucket2CosAvg = bucket2Angles.reduce((s, a) => s + Math.cos(a), 0) / bucket2Angles.length;

  const reAggResult = reAggregateFromSinCos([
    { sinAvg: bucket1SinAvg, cosAvg: bucket1CosAvg, sampleCount: 2 },
    { sinAvg: bucket2SinAvg, cosAvg: bucket2CosAvg, sampleCount: 2 },
  ]);
  assertClose(reAggResult, 0, 0.1, 'Re-aggregated [10°,350°] + [5°,355°] ≈ 0°');

  const bucket3Angles = [30].map(degToRad);
  const bucket3SinAvg = bucket3Angles.reduce((s, a) => s + Math.sin(a), 0) / bucket3Angles.length;
  const bucket3CosAvg = bucket3Angles.reduce((s, a) => s + Math.cos(a), 0) / bucket3Angles.length;
  const bucket4Angles = [330].map(degToRad);
  const bucket4SinAvg = bucket4Angles.reduce((s, a) => s + Math.sin(a), 0) / bucket4Angles.length;
  const bucket4CosAvg = bucket4Angles.reduce((s, a) => s + Math.cos(a), 0) / bucket4Angles.length;

  const weightedReAgg = reAggregateFromSinCos([
    { sinAvg: bucket3SinAvg, cosAvg: bucket3CosAvg, sampleCount: 3 },
    { sinAvg: bucket4SinAvg, cosAvg: bucket4CosAvg, sampleCount: 1 },
  ]);
  assertClose(weightedReAgg, 16.1, 0.2, 'Weighted re-agg: 3×30° + 1×330° ≈ 16°');
  console.log();
}

// ─── Part 2: Live API Tests ─────────────────────────────────────

async function runLiveApiTests() {
  log('═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  PART 2: LIVE API TESTS', colors.cyan);
  log('═══════════════════════════════════════════════════════════════\n', colors.cyan);

  let token;
  try {
    token = await getToken();
    assertTrue(!!token, 'Authenticated with SignalK server');
  } catch (e) {
    log(`  ✗ Cannot connect to ${BASE_URL}: ${e.message}`, colors.red);
    log('  Skipping live API tests (server not running)\n', colors.dim);
    return;
  }

  // Test: Trigger aggregation
  log('Test 6: Trigger aggregation', colors.yellow);
  const yesterday = new Date(Date.now() - 86400000).toISOString().slice(0, 10);
  const aggResult = await apiPost('/plugins/signalk-parquet/api/aggregate', { date: yesterday }, token);
  assertTrue(aggResult.success, `Aggregation succeeded for ${yesterday}`);
  if (aggResult.results) {
    const totalFiles = aggResult.results.reduce((s, r) => s + r.filesCreated, 0);
    const totalErrors = aggResult.results.reduce((s, r) => s + r.errors.length, 0);
    assertTrue(totalFiles > 0, `Created ${totalFiles} aggregated files`);
    assertTrue(totalErrors === 0, `No aggregation errors (got ${totalErrors})`);
  }
  console.log();

  // Test: Query angular path via history API
  log('Test 7: History API returns data for angular path', colors.yellow);
  const historyResult = await apiGet(
    `/signalk/v1/history/values?paths=navigation.headingMagnetic:average&resolution=300&from=${yesterday}T00:00:00Z&to=${yesterday}T23:59:59Z`,
    token
  );
  if (historyResult.data && historyResult.data.length > 0) {
    assertTrue(true, `Got ${historyResult.data.length} data points for headingMagnetic`);

    // Verify values are in valid radian range [−π, π] or [0, 2π]
    const values = historyResult.data
      .map(row => row[1])
      .filter(v => v !== null && v !== undefined);
    if (values.length > 0) {
      const allInRange = values.every(v => v >= -Math.PI && v <= 2 * Math.PI);
      assertTrue(allInRange, `All ${values.length} heading values in valid radian range`);

      // Spot check: values should be reasonable headings (not 180° arithmetic avg artifacts)
      const avgRad = values.reduce((s, v) => s + v, 0) / values.length;
      log(`  Info: average heading = ${radToDeg(avgRad).toFixed(1)}° (${avgRad.toFixed(4)} rad)`, colors.dim);
    } else {
      log('  Info: all values were null (no heading data in range)', colors.dim);
    }
  } else {
    log(`  ⚠ No heading data returned for ${yesterday} (may not have data for that date)`, colors.yellow);
  }
  console.log();

  // Test: Migration endpoint (dry run)
  log('Test 8: Vector averaging migration endpoint', colors.yellow);
  const migrateResult = await apiPost(
    '/plugins/signalk-parquet/api/migrate/vector-averaging',
    { dryRun: true },
    token
  );
  assertTrue(migrateResult.success, 'Migration dry run accepted');
  assertTrue(!!migrateResult.jobId, `Got job ID: ${migrateResult.jobId}`);

  if (migrateResult.jobId) {
    // Poll for completion
    await new Promise(resolve => setTimeout(resolve, 2000));
    const progress = await apiGet(
      `/plugins/signalk-parquet/api/migrate/vector-averaging/${migrateResult.jobId}`,
      token
    );
    assertTrue(progress.status === 'completed', `Dry run completed (status: ${progress.status})`);
    log(`  Info: found ${progress.angularPaths?.length || 0} angular paths, ${progress.totalDates || 0} dates`, colors.dim);
  }
  console.log();
}

// ─── Part 3: Parquet File Validation ─────────────────────────────

async function runParquetTests() {
  log('═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  PART 3: PARQUET FILE VALIDATION', colors.cyan);
  log('═══════════════════════════════════════════════════════════════\n', colors.cyan);

  let DuckDBInstance;
  try {
    ({ DuckDBInstance } = require('@duckdb/node-api'));
  } catch (e) {
    log('  ✗ @duckdb/node-api not available, skipping parquet tests', colors.red);
    return;
  }

  // Find the most recently modified aggregated heading file
  const { glob } = require('glob');
  const headingFiles = await glob(
    path.join(DATA_DIR, 'tier=5s', 'context=*', 'path=navigation__headingMagnetic', 'year=*', 'day=*', '*.parquet')
  );

  if (headingFiles.length === 0) {
    log('  ⚠ No aggregated headingMagnetic files found, skipping', colors.yellow);
    return;
  }

  // Pick the most recently modified file (the one we just aggregated)
  const testFile = headingFiles
    .map(f => ({ path: f, mtime: fs.statSync(f).mtimeMs }))
    .sort((a, b) => b.mtime - a.mtime)[0].path;
  log(`Test 9: Parquet schema for ${path.basename(path.dirname(path.dirname(testFile)))}/${path.basename(testFile)}`, colors.yellow);

  const db = await DuckDBInstance.create();
  const conn = await db.connect();

  try {
    // Check schema has sin/cos columns
    const schemaResult = await conn.runAndReadAll(
      `DESCRIBE SELECT * FROM read_parquet('${testFile}')`
    );
    const columns = schemaResult.getRowObjects().map(r => r.column_name);

    assertTrue(columns.includes('value_sin_avg'), 'Schema has value_sin_avg column');
    assertTrue(columns.includes('value_cos_avg'), 'Schema has value_cos_avg column');
    console.log();

    // Check data values
    log('Test 10: Aggregated heading data values', colors.yellow);
    const dataResult = await conn.runAndReadAll(
      `SELECT value_avg, value_min, value_max, value_sin_avg, value_cos_avg, sample_count::INTEGER as n
       FROM read_parquet('${testFile}') LIMIT 10`
    );
    const rows = dataResult.getRowObjects();

    assertTrue(rows.length > 0, `Got ${rows.length} aggregated rows`);

    if (rows.length > 0) {
      // value_min and value_max should be NULL for angular paths
      const allMinNull = rows.every(r => r.value_min === null);
      const allMaxNull = rows.every(r => r.value_max === null);
      assertTrue(allMinNull, 'value_min is NULL for all rows (angular path)');
      assertTrue(allMaxNull, 'value_max is NULL for all rows (angular path)');

      // value_sin_avg and value_cos_avg should be populated
      const allSinPopulated = rows.every(r => r.value_sin_avg !== null);
      const allCosPopulated = rows.every(r => r.value_cos_avg !== null);
      assertTrue(allSinPopulated, 'value_sin_avg populated for all rows');
      assertTrue(allCosPopulated, 'value_cos_avg populated for all rows');

      // Verify value_avg matches ATAN2(sin_avg, cos_avg)
      let reconstructionOk = true;
      for (const row of rows) {
        const reconstructed = Math.atan2(row.value_sin_avg, row.value_cos_avg);
        const diff = Math.abs(row.value_avg - reconstructed);
        if (diff > 0.0001) {
          reconstructionOk = false;
          log(`  ✗ value_avg=${row.value_avg} != ATAN2(${row.value_sin_avg}, ${row.value_cos_avg})=${reconstructed}`, colors.red);
          break;
        }
      }
      assertTrue(reconstructionOk, 'value_avg = ATAN2(value_sin_avg, value_cos_avg) for all rows');

      // Show sample values
      const sample = rows[0];
      log(`  Info: sample row — avg=${radToDeg(sample.value_avg).toFixed(1)}° sin=${sample.value_sin_avg.toFixed(4)} cos=${sample.value_cos_avg.toFixed(4)} n=${sample.n}`, colors.dim);
    }
    console.log();

    // Check that a non-angular path does NOT have sin/cos columns
    log('Test 11: Non-angular path should NOT have sin/cos columns', colors.yellow);
    const sogFiles = await glob(
      path.join(DATA_DIR, 'tier=5s', 'context=*', 'path=navigation__speedOverGround', 'year=*', 'day=*', '*.parquet')
    );

    if (sogFiles.length > 0) {
      const sogFile = sogFiles
        .map(f => ({ path: f, mtime: fs.statSync(f).mtimeMs }))
        .sort((a, b) => b.mtime - a.mtime)[0].path;
      const sogSchema = await conn.runAndReadAll(
        `DESCRIBE SELECT * FROM read_parquet('${sogFile}')`
      );
      const sogColumns = sogSchema.getRowObjects().map(r => r.column_name);
      assertTrue(!sogColumns.includes('value_sin_avg'), 'speedOverGround has no value_sin_avg');
      assertTrue(!sogColumns.includes('value_cos_avg'), 'speedOverGround has no value_cos_avg');
    } else {
      log('  ⚠ No speedOverGround files found, skipping', colors.yellow);
    }
    console.log();

    // Check tier-to-tier re-aggregation (60s tier)
    log('Test 12: Tier-to-tier re-aggregation (60s tier)', colors.yellow);
    const heading60sFiles = await glob(
      path.join(DATA_DIR, 'tier=60s', 'context=*', 'path=navigation__headingMagnetic', 'year=*', 'day=*', '*.parquet')
    );

    if (heading60sFiles.length > 0) {
      const heading60sFile = heading60sFiles
        .map(f => ({ path: f, mtime: fs.statSync(f).mtimeMs }))
        .sort((a, b) => b.mtime - a.mtime)[0].path;
      const schema60s = await conn.runAndReadAll(
        `DESCRIBE SELECT * FROM read_parquet('${heading60sFile}')`
      );
      const cols60s = schema60s.getRowObjects().map(r => r.column_name);
      assertTrue(cols60s.includes('value_sin_avg'), '60s tier has value_sin_avg');
      assertTrue(cols60s.includes('value_cos_avg'), '60s tier has value_cos_avg');

      const data60s = await conn.runAndReadAll(
        `SELECT value_avg, value_sin_avg, value_cos_avg FROM read_parquet('${heading60sFile}') LIMIT 3`
      );
      const rows60s = data60s.getRowObjects();
      if (rows60s.length > 0) {
        const sample60s = rows60s[0];
        const reconstructed = Math.atan2(sample60s.value_sin_avg, sample60s.value_cos_avg);
        const diff = Math.abs(sample60s.value_avg - reconstructed);
        assertTrue(diff < 0.0001, `60s tier: value_avg matches ATAN2(sin, cos) (diff=${diff.toFixed(6)})`);
      }
    } else {
      log('  ⚠ No 60s headingMagnetic files found, skipping', colors.yellow);
    }
    console.log();

  } finally {
    conn.disconnectSync();
  }
}

// ─── Part 4: Buffer Bucketing Tests ─────────────────────────────

async function runBufferBucketingTests() {
  log('Part 4: Buffer Bucketing — API returns correctly bucketed data\n', colors.yellow);

  let token;
  try {
    token = await getToken();
  } catch {
    log('  ⚠ Server not reachable, skipping buffer bucketing tests', colors.yellow);
    return;
  }

  const RESOLUTION_SECS = 300; // 5 minutes
  const to = new Date();
  const from = new Date(to.getTime() - 24 * 60 * 60 * 1000);

  // Test 13: Scalar path bucketing (headingMagnetic)
  log('Test 13: Scalar path at 5-min resolution returns bucketed data', colors.yellow);
  const scalarResult = await apiGet(
    `/signalk/v1/history/values?context=vessels.self` +
      `&from=${from.toISOString()}&to=${to.toISOString()}` +
      `&resolution=${RESOLUTION_SECS}` +
      `&paths=navigation.headingMagnetic:average`,
    token
  );

  const points = scalarResult.data || [];
  log(`  Info: ${points.length} points returned (expected ~288 for 24h at 5min)`, colors.dim);

  assertTrue(
    points.length <= 288,
    `Point count (${points.length}) is ≤ 288 (not flooded with raw buffer data)`
  );

  if (points.length >= 2) {
    // Check all timestamp gaps are multiples of the resolution
    let hasOneSecGaps = false;
    let allGapsValid = true;
    for (let i = 1; i < points.length; i++) {
      const gap = (new Date(points[i][0]).getTime() - new Date(points[i - 1][0]).getTime()) / 1000;
      if (gap <= 1) hasOneSecGaps = true;
      if (gap < RESOLUTION_SECS && gap > 0) allGapsValid = false;
    }
    assertTrue(!hasOneSecGaps, 'No 1-second gaps (raw buffer records not leaking)');
    assertTrue(allGapsValid, `All timestamp gaps ≥ ${RESOLUTION_SECS}s`);

    // Check timestamps are aligned to 5-min boundaries
    let allAligned = true;
    for (const [ts] of points) {
      const epochSec = Math.floor(new Date(ts).getTime() / 1000);
      if (epochSec % RESOLUTION_SECS !== 0) {
        allAligned = false;
        break;
      }
    }
    assertTrue(allAligned, 'All timestamps aligned to 5-minute boundaries');
  }

  // Test 14: Position (object) path bucketing
  log('\nTest 14: Object path (position) at 5-min resolution returns bucketed data', colors.yellow);
  const posResult = await apiGet(
    `/signalk/v1/history/values?context=vessels.self` +
      `&from=${from.toISOString()}&to=${to.toISOString()}` +
      `&resolution=${RESOLUTION_SECS}` +
      `&paths=navigation.position:average`,
    token
  );

  const posPoints = posResult.data || [];
  log(`  Info: ${posPoints.length} position points returned`, colors.dim);

  assertTrue(
    posPoints.length <= 288,
    `Position point count (${posPoints.length}) is ≤ 288`
  );

  if (posPoints.length >= 2) {
    let posHasOneSecGaps = false;
    for (let i = 1; i < posPoints.length; i++) {
      const gap = (new Date(posPoints[i][0]).getTime() - new Date(posPoints[i - 1][0]).getTime()) / 1000;
      if (gap <= 1) posHasOneSecGaps = true;
    }
    assertTrue(!posHasOneSecGaps, 'Position: no 1-second gaps');
  }
  console.log();
}

// ─── Main ────────────────────────────────────────────────────────

async function main() {
  log('\n═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  VECTOR AVERAGING FOR ANGULAR DATA — FULL TEST SUITE', colors.cyan);
  log('═══════════════════════════════════════════════════════════════\n', colors.cyan);

  runMathTests();
  await runLiveApiTests();
  await runParquetTests();
  await runBufferBucketingTests();

  log('═══════════════════════════════════════════════════════════════', colors.cyan);
  if (failed === 0) {
    log(`  ALL ${passed} TESTS PASSED`, colors.green);
  } else {
    log(`  ${passed} passed, ${failed} FAILED`, colors.red);
  }
  log('═══════════════════════════════════════════════════════════════\n', colors.cyan);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
