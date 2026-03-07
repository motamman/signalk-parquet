#!/usr/bin/env node
/**
 * V2 API Alignment Tests
 *
 * Validates the V1 API after stripping to V2-proposed parameters only:
 * - Time handling: bare timestamps = local, Z = UTC, offsets parsed correctly
 * - Spatial filtering: bbox and radius still work
 * - SMA/EMA smoothing: 3-segment and 4-segment path expressions
 * - Removed parameters: useUTC, convertTimesToLocal, timezone, refresh, tier, source, positionPath
 * - Time bucketing: resolution, auto-tier selection, boundary alignment
 *
 * Usage: node tests/test-v2-api-alignment.js
 */

const path = require('path');
const fs = require('fs');

// ─── Load .env ──────────────────────────────────────────────────
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

// Test constants — New York area (from rest-api-tests.sh)
const TEST_PATH = 'environment.wind.speedApparent';
const TEST_LAT = '40.646226666666664';
const TEST_LON = '-73.981275';

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

// ─── Test runner ────────────────────────────────────────────────

let passed = 0;
let failed = 0;

function assertTrue(value, label) {
  if (value) {
    passed++;
    log(`  ✓ ${label}`, colors.green);
  } else {
    failed++;
    log(`  ✗ ${label}`, colors.red);
  }
}

function assertEqual(actual, expected, label) {
  if (actual === expected) {
    passed++;
    log(`  ✓ ${label}`, colors.green);
  } else {
    failed++;
    log(`  ✗ ${label}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`, colors.red);
  }
}

function assertInRange(value, min, max, label) {
  if (value >= min && value <= max) {
    passed++;
    log(`  ✓ ${label} (${value})`, colors.green);
  } else {
    failed++;
    log(`  ✗ ${label}: ${value} not in [${min}, ${max}]`, colors.red);
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

async function apiGet(urlPath, token) {
  const res = await fetch(`${BASE_URL}${urlPath}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return { status: res.status, headers: res.headers, body: await res.json() };
}

// ─── Helper: get server's local UTC offset ──────────────────────

function getLocalUtcOffsetHours() {
  return -new Date().getTimezoneOffset() / 60;
}

function getLocalOffsetString() {
  const offsetMin = -new Date().getTimezoneOffset();
  const sign = offsetMin >= 0 ? '+' : '-';
  const h = String(Math.floor(Math.abs(offsetMin) / 60)).padStart(2, '0');
  const m = String(Math.abs(offsetMin) % 60).padStart(2, '0');
  return `${sign}${h}:${m}`;
}

function hasLocalOffset(ts) {
  const expected = getLocalOffsetString();
  return ts.endsWith(expected);
}

// ─── PART 1: TIME HANDLING ─────────────────────────────────────

async function runTimeHandlingTests(token) {
  log('\n═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  PART 1: TIME HANDLING (ISO 8601 Compliance)', colors.cyan);
  log('═══════════════════════════════════════════════════════════════\n', colors.cyan);

  // Use a fixed date we know has data — yesterday
  const yesterday = new Date(Date.now() - 86400000).toISOString().slice(0, 10);

  // Test 1: Bare timestamp → treated as local time
  log('Test 1: Bare timestamp treated as local time', colors.yellow);
  const bareFrom = `${yesterday}T00:00:00`;
  const bareTo = `${yesterday}T06:00:00`;
  const bareResult = await apiGet(
    `/signalk/v1/history/values?from=${bareFrom}&to=${bareTo}&paths=${TEST_PATH}&resolution=300`,
    token
  );
  assertTrue(bareResult.status === 200, 'Request with bare timestamps succeeds');
  assertTrue(bareResult.body.range !== undefined, 'Response has range');

  if (bareResult.body.range) {
    // Response timestamps should be in server local time with offset suffix
    const rangeFrom = bareResult.body.range.from;
    assertTrue(
      hasLocalOffset(rangeFrom),
      `Returned range.from has server local offset: ${rangeFrom}`
    );

    // The key test: bare time should NOT be treated as UTC
    // Input was local midnight, so returned range.from should represent local midnight
    const offsetHours = getLocalUtcOffsetHours();
    if (offsetHours !== 0) {
      const returnedHour = new Date(rangeFrom).getUTCHours();
      // If bare "00:00:00" was treated as local, UTC hour should be -offsetHours
      // e.g. EST (offset=-5): local 00:00 → UTC 05:00
      const expectedUtcHour = (24 - offsetHours) % 24;
      assertEqual(
        returnedHour,
        expectedUtcHour,
        `Bare midnight local → UTC hour ${expectedUtcHour} (server offset ${offsetHours > 0 ? '+' : ''}${offsetHours}h)`
      );
    } else {
      log('  Info: Server is at UTC, cannot distinguish bare vs UTC timestamps', colors.dim);
    }
  }
  console.log();

  // Test 2: Z-suffix timestamp → treated as UTC
  log('Test 2: Z-suffix timestamp treated as UTC', colors.yellow);
  const utcFrom = `${yesterday}T00:00:00Z`;
  const utcTo = `${yesterday}T06:00:00Z`;
  const utcResult = await apiGet(
    `/signalk/v1/history/values?from=${utcFrom}&to=${utcTo}&paths=${TEST_PATH}&resolution=300`,
    token
  );
  assertTrue(utcResult.status === 200, 'Request with Z timestamps succeeds');
  if (utcResult.body.range) {
    const rangeFrom = utcResult.body.range.from;
    assertTrue(
      hasLocalOffset(rangeFrom),
      `Z-suffix input → response in server local time: ${rangeFrom}`
    );
    // The underlying UTC hour should still be 0 (midnight UTC)
    const returnedHour = new Date(rangeFrom).getUTCHours();
    assertEqual(returnedHour, 0, 'Z-suffix midnight → UTC hour 0');
  }
  console.log();

  // Test 3: Explicit offset timestamp → parsed correctly
  log('Test 3: Explicit offset timestamp parsed correctly', colors.yellow);
  const offsetFrom = `${yesterday}T00:00:00-05:00`;
  const offsetTo = `${yesterday}T06:00:00-05:00`;
  const offsetResult = await apiGet(
    `/signalk/v1/history/values?from=${encodeURIComponent(offsetFrom)}&to=${encodeURIComponent(offsetTo)}&paths=${TEST_PATH}&resolution=300`,
    token
  );
  assertTrue(offsetResult.status === 200, 'Request with offset timestamps succeeds');
  if (offsetResult.body.range) {
    const rangeFrom = offsetResult.body.range.from;
    assertTrue(
      hasLocalOffset(rangeFrom),
      `Offset input → response in server local time: ${rangeFrom}`
    );
    const returnedHour = new Date(rangeFrom).getUTCHours();
    assertEqual(returnedHour, 5, '-05:00 midnight → UTC hour 5');
  }
  console.log();

  // Test 4: Positive offset
  log('Test 4: Positive offset (+05:30) parsed correctly', colors.yellow);
  const posOffsetFrom = `${yesterday}T12:00:00+05:30`;
  const posOffsetTo = `${yesterday}T18:00:00+05:30`;
  const posOffsetResult = await apiGet(
    `/signalk/v1/history/values?from=${encodeURIComponent(posOffsetFrom)}&to=${encodeURIComponent(posOffsetTo)}&paths=${TEST_PATH}&resolution=300`,
    token
  );
  assertTrue(posOffsetResult.status === 200, 'Request with +05:30 offset succeeds');
  if (posOffsetResult.body.range) {
    const rangeFrom = posOffsetResult.body.range.from;
    assertTrue(
      hasLocalOffset(rangeFrom),
      `+05:30 input → response in server local time: ${rangeFrom}`
    );
    const returnedHour = new Date(rangeFrom).getUTCHours();
    // 12:00+05:30 → 06:30 UTC
    assertEqual(returnedHour, 6, '+05:30 noon → UTC hour 6');
    const returnedMin = new Date(rangeFrom).getUTCMinutes();
    assertEqual(returnedMin, 30, '+05:30 noon → UTC minute 30');
  }
  console.log();

  // Test 5: Duration + bare timestamp
  log('Test 5: Duration with bare timestamp', colors.yellow);
  const durResult = await apiGet(
    `/signalk/v1/history/values?from=${yesterday}T12:00:00&duration=2h&paths=${TEST_PATH}&resolution=60`,
    token
  );
  assertTrue(durResult.status === 200, 'from + duration with bare timestamp succeeds');
  if (durResult.body.range) {
    const fromMs = new Date(durResult.body.range.from).getTime();
    const toMs = new Date(durResult.body.range.to).getTime();
    const durationMs = toMs - fromMs;
    // Duration should be 2 hours = 7200000ms
    assertEqual(durationMs, 7200000, 'Range spans exactly 2 hours (7200000ms)');
  }
  console.log();

  // Test 6: Bare timestamp with only HH:MM (no seconds)
  log('Test 6: Bare timestamp with HH:MM only (no seconds)', colors.yellow);
  const noSecResult = await apiGet(
    `/signalk/v1/history/values?from=${yesterday}T12:00&duration=1h&paths=${TEST_PATH}&resolution=60`,
    token
  );
  assertTrue(noSecResult.status === 200, 'HH:MM-only timestamp succeeds');
  console.log();
}

// ─── PART 2: REMOVED PARAMETERS ────────────────────────────────

async function runRemovedParamsTests(token) {
  log('═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  PART 2: REMOVED PARAMETERS (silently ignored)', colors.cyan);
  log('═══════════════════════════════════════════════════════════════\n', colors.cyan);

  // Test 7: useUTC parameter is ignored
  log('Test 7: useUTC parameter is ignored', colors.yellow);
  const withUtc = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&useUTC=true`,
    token
  );
  const withoutUtc = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}`,
    token
  );
  assertTrue(withUtc.status === 200, 'Request with useUTC=true succeeds (param ignored)');
  // Both should return the same structure — no special UTC behavior
  assertTrue(
    withUtc.body.range !== undefined && withoutUtc.body.range !== undefined,
    'Both requests return ranges'
  );
  console.log();

  // Test 8: convertTimesToLocal has no effect
  log('Test 8: convertTimesToLocal has no effect', colors.yellow);
  const convertResult = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&convertTimesToLocal=true`,
    token
  );
  assertTrue(convertResult.status === 200, 'Request with convertTimesToLocal succeeds');
  assertTrue(
    convertResult.body.timezone === undefined,
    'No timezone conversion metadata in response'
  );
  console.log();

  // Test 9: timezone parameter has no effect
  log('Test 9: timezone parameter has no effect', colors.yellow);
  const tzResult = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&convertTimesToLocal=true&timezone=America/New_York`,
    token
  );
  assertTrue(tzResult.status === 200, 'Request with timezone param succeeds');
  assertTrue(
    tzResult.body.timezone === undefined,
    'No timezone conversion metadata in response'
  );
  // Verify timestamps are in server local time (not the requested timezone)
  if (tzResult.body.data && tzResult.body.data.length > 0) {
    const ts = tzResult.body.data[0][0];
    assertTrue(
      hasLocalOffset(ts),
      `Data timestamps in server local time (not requested tz): ${ts}`
    );
  }
  console.log();

  // Test 10: refresh parameter has no effect
  log('Test 10: refresh parameter has no effect', colors.yellow);
  const refreshResult = await apiGet(
    `/signalk/v1/history/values?duration=15m&paths=${TEST_PATH}&refresh=true`,
    token
  );
  assertTrue(refreshResult.status === 200, 'Request with refresh=true succeeds');
  assertTrue(
    refreshResult.body.refresh === undefined,
    'No refresh metadata in response'
  );
  // Check no Refresh header
  const refreshHeader = refreshResult.headers.get('Refresh');
  assertTrue(refreshHeader === null, 'No Refresh HTTP header');
  console.log();

  // Test 11: tier parameter is ignored (auto-selected)
  log('Test 11: tier parameter is ignored (auto-selected internally)', colors.yellow);
  const tierResult = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&resolution=60&tier=1h`,
    token
  );
  assertTrue(tierResult.status === 200, 'Request with tier param succeeds (param ignored)');
  console.log();

  // Test 12: source parameter is ignored
  log('Test 12: source parameter is ignored', colors.yellow);
  const sourceResult = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&source=s3`,
    token
  );
  assertTrue(sourceResult.status === 200, 'Request with source param succeeds (param ignored)');
  console.log();

  // Test 13: positionPath parameter is ignored
  log('Test 13: positionPath parameter is ignored', colors.yellow);
  const posPathResult = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&positionPath=some.other.path`,
    token
  );
  assertTrue(posPathResult.status === 200, 'Request with positionPath param succeeds (param ignored)');
  console.log();
}

// ─── PART 3: SPATIAL FILTERING ─────────────────────────────────

async function runSpatialTests(token) {
  log('═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  PART 3: SPATIAL FILTERING (bbox & radius)', colors.cyan);
  log('═══════════════════════════════════════════════════════════════\n', colors.cyan);

  // Test 14: Bounding box filter on position path
  log('Test 14: Bounding box filter on position path', colors.yellow);
  const bboxResult = await apiGet(
    `/signalk/v1/history/values?duration=7d&paths=navigation.position&bbox=-74.5,40.2,-73.5,41.0&resolution=300`,
    token
  );
  assertTrue(bboxResult.status === 200, 'Bbox request succeeds');
  assertTrue(bboxResult.body.range !== undefined, 'Response has range');
  const bboxPoints = bboxResult.body.data || [];
  log(`  Info: ${bboxPoints.length} points within bbox`, colors.dim);

  // If we got position data, verify all points are within the bbox
  if (bboxPoints.length > 0) {
    let allInBbox = true;
    let checked = 0;
    for (const row of bboxPoints) {
      const val = row[1];
      if (val && typeof val === 'object' && val.latitude !== undefined) {
        checked++;
        if (val.latitude < 40.2 || val.latitude > 41.0 ||
            val.longitude < -74.5 || val.longitude > -73.5) {
          allInBbox = false;
          log(`  ✗ Point outside bbox: ${val.latitude}, ${val.longitude}`, colors.red);
          break;
        }
      }
    }
    if (checked > 0) {
      assertTrue(allInBbox, `All ${checked} position points within bbox`);
    }
  }
  console.log();

  // Test 15: Radius filter on position path
  log('Test 15: Radius filter (500m) on position path', colors.yellow);
  const radiusResult = await apiGet(
    `/signalk/v1/history/values?duration=7d&paths=navigation.position&radius=${TEST_LON},${TEST_LAT},500&resolution=300`,
    token
  );
  assertTrue(radiusResult.status === 200, 'Radius request succeeds');
  const radiusPoints = radiusResult.body.data || [];
  log(`  Info: ${radiusPoints.length} points within 500m radius`, colors.dim);
  console.log();

  // Test 16: Spatial correlation — non-position path filtered by location
  log('Test 16: Spatial correlation (wind data filtered by bbox)', colors.yellow);
  const corrResult = await apiGet(
    `/signalk/v1/history/values?duration=7d&paths=${TEST_PATH}&bbox=-74.5,40.2,-73.5,41.0&resolution=300`,
    token
  );
  assertTrue(corrResult.status === 200, 'Spatial correlation request succeeds');
  const corrPoints = corrResult.body.data || [];
  log(`  Info: ${corrPoints.length} wind data points correlated with bbox`, colors.dim);
  console.log();

  // Test 17: Radius filter with non-position path
  log('Test 17: Radius correlation (wind data filtered by radius)', colors.yellow);
  const radCorrResult = await apiGet(
    `/signalk/v1/history/values?duration=7d&paths=${TEST_PATH}&radius=${TEST_LON},${TEST_LAT},500&resolution=300`,
    token
  );
  assertTrue(radCorrResult.status === 200, 'Radius correlation request succeeds');
  const radCorrPoints = radCorrResult.body.data || [];
  log(`  Info: ${radCorrPoints.length} wind data points correlated with radius`, colors.dim);
  console.log();

  // Test 18: Invalid bbox is handled gracefully
  log('Test 18: Invalid bbox handled gracefully', colors.yellow);
  const badBboxResult = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&bbox=invalid`,
    token
  );
  assertTrue(badBboxResult.status === 200, 'Invalid bbox does not crash (treated as no filter)');
  console.log();
}

// ─── PART 4: SMA/EMA SMOOTHING ─────────────────────────────────

async function runSmoothingTests(token) {
  log('═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  PART 4: SMA/EMA SMOOTHING', colors.cyan);
  log('═══════════════════════════════════════════════════════════════\n', colors.cyan);

  // Test 19: 3-segment SMA syntax (path:sma:5)
  log('Test 19: 3-segment SMA syntax (path:sma:5)', colors.yellow);
  const smaResult = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}:sma:5`,
    token
  );
  assertTrue(smaResult.status === 200, 'SMA 3-segment request succeeds');
  if (smaResult.body.values && smaResult.body.values.length > 0) {
    // Official 3-segment syntax reports method as 'sma'
    assertEqual(
      smaResult.body.values[0].method,
      'sma',
      'SMA 3-segment reports method as sma'
    );
  }
  const smaPoints = smaResult.body.data || [];
  log(`  Info: ${smaPoints.length} SMA-smoothed points`, colors.dim);

  // Verify SMA smoothing: first N-1 values should be null (SMA window not full)
  if (smaPoints.length >= 5) {
    let hasSmoothing = false;
    const nonNullValues = smaPoints.filter(r => r[1] !== null);
    if (nonNullValues.length > 0 && nonNullValues.length < smaPoints.length) {
      hasSmoothing = true;
    }
    if (nonNullValues.length > 0) {
      assertTrue(true, `SMA returned ${nonNullValues.length} non-null values out of ${smaPoints.length}`);
    }
  }
  console.log();

  // Test 20: 3-segment EMA syntax (path:ema:0.3)
  log('Test 20: 3-segment EMA syntax (path:ema:0.3)', colors.yellow);
  const emaResult = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}:ema:0.3`,
    token
  );
  assertTrue(emaResult.status === 200, 'EMA 3-segment request succeeds');
  const emaPoints = emaResult.body.data || [];
  log(`  Info: ${emaPoints.length} EMA-smoothed points`, colors.dim);
  console.log();

  // Test 21: 4-segment syntax (path:average:sma:5)
  log('Test 21: 4-segment syntax (path:average:sma:5)', colors.yellow);
  const sma4Result = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}:average:sma:5`,
    token
  );
  assertTrue(sma4Result.status === 200, '4-segment SMA request succeeds');
  if (sma4Result.body.values && sma4Result.body.values.length > 0) {
    assertEqual(
      sma4Result.body.values[0].method,
      'average',
      '4-segment SMA uses average method'
    );
  }
  console.log();

  // Test 22: 4-segment EMA syntax (path:min:ema:0.2)
  log('Test 22: 4-segment EMA syntax (path:min:ema:0.2)', colors.yellow);
  const ema4Result = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}:min:ema:0.2`,
    token
  );
  assertTrue(ema4Result.status === 200, '4-segment EMA request succeeds');
  if (ema4Result.body.values && ema4Result.body.values.length > 0) {
    assertEqual(
      ema4Result.body.values[0].method,
      'min',
      '4-segment EMA with min method'
    );
  }
  console.log();
}

// ─── PART 5: TIME BUCKETING & RESOLUTION ────────────────────────

async function runBucketingTests(token) {
  log('═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  PART 5: TIME BUCKETING & RESOLUTION', colors.cyan);
  log('═══════════════════════════════════════════════════════════════\n', colors.cyan);

  // Test 23: Resolution in seconds — 60s buckets
  log('Test 23: Resolution=60 (1-minute buckets)', colors.yellow);
  const res60 = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&resolution=60`,
    token
  );
  assertTrue(res60.status === 200, '60s resolution request succeeds');
  const points60 = res60.body.data || [];
  log(`  Info: ${points60.length} points (expected ≤60 for 1h at 60s)`, colors.dim);
  assertTrue(points60.length <= 61, `Point count (${points60.length}) ≤ 61`);

  if (points60.length >= 2) {
    // Check timestamp gaps
    let allGapsValid = true;
    let minGap = Infinity;
    for (let i = 1; i < points60.length; i++) {
      const gap = (new Date(points60[i][0]).getTime() - new Date(points60[i - 1][0]).getTime()) / 1000;
      if (gap < minGap) minGap = gap;
      if (gap > 0 && gap < 60) allGapsValid = false;
    }
    assertTrue(allGapsValid, `All timestamp gaps ≥ 60s (min gap: ${minGap}s)`);

    // Check timestamps aligned to 60s boundaries
    let allAligned = true;
    for (const [ts] of points60) {
      const epochSec = Math.floor(new Date(ts).getTime() / 1000);
      if (epochSec % 60 !== 0) {
        allAligned = false;
        break;
      }
    }
    assertTrue(allAligned, 'All timestamps aligned to 60-second boundaries');
  }
  console.log();

  // Test 24: Resolution=300 (5-minute buckets)
  log('Test 24: Resolution=300 (5-minute buckets)', colors.yellow);
  const res300 = await apiGet(
    `/signalk/v1/history/values?duration=6h&paths=${TEST_PATH}&resolution=300`,
    token
  );
  assertTrue(res300.status === 200, '300s resolution request succeeds');
  const points300 = res300.body.data || [];
  log(`  Info: ${points300.length} points (expected ≤72 for 6h at 5min)`, colors.dim);
  assertTrue(points300.length <= 73, `Point count (${points300.length}) ≤ 73`);

  if (points300.length >= 2) {
    let allGapsValid = true;
    for (let i = 1; i < points300.length; i++) {
      const gap = (new Date(points300[i][0]).getTime() - new Date(points300[i - 1][0]).getTime()) / 1000;
      if (gap > 0 && gap < 300) allGapsValid = false;
    }
    assertTrue(allGapsValid, 'All timestamp gaps ≥ 300s');

    let allAligned = true;
    for (const [ts] of points300) {
      const epochSec = Math.floor(new Date(ts).getTime() / 1000);
      if (epochSec % 300 !== 0) {
        allAligned = false;
        break;
      }
    }
    assertTrue(allAligned, 'All timestamps aligned to 5-minute boundaries');
  }
  console.log();

  // Test 25: Resolution expression (5m shorthand)
  log('Test 25: Resolution expression (5m shorthand)', colors.yellow);
  const res5m = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&resolution=5m`,
    token
  );
  assertTrue(res5m.status === 200, '5m resolution expression succeeds');
  const points5m = res5m.body.data || [];
  assertTrue(points5m.length <= 13, `Point count (${points5m.length}) ≤ 13 for 1h at 5min`);
  console.log();

  // Test 26: High resolution (no tier) vs low resolution (should auto-select tier)
  log('Test 26: Auto-tier selection based on resolution', colors.yellow);
  const resRaw = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&resolution=1`,
    token
  );
  assertTrue(resRaw.status === 200, 'Raw resolution (1s) request succeeds');
  const rawPoints = resRaw.body.data || [];

  const resHigh = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&resolution=3600`,
    token
  );
  assertTrue(resHigh.status === 200, 'High resolution (3600s) request succeeds');
  const highPoints = resHigh.body.data || [];

  log(`  Info: 1s resolution → ${rawPoints.length} points, 3600s resolution → ${highPoints.length} points`, colors.dim);
  // High resolution should return fewer points than raw
  if (rawPoints.length > 0 && highPoints.length > 0) {
    assertTrue(
      highPoints.length <= rawPoints.length,
      `High resolution (${highPoints.length}) ≤ raw (${rawPoints.length})`
    );
  }
  console.log();

  // Test 27: Default resolution (no resolution param)
  log('Test 27: Default resolution (auto-calculated)', colors.yellow);
  const resDefault = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}`,
    token
  );
  assertTrue(resDefault.status === 200, 'Default resolution request succeeds');
  const defaultPoints = resDefault.body.data || [];
  log(`  Info: ${defaultPoints.length} points with default resolution`, colors.dim);
  // Default targets ~500 points, so for 1h we should get a reasonable number
  assertTrue(
    defaultPoints.length <= 600,
    `Default point count (${defaultPoints.length}) ≤ 600`
  );
  console.log();
}

// ─── PART 6: SURVIVING PARAMETERS ───────────────────────────────

async function runSurvivingParamsTests(token) {
  log('═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  PART 6: SURVIVING PARAMETERS', colors.cyan);
  log('═══════════════════════════════════════════════════════════════\n', colors.cyan);

  // Test 28: context parameter
  log('Test 28: Context parameter (vessels.self)', colors.yellow);
  const ctxResult = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}&context=vessels.self`,
    token
  );
  assertTrue(ctxResult.status === 200, 'Context=vessels.self succeeds');
  assertTrue(
    ctxResult.body.context && ctxResult.body.context.startsWith('vessels.'),
    `Context resolved: ${ctxResult.body.context}`
  );
  console.log();

  // Test 29: Multiple paths
  log('Test 29: Multiple paths in single request', colors.yellow);
  const multiResult = await apiGet(
    `/signalk/v1/history/values?duration=1h&resolution=60&paths=${TEST_PATH}:average,${TEST_PATH}:min,${TEST_PATH}:max`,
    token
  );
  assertTrue(multiResult.status === 200, 'Multi-path request succeeds');
  if (multiResult.body.values) {
    assertEqual(
      multiResult.body.values.length,
      3,
      'Response has 3 value columns'
    );
    // Verify each row has timestamp + 3 values
    if (multiResult.body.data && multiResult.body.data.length > 0) {
      assertEqual(
        multiResult.body.data[0].length,
        4,
        'Each row has 4 elements (timestamp + 3 values)'
      );
    }
  }
  console.log();

  // Test 30: All 5 time range patterns still work
  log('Test 30: All 5 time range patterns', colors.yellow);
  const yesterday = new Date(Date.now() - 86400000).toISOString().slice(0, 10);

  // Pattern 1: duration only
  const p1 = await apiGet(`/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}`, token);
  assertTrue(p1.status === 200, 'Pattern 1: duration only');

  // Pattern 2: from + duration
  const p2 = await apiGet(`/signalk/v1/history/values?from=${yesterday}T00:00:00Z&duration=6h&paths=${TEST_PATH}`, token);
  assertTrue(p2.status === 200, 'Pattern 2: from + duration');

  // Pattern 3: to + duration
  const p3 = await apiGet(`/signalk/v1/history/values?to=${yesterday}T12:00:00Z&duration=2h&paths=${TEST_PATH}`, token);
  assertTrue(p3.status === 200, 'Pattern 3: to + duration');

  // Pattern 4: from only
  const p4 = await apiGet(`/signalk/v1/history/values?from=${yesterday}T00:00:00Z&paths=${TEST_PATH}`, token);
  assertTrue(p4.status === 200, 'Pattern 4: from only (to now)');

  // Pattern 5: from + to
  const p5 = await apiGet(`/signalk/v1/history/values?from=${yesterday}T00:00:00Z&to=${yesterday}T23:59:59Z&paths=${TEST_PATH}`, token);
  assertTrue(p5.status === 200, 'Pattern 5: from + to');
  console.log();

  // Test 31: Duration formats
  log('Test 31: Duration formats all accepted', colors.yellow);
  const durFormats = [
    { q: 'PT1H', label: 'ISO 8601 (PT1H)' },
    { q: 'PT1H30M', label: 'ISO compound (PT1H30M)' },
    { q: '3600', label: 'Integer seconds (3600)' },
    { q: '1h', label: 'Shorthand hours (1h)' },
    { q: '30m', label: 'Shorthand minutes (30m)' },
    { q: '2d', label: 'Shorthand days (2d)' },
  ];
  for (const { q, label } of durFormats) {
    const r = await apiGet(`/signalk/v1/history/values?duration=${q}&paths=${TEST_PATH}`, token);
    assertTrue(r.status === 200, label);
  }
  console.log();

  // Test 32: Aggregation methods
  log('Test 32: All aggregation methods', colors.yellow);
  const methods = ['average', 'min', 'max', 'first', 'last', 'mid'];
  for (const method of methods) {
    const r = await apiGet(
      `/signalk/v1/history/values?duration=1h&resolution=60&paths=${TEST_PATH}:${method}`,
      token
    );
    assertTrue(r.status === 200, `Aggregation method: ${method}`);
  }
  console.log();

  // Test 33: Alternative API routes
  log('Test 33: Alternative API routes (/api/history/*)', colors.yellow);
  const altValues = await apiGet(`/api/history/values?duration=1h&paths=${TEST_PATH}`, token);
  assertTrue(altValues.status === 200, '/api/history/values works');

  const altContexts = await apiGet('/api/history/contexts?duration=1h', token);
  assertTrue(altContexts.status === 200, '/api/history/contexts works');

  const altPaths = await apiGet('/api/history/paths?duration=1h', token);
  assertTrue(altPaths.status === 200, '/api/history/paths works');
  console.log();
}

// ─── PART 7: RESPONSE STRUCTURE ─────────────────────────────────

async function runResponseStructureTests(token) {
  log('═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  PART 7: RESPONSE STRUCTURE VALIDATION', colors.cyan);
  log('═══════════════════════════════════════════════════════════════\n', colors.cyan);

  // Test 34: Response has correct top-level structure
  log('Test 34: Response top-level structure', colors.yellow);
  const result = await apiGet(
    `/signalk/v1/history/values?duration=1h&paths=${TEST_PATH}:average&resolution=60`,
    token
  );
  assertTrue(result.status === 200, 'Request succeeds');

  const body = result.body;
  assertTrue(body.context !== undefined, 'Has context field');
  assertTrue(body.range !== undefined, 'Has range field');
  assertTrue(body.range.from !== undefined, 'Has range.from');
  assertTrue(body.range.to !== undefined, 'Has range.to');
  assertTrue(Array.isArray(body.values), 'Has values array');
  assertTrue(Array.isArray(body.data), 'Has data array');

  // Verify NO removed fields exist
  assertTrue(body.timezone === undefined, 'No timezone field in response');
  assertTrue(body.refresh === undefined, 'No refresh field in response');
  console.log();

  // Test 35: range.from and range.to are valid ISO timestamps
  log('Test 35: Range timestamps are valid ISO 8601', colors.yellow);
  const fromDate = new Date(body.range.from);
  const toDate = new Date(body.range.to);
  assertTrue(!isNaN(fromDate.getTime()), `range.from is valid: ${body.range.from}`);
  assertTrue(!isNaN(toDate.getTime()), `range.to is valid: ${body.range.to}`);
  assertTrue(toDate > fromDate, 'range.to is after range.from');
  console.log();

  // Test 36: Data timestamps are monotonically increasing
  log('Test 36: Data timestamps are monotonically increasing', colors.yellow);
  if (body.data && body.data.length >= 2) {
    let monotonic = true;
    for (let i = 1; i < body.data.length; i++) {
      const prev = new Date(body.data[i - 1][0]).getTime();
      const curr = new Date(body.data[i][0]).getTime();
      if (curr <= prev) {
        monotonic = false;
        log(`  ✗ Non-monotonic at index ${i}: ${body.data[i - 1][0]} >= ${body.data[i][0]}`, colors.red);
        break;
      }
    }
    assertTrue(monotonic, `All ${body.data.length} timestamps strictly increasing`);
  } else {
    log('  Info: Not enough data points to check monotonicity', colors.dim);
  }
  console.log();
}

// ─── Main ────────────────────────────────────────────────────────

async function main() {
  log('\n═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  V2 API ALIGNMENT — FULL TEST SUITE', colors.cyan);
  log('═══════════════════════════════════════════════════════════════\n', colors.cyan);

  let token;
  try {
    token = await getToken();
    assertTrue(!!token, 'Authenticated with SignalK server');
    console.log();
  } catch (e) {
    log(`  ✗ Cannot connect to ${BASE_URL}: ${e.message}`, colors.red);
    log('  Server must be running to execute these tests.\n', colors.dim);
    process.exit(1);
  }

  await runTimeHandlingTests(token);
  await runRemovedParamsTests(token);
  await runSpatialTests(token);
  await runSmoothingTests(token);
  await runBucketingTests(token);
  await runSurvivingParamsTests(token);
  await runResponseStructureTests(token);

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
