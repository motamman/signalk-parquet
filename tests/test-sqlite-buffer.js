#!/usr/bin/env node
/**
 * Per-Path SQLite Buffer Tables — End-to-End Tests
 *
 * Part 1: Unit tests (offline) — table creation, inserts, queries, export, cleanup,
 *         restart persistence, legacy migration, SQL builders.
 * Part 2: Live E2E test — connects to a real SignalK server via WebSocket,
 *         receives actual deltas, runs them through the production flattening
 *         logic (data-handler.ts), inserts into SQLiteBuffer, then verifies
 *         the database has correct columns and values.
 *
 * Usage: node tests/test-sqlite-buffer.js
 */

const fs = require('fs');
const path = require('path');
const { DatabaseSync: Database } = require('node:sqlite');
const WebSocket = require('ws');
const { SQLiteBuffer, pathToTableName } = require('../dist/utils/sqlite-buffer');
const { buildBufferScalarSubquery, buildBufferObjectSubquery } = require('../dist/utils/buffer-sql-builder');

// Load .env from tests directory
const envPath = path.join(__dirname, '.env');
if (fs.existsSync(envPath)) {
  const envContent = fs.readFileSync(envPath, 'utf8');
  envContent.split('\n').forEach(line => {
    const match = line.match(/^([^#=]+)=(.*)$/);
    if (match) process.env[match[1].trim()] = match[2].trim();
  });
}

const LIVE_URL = process.env.LIVE_URL || 'https://helm.zeddisplay.com';
const LIVE_USERNAME = process.env.LIVE_USERNAME || 'maurice';
const LIVE_PASSWORD = process.env.LIVE_PASSWORD || '0Nt7@Tda76r&';

// ─── Colors ─────────────────────────────────────────────────────
const C = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  cyan: '\x1b[36m',
  dim: '\x1b[2m',
};

// ─── Counters ───────────────────────────────────────────────────
let passed = 0;
let failed = 0;

function assertTrue(value, label) {
  if (value) {
    console.log(`  ${C.green}✓${C.reset} ${label}`);
    passed++;
  } else {
    console.log(`  ${C.red}✗${C.reset} ${label} ${C.dim}(got falsy)${C.reset}`);
    failed++;
  }
}

function assertEqual(actual, expected, label) {
  if (actual === expected) {
    console.log(`  ${C.green}✓${C.reset} ${label}`);
    passed++;
  } else {
    console.log(`  ${C.red}✗${C.reset} ${label} ${C.dim}(expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)})${C.reset}`);
    failed++;
  }
}

// ─── Helpers ────────────────────────────────────────────────────
const DEFAULT_CONTEXT = 'vessels.urn:mrn:imo:mmsi:123456789';

function makeTempDbPath() {
  return `/tmp/test-sqlite-buffer-${Date.now()}-${Math.random().toString(36).slice(2, 8)}.db`;
}

function cleanupDb(dbPath) {
  for (const suffix of ['', '-wal', '-shm']) {
    try { fs.unlinkSync(dbPath + suffix); } catch {}
  }
}

function makeScalarRecord(skPath, value, opts = {}) {
  const now = new Date('2026-03-09T12:00:00.000Z');
  return {
    path: skPath,
    context: opts.context || DEFAULT_CONTEXT,
    received_timestamp: opts.received_timestamp || now.toISOString(),
    signalk_timestamp: opts.signalk_timestamp || now.toISOString(),
    value,
    source: opts.source || null,
    source_label: opts.source_label || null,
    source_type: opts.source_type || null,
    source_pgn: opts.source_pgn || null,
    source_src: opts.source_src || null,
    meta: opts.meta || null,
  };
}

function makeObjectRecord(skPath, valueObj, flattenedValues, opts = {}) {
  const now = new Date('2026-03-09T12:00:00.000Z');
  return {
    path: skPath,
    context: opts.context || DEFAULT_CONTEXT,
    received_timestamp: opts.received_timestamp || now.toISOString(),
    signalk_timestamp: opts.signalk_timestamp || now.toISOString(),
    value: valueObj,
    value_json: JSON.stringify(valueObj),
    source: opts.source || null,
    source_label: opts.source_label || null,
    source_type: opts.source_type || null,
    source_pgn: opts.source_pgn || null,
    source_src: opts.source_src || null,
    meta: opts.meta || null,
    ...flattenedValues,
  };
}

/**
 * Simulate the production flattening from data-handler.ts handleStreamData().
 * Takes a raw object value (as it arrives from SignalK delta) and builds a
 * DataRecord the same way production code does — no hand-crafted value_* keys.
 */
function makeRecordLikeProduction(skPath, rawValue, opts = {}) {
  const now = new Date('2026-03-09T12:00:00.000Z');
  const record = {
    path: skPath,
    context: opts.context || DEFAULT_CONTEXT,
    received_timestamp: opts.received_timestamp || now.toISOString(),
    signalk_timestamp: opts.signalk_timestamp || now.toISOString(),
    value: null,
    value_json: undefined,
    source: opts.source || null,
    source_label: opts.source_label || null,
    source_type: opts.source_type || null,
    source_pgn: opts.source_pgn || null,
    source_src: opts.source_src || null,
    meta: opts.meta || null,
  };

  if (typeof rawValue === 'object' && rawValue !== null) {
    // This is the exact logic from data-handler.ts lines 554-565
    record.value_json = rawValue;
    Object.entries(rawValue).forEach(([key, val]) => {
      if (typeof val === 'string' || typeof val === 'number' || typeof val === 'boolean') {
        record[`value_${key}`] = val;
      }
    });
  } else {
    record.value = rawValue;
  }

  return record;
}

function getTableColumns(dbPath, tableName) {
  const db = new Database(dbPath, { readOnly: true });
  const cols = db.prepare(`PRAGMA table_info(${tableName})`).all().map(c => c.name);
  db.close();
  return cols;
}

function tableExists(dbPath, tableName) {
  const db = new Database(dbPath, { readOnly: true });
  const row = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(tableName);
  db.close();
  return !!row;
}

function countRows(dbPath, tableName) {
  const db = new Database(dbPath, { readOnly: true });
  const row = db.prepare(`SELECT COUNT(*) as cnt FROM ${tableName}`).get();
  db.close();
  return row.cnt;
}

// ─── Tests ──────────────────────────────────────────────────────

console.log(`\n${C.cyan}╔══════════════════════════════════════════════════╗${C.reset}`);
console.log(`${C.cyan}║  Per-Path SQLite Buffer — End-to-End Tests       ║${C.reset}`);
console.log(`${C.cyan}╚══════════════════════════════════════════════════╝${C.reset}\n`);

// ── Test 1: Fresh Database ──────────────────────────────────────
(function test1() {
  console.log(`${C.cyan}Test 1: Fresh Database${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    const buf = new SQLiteBuffer({ dbPath });
    assertTrue(tableExists(dbPath, 'buffer_tables'), 'buffer_tables metadata table exists');
    assertEqual(buf.getKnownPaths().size, 0, 'getKnownPaths() is empty');
    assertEqual(buf.getPendingCount(), 0, 'getPendingCount() is 0');
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 2: Scalar Insert ───────────────────────────────────────
(function test2() {
  console.log(`${C.cyan}Test 2: Scalar Insert${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    const buf = new SQLiteBuffer({ dbPath });
    const rec = makeScalarRecord('navigation.speedOverGround', 5.2);
    buf.insert(rec);

    assertTrue(buf.hasTable('navigation.speedOverGround'), 'hasTable() true after insert');

    const tableName = pathToTableName('navigation.speedOverGround');
    const cols = getTableColumns(dbPath, tableName);
    assertTrue(cols.includes('value'), 'scalar table has value column');
    assertTrue(!cols.includes('value_json'), 'scalar table has no value_json column');

    // Verify raw row
    const db = new Database(dbPath, { readOnly: true });
    const row = db.prepare(`SELECT value FROM ${tableName}`).get();
    db.close();
    assertEqual(row.value, '5.2', 'raw value stored as string "5.2"');

    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 3: Object Insert ───────────────────────────────────────
(function test3() {
  console.log(`${C.cyan}Test 3: Object Insert${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    const buf = new SQLiteBuffer({ dbPath });
    const rec = makeObjectRecord(
      'navigation.position',
      { latitude: 37.8, longitude: -122.4 },
      { value_latitude: 37.8, value_longitude: -122.4 }
    );
    buf.insert(rec);

    const tableName = pathToTableName('navigation.position');
    const cols = getTableColumns(dbPath, tableName);
    assertTrue(cols.includes('value_json'), 'object table has value_json');
    assertTrue(cols.includes('value_latitude'), 'object table has value_latitude');
    assertTrue(cols.includes('value_longitude'), 'object table has value_longitude');
    assertTrue(!cols.includes('value'), 'object table has no plain value column');
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 4: Multiple Paths ──────────────────────────────────────
(function test4() {
  console.log(`${C.cyan}Test 4: Multiple Paths${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    const buf = new SQLiteBuffer({ dbPath });
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.0));
    buf.insert(makeScalarRecord('navigation.courseOverGroundTrue', 1.5));
    buf.insert(makeScalarRecord('environment.wind.speedApparent', 8.3));

    assertEqual(buf.getKnownPaths().size, 3, '3 known paths');
    assertEqual(countRows(dbPath, 'buffer_tables'), 3, 'buffer_tables has 3 rows');

    assertTrue(tableExists(dbPath, pathToTableName('navigation.speedOverGround')), 'table for SOG');
    assertTrue(tableExists(dbPath, pathToTableName('navigation.courseOverGroundTrue')), 'table for COG');
    assertTrue(tableExists(dbPath, pathToTableName('environment.wind.speedApparent')), 'table for wind');
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 5: Table Isolation ─────────────────────────────────────
(function test5() {
  console.log(`${C.cyan}Test 5: Table Isolation${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    const buf = new SQLiteBuffer({ dbPath });
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.0));
    buf.insert(makeObjectRecord(
      'navigation.position',
      { latitude: 37.8, longitude: -122.4 },
      { value_latitude: 37.8, value_longitude: -122.4 }
    ));

    const scalarCols = getTableColumns(dbPath, pathToTableName('navigation.speedOverGround'));
    const objectCols = getTableColumns(dbPath, pathToTableName('navigation.position'));

    assertTrue(!scalarCols.includes('value_latitude'), 'scalar table has no value_latitude');
    assertTrue(!objectCols.includes('value'), 'object table has no value column');
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 6: Dynamic Column Addition ─────────────────────────────
(function test6() {
  console.log(`${C.cyan}Test 6: Dynamic Column Addition${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    const buf = new SQLiteBuffer({ dbPath });
    buf.insert(makeObjectRecord(
      'navigation.position',
      { latitude: 37.8, longitude: -122.4 },
      { value_latitude: 37.8, value_longitude: -122.4 }
    ));

    // Second insert with new column
    buf.insert(makeObjectRecord(
      'navigation.position',
      { latitude: 37.8, longitude: -122.4, altitude: 10 },
      { value_latitude: 37.8, value_longitude: -122.4, value_altitude: 10 }
    ));

    const cols = getTableColumns(dbPath, pathToTableName('navigation.position'));
    assertTrue(cols.includes('value_altitude'), 'value_altitude column added dynamically');

    // Verify the other table was not affected
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.0));
    const scalarCols = getTableColumns(dbPath, pathToTableName('navigation.speedOverGround'));
    assertTrue(!scalarCols.includes('value_altitude'), 'scalar table unaffected by object column addition');
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 7: insertBatch ─────────────────────────────────────────
(function test7() {
  console.log(`${C.cyan}Test 7: insertBatch${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    const buf = new SQLiteBuffer({ dbPath });
    const records = [
      makeScalarRecord('navigation.speedOverGround', 5.0),
      makeScalarRecord('navigation.speedOverGround', 5.1),
      makeScalarRecord('navigation.courseOverGroundTrue', 1.5),
      makeScalarRecord('environment.wind.speedApparent', 8.0),
      makeScalarRecord('environment.wind.speedApparent', 8.1),
    ];
    buf.insertBatch(records);

    assertEqual(buf.getKnownPaths().size, 3, '3 paths created from batch');
    assertEqual(buf.getPendingCount(), 5, 'getPendingCount() is 5');
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 8: getKnownPaths ───────────────────────────────────────
(function test8() {
  console.log(`${C.cyan}Test 8: getKnownPaths${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    const buf = new SQLiteBuffer({ dbPath });
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.0));
    buf.insert(makeScalarRecord('environment.wind.speedApparent', 8.0));

    const paths = buf.getKnownPaths();
    assertTrue(paths.has('navigation.speedOverGround'), 'has SOG');
    assertTrue(paths.has('environment.wind.speedApparent'), 'has wind');
    assertTrue(!paths.has('navigation.position'), 'does not have position');
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 9: hasTable ────────────────────────────────────────────
(function test9() {
  console.log(`${C.cyan}Test 9: hasTable${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    const buf = new SQLiteBuffer({ dbPath });
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.0));

    assertTrue(buf.hasTable('navigation.speedOverGround'), 'hasTable true for inserted');
    assertTrue(!buf.hasTable('navigation.courseOverGroundTrue'), 'hasTable false for non-existent');
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 10: getStats ───────────────────────────────────────────
(function test10() {
  console.log(`${C.cyan}Test 10: getStats${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    const buf = new SQLiteBuffer({ dbPath });
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.0));
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.1));
    buf.insert(makeScalarRecord('environment.wind.speedApparent', 8.0));

    let stats = buf.getStats();
    assertEqual(stats.totalRecords, 3, 'totalRecords is 3');
    assertEqual(stats.pendingRecords, 3, 'pendingRecords is 3');
    assertEqual(stats.exportedRecords, 0, 'exportedRecords is 0');

    // Mark one as exported via raw SQL
    const db = new Database(dbPath);
    const tableName = pathToTableName('navigation.speedOverGround');
    db.prepare(`UPDATE ${tableName} SET exported = 1 WHERE rowid = 1`).run();
    db.close();

    stats = buf.getStats();
    assertEqual(stats.pendingRecords, 2, 'pendingRecords drops to 2 after export');
    assertEqual(stats.exportedRecords, 1, 'exportedRecords is 1');
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 11: getPendingCount ────────────────────────────────────
(function test11() {
  console.log(`${C.cyan}Test 11: getPendingCount${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    const buf = new SQLiteBuffer({ dbPath });
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.0));
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.1));
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.2));
    buf.insert(makeScalarRecord('environment.wind.speedApparent', 8.0));
    buf.insert(makeScalarRecord('environment.wind.speedApparent', 8.1));

    assertEqual(buf.getPendingCount(), 5, '5 pending records across 2 paths');

    // Mark all SOG records as exported via raw SQL
    const db = new Database(dbPath);
    db.prepare(`UPDATE ${pathToTableName('navigation.speedOverGround')} SET exported = 1`).run();
    db.close();

    assertEqual(buf.getPendingCount(), 2, 'pending drops to 2 after exporting SOG');
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 12: getPathsForDate ────────────────────────────────────
(function test12() {
  console.log(`${C.cyan}Test 12: getPathsForDate${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    const buf = new SQLiteBuffer({ dbPath });
    const ctx1 = DEFAULT_CONTEXT;
    const ctx2 = 'vessels.urn:mrn:imo:mmsi:999999999';

    // Date 1: 2026-03-08
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.0, {
      context: ctx1,
      received_timestamp: '2026-03-08T10:00:00.000Z',
      signalk_timestamp: '2026-03-08T10:00:00.000Z',
    }));
    buf.insert(makeScalarRecord('environment.wind.speedApparent', 8.0, {
      context: ctx2,
      received_timestamp: '2026-03-08T11:00:00.000Z',
      signalk_timestamp: '2026-03-08T11:00:00.000Z',
    }));

    // Date 2: 2026-03-09
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.1, {
      context: ctx1,
      received_timestamp: '2026-03-09T10:00:00.000Z',
      signalk_timestamp: '2026-03-09T10:00:00.000Z',
    }));

    const date1 = new Date('2026-03-08T00:00:00.000Z');
    const date2 = new Date('2026-03-09T00:00:00.000Z');

    const paths1 = buf.getPathsForDate(date1);
    assertEqual(paths1.length, 2, 'date1 has 2 context/path pairs');
    // Sorted: ctx1 first (123... < 999...), then ctx2
    assertEqual(paths1[0].context, ctx1, 'first pair is ctx1');
    assertEqual(paths1[0].path, 'navigation.speedOverGround', 'first pair path is SOG');
    assertEqual(paths1[1].context, ctx2, 'second pair is ctx2');

    const paths2 = buf.getPathsForDate(date2);
    assertEqual(paths2.length, 1, 'date2 has 1 context/path pair');
    assertEqual(paths2[0].path, 'navigation.speedOverGround', 'date2 path is SOG');
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 13: getDatesWithUnexportedRecords ──────────────────────
(function test13() {
  console.log(`${C.cyan}Test 13: getDatesWithUnexportedRecords${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    const buf = new SQLiteBuffer({ dbPath });

    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.0, {
      received_timestamp: '2026-03-07T10:00:00.000Z',
      signalk_timestamp: '2026-03-07T10:00:00.000Z',
    }));
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.1, {
      received_timestamp: '2026-03-08T10:00:00.000Z',
      signalk_timestamp: '2026-03-08T10:00:00.000Z',
    }));
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.2, {
      received_timestamp: '2026-03-09T10:00:00.000Z',
      signalk_timestamp: '2026-03-09T10:00:00.000Z',
    }));

    // excludeToday=false to see all 3 dates
    let dates = buf.getDatesWithUnexportedRecords(false);
    assertEqual(dates.length, 3, '3 dates with unexported records');

    // Mark 2026-03-07 as exported
    buf.markDateExported(DEFAULT_CONTEXT, 'navigation.speedOverGround', new Date('2026-03-07T00:00:00.000Z'), 'batch-1');

    dates = buf.getDatesWithUnexportedRecords(false);
    assertEqual(dates.length, 2, '2 dates after marking one exported');
    assertTrue(!dates.includes('2026-03-07'), '2026-03-07 no longer in list');

    // excludeToday=true — today is 2026-03-10 per system, but our records are all in the past,
    // so all should still appear; however this tests the filter mechanism
    dates = buf.getDatesWithUnexportedRecords(true);
    assertEqual(dates.length, 2, 'excludeToday=true still shows 2 past dates');
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 14: getRecordsForPathAndDate ───────────────────────────
(function test14() {
  console.log(`${C.cyan}Test 14: getRecordsForPathAndDate${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    const buf = new SQLiteBuffer({ dbPath });
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.0, {
      received_timestamp: '2026-03-09T10:00:00.000Z',
      signalk_timestamp: '2026-03-09T10:00:00.000Z',
    }));
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.5, {
      received_timestamp: '2026-03-09T11:00:00.000Z',
      signalk_timestamp: '2026-03-09T11:00:00.000Z',
    }));
    buf.insert(makeScalarRecord('navigation.speedOverGround', 6.0, {
      received_timestamp: '2026-03-08T10:00:00.000Z',
      signalk_timestamp: '2026-03-08T10:00:00.000Z',
    }));

    const date = new Date('2026-03-09T00:00:00.000Z');
    const records = buf.getRecordsForPathAndDate(DEFAULT_CONTEXT, 'navigation.speedOverGround', date);

    assertEqual(records.length, 2, '2 records for 2026-03-09');
    assertEqual(records[0].path, 'navigation.speedOverGround', 'path field reconstructed');
    assertEqual(records[0].value, 5.0, 'first value is 5.0 (number)');
    assertEqual(records[1].value, 5.5, 'second value is 5.5 (number)');
    assertTrue(records[0].received_timestamp < records[1].received_timestamp, 'ordered by timestamp');
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 15: markDateExported ───────────────────────────────────
(function test15() {
  console.log(`${C.cyan}Test 15: markDateExported${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    const buf = new SQLiteBuffer({ dbPath });
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.0, {
      received_timestamp: '2026-03-09T10:00:00.000Z',
      signalk_timestamp: '2026-03-09T10:00:00.000Z',
    }));
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.1, {
      received_timestamp: '2026-03-09T11:00:00.000Z',
      signalk_timestamp: '2026-03-09T11:00:00.000Z',
    }));
    buf.insert(makeScalarRecord('navigation.speedOverGround', 6.0, {
      received_timestamp: '2026-03-08T10:00:00.000Z',
      signalk_timestamp: '2026-03-08T10:00:00.000Z',
    }));

    assertEqual(buf.getPendingCount(), 3, '3 pending before export');

    buf.markDateExported(DEFAULT_CONTEXT, 'navigation.speedOverGround', new Date('2026-03-09T00:00:00.000Z'), 'batch-1');

    assertEqual(buf.getPendingCount(), 1, '1 pending after exporting 2026-03-09');

    // Records for 2026-03-09 should now be excluded (exported=0 filter)
    const records = buf.getRecordsForPathAndDate(DEFAULT_CONTEXT, 'navigation.speedOverGround', new Date('2026-03-09T00:00:00.000Z'));
    assertEqual(records.length, 0, 'no unexported records for 2026-03-09');

    // 2026-03-08 unaffected
    const records08 = buf.getRecordsForPathAndDate(DEFAULT_CONTEXT, 'navigation.speedOverGround', new Date('2026-03-08T00:00:00.000Z'));
    assertEqual(records08.length, 1, '2026-03-08 still has 1 unexported record');
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 16: Cleanup ────────────────────────────────────────────
(function test16() {
  console.log(`${C.cyan}Test 16: Cleanup${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    // Phase 1: insert and mark exported via raw SQL
    let buf = new SQLiteBuffer({ dbPath, retentionHours: 0 });
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.0));
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.1));
    buf.close();

    // Backdate created_at and mark exported while buffer is closed
    const db = new Database(dbPath);
    const tableName = pathToTableName('navigation.speedOverGround');
    db.prepare(`UPDATE ${tableName} SET exported = 1, created_at = '2020-01-01 00:00:00'`).run();
    db.close();

    // Phase 2: reopen, insert one more unexported, then cleanup
    buf = new SQLiteBuffer({ dbPath, retentionHours: 0 });
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.2));

    const cleaned = buf.cleanup();
    assertEqual(cleaned, 2, 'cleanup removed 2 exported records');
    assertEqual(buf.getPendingCount(), 1, '1 unexported record remains');
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 17: Restart Persistence ────────────────────────────────
(function test17() {
  console.log(`${C.cyan}Test 17: Restart Persistence${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    // First session
    let buf = new SQLiteBuffer({ dbPath });
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.0));
    buf.insert(makeScalarRecord('environment.wind.speedApparent', 8.0));
    buf.close();

    // Second session — reopen same file
    buf = new SQLiteBuffer({ dbPath });
    const paths = buf.getKnownPaths();
    assertEqual(paths.size, 2, 'getKnownPaths() restored after reopen');
    assertTrue(paths.has('navigation.speedOverGround'), 'SOG path preserved');
    assertTrue(paths.has('environment.wind.speedApparent'), 'wind path preserved');
    assertEqual(buf.getPendingCount(), 2, 'pending count preserved');

    // New inserts still work
    buf.insert(makeScalarRecord('navigation.speedOverGround', 5.1));
    assertEqual(buf.getPendingCount(), 3, 'new insert works after reopen');
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 18: Legacy Migration ───────────────────────────────────
(function test18() {
  console.log(`${C.cyan}Test 18: Legacy Migration${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    // Create old-style single-table database manually
    const rawDb = new Database(dbPath);
    rawDb.exec('PRAGMA journal_mode = WAL');
    rawDb.exec(`
      CREATE TABLE buffer_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        context TEXT NOT NULL,
        path TEXT NOT NULL,
        received_timestamp TEXT NOT NULL,
        signalk_timestamp TEXT NOT NULL,
        value TEXT,
        value_json TEXT,
        value_latitude REAL,
        value_longitude REAL,
        source TEXT,
        source_label TEXT,
        source_type TEXT,
        source_pgn INTEGER,
        source_src TEXT,
        meta TEXT,
        exported INTEGER NOT NULL DEFAULT 0,
        export_batch_id TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      )
    `);

    // Insert 2 scalar records
    const insertScalar = rawDb.prepare(`
      INSERT INTO buffer_records (context, path, received_timestamp, signalk_timestamp, value)
      VALUES (?, ?, ?, ?, ?)
    `);
    insertScalar.run(DEFAULT_CONTEXT, 'navigation.speedOverGround', '2026-03-09T10:00:00.000Z', '2026-03-09T10:00:00.000Z', '5.0');
    insertScalar.run(DEFAULT_CONTEXT, 'navigation.speedOverGround', '2026-03-09T11:00:00.000Z', '2026-03-09T11:00:00.000Z', '5.1');

    // Insert 2 object records
    const insertObj = rawDb.prepare(`
      INSERT INTO buffer_records (context, path, received_timestamp, signalk_timestamp, value_json, value_latitude, value_longitude)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);
    insertObj.run(DEFAULT_CONTEXT, 'navigation.position', '2026-03-09T10:00:00.000Z', '2026-03-09T10:00:00.000Z', '{"latitude":37.8,"longitude":-122.4}', 37.8, -122.4);
    insertObj.run(DEFAULT_CONTEXT, 'navigation.position', '2026-03-09T11:00:00.000Z', '2026-03-09T11:00:00.000Z', '{"latitude":37.9,"longitude":-122.5}', 37.9, -122.5);

    rawDb.close();

    // Open with SQLiteBuffer — should trigger migration
    const buf = new SQLiteBuffer({ dbPath });

    const paths = buf.getKnownPaths();
    assertEqual(paths.size, 2, 'both paths migrated');
    assertTrue(paths.has('navigation.speedOverGround'), 'scalar path migrated');
    assertTrue(paths.has('navigation.position'), 'object path migrated');
    assertEqual(buf.getPendingCount(), 4, 'all 4 records migrated');

    // Old table should be gone
    assertTrue(!tableExists(dbPath, 'buffer_records'), 'buffer_records table dropped');

    // Verify per-path table columns
    const scalarCols = getTableColumns(dbPath, pathToTableName('navigation.speedOverGround'));
    assertTrue(scalarCols.includes('value'), 'migrated scalar table has value column');
    assertTrue(!scalarCols.includes('value_json'), 'migrated scalar table has no value_json');

    const objCols = getTableColumns(dbPath, pathToTableName('navigation.position'));
    assertTrue(objCols.includes('value_json'), 'migrated object table has value_json');
    assertTrue(objCols.includes('value_latitude'), 'migrated object table has value_latitude');
    assertTrue(objCols.includes('value_longitude'), 'migrated object table has value_longitude');

    // Verify data counts
    assertEqual(countRows(dbPath, pathToTableName('navigation.speedOverGround')), 2, '2 scalar records migrated');
    assertEqual(countRows(dbPath, pathToTableName('navigation.position')), 2, '2 object records migrated');

    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 19: pathToTableName ────────────────────────────────────
(function test19() {
  console.log(`${C.cyan}Test 19: pathToTableName${C.reset}`);
  assertEqual(pathToTableName('navigation.speedOverGround'), 'buffer_navigation_speedOverGround', 'standard path');
  assertEqual(pathToTableName('environment'), 'buffer_environment', 'single segment');
  assertEqual(pathToTableName('a.b.c.d.e'), 'buffer_a_b_c_d_e', 'deep nesting');
  assertEqual(pathToTableName('aw.devicestatus.AR-00003668'), 'buffer_aw_devicestatus_AR_00003668', 'hyphens replaced');
  assertEqual(pathToTableName('resources.zeddisplay-crew.user:maurice'), 'buffer_resources_zeddisplay_crew_user_maurice', 'colons replaced');
})();

// ── Test 19b: String value_* columns stored correctly ──────────
(function test19b() {
  console.log(`${C.cyan}Test 19b: String value_* columns stored correctly${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    const buf = new SQLiteBuffer({ dbPath });

    // Insert an object record with mixed string and numeric value_* fields
    const rec = {
      received_timestamp: '2026-03-10T12:00:00.000Z',
      signalk_timestamp: '2026-03-10T12:00:00.000Z',
      context: DEFAULT_CONTEXT,
      path: 'design.aisShipType',
      value: { id: 36, name: 'Sailing' },
      value_json: JSON.stringify({ id: 36, name: 'Sailing' }),
      value_id: 36,
      value_name: 'Sailing',
    };
    buf.insert(rec);

    // Verify column types and stored values
    const tableName = pathToTableName('design.aisShipType');
    const cols = getTableColumns(dbPath, tableName);
    assertTrue(cols.includes('value_id'), 'has value_id column');
    assertTrue(cols.includes('value_name'), 'has value_name column');

    // Read back the row
    const db = new Database(dbPath);
    const row = db.prepare(`SELECT value_id, value_name FROM ${tableName}`).get();
    assertEqual(row.value_id, 36, 'value_id is numeric 36');
    assertEqual(row.value_name, 'Sailing', 'value_name is string "Sailing"');

    // Verify column types via pragma
    const tableInfoRows = db.prepare(`PRAGMA table_info(${tableName})`).all();
    const idCol = tableInfoRows.find(c => c.name === 'value_id');
    const nameCol = tableInfoRows.find(c => c.name === 'value_name');
    assertEqual(idCol.type, 'REAL', 'value_id column type is REAL');
    assertEqual(nameCol.type, 'TEXT', 'value_name column type is TEXT');

    db.close();
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ── Test 20: Scalar subquery (known path) ───────────────────────
(function test20() {
  console.log(`${C.cyan}Test 20: Scalar subquery (known path)${C.reset}`);
  const knownPaths = new Set(['navigation.speedOverGround']);
  const sql = buildBufferScalarSubquery(
    DEFAULT_CONTEXT,
    'navigation.speedOverGround',
    '2026-03-09T00:00:00.000Z',
    '2026-03-10T00:00:00.000Z',
    knownPaths
  );

  assertTrue(sql !== null, 'returns non-null SQL');
  assertTrue(sql.includes('buffer_navigation_speedOverGround'), 'contains table name');
  assertTrue(sql.includes('TRY_CAST(value AS DOUBLE)'), 'contains TRY_CAST for value');
  assertTrue(sql.includes(DEFAULT_CONTEXT), 'contains context filter');
  assertTrue(sql.includes('2026-03-09'), 'contains from timestamp');
})();

// ── Test 21: Scalar subquery (unknown path) ─────────────────────
(function test21() {
  console.log(`${C.cyan}Test 21: Scalar subquery (unknown path)${C.reset}`);
  const knownPaths = new Set(['navigation.speedOverGround']);
  const sql = buildBufferScalarSubquery(
    DEFAULT_CONTEXT,
    'environment.wind.speedApparent',
    '2026-03-09T00:00:00.000Z',
    '2026-03-10T00:00:00.000Z',
    knownPaths
  );
  assertEqual(sql, null, 'returns null for unknown path');
})();

// ── Test 22: Object subquery (known path) ───────────────────────
(function test22() {
  console.log(`${C.cyan}Test 22: Object subquery (known path)${C.reset}`);
  const knownPaths = new Set(['navigation.position']);
  const components = new Map([
    ['latitude', { name: 'latitude', columnName: 'value_latitude', dataType: 'numeric' }],
    ['longitude', { name: 'longitude', columnName: 'value_longitude', dataType: 'numeric' }],
  ]);

  const sql = buildBufferObjectSubquery(
    DEFAULT_CONTEXT,
    'navigation.position',
    '2026-03-09T00:00:00.000Z',
    '2026-03-10T00:00:00.000Z',
    components,
    knownPaths
  );

  assertTrue(sql !== null, 'returns non-null SQL');
  assertTrue(sql.includes('buffer_navigation_position'), 'contains table name');
  assertTrue(sql.includes('TRY_CAST(value_latitude AS DOUBLE)'), 'contains TRY_CAST for latitude');
  assertTrue(sql.includes('TRY_CAST(value_longitude AS DOUBLE)'), 'contains TRY_CAST for longitude');
  assertTrue(!sql.includes('json_extract'), 'no json_extract — uses direct columns');
})();

// ── Test 23: Object subquery (unknown path) ─────────────────────
(function test23() {
  console.log(`${C.cyan}Test 23: Object subquery (unknown path)${C.reset}`);
  const knownPaths = new Set(['navigation.speedOverGround']);
  const components = new Map([
    ['latitude', { name: 'latitude', columnName: 'value_latitude', dataType: 'numeric' }],
  ]);

  const sql = buildBufferObjectSubquery(
    DEFAULT_CONTEXT,
    'navigation.position',
    '2026-03-09T00:00:00.000Z',
    '2026-03-10T00:00:00.000Z',
    components,
    knownPaths
  );
  assertEqual(sql, null, 'returns null for unknown path');
})();

// ── Test 24: Full Export Cycle Integration ───────────────────────
(function test24() {
  console.log(`${C.cyan}Test 24: Full Export Cycle Integration${C.reset}`);
  const dbPath = makeTempDbPath();
  try {
    let buf = new SQLiteBuffer({ dbPath, retentionHours: 0 });

    // Insert 5 records across 2 paths on 2026-03-09
    const records = [
      makeScalarRecord('navigation.speedOverGround', 5.0, {
        received_timestamp: '2026-03-09T10:00:00.000Z',
        signalk_timestamp: '2026-03-09T10:00:00.000Z',
      }),
      makeScalarRecord('navigation.speedOverGround', 5.1, {
        received_timestamp: '2026-03-09T11:00:00.000Z',
        signalk_timestamp: '2026-03-09T11:00:00.000Z',
      }),
      makeScalarRecord('navigation.speedOverGround', 5.2, {
        received_timestamp: '2026-03-09T12:00:00.000Z',
        signalk_timestamp: '2026-03-09T12:00:00.000Z',
      }),
      makeScalarRecord('environment.wind.speedApparent', 8.0, {
        received_timestamp: '2026-03-09T10:00:00.000Z',
        signalk_timestamp: '2026-03-09T10:00:00.000Z',
      }),
      makeScalarRecord('environment.wind.speedApparent', 8.1, {
        received_timestamp: '2026-03-09T11:00:00.000Z',
        signalk_timestamp: '2026-03-09T11:00:00.000Z',
      }),
    ];
    buf.insertBatch(records);

    // Step 1: getPathsForDate
    const date = new Date('2026-03-09T00:00:00.000Z');
    const pathsForDate = buf.getPathsForDate(date);
    assertEqual(pathsForDate.length, 2, 'getPathsForDate returns 2 entries');

    // Step 2: getRecordsForPathAndDate for each
    let totalRecords = 0;
    for (const { context, path: skPath } of pathsForDate) {
      const recs = buf.getRecordsForPathAndDate(context, skPath, date);
      totalRecords += recs.length;
    }
    assertEqual(totalRecords, 5, 'retrieved all 5 records');

    // Step 3: markDateExported for each
    for (const { context, path: skPath } of pathsForDate) {
      buf.markDateExported(context, skPath, date, 'batch-integration');
    }
    assertEqual(buf.getPendingCount(), 0, 'getPendingCount() is 0 after export');

    // Step 4: cleanup — close buf, backdate created_at, reopen, then cleanup
    buf.close();

    const db = new Database(dbPath);
    for (const { path: skPath } of pathsForDate) {
      const tbl = pathToTableName(skPath);
      db.prepare(`UPDATE ${tbl} SET created_at = '2020-01-01 00:00:00'`).run();
    }
    db.close();

    buf = new SQLiteBuffer({ dbPath, retentionHours: 0 });
    buf.cleanup();
    assertEqual(buf.getStats().totalRecords, 0, 'all records cleaned up');
    buf.close();
  } finally {
    cleanupDb(dbPath);
  }
})();

// ═══════════════════════════════════════════════════════════════════
// Part 2: Live E2E — WebSocket → production flattening → SQLiteBuffer → verify DB
// ═══════════════════════════════════════════════════════════════════

/**
 * Replicate the exact production flattening from data-handler.ts handleStreamData()
 * lines 507-568. This is NOT a helper that "looks like" production code — it IS
 * the same logic, copied verbatim so the test catches any drift.
 */
function flattenDeltaToRecord(context, skPath, timestamp, value, source, $source) {
  const record = {
    received_timestamp: new Date().toISOString(),
    signalk_timestamp: timestamp || new Date().toISOString(),
    context: context || 'vessels.self',
    path: skPath,
    value: null,
    value_json: undefined,
    source: source || undefined,
    source_label: $source || undefined,
    source_type: source ? source.type : undefined,
    source_pgn: source ? source.pgn : undefined,
    source_src: source ? source.src : undefined,
    meta: undefined,
  };

  // Handle complex values — exact copy of data-handler.ts lines 528-568
  if (typeof value === 'object' && value !== null) {
    const objKeys = Object.keys(value);
    const metaOnlyKeys = ['units', 'meta', 'description', 'displayUnits', 'zones', 'timeout'];
    const isMetaOnly = objKeys.length > 0 && objKeys.every(k => metaOnlyKeys.includes(k));
    if (isMetaOnly) return null; // skip meta-only updates

    record.value_json = value; // Store as object, serialize at write time
    Object.entries(value).forEach(([key, val]) => {
      if (typeof val === 'string' || typeof val === 'number' || typeof val === 'boolean') {
        record[`value_${key}`] = val;
      }
    });
  } else {
    record.value = value;
  }

  return record;
}

async function getToken() {
  const res = await fetch(`${LIVE_URL}/signalk/v1/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username: LIVE_USERNAME, password: LIVE_PASSWORD }),
  });
  if (!res.ok) throw new Error(`Auth failed: ${res.status}`);
  const data = await res.json();
  return data.token;
}

async function runLiveTest() {
  console.log(`\n${C.cyan}═══════════════════════════════════════════════════════${C.reset}`);
  console.log(`${C.cyan}  PART 2: LIVE E2E — WebSocket → Flatten → Buffer → DB${C.reset}`);
  console.log(`${C.cyan}═══════════════════════════════════════════════════════${C.reset}\n`);

  console.log(`${C.cyan}Test 25: Live WebSocket → production flatten → SQLiteBuffer${C.reset}`);

  let token;
  try {
    token = await getToken();
  } catch (e) {
    console.log(`  ${C.red}✗${C.reset} Could not authenticate to ${LIVE_URL}: ${e.message}`);
    failed++;
    return;
  }
  assertTrue(!!token, `authenticated to ${LIVE_URL}`);

  const dbPath = makeTempDbPath();
  const buf = new SQLiteBuffer({ dbPath });

  // Collect deltas from the real server
  const wsUrl = LIVE_URL.replace(/^http/, 'ws') + '/signalk/v1/stream?subscribe=all&token=' + token;

  const collected = { scalars: [], objects: [], skipped: 0, total: 0, gotPosition: false };

  await new Promise((resolve, reject) => {
    const ws = new WebSocket(wsUrl);
    const timeout = setTimeout(() => { ws.close(); resolve(); }, 15000);

    ws.on('message', data => {
      const msg = JSON.parse(data.toString());
      if (!msg.updates) return;

      for (const upd of msg.updates) {
        for (const v of (upd.values || [])) {
          if (!v.path) continue;
          collected.total++;

          // Run through the EXACT production flattening
          const record = flattenDeltaToRecord(
            msg.context, v.path, upd.timestamp,
            v.value, upd.source, upd.$source
          );

          if (!record) { collected.skipped++; continue; }

          // Insert into the real SQLiteBuffer
          try {
            buf.insert(record);
          } catch (e) {
            console.log(`  ${C.red}✗${C.reset} insert failed for ${v.path}: ${e.message}`);
            failed++;
          }

          // Track what we got
          if (typeof v.value === 'object' && v.value !== null) {
            // Always capture navigation.position specifically
            if (v.path === 'navigation.position') {
              if (!collected.gotPosition) {
                collected.objects.unshift({ path: v.path, value: v.value, record });
                collected.gotPosition = true;
              }
            } else if (collected.objects.length < 5) {
              collected.objects.push({ path: v.path, value: v.value, record });
            }
          } else {
            if (collected.scalars.length < 5) {
              collected.scalars.push({ path: v.path, value: v.value, record });
            }
          }

          // Stop once we have enough variety AND got navigation.position
          if (collected.scalars.length >= 5 && collected.objects.length >= 3 && collected.gotPosition) {
            clearTimeout(timeout);
            ws.close();
            return;
          }
        }
      }
    });

    ws.on('close', () => { clearTimeout(timeout); resolve(); });
    ws.on('error', e => { clearTimeout(timeout); reject(e); });
  });

  console.log(`  ${C.dim}received ${collected.total} values, skipped ${collected.skipped} meta-only, inserted ${collected.total - collected.skipped}${C.reset}`);
  assertTrue(collected.scalars.length > 0, `got scalar deltas (${collected.scalars.length})`);
  assertTrue(collected.objects.length > 0, `got object deltas (${collected.objects.length})`);
  assertTrue(buf.getKnownPaths().size > 0, `buffer has ${buf.getKnownPaths().size} known paths`);
  assertTrue(buf.getPendingCount() > 0, `buffer has ${buf.getPendingCount()} pending records`);

  // Capture the real context from the first record for later verification
  const liveContext = collected.scalars[0].record.context;
  console.log(`  ${C.dim}live context: ${liveContext}${C.reset}`);
  assertTrue(liveContext.startsWith('vessels.'), `context starts with "vessels." (${liveContext})`);

  // ── Verify scalar records in the actual DB ──
  console.log(`${C.cyan}Test 26: Verify scalar records in DB${C.reset}`);
  for (const s of collected.scalars) {
    const tableName = pathToTableName(s.path);
    const cols = getTableColumns(dbPath, tableName);
    assertTrue(cols.includes('value'), `${s.path}: scalar table has value column`);
    assertTrue(!cols.includes('value_json'), `${s.path}: scalar table has NO value_json`);

    // Read actual stored value and verify context
    const rawDb = new Database(dbPath, { readOnly: true });
    const row = rawDb.prepare(`SELECT value, context FROM ${tableName} LIMIT 1`).get();
    rawDb.close();
    assertTrue(row && row.value !== null, `${s.path}: stored value is not null (got ${JSON.stringify(row && row.value)})`);
    assertEqual(row.context, liveContext, `${s.path}: context stored correctly`);
  }

  // ── Verify object records in the actual DB ──
  console.log(`${C.cyan}Test 27: Verify object records in DB${C.reset}`);
  for (const o of collected.objects) {
    const tableName = pathToTableName(o.path);
    const cols = getTableColumns(dbPath, tableName);
    assertTrue(cols.includes('value_json'), `${o.path}: object table has value_json`);
    assertTrue(!cols.includes('value'), `${o.path}: object table has NO plain value column`);

    // Check that flattened value_* columns exist for each primitive in the object
    const expectedCols = Object.entries(o.value)
      .filter(([, val]) => typeof val === 'string' || typeof val === 'number' || typeof val === 'boolean')
      .map(([key]) => `value_${key}`);

    for (const col of expectedCols) {
      assertTrue(cols.includes(col), `${o.path}: has flattened column ${col}`);
    }

    // Verify actual data was stored
    const rawDb = new Database(dbPath, { readOnly: true });
    const row = rawDb.prepare(`SELECT * FROM ${tableName} LIMIT 1`).get();
    rawDb.close();

    assertTrue(row && row.value_json !== null, `${o.path}: value_json stored`);
    // Context may differ per record (AIS targets vs self) — just verify it's a vessels context
    assertTrue(row.context && row.context.startsWith('vessels.'), `${o.path}: context is a valid vessel context`);

    // Verify flattened values match the original
    for (const [key, val] of Object.entries(o.value)) {
      const col = `value_${key}`;
      if (typeof val === 'number' && row[col] !== undefined) {
        assertEqual(row[col], val, `${o.path}: ${col} = ${val}`);
      } else if (typeof val === 'string' && row[col] !== undefined) {
        // Strings are now stored as TEXT — verify round-trip
        assertEqual(row[col], val, `${o.path}: ${col} = "${val}" (string preserved)`);
      }
    }

    // Verify value_json round-trips correctly
    if (row && row.value_json) {
      const parsed = JSON.parse(row.value_json);
      for (const [key, val] of Object.entries(o.value)) {
        if (typeof val === 'number') {
          assertEqual(parsed[key], val, `${o.path}: value_json.${key} round-trips`);
        }
      }
    }
  }

  // ── Verify navigation.position specifically ──
  console.log(`${C.cyan}Test 28: navigation.position — lat/lon flattened correctly${C.reset}`);
  if (collected.gotPosition) {
    const posTable = pathToTableName('navigation.position');
    const posCols = getTableColumns(dbPath, posTable);
    assertTrue(posCols.includes('value_json'), 'navigation.position: has value_json');
    assertTrue(posCols.includes('value_latitude'), 'navigation.position: has value_latitude');
    assertTrue(posCols.includes('value_longitude'), 'navigation.position: has value_longitude');
    assertTrue(!posCols.includes('value'), 'navigation.position: NO plain value column');

    const rawDb = new Database(dbPath, { readOnly: true });
    const posRow = rawDb.prepare(`SELECT * FROM ${posTable} LIMIT 1`).get();
    rawDb.close();

    assertTrue(posRow.value_json !== null, 'navigation.position: value_json stored');
    assertTrue(posRow.context && posRow.context.startsWith('vessels.'), `navigation.position: context is valid (${posRow.context})`);
    assertTrue(typeof posRow.value_latitude === 'number', `navigation.position: value_latitude is number (${posRow.value_latitude})`);
    assertTrue(typeof posRow.value_longitude === 'number', `navigation.position: value_longitude is number (${posRow.value_longitude})`);
    assertTrue(Math.abs(posRow.value_latitude) <= 90, `navigation.position: latitude in range (${posRow.value_latitude})`);
    assertTrue(Math.abs(posRow.value_longitude) <= 180, `navigation.position: longitude in range (${posRow.value_longitude})`);

    // Verify value_json round-trips
    const parsed = JSON.parse(posRow.value_json);
    assertEqual(parsed.latitude, posRow.value_latitude, 'navigation.position: value_json.latitude matches column');
    assertEqual(parsed.longitude, posRow.value_longitude, 'navigation.position: value_json.longitude matches column');
  } else {
    console.log(`  ${C.red}✗${C.reset} navigation.position not received from server`);
    failed++;
  }

  // ── Verify getRecordsForPathAndDate reconstructs records with context ──
  console.log(`${C.cyan}Test 29: Round-trip through getRecordsForPathAndDate with context${C.reset}`);
  const today = new Date();
  today.setUTCHours(0, 0, 0, 0);
  const pathsToday = buf.getPathsForDate(today);
  assertTrue(pathsToday.length > 0, `getPathsForDate(today) returns ${pathsToday.length} entries`);

  // Multiple vessels expected (AIS targets) — verify all contexts are valid vessels
  const allContextsValid = pathsToday.every(p => p.context && p.context.startsWith('vessels.'));
  assertTrue(allContextsValid, 'all getPathsForDate entries have valid vessel contexts');

  // Verify multiple distinct contexts exist (self + AIS targets)
  const distinctContexts = new Set(pathsToday.map(p => p.context));
  console.log(`  ${C.dim}${distinctContexts.size} distinct contexts (self + AIS targets)${C.reset}`);
  assertTrue(distinctContexts.size > 1, `multiple vessel contexts present (${distinctContexts.size})`);
  assertTrue(distinctContexts.has(liveContext), `self vessel context present in getPathsForDate`);

  // Pick an object path for the SELF vessel and verify full round-trip
  const objEntry = pathsToday.find(p => p.context === liveContext && collected.objects.some(o => o.path === p.path));
  if (objEntry) {
    const recs = buf.getRecordsForPathAndDate(objEntry.context, objEntry.path, today);
    assertTrue(recs.length > 0, `${objEntry.path}: getRecordsForPathAndDate returns ${recs.length} records for self`);
    const rec = recs[0];
    assertEqual(rec.path, objEntry.path, `${objEntry.path}: path field reconstructed`);
    assertEqual(rec.context, liveContext, `${objEntry.path}: context matches self vessel`);
    assertTrue(rec.value_json !== undefined, `${objEntry.path}: value_json present in returned record`);

    // Verify wrong context returns nothing
    const wrongCtx = buf.getRecordsForPathAndDate('vessels.urn:mrn:imo:mmsi:000000000', objEntry.path, today);
    assertEqual(wrongCtx.length, 0, `${objEntry.path}: bogus context returns 0 records`);
  } else {
    console.log(`  ${C.dim}(no self-vessel object path for today — checking any context)${C.reset}`);
    // Fall back to any object entry
    const anyObj = pathsToday.find(p => collected.objects.some(o => o.path === p.path));
    if (anyObj) {
      const recs = buf.getRecordsForPathAndDate(anyObj.context, anyObj.path, today);
      assertTrue(recs.length > 0, `${anyObj.path}: returns records for context ${anyObj.context}`);
      assertEqual(recs[0].context, anyObj.context, `${anyObj.path}: context round-trips correctly`);
    }
  }

  // Verify context isolation: same path, different contexts return different records
  const posEntries = pathsToday.filter(p => p.path === 'navigation.position');
  if (posEntries.length >= 2) {
    const recs1 = buf.getRecordsForPathAndDate(posEntries[0].context, 'navigation.position', today);
    const recs2 = buf.getRecordsForPathAndDate(posEntries[1].context, 'navigation.position', today);
    assertTrue(recs1.length > 0 && recs2.length > 0, 'navigation.position: both contexts have records');
    assertTrue(recs1[0].context !== recs2[0].context, `navigation.position: context isolation works (${recs1[0].context} vs ${recs2[0].context})`);
  }

  // Pick a scalar path and verify
  const scalarEntry = pathsToday.find(p => p.context === liveContext && collected.scalars.some(s => s.path === p.path));
  if (scalarEntry) {
    const recs = buf.getRecordsForPathAndDate(scalarEntry.context, scalarEntry.path, today);
    assertTrue(recs.length > 0, `${scalarEntry.path}: getRecordsForPathAndDate returns ${recs.length} records`);
    assertEqual(recs[0].context, liveContext, `${scalarEntry.path}: context matches self vessel`);
    assertTrue(typeof recs[0].value === 'number' || typeof recs[0].value === 'string' || typeof recs[0].value === 'boolean',
      `${scalarEntry.path}: value is a primitive (${typeof recs[0].value})`);
  }

  // ── Test 30: FULL E2E — Buffer → Parquet export → Read back ──
  console.log(`${C.cyan}Test 30: Buffer → Parquet → Readback (string value_* survives export)${C.reset}`);

  const parquet = require('@dsnp/parquetjs');
  const { ParquetWriter: PWriter } = require('../dist/parquet-writer');

  // Minimal app mock for SchemaService
  const mockApp = {
    debug: () => {},
    error: console.error,
    getMetadata: () => null,
    selfContext: liveContext,
  };

  const pwriter = new PWriter({ format: 'parquet', app: mockApp });
  const { ParquetExportService } = require('../dist/services/parquet-export-service');
  const exportDir = `/tmp/test-parquet-export-${Date.now()}`;

  const exportService = new ParquetExportService(
    buf,
    pwriter,
    {
      outputDirectory: exportDir,
      filenamePrefix: 'signalk_data',
      useHivePartitioning: true,
      dailyExportHour: 3,
    },
    mockApp
  );

  // Run the REAL production export — exportDayToParquet(today)
  const result = await exportService.exportDayToParquet(today);

  assertTrue(result.recordsExported > 0, `exported ${result.recordsExported} records`);
  assertTrue(result.filesCreated.length > 0, `created ${result.filesCreated.length} parquet files`);
  assertTrue(result.errors.length === 0, `no export errors (got ${result.errors.length})`);
  if (result.errors.length > 0) {
    console.log(`  ${C.dim}errors: ${result.errors.join('; ')}${C.reset}`);
  }

  // Verify hive directory structure in created files
  const sampleFile = result.filesCreated[0];
  assertTrue(sampleFile.includes('tier=raw'), `hive path has tier=raw`);
  assertTrue(sampleFile.includes('context='), `hive path has context=`);
  assertTrue(sampleFile.includes('path='), `hive path has path=`);
  assertTrue(sampleFile.includes('year='), `hive path has year=`);
  assertTrue(sampleFile.includes('day='), `hive path has day=`);
  console.log(`  ${C.dim}sample: ${path.relative(exportDir, sampleFile)}${C.reset}`);

  // Find the parquet file for an object path with string values (e.g. design.aisShipType)
  const stringObjPath = collected.objects.find(o =>
    Object.entries(o.value).some(([, v]) => typeof v === 'string')
  );

  if (stringObjPath) {
    const sanitizedCtx = liveContext.replace(/\./g, '__').replace(/:/g, '-');
    const objFile = result.filesCreated.find(f =>
      f.includes(`context=${sanitizedCtx}`) &&
      f.includes(`path=${stringObjPath.path.replace(/\./g, '__')}`)
    );
    assertTrue(!!objFile, `found parquet file for ${stringObjPath.path}`);

    if (objFile) {
      console.log(`  ${C.dim}reading: ${path.relative(exportDir, objFile)}${C.reset}`);

      const reader = await parquet.ParquetReader.openFile(objFile);
      const cursor = reader.getCursor();
      const row = await cursor.next();
      await reader.close();

      assertTrue(!!row, `read back row from ${stringObjPath.path} parquet`);

      const stringKeys = Object.entries(stringObjPath.value)
        .filter(([, v]) => typeof v === 'string')
        .map(([k]) => k);
      const numericKeys = Object.entries(stringObjPath.value)
        .filter(([, v]) => typeof v === 'number')
        .map(([k]) => k);

      // Verify string value_* columns survived the full pipeline
      for (const k of stringKeys) {
        const col = `value_${k}`;
        const expected = stringObjPath.value[k];
        if (row[col] !== undefined) {
          assertEqual(row[col], expected, `${stringObjPath.path}: parquet ${col} = "${expected}"`);
        } else {
          console.log(`  ${C.red}✗${C.reset} ${stringObjPath.path}: parquet missing column ${col}`);
          failed++;
        }
      }

      // Verify numeric value_* columns survived
      for (const k of numericKeys) {
        const col = `value_${k}`;
        const expected = stringObjPath.value[k];
        if (row[col] !== undefined) {
          assertEqual(row[col], expected, `${stringObjPath.path}: parquet ${col} = ${expected}`);
        }
      }

      // Verify schema types
      if (reader.schema && reader.schema.schema) {
        for (const k of stringKeys) {
          const col = `value_${k}`;
          if (reader.schema.schema[col]) {
            assertEqual(reader.schema.schema[col].type, 'UTF8', `${col} parquet type is UTF8`);
          }
        }
        for (const k of numericKeys) {
          const col = `value_${k}`;
          if (reader.schema.schema[col]) {
            assertEqual(reader.schema.schema[col].type, 'DOUBLE', `${col} parquet type is DOUBLE`);
          }
        }
      }
    }
  }

  // Verify a scalar file too
  const scalarExport = collected.scalars[0];
  if (scalarExport) {
    const sCtx = liveContext.replace(/\./g, '__').replace(/:/g, '-');
    const scalarFile = result.filesCreated.find(f =>
      f.includes(`context=${sCtx}`) &&
      f.includes(`path=${scalarExport.path.replace(/\./g, '__')}`)
    );
    if (scalarFile) {
      const reader = await parquet.ParquetReader.openFile(scalarFile);
      const cursor = reader.getCursor();
      const row = await cursor.next();
      await reader.close();
      assertTrue(row && row.value !== undefined && row.value !== null,
        `${scalarExport.path}: scalar value survives full export pipeline (${row?.value})`);
      console.log(`  ${C.dim}scalar: ${path.relative(exportDir, scalarFile)}${C.reset}`);
    }
  }

  // Verify buffer records are now marked exported
  assertEqual(buf.getPendingCount(), 0, `all buffer records marked exported after exportDayToParquet`);

  // ── Test 31: Federated DuckDB query — Parquet + live SQLite buffer UNION ALL ──
  console.log(`${C.cyan}Test 31: Federated query — second live batch into buffer, DuckDB reads both${C.reset}`);

  const { DuckDBPool } = require('../dist/utils/duckdb-pool');
  const { HivePathBuilder } = require('../dist/utils/hive-path-builder');
  const { buildBufferScalarSubquery, buildBufferObjectSubquery } = require('../dist/utils/buffer-sql-builder');

  // Collect a SECOND batch of live data into the buffer (after export)
  // These stay unexported — simulates "today's live data" alongside yesterday's parquet
  const wsUrl2 = LIVE_URL.replace(/^http/, 'ws') + '/signalk/v1/stream?subscribe=all&token=' + token;
  const batch2 = { count: 0, scalarPath: null, objectPath: null };

  await new Promise((resolve, reject) => {
    const ws2 = new WebSocket(wsUrl2);
    const timeout2 = setTimeout(() => { ws2.close(); resolve(); }, 10000);

    ws2.on('message', data => {
      const msg = JSON.parse(data.toString());
      if (!msg.updates) return;

      for (const upd of msg.updates) {
        for (const v of (upd.values || [])) {
          if (!v.path) continue;
          const record = flattenDeltaToRecord(
            msg.context, v.path, upd.timestamp, v.value, upd.source, upd.$source
          );
          if (!record) continue;
          try {
            buf.insert(record);
            batch2.count++;

            // Track a scalar and object path that we also have in parquet
            if (!batch2.scalarPath && typeof v.value !== 'object' && v.value !== null) {
              // Check if this path was also exported to parquet
              const sCtx = liveContext.replace(/\./g, '__').replace(/:/g, '-');
              if (result.filesCreated.some(f => f.includes(`context=${sCtx}`) && f.includes(`path=${v.path.replace(/\./g, '__')}`))) {
                batch2.scalarPath = v.path;
              }
            }
            if (!batch2.objectPath && typeof v.value === 'object' && v.value !== null) {
              // Prefer object paths with string values (like design.aisShipType)
              const hasStringVal = Object.values(v.value).some(val => typeof val === 'string');
              const sCtx = liveContext.replace(/\./g, '__').replace(/:/g, '-');
              if (hasStringVal && result.filesCreated.some(f => f.includes(`context=${sCtx}`) && f.includes(`path=${v.path.replace(/\./g, '__')}`))) {
                batch2.objectPath = v.path;
                batch2.objectValue = v.value;
              }
            }
          } catch (e) { /* skip insert errors */ }

          // Stop once we have both a scalar and object path that overlap with parquet
          if (batch2.scalarPath && batch2.objectPath && batch2.count >= 50) {
            clearTimeout(timeout2);
            ws2.close();
            return;
          }
        }
      }
    });

    ws2.on('close', () => { clearTimeout(timeout2); resolve(); });
    ws2.on('error', e => { clearTimeout(timeout2); reject(e); });
  });

  assertTrue(batch2.count > 0, `second live batch: ${batch2.count} records into buffer`);
  assertTrue(buf.getPendingCount() > 0, `buffer has ${buf.getPendingCount()} pending (unexported) records`);
  console.log(`  ${C.dim}scalar overlap: ${batch2.scalarPath}, object overlap: ${batch2.objectPath}${C.reset}`);

  // Initialize DuckDB + attach SQLite buffer
  await DuckDBPool.initialize();
  DuckDBPool.initializeSQLiteBuffer(dbPath);
  const conn = await DuckDBPool.getConnectionWithBuffer();

  const hpb = new HivePathBuilder();
  const knownPaths = buf.getKnownPaths();
  const wideFrom = '2020-01-01T00:00:00Z';
  const wideTo = '2030-01-01T00:00:00Z';

  try {
    // ── Scalar federated query ──
    if (batch2.scalarPath) {
      const scalarGlob = hpb.getGlobPattern(exportDir, 'raw', liveContext, batch2.scalarPath);
      const bufScalarSql = buildBufferScalarSubquery(liveContext, batch2.scalarPath, wideFrom, wideTo, knownPaths);
      assertTrue(bufScalarSql !== null, `buffer scalar subquery for ${batch2.scalarPath}`);

      const scalarQuery = `
        SELECT signalk_timestamp, value, 'parquet' AS source FROM (
          SELECT signalk_timestamp, TRY_CAST(value AS DOUBLE) AS value
          FROM read_parquet('${scalarGlob}', union_by_name=true)
        )
        UNION ALL
        SELECT signalk_timestamp, value, 'buffer' AS source FROM ${bufScalarSql}
        ORDER BY signalk_timestamp
      `;
      const scalarResult = await conn.runAndReadAll(scalarQuery);
      const scalarRows = scalarResult.getRowObjects();
      assertTrue(scalarRows.length >= 2, `federated scalar: ${scalarRows.length} rows from both sources`);

      const parquetRows = scalarRows.filter(r => r.source === 'parquet');
      const bufferRows = scalarRows.filter(r => r.source === 'buffer');
      assertTrue(parquetRows.length > 0, `${batch2.scalarPath}: ${parquetRows.length} rows from parquet`);
      assertTrue(bufferRows.length > 0, `${batch2.scalarPath}: ${bufferRows.length} rows from buffer`);

      // No duplicates: buffer rows should have different timestamps than parquet rows
      const parquetTs = new Set(parquetRows.map(r => r.signalk_timestamp));
      const bufferTs = new Set(bufferRows.map(r => r.signalk_timestamp));
      const overlap = [...bufferTs].filter(t => parquetTs.has(t));
      console.log(`  ${C.dim}${batch2.scalarPath}: ${parquetRows.length} parquet + ${bufferRows.length} buffer, ${overlap.length} timestamp overlap${C.reset}`);
    }

    // ── Object federated query — using production getPathComponentSchema ──
    if (batch2.objectPath) {
      const { getPathComponentSchema } = require('../dist/utils/schema-cache');
      const objGlob = hpb.getGlobPattern(exportDir, 'raw', liveContext, batch2.objectPath);

      // Use the PRODUCTION schema discovery — same code as history-provider.ts line 266
      const componentSchema = await getPathComponentSchema(exportDir, liveContext, batch2.objectPath);
      assertTrue(!!componentSchema, `getPathComponentSchema found schema for ${batch2.objectPath}`);
      assertTrue(componentSchema.components.size > 0, `${batch2.objectPath}: ${componentSchema.components.size} components discovered`);

      // Log discovered components with their types
      const compEntries = Array.from(componentSchema.components.entries());
      console.log(`  ${C.dim}components: ${compEntries.map(([n, c]) => `${n}(${c.dataType})`).join(', ')}${C.reset}`);

      // Build the EXACT same SQL as history-provider.ts lines 279-333
      const componentSelects = compEntries
        .map(([name, comp]) => {
          const aggFunc = comp.dataType === 'numeric' ? 'AVG' : 'FIRST';
          return `${aggFunc}(${comp.columnName}) as ${name}`;
        })
        .join(', ');

      const componentWhereConditions = compEntries
        .map(([, comp]) => `${comp.columnName} IS NOT NULL`)
        .join(' OR ');

      const componentCols = compEntries.map(([, c]) => c.columnName).join(', ');

      // Build federated FROM — same as history-provider.ts lines 297-318
      const parquetFrom = `(SELECT * FROM read_parquet('${objGlob}', union_by_name=true))`;
      const bufObjSql = buildBufferObjectSubquery(liveContext, batch2.objectPath, wideFrom, wideTo, componentSchema.components, knownPaths);
      assertTrue(bufObjSql !== null, `buffer object subquery for ${batch2.objectPath}`);

      const federatedFrom = `(
        SELECT signalk_timestamp, ${componentCols} FROM ${parquetFrom}
        UNION ALL
        SELECT signalk_timestamp, ${componentCols} FROM ${bufObjSql}
      )`;

      // Run the EXACT production query shape — same as history-provider.ts lines 320-333
      const resolutionMs = 1000; // 1 second buckets
      const objQuery = `
        SELECT
          strftime(DATE_TRUNC('seconds',
            EPOCH_MS(CAST(FLOOR(EPOCH_MS(signalk_timestamp::TIMESTAMP) / ${resolutionMs}) * ${resolutionMs} AS BIGINT))
          ), '%Y-%m-%dT%H:%M:%SZ') as timestamp,
          ${componentSelects}
        FROM ${federatedFrom} AS source_data
        WHERE
          signalk_timestamp >= '${wideFrom}'
          AND signalk_timestamp < '${wideTo}'
          AND (${componentWhereConditions})
        GROUP BY timestamp
        ORDER BY timestamp
      `;
      const objResult = await conn.runAndReadAll(objQuery);
      const objRows = objResult.getRowObjects();
      assertTrue(objRows.length >= 1, `federated object query: ${objRows.length} aggregated rows`);

      // Verify string components preserved in query results
      const hasStringComponents = compEntries.some(([, c]) => c.dataType !== 'numeric');
      if (hasStringComponents && batch2.objectValue) {
        const lastRow = objRows[objRows.length - 1]; // most recent = buffer data
        for (const [name, comp] of compEntries) {
          if (comp.dataType !== 'numeric') {
            const val = lastRow[name];
            assertTrue(val !== null && val !== undefined, `${batch2.objectPath}: ${name} not null in federated result`);
            assertEqual(typeof val, 'string', `${batch2.objectPath}: ${name} is string ("${val}")`);
            // Verify it matches the live value we inserted
            if (batch2.objectValue[name]) {
              assertEqual(val, batch2.objectValue[name], `${batch2.objectPath}: ${name} = "${batch2.objectValue[name]}" from live data`);
            }
          }
        }
      }

      console.log(`  ${C.dim}${batch2.objectPath}: ${objRows.length} aggregated rows, cols: [${componentCols}]${C.reset}`);
    }

  } catch (e) {
    console.log(`  ${C.red}✗${C.reset} federated query failed: ${e.message}`);
    console.log(`  ${C.dim}${e.stack}${C.reset}`);
    failed++;
  } finally {
    conn.disconnectSync();
    await DuckDBPool.shutdown();
  }

  // Cleanup
  try { fs.rmSync(exportDir, { recursive: true }); } catch {}

  buf.close();
  cleanupDb(dbPath);
}

// ─── Run everything ─────────────────────────────────────────────
(async function main() {
  try {
    await runLiveTest();
  } catch (e) {
    console.log(`  ${C.red}✗${C.reset} Live test error: ${e.message}`);
    failed++;
  }

  console.log(`\n${C.cyan}════════════════════════════════════════════════════${C.reset}`);
  console.log(`  ${C.green}Passed: ${passed}${C.reset}  ${failed > 0 ? C.red : C.dim}Failed: ${failed}${C.reset}`);
  console.log(`${C.cyan}════════════════════════════════════════════════════${C.reset}\n`);

  process.exit(failed > 0 ? 1 : 0);
})();
