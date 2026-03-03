#!/usr/bin/env node
/**
 * Data Pipeline Diagnostic Report
 *
 * Generates a comprehensive report of SQLite buffer and Parquet file status
 * to verify the data pipeline is working correctly.
 *
 * Usage: node tests/test-data-pipeline.js [--verbose]
 */

const path = require('path');
const fs = require('fs-extra');
const { glob } = require('glob');

// Configuration
const DATA_DIR = process.env.SIGNALK_DATA_DIR || '/Users/mauricetamman/.signalk/data';
const BUFFER_DB = path.join(DATA_DIR, 'buffer.db');
const PARQUET_DIR = path.join(DATA_DIR, 'tier=raw');
const VERBOSE = process.argv.includes('--verbose');

// Colors for output
const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
  dim: '\x1b[2m',
};

function log(msg, color = '') {
  console.log(color + msg + colors.reset);
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatNumber(num) {
  return num.toLocaleString();
}

async function checkSQLiteBuffer() {
  log('\n═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  SQLITE BUFFER STATUS', colors.cyan);
  log('═══════════════════════════════════════════════════════════════', colors.cyan);

  let Database;
  try {
    Database = require('better-sqlite3');
  } catch (e) {
    log('  ✗ better-sqlite3 not available: ' + e.message, colors.red);
    return null;
  }

  if (!fs.existsSync(BUFFER_DB)) {
    log('  ✗ Buffer database not found: ' + BUFFER_DB, colors.red);
    return null;
  }

  const db = new Database(BUFFER_DB, { readonly: true });
  const stats = {};

  try {
    // Database file sizes
    const dbStats = fs.statSync(BUFFER_DB);
    const walPath = BUFFER_DB + '-wal';
    const walStats = fs.existsSync(walPath) ? fs.statSync(walPath) : { size: 0 };

    log('\n  Database Files:', colors.blue);
    log(`    DB Size:  ${formatBytes(dbStats.size)}`);
    log(`    WAL Size: ${formatBytes(walStats.size)}`);
    log(`    Total:    ${formatBytes(dbStats.size + walStats.size)}`);

    // Total records
    const totalRow = db.prepare('SELECT COUNT(*) as count FROM buffer_records').get();
    stats.totalRecords = totalRow.count;
    log(`\n  Total Records: ${formatNumber(stats.totalRecords)}`, colors.blue);

    // Records by date and export status
    const byDate = db.prepare(`
      SELECT
        DATE(received_timestamp) as date,
        COUNT(*) as total,
        SUM(CASE WHEN exported = 0 THEN 1 ELSE 0 END) as unexported,
        SUM(CASE WHEN exported = 1 THEN 1 ELSE 0 END) as exported
      FROM buffer_records
      GROUP BY date
      ORDER BY date
    `).all();

    log('\n  Records by Date:', colors.blue);
    log('    ┌────────────┬───────────┬────────────┬──────────┐');
    log('    │    Date    │   Total   │ Unexported │ Exported │');
    log('    ├────────────┼───────────┼────────────┼──────────┤');

    let totalUnexported = 0;
    let totalExported = 0;

    for (const row of byDate) {
      totalUnexported += row.unexported;
      totalExported += row.exported;
      const unexportedColor = row.unexported > 0 ? colors.yellow : colors.dim;
      log(`    │ ${row.date} │ ${formatNumber(row.total).padStart(9)} │ ${colors.reset}${unexportedColor}${formatNumber(row.unexported).padStart(10)}${colors.reset} │ ${formatNumber(row.exported).padStart(8)} │`);
    }

    log('    └────────────┴───────────┴────────────┴──────────┘');

    stats.totalUnexported = totalUnexported;
    stats.totalExported = totalExported;
    stats.dateRange = byDate.length > 0 ? { from: byDate[0].date, to: byDate[byDate.length - 1].date } : null;

    // Summary
    log('\n  Summary:', colors.blue);
    log(`    Unexported: ${formatNumber(totalUnexported)}${totalUnexported > 0 ? ' (will be exported on next restart or daily export)' : ''}`);
    log(`    Exported:   ${formatNumber(totalExported)}`);

    // Recent export batches
    const batches = db.prepare(`
      SELECT
        export_batch_id,
        COUNT(*) as count,
        MIN(received_timestamp) as min_ts,
        MAX(received_timestamp) as max_ts
      FROM buffer_records
      WHERE exported = 1 AND export_batch_id IS NOT NULL
      GROUP BY export_batch_id
      ORDER BY export_batch_id DESC
      LIMIT 5
    `).all();

    if (batches.length > 0) {
      log('\n  Recent Export Batches:', colors.blue);
      for (const batch of batches) {
        log(`    ${batch.export_batch_id}: ${formatNumber(batch.count)} records`);
      }
    }

    // Distinct paths
    const pathCount = db.prepare('SELECT COUNT(DISTINCT path) as count FROM buffer_records').get();
    log(`\n  Distinct Paths: ${pathCount.count}`, colors.blue);

    if (VERBOSE) {
      const paths = db.prepare(`
        SELECT path, COUNT(*) as count
        FROM buffer_records
        GROUP BY path
        ORDER BY count DESC
        LIMIT 10
      `).all();
      log('  Top 10 Paths by Record Count:');
      for (const p of paths) {
        log(`    ${p.path}: ${formatNumber(p.count)}`);
      }
    }

    return stats;
  } finally {
    db.close();
  }
}

async function checkParquetFiles() {
  log('\n═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  PARQUET FILE STATUS', colors.cyan);
  log('═══════════════════════════════════════════════════════════════', colors.cyan);

  if (!fs.existsSync(PARQUET_DIR)) {
    log('  ✗ Parquet directory not found: ' + PARQUET_DIR, colors.red);
    return null;
  }

  const stats = {};

  // Find all parquet files
  const allFiles = await glob('**/*.parquet', {
    cwd: PARQUET_DIR,
    absolute: true,
  });

  // Filter out processed/quarantine/failed
  const activeFiles = allFiles.filter(f =>
    !f.includes('/processed/') &&
    !f.includes('/quarantine/') &&
    !f.includes('/failed/')
  );

  stats.totalFiles = activeFiles.length;
  stats.excludedFiles = allFiles.length - activeFiles.length;

  log(`\n  Total Active Files: ${formatNumber(stats.totalFiles)}`, colors.blue);
  if (stats.excludedFiles > 0) {
    log(`  Excluded (processed/quarantine/failed): ${formatNumber(stats.excludedFiles)}`, colors.dim);
  }

  // Calculate total size
  let totalSize = 0;
  const filesByDay = {};
  const filesByPath = {};

  for (const file of activeFiles) {
    const stat = fs.statSync(file);
    totalSize += stat.size;

    // Extract day from path (year=YYYY/day=DDD)
    const dayMatch = file.match(/year=(\d+)\/day=(\d+)/);
    if (dayMatch) {
      const key = `${dayMatch[1]}-${dayMatch[2].padStart(3, '0')}`;
      filesByDay[key] = (filesByDay[key] || 0) + 1;
    }

    // Extract path from directory
    const pathMatch = file.match(/path=([^/]+)/);
    if (pathMatch) {
      const signalkPath = pathMatch[1].replace(/__/g, '.');
      filesByPath[signalkPath] = (filesByPath[signalkPath] || 0) + 1;
    }
  }

  stats.totalSize = totalSize;
  log(`  Total Size: ${formatBytes(totalSize)}`, colors.blue);

  // Files by day
  const days = Object.keys(filesByDay).sort();
  if (days.length > 0) {
    log('\n  Files by Day:', colors.blue);
    log('    ┌─────────────┬───────┐');
    log('    │  Year-Day   │ Files │');
    log('    ├─────────────┼───────┤');

    // Show last 7 days
    const recentDays = days.slice(-7);
    for (const day of recentDays) {
      log(`    │ ${day.padEnd(11)} │ ${formatNumber(filesByDay[day]).padStart(5)} │`);
    }
    log('    └─────────────┴───────┘');

    if (days.length > 7) {
      log(`    ... and ${days.length - 7} earlier days`, colors.dim);
    }

    stats.dayRange = { from: days[0], to: days[days.length - 1] };
  }

  // Distinct paths
  const pathCount = Object.keys(filesByPath).length;
  log(`\n  Distinct Paths: ${pathCount}`, colors.blue);

  if (VERBOSE) {
    const topPaths = Object.entries(filesByPath)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10);
    log('  Top 10 Paths by File Count:');
    for (const [p, count] of topPaths) {
      log(`    ${p}: ${count} files`);
    }
  }

  // Check for recent files (last hour)
  const oneHourAgo = Date.now() - (60 * 60 * 1000);
  const recentFiles = activeFiles.filter(f => {
    const stat = fs.statSync(f);
    return stat.mtimeMs > oneHourAgo;
  });

  log(`\n  Files Modified in Last Hour: ${recentFiles.length}`, colors.blue);

  if (VERBOSE && recentFiles.length > 0) {
    log('  Recent files:');
    for (const f of recentFiles.slice(0, 5)) {
      log(`    ${path.basename(f)}`);
    }
    if (recentFiles.length > 5) {
      log(`    ... and ${recentFiles.length - 5} more`);
    }
  }

  return stats;
}

async function checkDataIntegrity(sqliteStats, parquetStats) {
  log('\n═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  DATA INTEGRITY CHECK', colors.cyan);
  log('═══════════════════════════════════════════════════════════════', colors.cyan);

  const issues = [];
  const warnings = [];

  // Check 1: Unexported records
  if (sqliteStats && sqliteStats.totalUnexported > 0) {
    if (sqliteStats.totalUnexported > 10000) {
      warnings.push(`Large number of unexported records: ${formatNumber(sqliteStats.totalUnexported)}`);
    } else {
      log(`\n  ℹ Unexported records: ${formatNumber(sqliteStats.totalUnexported)} (normal - will export on next cycle)`, colors.dim);
    }
  }

  // Check 2: Parquet files exist
  if (parquetStats && parquetStats.totalFiles === 0) {
    issues.push('No active Parquet files found');
  }

  // Check 3: Recent activity
  if (sqliteStats && sqliteStats.totalRecords === 0) {
    issues.push('SQLite buffer is empty - no data being collected');
  }

  // Report issues
  if (issues.length > 0) {
    log('\n  Issues Found:', colors.red);
    for (const issue of issues) {
      log(`    ✗ ${issue}`, colors.red);
    }
  }

  if (warnings.length > 0) {
    log('\n  Warnings:', colors.yellow);
    for (const warning of warnings) {
      log(`    ⚠ ${warning}`, colors.yellow);
    }
  }

  if (issues.length === 0 && warnings.length === 0) {
    log('\n  ✓ All checks passed', colors.green);
  }

  return { issues, warnings };
}

async function queryWithDuckDB() {
  log('\n═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  DUCKDB NODE-API QUERY TEST', colors.cyan);
  log('═══════════════════════════════════════════════════════════════', colors.cyan);

  let DuckDBInstance;
  try {
    DuckDBInstance = require('@duckdb/node-api').DuckDBInstance;
  } catch (e) {
    log(`  ✗ @duckdb/node-api not available: ${e.message}`, colors.red);
    return null;
  }

  try {
    log('\n  Testing @duckdb/node-api...', colors.blue);

    // Test 1: Create instance
    const instance = await DuckDBInstance.create();
    log('    ✓ DuckDBInstance.create() succeeded', colors.green);

    // Test 2: Connect
    const connection = await instance.connect();
    log('    ✓ instance.connect() succeeded', colors.green);

    // Test 3: Load spatial extension (like DuckDBPool does)
    await connection.runAndReadAll('INSTALL spatial;');
    await connection.runAndReadAll('LOAD spatial;');
    log('    ✓ Spatial extension loaded', colors.green);

    // Test 4: Query parquet files
    const parquetPattern = path.join(PARQUET_DIR, '**/*.parquet');

    const result = await connection.runAndReadAll(`
      SELECT
        DATE(received_timestamp) as date,
        COUNT(*) as records
      FROM read_parquet('${parquetPattern}', union_by_name=true, filename=true)
      WHERE filename NOT LIKE '%/processed/%'
        AND filename NOT LIKE '%/quarantine/%'
        AND filename NOT LIKE '%/failed/%'
        AND filename NOT LIKE '%/repaired/%'
      GROUP BY date
      ORDER BY date DESC
      LIMIT 7
    `);

    const rows = result.getRowObjects();
    log('    ✓ Parquet query succeeded', colors.green);

    log('\n  Recent Data in Parquet (by date):', colors.blue);
    log('    ┌────────────┬────────────┐');
    log('    │    Date    │  Records   │');
    log('    ├────────────┼────────────┤');

    for (const row of rows) {
      const dateStr = row.date ? String(row.date).slice(0, 10) : 'unknown';
      const count = typeof row.records === 'bigint' ? Number(row.records) : row.records;
      log(`    │ ${dateStr.padEnd(10)} │ ${formatNumber(count).padStart(10)} │`);
    }
    log('    └────────────┴────────────┘');

    // Cleanup
    connection.disconnectSync();
    log('    ✓ Connection cleanup succeeded', colors.green);

    return rows;
  } catch (err) {
    log(`  ✗ DuckDB test failed: ${err.message}`, colors.red);
    if (VERBOSE) {
      log(`    Stack: ${err.stack}`, colors.dim);
    }
    return null;
  }
}

async function testHivePathDiscovery() {
  log('\n═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  HIVE PATH DISCOVERY TEST', colors.cyan);
  log('═══════════════════════════════════════════════════════════════', colors.cyan);

  try {
    // Check Hive structure exists
    if (!fs.existsSync(PARQUET_DIR)) {
      log('  ✗ Hive directory not found: ' + PARQUET_DIR, colors.red);
      return null;
    }
    log('  ✓ Hive directory exists: tier=raw/', colors.green);

    // Find all context directories
    const contextDirs = fs.readdirSync(PARQUET_DIR)
      .filter(d => d.startsWith('context=') && fs.statSync(path.join(PARQUET_DIR, d)).isDirectory());

    log(`\n  Found ${contextDirs.length} context(s):`, colors.blue);

    const results = { contexts: [], totalPaths: 0, totalFiles: 0 };

    for (const contextDir of contextDirs) {
      const contextName = contextDir.replace('context=', '').replace(/__/g, '.');
      const contextPath = path.join(PARQUET_DIR, contextDir);

      // Find all path directories
      const pathDirs = fs.readdirSync(contextPath)
        .filter(d => d.startsWith('path=') && fs.statSync(path.join(contextPath, d)).isDirectory());

      log(`    ${contextName}: ${pathDirs.length} paths`, colors.dim);

      results.contexts.push({
        name: contextName,
        pathCount: pathDirs.length
      });
      results.totalPaths += pathDirs.length;

      // Count files in first few paths (for verification)
      if (VERBOSE && pathDirs.length > 0) {
        for (const pathDir of pathDirs.slice(0, 3)) {
          const pathName = pathDir.replace('path=', '').replace(/__/g, '.');
          const fullPath = path.join(contextPath, pathDir);
          const files = await glob('**/*.parquet', { cwd: fullPath });
          log(`      ${pathName}: ${files.length} files`, colors.dim);
          results.totalFiles += files.length;
        }
        if (pathDirs.length > 3) {
          log(`      ... and ${pathDirs.length - 3} more paths`, colors.dim);
        }
      }
    }

    // Verify year/day structure in a sample path
    if (contextDirs.length > 0) {
      const sampleContext = path.join(PARQUET_DIR, contextDirs[0]);
      const samplePathDirs = fs.readdirSync(sampleContext).filter(d => d.startsWith('path='));

      if (samplePathDirs.length > 0) {
        const samplePath = path.join(sampleContext, samplePathDirs[0]);
        const yearDirs = fs.readdirSync(samplePath).filter(d => d.startsWith('year='));

        if (yearDirs.length > 0) {
          log('\n  Hive partition structure verified:', colors.green);
          log(`    tier=raw/context=.../path=.../year=.../day=.../`, colors.dim);

          const sampleYear = path.join(samplePath, yearDirs[0]);
          const dayDirs = fs.readdirSync(sampleYear).filter(d => d.startsWith('day='));
          log(`    Sample: ${yearDirs[0]}/ has ${dayDirs.length} day partitions`, colors.dim);
        }
      }
    }

    log('\n  ✓ Hive path discovery working correctly', colors.green);
    return results;
  } catch (err) {
    log(`  ✗ Hive path discovery failed: ${err.message}`, colors.red);
    return null;
  }
}

async function main() {
  log('\n╔═══════════════════════════════════════════════════════════════╗', colors.cyan);
  log('║           DATA PIPELINE DIAGNOSTIC REPORT                     ║', colors.cyan);
  log('║           ' + new Date().toISOString() + '                ║', colors.cyan);
  log('╚═══════════════════════════════════════════════════════════════╝', colors.cyan);

  log(`\nData Directory: ${DATA_DIR}`, colors.dim);
  if (VERBOSE) log('Verbose mode: ON', colors.dim);

  const sqliteStats = await checkSQLiteBuffer();
  const parquetStats = await checkParquetFiles();
  await checkDataIntegrity(sqliteStats, parquetStats);
  await testHivePathDiscovery();
  await queryWithDuckDB();

  log('\n═══════════════════════════════════════════════════════════════', colors.cyan);
  log('  REPORT COMPLETE', colors.cyan);
  log('═══════════════════════════════════════════════════════════════\n', colors.cyan);
}

main().catch(err => {
  console.error('Error running diagnostic:', err);
  process.exit(1);
});
