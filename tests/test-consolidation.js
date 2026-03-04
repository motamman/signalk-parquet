#!/usr/bin/env node
/**
 * Test script to diagnose consolidation issues
 */

const path = require('path');
const fs = require('fs-extra');

const DATA_DIR = '/Users/mauricetamman/.signalk/data';
const TEST_DATE = '2025-10-25';

async function runTests() {
  console.log('=== CONSOLIDATION DIAGNOSTIC TESTS ===\n');

  // Test 1: Parquet library
  console.log('TEST 1: Parquet library loading');
  let parquet;
  try {
    parquet = require('@dsnp/parquetjs');
    console.log('  ✓ @dsnp/parquetjs loaded successfully\n');
  } catch (e) {
    console.log('  ✗ FAILED to load @dsnp/parquetjs:', e.message, '\n');
    return;
  }

  // Test 2: Find flat files for test date
  console.log('TEST 2: Finding flat files for', TEST_DATE);
  const { glob } = require('glob');
  const { promisify } = require('util');

  const globAsync = async (pattern, options) => {
    const result = glob(pattern, options || {});
    if (result && typeof result.then === 'function') {
      return result;
    }
    const globPromise = promisify(glob);
    return globPromise(pattern, options || {});
  };

  const pattern = path.join(DATA_DIR, `**/signalk_data_${TEST_DATE}T*.parquet`);
  let files;
  try {
    files = await globAsync(pattern);
    // Filter out processed/quarantine
    files = files.filter(f => !f.includes('/processed/') && !f.includes('/quarantine/'));
    console.log('  ✓ Found', files.length, 'files');
    if (files.length > 0) {
      console.log('  Sample:', files[0]);
    }
    console.log();
  } catch (e) {
    console.log('  ✗ FAILED:', e.message, '\n');
    return;
  }

  if (files.length === 0) {
    console.log('  No files found to test!\n');
    return;
  }

  // Test 3: Read a parquet file
  console.log('TEST 3: Reading parquet file');
  const testFile = files[0];
  let records = [];
  try {
    const reader = await parquet.ParquetReader.openFile(testFile);
    const cursor = reader.getCursor();
    let record;
    while ((record = await cursor.next())) {
      records.push(record);
      if (records.length >= 5) break; // Just read a few
    }
    await reader.close();
    console.log('  ✓ Read', records.length, 'records from file');
    console.log('  Sample record keys:', Object.keys(records[0] || {}));
    console.log();
  } catch (e) {
    console.log('  ✗ FAILED to read parquet:', e.message);
    console.log('  File:', testFile);
    console.log();
    return;
  }

  // Test 4: DirectoryScanner
  console.log('TEST 4: DirectoryScanner');
  try {
    const { DirectoryScanner } = require('./dist/utils/directory-scanner');
    const scanner = new DirectoryScanner(5 * 60 * 1000);
    const foundFiles = await scanner.findFilesByDate(DATA_DIR, TEST_DATE, true);
    console.log('  ✓ DirectoryScanner found', foundFiles.length, 'files');
    if (foundFiles.length > 0) {
      console.log('  Sample:', foundFiles[0].path);
    }
    console.log();
  } catch (e) {
    console.log('  ✗ FAILED:', e.message, '\n');
  }

  // Test 5: Direct parquet write with proper schema
  console.log('TEST 5: Direct parquet write (testing null handling)');
  const testOutputDir = '/tmp/consolidation-test';
  const testOutputFile = path.join(testOutputDir, 'test_consolidated.parquet');
  try {
    await fs.ensureDir(testOutputDir);

    // Read all records from first few files
    const allRecords = [];
    for (const file of files.slice(0, 3)) {
      try {
        const reader = await parquet.ParquetReader.openFile(file);
        const cursor = reader.getCursor();
        let record;
        while ((record = await cursor.next())) {
          allRecords.push(record);
        }
        await reader.close();
      } catch (e) {
        console.log('  Warning: Could not read', file, '-', e.message);
      }
    }

    console.log('  Read', allRecords.length, 'total records');
    console.log('  First record:', JSON.stringify(allRecords[0], null, 2));

    // Create schema with optional=true
    const schema = {
      context: { type: 'UTF8', optional: true },
      meta: { type: 'UTF8', optional: true },
      path: { type: 'UTF8', optional: true },
      received_timestamp: { type: 'UTF8', optional: true },
      signalk_timestamp: { type: 'UTF8', optional: true },
      source: { type: 'UTF8', optional: true },
      source_label: { type: 'UTF8', optional: true },
      source_pgn: { type: 'UTF8', optional: true },
      source_src: { type: 'UTF8', optional: true },
      source_type: { type: 'UTF8', optional: true },
      value: { type: 'UTF8', optional: true },
    };

    const pqSchema = new parquet.ParquetSchema(schema);
    const writer = await parquet.ParquetWriter.openFile(pqSchema, testOutputFile);

    for (const record of allRecords) {
      // Convert nulls - parquet might not like actual null values
      const cleanRecord = {};
      for (const [key, value] of Object.entries(record)) {
        cleanRecord[key] = value === null ? undefined :
                          (typeof value === 'object' ? JSON.stringify(value) : value);
      }
      console.log('  Writing record with keys:', Object.keys(cleanRecord).join(', '));
      try {
        await writer.appendRow(cleanRecord);
      } catch (e) {
        console.log('  ✗ appendRow failed:', e.message);
        console.log('  Record:', JSON.stringify(cleanRecord));
        throw e;
      }
    }
    await writer.close();

    const stats = await fs.stat(testOutputFile);
    console.log('  ✓ Wrote consolidated file:', stats.size, 'bytes');
    console.log();
  } catch (e) {
    console.log('  ✗ FAILED:', e.message);
    console.log();
  }

  // Mock app for all tests - reused below
  const mockApp = {
    debug: (msg) => {}, // silent for cleaner output
    error: (msg) => console.log('  [error]', msg),
    getMetadata: () => null
  };

  const mockAppVerbose = {
    debug: (msg) => console.log('  [debug]', msg),
    error: (msg) => console.log('  [error]', msg),
    getMetadata: () => null
  };

  // Test 6: SchemaService.detectOptimalSchema (actual implementation)
  console.log('TEST 6: SchemaService.detectOptimalSchema');
  try {
    const { SchemaService } = require('./dist/schema-service');

    const schemaService = new SchemaService(mockApp);

    // Read records from a file with actual data
    const testRecords = [];
    const reader = await parquet.ParquetReader.openFile(files[0]);
    const cursor = reader.getCursor();
    let rec;
    while ((rec = await cursor.next())) {
      testRecords.push(rec);
    }
    await reader.close();

    const result = await schemaService.detectOptimalSchema(testRecords, 'navigation.position');
    console.log('  ✓ Schema detected with', result.fieldCount, 'fields');
    console.log('  isExplodedFile:', result.isExplodedFile);
    console.log('  Fields:', Object.keys(result.schema.schema).join(', '));

    // Show detected types
    const types = {};
    for (const [field, config] of Object.entries(result.schema.schema)) {
      types[field] = config.type;
    }
    console.log('  Types:', JSON.stringify(types, null, 2));
    console.log();
  } catch (e) {
    console.log('  ✗ FAILED:', e.message);
    console.log('  Stack:', e.stack);
    console.log();
  }

  // Test 7: prepareRecordForParquet (actual implementation)
  console.log('TEST 7: prepareRecordForParquet with type conversion');
  try {
    const { ParquetWriter: PW } = require('./dist/parquet-writer');
    const { SchemaService } = require('./dist/schema-service');

    const schemaService = new SchemaService(mockApp);
    const pw = new PW({ format: 'parquet', app: mockApp });

    // Test record with various types including nulls
    const testRecord = {
      context: 'vessels.test',
      meta: null,
      path: 'test.path',
      received_timestamp: '2025-01-01T00:00:00Z',
      signalk_timestamp: '2025-01-01T00:00:00Z',
      source: { label: 'test', type: 'test' },  // object that needs serialization
      source_label: 'test',
      source_pgn: null,
      source_src: null,
      source_type: null,
      value: 123.456  // numeric value
    };

    const schemaResult = await schemaService.detectOptimalSchema([testRecord], 'test.path');
    console.log('  Schema value type:', schemaResult.schema.schema.value?.type || 'NOT IN SCHEMA');

    const prepared = pw.prepareRecordForParquet(testRecord, schemaResult.schema);
    console.log('  Prepared record:');
    for (const [key, val] of Object.entries(prepared)) {
      console.log(`    ${key}: ${val === undefined ? 'undefined' : JSON.stringify(val)} (${typeof val})`);
    }

    // Check null handling
    const nullFields = Object.entries(prepared).filter(([k, v]) => v === null);
    const undefinedFields = Object.entries(prepared).filter(([k, v]) => v === undefined);
    console.log('  Null fields:', nullFields.length);
    console.log('  Undefined fields:', undefinedFields.length);

    if (nullFields.length > 0) {
      console.log('  ✗ PROBLEM: Still has null values:', nullFields.map(([k]) => k).join(', '));
    } else {
      console.log('  ✓ No null values (all converted to undefined)');
    }
    console.log();
  } catch (e) {
    console.log('  ✗ FAILED:', e.message);
    console.log('  Stack:', e.stack);
    console.log();
  }

  // Test 8: Full ParquetWriter.writeParquet path
  console.log('TEST 8: Full ParquetWriter.writeParquet (actual implementation)');
  const test8OutputFile = '/tmp/consolidation-test/test8_actual_impl.parquet';
  try {
    const { ParquetWriter: PW } = require('./dist/parquet-writer');

    const pw = new PW({ format: 'parquet', app: mockAppVerbose });

    // Read records from first 3 files
    const allRecords = [];
    for (const file of files.slice(0, 3)) {
      try {
        const reader = await parquet.ParquetReader.openFile(file);
        const cursor = reader.getCursor();
        let record;
        while ((record = await cursor.next())) {
          allRecords.push(record);
        }
        await reader.close();
      } catch (e) {
        console.log('  Warning: Could not read', file);
      }
    }
    console.log('  Read', allRecords.length, 'records from', files.slice(0, 3).length, 'files');

    // Use actual writeParquet method
    await fs.remove(test8OutputFile).catch(() => {});
    const result = await pw.writeParquet(test8OutputFile, allRecords);

    const stats = await fs.stat(test8OutputFile);
    console.log('  ✓ writeParquet succeeded:', stats.size, 'bytes');

    // Verify by reading back
    const verifyReader = await parquet.ParquetReader.openFile(test8OutputFile);
    const verifyCursor = verifyReader.getCursor();
    let verifyCount = 0;
    while (await verifyCursor.next()) verifyCount++;
    await verifyReader.close();
    console.log('  ✓ Verified: read back', verifyCount, 'records');
    console.log();
  } catch (e) {
    console.log('  ✗ FAILED:', e.message);
    console.log('  Stack:', e.stack);
    console.log();
  }

  // Test 9: mergeFiles (actual implementation)
  console.log('TEST 9: mergeFiles (actual implementation)');
  const test9OutputFile = '/tmp/consolidation-test/test9_merged.parquet';
  try {
    const { ParquetWriter: PW } = require('./dist/parquet-writer');

    const pw = new PW({ format: 'parquet', app: mockApp });

    // Pick 3 files from same directory to simulate real consolidation
    const sampleDir = path.dirname(files[0]);
    const filesInSameDir = files.filter(f => path.dirname(f) === sampleDir).slice(0, 3);
    console.log('  Merging', filesInSameDir.length, 'files from:', sampleDir);

    await fs.remove(test9OutputFile).catch(() => {});
    const recordCount = await pw.mergeFiles(filesInSameDir, test9OutputFile);

    console.log('  ✓ mergeFiles returned:', recordCount, 'records');

    if (recordCount > 0) {
      const stats = await fs.stat(test9OutputFile);
      console.log('  ✓ Output file:', stats.size, 'bytes');

      // Verify
      const verifyReader = await parquet.ParquetReader.openFile(test9OutputFile);
      const verifyCursor = verifyReader.getCursor();
      let verifyCount = 0;
      while (await verifyCursor.next()) verifyCount++;
      await verifyReader.close();
      console.log('  ✓ Verified: read back', verifyCount, 'records');
    }
    console.log();
  } catch (e) {
    console.log('  ✗ FAILED:', e.message);
    console.log('  Stack:', e.stack);
    console.log();
  }

  // Test 10: Single directory consolidation (simulated consolidateDaily for one path)
  console.log('TEST 10: Single directory consolidation (mini consolidateDaily)');
  const test10OutputDir = '/tmp/consolidation-test/test10';
  try {
    const { ParquetWriter: PW } = require('./dist/parquet-writer');
    const { DirectoryScanner } = require('./dist/utils/directory-scanner');

    await fs.ensureDir(test10OutputDir);
    await fs.emptyDir(test10OutputDir);

    // Copy a few source files to test directory
    const sourceFiles = files.slice(0, 5);
    for (const f of sourceFiles) {
      const dest = path.join(test10OutputDir, path.basename(f));
      await fs.copy(f, dest);
    }
    console.log('  Copied', sourceFiles.length, 'files to test directory');

    const pw = new PW({ format: 'parquet', app: mockApp });

    // Run consolidateDaily on test directory
    const testDate = new Date(TEST_DATE);
    const consolidatedCount = await pw.consolidateDaily(test10OutputDir, testDate, 'signalk_data');

    console.log('  ✓ consolidateDaily returned:', consolidatedCount, 'groups consolidated');

    // Check results
    const consolidatedFiles = await fs.readdir(test10OutputDir);
    const consolidated = consolidatedFiles.filter(f => f.includes('_consolidated'));
    const processed = await fs.pathExists(path.join(test10OutputDir, 'processed'));

    console.log('  Consolidated files:', consolidated);
    console.log('  Processed folder exists:', processed);

    if (processed) {
      const processedFiles = await fs.readdir(path.join(test10OutputDir, 'processed'));
      console.log('  Files moved to processed:', processedFiles.length);
    }

    // Verify consolidated file
    if (consolidated.length > 0) {
      const consolidatedPath = path.join(test10OutputDir, consolidated[0]);
      const stats = await fs.stat(consolidatedPath);
      console.log('  ✓ Consolidated file size:', stats.size, 'bytes');

      const verifyReader = await parquet.ParquetReader.openFile(consolidatedPath);
      const verifyCursor = verifyReader.getCursor();
      let verifyCount = 0;
      while (await verifyCursor.next()) verifyCount++;
      await verifyReader.close();
      console.log('  ✓ Verified: consolidated file has', verifyCount, 'records');
    }
    console.log();
  } catch (e) {
    console.log('  ✗ FAILED:', e.message);
    console.log('  Stack:', e.stack);
    console.log();
  }

  // Test 11: Mixed type handling (numeric + null values)
  console.log('TEST 11: Mixed type handling (numeric values with nulls)');
  try {
    const { ParquetWriter: PW } = require('./dist/parquet-writer');

    const pw = new PW({ format: 'parquet', app: mockApp });

    // Records with mixed: some have numeric value, some have null
    const mixedRecords = [
      {
        context: 'vessels.test',
        path: 'navigation.speedOverGround',
        received_timestamp: '2025-01-01T00:00:00Z',
        value: 5.5
      },
      {
        context: 'vessels.test',
        path: 'navigation.speedOverGround',
        received_timestamp: '2025-01-01T00:00:01Z',
        value: null  // null value - should schema be DOUBLE or UTF8?
      },
      {
        context: 'vessels.test',
        path: 'navigation.speedOverGround',
        received_timestamp: '2025-01-01T00:00:02Z',
        value: 6.2
      }
    ];

    const test11OutputFile = '/tmp/consolidation-test/test11_mixed.parquet';
    await fs.remove(test11OutputFile).catch(() => {});

    const result = await pw.writeParquet(test11OutputFile, mixedRecords);
    const stats = await fs.stat(test11OutputFile);
    console.log('  ✓ Wrote mixed records:', stats.size, 'bytes');

    // Read back and check
    const verifyReader = await parquet.ParquetReader.openFile(test11OutputFile);
    const verifyCursor = verifyReader.getCursor();
    const readBack = [];
    let rec;
    while ((rec = await verifyCursor.next())) {
      readBack.push(rec);
    }
    await verifyReader.close();

    console.log('  Read back', readBack.length, 'records:');
    for (const r of readBack) {
      console.log(`    value: ${r.value} (${typeof r.value})`);
    }
    console.log();
  } catch (e) {
    console.log('  ✗ FAILED:', e.message);
    console.log('  Stack:', e.stack);
    console.log();
  }

  console.log('=== TESTS COMPLETE ===');
}

runTests().catch(e => {
  console.error('Test runner error:', e);
  process.exit(1);
});
