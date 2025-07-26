#!/usr/bin/env ts-node

/**
 * Schema Migration Script
 *
 * Migrates old parquet files with UTF8-only schemas to new intelligent schemas.
 * Backs up originals and recreates files with proper data types.
 */

import * as fs from 'fs-extra';
import { glob } from 'glob';
import { DataRecord, ParquetField } from './types';

// Try to import ParquetJS, fall back if not available
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let parquet: any;
try {
  parquet = require('@dsnp/parquetjs');
} catch (error) {
  console.error('ParquetJS not available:', error);
  process.exit(1);
}

interface MigrationStats {
  migrated: number;
  skipped: number;
  errors: number;
}

async function migrateSchemas(dataDir = 'data'): Promise<MigrationStats> {
  console.log(`🔍 Scanning for parquet files in ${dataDir}...`);

  // Find all parquet files (excluding processed directories)
  const parquetFiles = await glob(`${dataDir}/**/*.parquet`, {
    ignore: '**/processed/**',
  });
  console.log(`📁 Found ${parquetFiles.length} parquet files`);

  const stats: MigrationStats = {
    migrated: 0,
    skipped: 0,
    errors: 0,
  };

  for (const filePath of parquetFiles) {
    try {
      console.log(`\n🔍 Checking: ${filePath}`);

      // Check if file needs migration (has UTF8 value column)
      const needsMigration = await checkNeedsMigration(filePath);

      if (!needsMigration) {
        console.log(`✅ Already has proper schema, skipping`);
        stats.skipped++;
        continue;
      }

      console.log(`🔄 Migrating schema...`);

      // Read all records from the file
      const records = await readParquetFile(filePath);

      if (records.length === 0) {
        console.log(`⚠️  Empty file, skipping`);
        stats.skipped++;
        continue;
      }

      // Create new file with intelligent schema first
      const tempPath = `${filePath}.migrated`;
      await writeParquetWithIntelligentSchema(tempPath, records);

      // Backup original and replace
      const backupPath = `${filePath}.backup-utf8`;

      // If backup already exists, remove it first (from previous migration attempt)
      if (await fs.pathExists(backupPath)) {
        await fs.remove(backupPath);
      }

      await fs.move(filePath, backupPath);
      await fs.move(tempPath, filePath);
      console.log(`💾 Original backed up to: ${backupPath}`);

      console.log(`✅ Migrated: ${records.length} records`);
      stats.migrated++;
    } catch (error) {
      console.error(
        `❌ Error migrating ${filePath}:`,
        (error as Error).message
      );
      stats.errors++;
    }
  }

  console.log(`\n📊 Migration Summary:`);
  console.log(`   ✅ Migrated: ${stats.migrated} files`);
  console.log(`   ⏭️  Skipped: ${stats.skipped} files`);
  console.log(`   ❌ Errors: ${stats.errors} files`);

  return stats;
}

async function checkNeedsMigration(filePath: string): Promise<boolean> {
  try {
    const reader = await parquet.ParquetReader.openFile(filePath);
    const schema = reader.schema;

    // Check all value-related columns (value, value_latitude, value_longitude, etc.)
    // But exclude metadata fields like value_json which should stay as strings
    const schemaFields = schema.schema || {};
    const valueFieldNames = Object.keys(schemaFields).filter(
      name =>
        name === 'value' || (name.startsWith('value_') && name !== 'value_json')
    );

    if (valueFieldNames.length === 0) {
      await reader.close();
      return false;
    }

    // Check if any value field is UTF8/BYTE_ARRAY (string type)
    for (const fieldName of valueFieldNames) {
      const field = schemaFields[fieldName];
      if (field) {
        const needsMigration =
          field.primitiveType === 'UTF8' ||
          field.type === 'UTF8' ||
          field.primitiveType === 'BYTE_ARRAY' ||
          field.type === 'BYTE_ARRAY' ||
          (field.logicalType && field.logicalType.type === 'UTF8') ||
          (field.logicalType && field.logicalType.type === 'STRING');

        if (needsMigration) {
          await reader.close();
          return true;
        }
      }
    }

    await reader.close();
    return false;
  } catch (error) {
    console.warn(`Warning: Could not read schema from ${filePath}`);
    return false;
  }
}

async function readParquetFile(filePath: string): Promise<DataRecord[]> {
  const reader = await parquet.ParquetReader.openFile(filePath);
  const cursor = reader.getCursor();

  const records: DataRecord[] = [];
  let record: DataRecord | null = null;
  while ((record = await cursor.next())) {
    records.push(record);
  }

  await reader.close();
  return records;
}

// Replicate the intelligent schema logic from ParquetWriter
function createIntelligentSchema(
  records: DataRecord[]
): typeof parquet.ParquetSchema {
  if (!parquet || records.length === 0) {
    throw new Error('Cannot create Parquet schema');
  }

  // Get all unique column names from all records
  const allColumns = new Set<string>();
  records.forEach(record => {
    Object.keys(record).forEach(key => allColumns.add(key));
  });

  const columns = Array.from(allColumns).sort();
  console.log(`🔍 Schema columns: ${columns.join(', ')}`);

  const schemaFields: { [key: string]: ParquetField } = {};

  // Analyze each column to determine the best Parquet type
  columns.forEach(colName => {
    const values = records
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      .map(r => (r as any)[colName])
      .filter(v => v !== null && v !== undefined);

    if (values.length === 0) {
      // All null values, default to string
      schemaFields[colName] = { type: 'UTF8', optional: true };
      return;
    }

    const hasNumbers = values.some(v => typeof v === 'number');
    const hasStrings = values.some(v => typeof v === 'string');
    const hasBooleans = values.some(v => typeof v === 'boolean');

    // Only log details for the value column that we care about
    if (colName === 'value') {
      console.log(
        `🎯 Value column analysis - numbers: ${hasNumbers}, strings: ${hasStrings}, booleans: ${hasBooleans}`
      );
      console.log(`📊 Value samples: ${JSON.stringify(values.slice(0, 3))}`);
    }

    if (hasNumbers && !hasStrings && !hasBooleans) {
      // All numbers - check if integers or floats
      const allIntegers = values.every(v => Number.isInteger(v));
      schemaFields[colName] = {
        type: allIntegers ? 'INT64' : 'DOUBLE',
        optional: true,
      };
      if (colName === 'value') {
        console.log(`🔢 Value column -> ${allIntegers ? 'INT64' : 'DOUBLE'}`);
      }
    } else if (hasBooleans && !hasNumbers && !hasStrings) {
      schemaFields[colName] = { type: 'BOOLEAN', optional: true };
      if (colName === 'value') {
        console.log(`✅ Value column -> BOOLEAN`);
      }
    } else {
      // Mixed types or strings - use UTF8
      schemaFields[colName] = { type: 'UTF8', optional: true };
      if (colName === 'value') {
        console.log(`📝 Value column -> UTF8 (mixed/strings)`);
      }
    }
  });

  return new parquet.ParquetSchema(schemaFields);
}

// Replicate the record preparation logic from ParquetWriter
function prepareRecordForParquet(
  record: DataRecord,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  schema: any
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
): { [key: string]: any } {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const cleanRecord: { [key: string]: any } = {};

  const schemaFields = schema.schema;

  Object.keys(schemaFields).forEach(fieldName => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const value = (record as any)[fieldName];
    const fieldType = schemaFields[fieldName].type;

    if (value === null || value === undefined) {
      cleanRecord[fieldName] = null;
    } else {
      switch (fieldType) {
        case 'DOUBLE':
        case 'FLOAT':
          cleanRecord[fieldName] =
            typeof value === 'number' ? value : parseFloat(String(value));
          break;
        case 'INT64':
        case 'INT32':
          cleanRecord[fieldName] =
            typeof value === 'number'
              ? Math.round(value)
              : parseInt(String(value));
          break;
        case 'BOOLEAN':
          cleanRecord[fieldName] =
            typeof value === 'boolean' ? value : Boolean(value);
          break;
        case 'UTF8':
        default:
          if (typeof value === 'object') {
            cleanRecord[fieldName] = JSON.stringify(value);
          } else {
            cleanRecord[fieldName] = String(value);
          }
          break;
      }
    }
  });

  return cleanRecord;
}

async function writeParquetWithIntelligentSchema(
  filepath: string,
  records: DataRecord[]
): Promise<void> {
  if (records.length === 0) {
    throw new Error('No records to write');
  }

  // Create intelligent schema
  const schema = createIntelligentSchema(records);
  console.log(
    `📋 Created schema with ${Object.keys(schema.schema).length} fields`
  );

  // Create Parquet writer
  const writer = await parquet.ParquetWriter.openFile(schema, filepath);

  // Write records with proper type conversion
  for (const record of records) {
    const preparedRecord = prepareRecordForParquet(record, schema);
    await writer.appendRow(preparedRecord);
  }

  // Close the writer
  await writer.close();
}

// Run if called directly
if (require.main === module) {
  const dataDir = process.argv[2] || 'data';
  migrateSchemas(dataDir).catch(console.error);
}

export { migrateSchemas };
