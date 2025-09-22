#!/usr/bin/env node
import * as fs from 'fs-extra';
import * as path from 'path';
import { DuckDBInstance } from '@duckdb/node-api';
import { glob } from 'glob';

interface MigrationStats {
  totalFiles: number;
  migratedFiles: number;
  skippedFiles: number;
  errors: string[];
  processedPaths: string[];
}

async function migrateParquetDataTypes(dataDirectory: string): Promise<MigrationStats> {
  const stats: MigrationStats = {
    totalFiles: 0,
    migratedFiles: 0,
    skippedFiles: 0,
    errors: [],
    processedPaths: []
  };

  console.log(`Starting migration of parquet files in: ${dataDirectory}`);

  const instance = await DuckDBInstance.create();
  const conn = await instance.connect();

  try {
    const parquetFiles = await glob('**/*.parquet', {
      cwd: dataDirectory,
      absolute: true
    });

    stats.totalFiles = parquetFiles.length;
    console.log(`Found ${stats.totalFiles} parquet files to process`);

    for (const filePath of parquetFiles) {
      try {
        console.log(`Processing: ${path.relative(dataDirectory, filePath)}`);

        // First get column info to detect exploded values
        const columnsResult = await conn.runAndReadAll(`
          DESCRIBE FROM read_parquet('${filePath}')
        `);
        const columns = columnsResult.getRowObjects();
        const hasExplodedValues = columns.some(col =>
          typeof col.column_name === 'string' && col.column_name.startsWith('value_')
        );

        const result = await conn.runAndReadAll(`
          SELECT
            COUNT(*) as row_count,
            ANY_VALUE(typeof(value)) as value_type,
            ANY_VALUE(typeof(received_timestamp)) as received_timestamp_type,
            ANY_VALUE(typeof(signalk_timestamp)) as signalk_timestamp_type
          FROM read_parquet('${filePath}')
        `);

        const fileInfo = result.getRowObjects()[0];

        const needsMigration = fileInfo.value_type === 'VARCHAR' ||
                              fileInfo.received_timestamp_type === 'VARCHAR' ||
                              fileInfo.signalk_timestamp_type === 'VARCHAR' ||
                              (hasExplodedValues && columns.some(col =>
                                typeof col.column_name === 'string' &&
                                col.column_name.startsWith('value_') &&
                                col.column_type === 'VARCHAR'
                              ));

        if (needsMigration) {
          console.log(`  Migrating file with ${fileInfo.row_count} rows ${hasExplodedValues ? '(exploded values)' : ''}`);
          console.log(`  Current types: value=${fileInfo.value_type}, received_timestamp=${fileInfo.received_timestamp_type}, signalk_timestamp=${fileInfo.signalk_timestamp_type}`);

          const tempFile = filePath + '.migrating';

          // Build SELECT statement with proper column handling
          const selectColumns = [];

          for (const col of columns) {
            const colName = col.column_name;

            if (typeof colName !== 'string') continue;

            if (colName === 'value') {
              if (hasExplodedValues) {
                // Keep JSON objects as VARCHAR for exploded files
                selectColumns.push('value');
              } else {
                // Try to cast to DOUBLE for simple numeric values
                selectColumns.push('TRY_CAST(value AS DOUBLE) as value');
              }
            } else if (colName === 'received_timestamp' || colName === 'signalk_timestamp') {
              selectColumns.push(`TRY_CAST(${colName} AS TIMESTAMP) as ${colName}`);
            } else if (colName.startsWith('value_') && col.column_type === 'VARCHAR') {
              // Try to cast exploded value columns to DOUBLE
              selectColumns.push(`TRY_CAST(${colName} AS DOUBLE) as ${colName}`);
            } else {
              // Keep other columns as-is
              selectColumns.push(colName);
            }
          }

          const selectStatement = selectColumns.join(', ');

          await conn.runAndReadAll(`
            COPY (
              SELECT ${selectStatement}
              FROM read_parquet('${filePath}')
            ) TO '${tempFile}' (FORMAT 'parquet', COMPRESSION 'snappy')
          `);

          const backupFile = filePath + '.backup';
          await fs.move(filePath, backupFile);
          await fs.move(tempFile, filePath);

          const verifyResult = await conn.runAndReadAll(`
            SELECT
              typeof(value) as value_type,
              typeof(received_timestamp) as received_timestamp_type,
              typeof(signalk_timestamp) as signalk_timestamp_type
            FROM read_parquet('${filePath}')
            LIMIT 1
          `);

          const newTypes = verifyResult.getRowObjects()[0];
          console.log(`  ✓ Migrated types: value=${newTypes.value_type}, received_timestamp=${newTypes.received_timestamp_type}, signalk_timestamp=${newTypes.signalk_timestamp_type}`);

          stats.migratedFiles++;
          stats.processedPaths.push(path.relative(dataDirectory, filePath));

        } else {
          console.log(`  ✓ Already has proper types, skipping`);
          stats.skippedFiles++;
        }

      } catch (error) {
        const errorMsg = `Failed to process ${filePath}: ${error}`;
        console.error(`  ✗ ${errorMsg}`);
        stats.errors.push(errorMsg);
      }
    }

  } finally {
    conn.disconnectSync();
  }

  return stats;
}

async function main() {
  const dataDir = process.argv[2] || '/Users/mauricetamman/.signalk/data';

  if (!await fs.pathExists(dataDir)) {
    console.error(`Data directory does not exist: ${dataDir}`);
    process.exit(1);
  }

  console.log('SignalK Parquet Data Type Migration Tool');
  console.log('========================================');
  console.log(`Data directory: ${dataDir}`);
  console.log('');

  const startTime = Date.now();

  try {
    const stats = await migrateParquetDataTypes(dataDir);

    const duration = Math.round((Date.now() - startTime) / 1000);

    console.log('');
    console.log('Migration Summary:');
    console.log('==================');
    console.log(`Total files processed: ${stats.totalFiles}`);
    console.log(`Files migrated: ${stats.migratedFiles}`);
    console.log(`Files skipped (already correct): ${stats.skippedFiles}`);
    console.log(`Errors: ${stats.errors.length}`);
    console.log(`Duration: ${duration}s`);

    if (stats.errors.length > 0) {
      console.log('');
      console.log('Errors encountered:');
      stats.errors.forEach(error => console.log(`  - ${error}`));
    }

    if (stats.migratedFiles > 0) {
      console.log('');
      console.log('Successfully migrated files:');
      stats.processedPaths.forEach(filePath => console.log(`  - ${filePath}`));
    }

  } catch (error) {
    console.error('Migration failed:', error);
    process.exit(1);
  }
}

if (require.main === module) {
  main().catch(error => {
    console.error('Unexpected error:', error);
    process.exit(1);
  });
}