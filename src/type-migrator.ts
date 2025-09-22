import * as fs from 'fs-extra';
import * as path from 'path';
import { DuckDBInstance } from '@duckdb/node-api';

export interface TypeMigrationResult {
  wasMigrated: boolean;
  error?: string;
}

export class TypeMigrator {
  private static instance?: TypeMigrator;

  static getInstance(): TypeMigrator {
    if (!TypeMigrator.instance) {
      TypeMigrator.instance = new TypeMigrator();
    }
    return TypeMigrator.instance;
  }

  async migrateFileTypesIfNeeded(filePath: string): Promise<TypeMigrationResult> {
    try {
      if (!await fs.pathExists(filePath)) {
        return { wasMigrated: false, error: 'File does not exist' };
      }

      const instance = await DuckDBInstance.create();
      const conn = await instance.connect();

      try {
        // Check column info to detect exploded values
        const columnsResult = await conn.runAndReadAll(`
          DESCRIBE FROM read_parquet('${filePath}')
        `);
        const columns = columnsResult.getRowObjects();
        const hasExplodedValues = columns.some(col =>
          typeof col.column_name === 'string' && col.column_name.startsWith('value_')
        );

        // Check current types
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

        if (!needsMigration) {
          return { wasMigrated: false };
        }

        // Create temp file for migration
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

        // Create backup and replace original
        const backupFile = filePath + '.backup';
        await fs.move(filePath, backupFile);
        await fs.move(tempFile, filePath);

        return { wasMigrated: true };

      } finally {
        conn.disconnectSync();
      }

    } catch (error) {
      return {
        wasMigrated: false,
        error: `Migration failed: ${(error as Error).message}`
      };
    }
  }

  async migrateDirectoryFiles(directoryPath: string): Promise<{ migrated: number; errors: string[] }> {
    const stats = { migrated: 0, errors: [] as string[] };

    try {
      const files = await fs.readdir(directoryPath);

      for (const file of files) {
        if (file.endsWith('.parquet') && !file.includes('.backup') && !file.includes('.migrating')) {
          const filePath = path.join(directoryPath, file);
          const result = await this.migrateFileTypesIfNeeded(filePath);

          if (result.wasMigrated) {
            stats.migrated++;
          } else if (result.error) {
            stats.errors.push(`${file}: ${result.error}`);
          }
        }
      }
    } catch (error) {
      stats.errors.push(`Directory read error: ${(error as Error).message}`);
    }

    return stats;
  }
}