import * as fs from 'fs-extra';
import * as path from 'path';
import { glob } from 'glob';
import { DataRecord, ParquetField } from './types';
import { ServerAPI } from '@signalk/server-api';
import { EventEmitter } from 'events';

// Try to import ParquetJS, fall back if not available
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let parquet: any;
try {
  parquet = require('@dsnp/parquetjs');
} catch (error) {
  parquet = null;
}

export interface MigrationProgress {
  type: 'progress' | 'log' | 'complete' | 'error';
  message: string;
  progress?: number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data?: any;
}

export interface PathInfo {
  path: string;
  fileCount: number;
  files: string[];
  needsMigration: boolean;
}

export interface MigrationScanResult {
  totalFiles: number;
  problematicPaths: PathInfo[];
  summary: {
    pathsNeedingMigration: number;
    filesNeedingMigration: number;
  };
}

export class MigrationService extends EventEmitter {
  private app?: ServerAPI;

  constructor(app?: ServerAPI) {
    super();
    this.app = app;
  }

  private emitProgress(progress: MigrationProgress): void {
    super.emit('progress', progress);
    this.app?.debug(`Migration: ${progress.message}`);
  }

  async scanForProblematicFiles(dataDir: string): Promise<MigrationScanResult> {
    this.emitProgress({
      type: 'log',
      message: `🔍 Starting scan of ${dataDir}...`,
    });

    try {
      // Find all parquet files (excluding processed directories)
      const parquetFiles = await glob(`${dataDir}/**/*.parquet`, {
        ignore: '**/processed/**',
      });

      this.emitProgress({
        type: 'log',
        message: `📁 Found ${parquetFiles.length} parquet files to check`,
      });

      const problematicFiles: string[] = [];
      let checkedCount = 0;

      // Check each file for schema issues
      for (const filePath of parquetFiles) {
        try {
          const needsMigration = await this.checkFileNeedsMigration(filePath);
          if (needsMigration) {
            problematicFiles.push(filePath);
          }

          checkedCount++;
          const progress = Math.round(
            (checkedCount / parquetFiles.length) * 100
          );

          if (checkedCount % 50 === 0 || checkedCount === parquetFiles.length) {
            this.emitProgress({
              type: 'progress',
              message: `Checking schemas... (${checkedCount}/${parquetFiles.length})`,
              progress,
            });
          }
        } catch (error) {
          this.emitProgress({
            type: 'log',
            message: `⚠️ Could not check ${filePath}: ${(error as Error).message}`,
          });
        }
      }

      // Group problematic files by path
      const pathGroups = this.groupFilesByPath(problematicFiles);

      const result: MigrationScanResult = {
        totalFiles: parquetFiles.length,
        problematicPaths: pathGroups,
        summary: {
          pathsNeedingMigration: pathGroups.length,
          filesNeedingMigration: problematicFiles.length,
        },
      };

      this.emitProgress({
        type: 'complete',
        message: `✅ Scan complete: ${result.summary.filesNeedingMigration} files in ${result.summary.pathsNeedingMigration} paths need migration`,
        data: result,
      });

      return result;
    } catch (error) {
      this.emitProgress({
        type: 'error',
        message: `❌ Scan failed: ${(error as Error).message}`,
      });
      throw error;
    }
  }

  async repairSelectedPaths(
    dataDir: string,
    selectedPaths: string[]
  ): Promise<void> {
    this.emitProgress({
      type: 'log',
      message: `🔧 Starting repair of ${selectedPaths.length} selected paths...`,
    });

    let totalRepaired = 0;
    let totalErrors = 0;

    for (let i = 0; i < selectedPaths.length; i++) {
      const selectedPath = selectedPaths[i];

      try {
        this.emitProgress({
          type: 'progress',
          message: `Repairing path: ${selectedPath}`,
          progress: Math.round((i / selectedPaths.length) * 100),
        });

        // Find all problematic files in this path
        const pathPattern = path.join(dataDir, selectedPath, '**/*.parquet');
        const pathFiles = await glob(pathPattern, {
          ignore: '**/processed/**',
        });

        // Repair each file in this path
        for (const filePath of pathFiles) {
          const needsMigration = await this.checkFileNeedsMigration(filePath);
          if (needsMigration) {
            await this.repairSingleFile(filePath);
            totalRepaired++;

            this.emitProgress({
              type: 'log',
              message: `✅ Repaired: ${path.relative(dataDir, filePath)}`,
            });
          }
        }
      } catch (error) {
        totalErrors++;
        this.emitProgress({
          type: 'log',
          message: `❌ Error repairing ${selectedPath}: ${(error as Error).message}`,
        });
      }
    }

    this.emitProgress({
      type: 'complete',
      message: `🎉 Repair complete: ${totalRepaired} files repaired, ${totalErrors} errors`,
      data: { repaired: totalRepaired, errors: totalErrors },
    });
  }

  private async checkFileNeedsMigration(filePath: string): Promise<boolean> {
    if (!parquet) {
      return false;
    }

    try {
      const reader = await parquet.ParquetReader.openFile(filePath);
      const schema = reader.schema;

      // Check all value-related columns (value, value_latitude, value_longitude, etc.)
      // But exclude metadata fields like value_json which should stay as strings
      const schemaFields = schema.schema || {};
      const valueFieldNames = Object.keys(schemaFields).filter(name => 
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
      return false;
    }
  }

  private groupFilesByPath(files: string[]): PathInfo[] {
    const pathMap = new Map<string, string[]>();

    // Group files by their parent path structure
    files.forEach(filePath => {
      // Extract the logical path (remove data/ prefix and filename)
      const relativePath = path.dirname(filePath);
      const parts = relativePath.split(path.sep);

      // Find the vessels/* pattern and build logical path
      const vesselsIndex = parts.findIndex(p => p === 'vessels');
      if (vesselsIndex >= 0) {
        const logicalPath = parts.slice(vesselsIndex).join('/');

        if (!pathMap.has(logicalPath)) {
          pathMap.set(logicalPath, []);
        }
        pathMap.get(logicalPath)!.push(filePath);
      }
    });

    // Convert to PathInfo array
    return Array.from(pathMap.entries())
      .map(([path, files]) => ({
        path,
        fileCount: files.length,
        files,
        needsMigration: true,
      }))
      .sort((a, b) => a.path.localeCompare(b.path));
  }

  private async repairSingleFile(filePath: string): Promise<void> {
    if (!parquet) {
      throw new Error('ParquetJS not available');
    }

    try {
      // Read all records from the file
      this.emitProgress({
        type: 'log',
        message: `📖 Reading ${filePath}...`,
      });
      const records = await this.readParquetFile(filePath);

      if (records.length === 0) {
        this.emitProgress({
          type: 'log',
          message: `⏭️ Skipping empty file ${filePath}`,
        });
        return; // Skip empty files
      }

      // Create new file with intelligent schema first
      const tempPath = `${filePath}.migrating`;
      
      // Remove temp file if it already exists
      if (await fs.pathExists(tempPath)) {
        this.emitProgress({
          type: 'log',
          message: `🗑️ Removing existing temp file: ${tempPath}`,
        });
        await fs.remove(tempPath);
      }
      
      this.emitProgress({
        type: 'log',
        message: `🔄 Creating migrated file with ${records.length} records...`,
      });
      await this.writeParquetWithIntelligentSchema(tempPath, records);

      // Backup original and replace
      const backupPath = `${filePath}.backup-utf8`;
      
      // If backup already exists, remove it first (from previous migration attempt)
      if (await fs.pathExists(backupPath)) {
        this.emitProgress({
          type: 'log',
          message: `🗑️ Removing existing backup file: ${backupPath}`,
        });
        await fs.remove(backupPath);
      }
      
      this.emitProgress({
        type: 'log',
        message: `💾 Creating backup: ${filePath} → ${backupPath}`,
      });
      await fs.move(filePath, backupPath);
      
      this.emitProgress({
        type: 'log',
        message: `🔄 Replacing original: ${tempPath} → ${filePath}`,
      });
      await fs.move(tempPath, filePath);
      
    } catch (error) {
      // Clean up temp file if it exists
      const tempPath = `${filePath}.migrating`;
      if (await fs.pathExists(tempPath)) {
        await fs.remove(tempPath);
      }
      throw error; // Re-throw to be caught by the calling function
    }
  }

  private async readParquetFile(filePath: string): Promise<DataRecord[]> {
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

  private async writeParquetWithIntelligentSchema(
    filepath: string,
    records: DataRecord[]
  ): Promise<void> {
    if (records.length === 0) {
      throw new Error('No records to write');
    }

    // Create intelligent schema (reuse logic from ParquetWriter)
    const schema = this.createIntelligentSchema(records);

    // Create Parquet writer
    const writer = await parquet.ParquetWriter.openFile(schema, filepath);

    // Write records with proper type conversion
    for (const record of records) {
      const preparedRecord = this.prepareRecordForParquet(record, schema);
      await writer.appendRow(preparedRecord);
    }

    // Close the writer
    await writer.close();
  }

  // Replicate the intelligent schema logic from ParquetWriter
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private createIntelligentSchema(records: DataRecord[]): any {
    if (!parquet || records.length === 0) {
      throw new Error('Cannot create Parquet schema');
    }

    // Get all unique column names from all records
    const allColumns = new Set<string>();
    records.forEach(record => {
      Object.keys(record).forEach(key => allColumns.add(key));
    });

    const columns = Array.from(allColumns).sort();
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

      // For migration: attempt to parse string values as numbers
      const parsedValues = values.map(v => {
        if (typeof v === 'string') {
          // Try to parse as number
          const parsed = parseFloat(v);
          return !isNaN(parsed) ? parsed : v;
        }
        return v;
      });

      const hasNumbers = parsedValues.some(v => typeof v === 'number');
      const hasStrings = parsedValues.some(v => typeof v === 'string');
      const hasBooleans = parsedValues.some(v => typeof v === 'boolean');

      if (hasNumbers && !hasStrings && !hasBooleans) {
        // All numbers - check if integers or floats
        const allIntegers = parsedValues.every(
          v => typeof v === 'number' && Number.isInteger(v)
        );
        const detectedType = allIntegers ? 'INT64' : 'DOUBLE';
        schemaFields[colName] = {
          type: detectedType,
          optional: true,
        };
        // Debug log for value-related columns
        if (colName === 'value' || colName.startsWith('value_')) {
          this.emitProgress({
            type: 'log',
            message: `🔍 ${colName} detected as ${detectedType} (sample: ${parsedValues.slice(0, 3).join(', ')})`,
          });
        }
      } else if (hasBooleans && !hasNumbers && !hasStrings) {
        schemaFields[colName] = { type: 'BOOLEAN', optional: true };
      } else {
        // Mixed types or strings - use UTF8
        schemaFields[colName] = { type: 'UTF8', optional: true };
        // Debug log for value-related columns that stay as string
        if (colName === 'value' || colName.startsWith('value_')) {
          this.emitProgress({
            type: 'log',
            message: `⚠️  ${colName} staying as UTF8 - hasNumbers:${hasNumbers}, hasStrings:${hasStrings}, hasBooleans:${hasBooleans} (sample: ${values.slice(0, 3).join(', ')})`,
          });
        }
      }
    });

    return new parquet.ParquetSchema(schemaFields);
  }

  // Replicate the record preparation logic from ParquetWriter
  private prepareRecordForParquet(
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
}
