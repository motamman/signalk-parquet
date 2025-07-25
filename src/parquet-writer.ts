import * as fs from 'fs-extra';
import * as path from 'path';
import {
  DataRecord,
  ParquetWriterOptions,
  ParquetField,
  FileFormat,
} from './types';
import { ServerAPI } from '@signalk/server-api';

// Try to import ParquetJS, fall back if not available
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let parquet: any;
try {
  parquet = require('@dsnp/parquetjs');
} catch (error) {
  parquet = null;
}

export class ParquetWriter {
  private format: FileFormat;
  private app?: ServerAPI;

  constructor(options: ParquetWriterOptions = { format: 'json' }) {
    this.format = options.format || 'json';
    this.app = options.app;
  }

  async writeRecords(filepath: string, records: DataRecord[]): Promise<string> {
    try {
      await fs.ensureDir(path.dirname(filepath));

      switch (this.format) {
        case 'json':
          return await this.writeJSON(filepath, records);
        case 'csv':
          return await this.writeCSV(filepath, records);
        case 'parquet':
          return await this.writeParquet(filepath, records);
        default:
          throw new Error(`Unsupported format: ${this.format}`);
      }
    } catch (error) {
      throw new Error(`Failed to write records: ${(error as Error).message}`);
    }
  }

  async writeJSON(filepath: string, records: DataRecord[]): Promise<string> {
    const jsonPath = filepath.replace(/\.(parquet|csv)$/, '.json');
    await fs.writeJson(jsonPath, records, { spaces: 2 });
    return jsonPath;
  }

  async writeCSV(filepath: string, records: DataRecord[]): Promise<string> {
    if (records.length === 0) return filepath;

    const csvPath = filepath.replace(/\.(parquet|json)$/, '.csv');

    // Get all unique keys from all records
    const allKeys = new Set<string>();
    records.forEach(record => {
      Object.keys(record).forEach(key => allKeys.add(key));
    });

    const headers = Array.from(allKeys).sort();
    const csvRows = [headers.join(',')];

    records.forEach(record => {
      const row = headers.map(header => {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const value = (record as any)[header];
        if (value === null || value === undefined) return '';
        if (
          typeof value === 'string' &&
          (value.includes(',') || value.includes('"'))
        ) {
          return `"${value.replace(/"/g, '""')}"`;
        }
        return String(value);
      });
      csvRows.push(row.join(','));
    });

    await fs.writeFile(csvPath, csvRows.join('\n'));
    return csvPath;
  }

  async writeParquet(filepath: string, records: DataRecord[]): Promise<string> {
    try {
      if (records.length === 0) {
        this.app?.debug('No records to write to Parquet file');
        return filepath;
      }

      // Check if ParquetJS is available
      if (!parquet) {
        this.app?.debug('ParquetJS not available, falling back to JSON');
        return await this.writeJSON(filepath, records);
      }

      this.app?.debug(
        `Attempting to write ${records.length} records to Parquet`
      );
      this.app?.debug(
        `Sample record keys: ${Object.keys(records[0]).join(', ')}`
      );
      this.app?.debug(`Sample record: ${JSON.stringify(records[0], null, 2)}`);

      // Use intelligent schema detection for optimal data types
      const schema = this.createParquetSchema(records);
      this.app?.debug(
        `Creating Parquet schema with ${Object.keys(schema.schema).length} fields: ${Object.keys(schema.schema).join(', ')}`
      );

      // Create Parquet writer
      const writer = await parquet.ParquetWriter.openFile(schema, filepath);
      this.app?.debug('Parquet writer created successfully');

      // Write records to Parquet file
      for (let i = 0; i < records.length; i++) {
        const record = records[i];
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const cleanRecord: { [key: string]: any } = {};

        // Prepare record for typed Parquet schema
        const preparedRecord = this.prepareRecordForParquet(record, schema);
        Object.assign(cleanRecord, preparedRecord);

        this.app?.debug(`Writing record ${i + 1}/${records.length}`);
        await writer.appendRow(cleanRecord);
      }

      // Close the writer
      this.app?.debug('Closing Parquet writer...');
      await writer.close();

      // Validate the written file size
      const stats = await fs.stat(filepath);
      this.app?.debug(`Parquet file size: ${stats.size} bytes`);

      if (stats.size < 100) {
        throw new Error(
          `Parquet file too small (${stats.size} bytes) - likely empty or corrupted`
        );
      }

      this.app?.debug(
        `✅ Successfully wrote ${records.length} records to Parquet: ${filepath} (${stats.size} bytes)`
      );
      return filepath;
    } catch (error) {
      this.app?.debug(`❌ Parquet writing failed: ${(error as Error).message}`);
      this.app?.debug(`Error stack: ${(error as Error).stack}`);

      // Save to failed directory to maintain schema consistency
      const failedDir = path.join(path.dirname(filepath), 'failed');
      await fs.ensureDir(failedDir);
      const failedPath = path.join(
        failedDir,
        path.basename(filepath).replace('.parquet', '_FAILED.json')
      );

      this.app?.debug(
        `💾 Saving failed Parquet data as JSON to: ${failedPath}`
      );
      this.app?.debug(
        '⚠️  This data will need manual conversion to maintain DuckDB schema consistency'
      );

      await this.writeJSON(failedPath, records);

      // Throw error to alert system that Parquet writing is broken
      throw new Error(
        `Parquet writing failed for ${filepath}. Data saved to ${failedPath} for recovery.`
      );
    }
  }

  // Create Parquet schema based on sample records
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  createParquetSchema(records: DataRecord[]): any {
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

      const hasNumbers = values.some(v => typeof v === 'number');
      const hasStrings = values.some(v => typeof v === 'string');
      const hasBooleans = values.some(v => typeof v === 'boolean');

      if (hasNumbers && !hasStrings && !hasBooleans) {
        // All numbers - check if integers or floats
        const allIntegers = values.every(v => Number.isInteger(v));
        schemaFields[colName] = {
          type: allIntegers ? 'INT64' : 'DOUBLE',
          optional: true,
        };
      } else if (hasBooleans && !hasNumbers && !hasStrings) {
        schemaFields[colName] = { type: 'BOOLEAN', optional: true };
      } else {
        // Mixed types or strings - use UTF8
        schemaFields[colName] = { type: 'UTF8', optional: true };
      }
    });

    return new parquet.ParquetSchema(schemaFields);
  }

  // Prepare a record for typed Parquet writing
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  prepareRecordForParquet(
    record: DataRecord,
    schema: any
  ): { [key: string]: any } {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const cleanRecord: { [key: string]: any } = {};
    const schemaFields = schema.schema;

    Object.keys(schemaFields).forEach(fieldName => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const value = (record as any)[fieldName];
      const fieldType = schemaFields[fieldName].primitiveType;

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

  // Merge multiple files (for daily consolidation like Python version)
  async mergeFiles(sourceFiles: string[], targetFile: string): Promise<number> {
    try {
      const allRecords: DataRecord[] = [];

      for (const sourceFile of sourceFiles) {
        if (await fs.pathExists(sourceFile)) {
          const ext = path.extname(sourceFile).toLowerCase();

          if (ext === '.json') {
            const records = await fs.readJson(sourceFile);
            allRecords.push(...(Array.isArray(records) ? records : [records]));
          } else if (ext === '.parquet') {
            // Read Parquet file
            if (parquet) {
              try {
                const reader = await parquet.ParquetReader.openFile(sourceFile);
                const cursor = reader.getCursor();
                let record: DataRecord | null = null;
                while ((record = await cursor.next())) {
                  allRecords.push(record);
                }
                await reader.close();
              } catch (parquetError) {
                this.app?.debug(
                  `Failed to read Parquet file ${sourceFile}: ${(parquetError as Error).message}`
                );
              }
            }
          } else if (ext === '.csv') {
            // Could implement CSV reading if needed
            this.app?.debug(`CSV merging not implemented for ${sourceFile}`);
          }
        }
      }

      if (allRecords.length > 0) {
        // Sort by timestamp
        allRecords.sort((a, b) => {
          const timeA = a.received_timestamp || a.signalk_timestamp || '';
          const timeB = b.received_timestamp || b.signalk_timestamp || '';
          return timeA.localeCompare(timeB);
        });

        await this.writeRecords(targetFile, allRecords);
        return allRecords.length;
      }

      return 0;
    } catch (error) {
      throw new Error(`Failed to merge files: ${(error as Error).message}`);
    }
  }

  // Daily file consolidation (matching Python behavior)
  async consolidateDaily(
    dataDir: string,
    date: Date,
    filenamePrefix: string = 'signalk_data'
  ): Promise<number> {
    try {
      const dateStr = date.toISOString().split('T')[0]; // YYYY-MM-DD
      const consolidatedFiles: Array<{ target: string; sources: string[] }> =
        [];

      // Walk through all topic directories
      const walkDir = async (dir: string): Promise<void> => {
        const items = await fs.readdir(dir);

        for (const item of items) {
          const itemPath = path.join(dir, item);
          const stat = await fs.stat(itemPath);

          if (stat.isDirectory()) {
            await walkDir(itemPath);
          } else if (
            item.includes(dateStr) &&
            !item.includes('_consolidated')
          ) {
            // This is a file for our target date
            const topicDir = path.dirname(itemPath);
            const consolidatedFile = path.join(
              topicDir,
              `${filenamePrefix}_${dateStr}_consolidated.parquet`
            );

            if (!consolidatedFiles.find(f => f.target === consolidatedFile)) {
              consolidatedFiles.push({
                target: consolidatedFile,
                sources: [],
              });
            }

            const entry = consolidatedFiles.find(
              f => f.target === consolidatedFile
            );
            if (entry) {
              entry.sources.push(itemPath);
            }
          }
        }
      };

      await walkDir(dataDir);

      // Consolidate each topic's files
      for (const entry of consolidatedFiles) {
        const recordCount = await this.mergeFiles(entry.sources, entry.target);
        this.app?.debug(
          `Consolidated ${entry.sources.length} files into ${entry.target} (${recordCount} records)`
        );

        // Move source files to processed folder
        const processedDir = path.join(path.dirname(entry.target), 'processed');
        await fs.ensureDir(processedDir);

        for (const sourceFile of entry.sources) {
          const basename = path.basename(sourceFile);
          const processedFile = path.join(processedDir, basename);
          await fs.move(sourceFile, processedFile);
        }
      }

      return consolidatedFiles.length;
    } catch (error) {
      throw new Error(
        `Failed to consolidate daily files: ${(error as Error).message}`
      );
    }
  }
}
