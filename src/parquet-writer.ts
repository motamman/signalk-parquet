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


      // Extract path from records for intelligent schema detection
      const currentPath = records.length > 0 ? records[0].path : undefined;

      // Extract output directory from filepath (go up to find the base data directory)
      const outputDirectory = this.extractOutputDirectory(filepath);

      // Extract filename prefix from filepath (everything before the date part)
      const filename = path.basename(filepath, '.parquet');
      const match = filename.match(/^(.+)_\d{4}-\d{2}-\d{2}/);
      const filenamePrefix = match ? match[1] : 'signalk_data';

      // Check if parquet library is available
      if (!parquet) {
        throw new Error('ParquetJS not available');
      }

      // Use intelligent schema detection for optimal data types
      const schema = this.createParquetSchema(records, currentPath, outputDirectory, filenamePrefix);

      // Create Parquet writer
      const writer = await parquet.ParquetWriter.openFile(schema, filepath);

      // Write records to Parquet file
      for (let i = 0; i < records.length; i++) {
        const record = records[i];
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const cleanRecord: { [key: string]: any } = {};

        // Prepare record for typed Parquet schema
        const preparedRecord = this.prepareRecordForParquet(record, schema);
        Object.assign(cleanRecord, preparedRecord);

        await writer.appendRow(cleanRecord);
      }

      // Close the writer
      await writer.close();

      // Validate the written file
      const isValid = await this.validateParquetFile(filepath);
      if (!isValid) {
        // Move invalid file to quarantine and log
        const quarantineDir = path.join(path.dirname(filepath), 'quarantine');
        await fs.ensureDir(quarantineDir);
        const quarantineFile = path.join(quarantineDir, path.basename(filepath));
        await fs.move(filepath, quarantineFile);
        
        await this.logQuarantine(quarantineFile, 'write', 'File failed validation after write');
        
        throw new Error(
          `Parquet file failed validation after write, moved to quarantine: ${quarantineFile}`
        );
      }

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
  createParquetSchema(records: DataRecord[], currentPath?: string, outputDirectory?: string, filenamePrefix?: string): any {

    if (!parquet || records.length === 0) {
      this.app?.debug(
        'createParquetSchema: No parquet lib or empty records, throwing error'
      );
      throw new Error('Cannot create Parquet schema');
    }

    this.app?.debug(`🔍 Schema Detection: Starting for ${records.length} records`);
    this.app?.debug(`📍 Current Path: ${currentPath || 'unknown'}`);
    this.app?.debug(`📁 Output Directory: ${outputDirectory || 'unknown'}`);

    // Cache for metadata lookups to avoid repeated API calls
    const metadataCache = new Map<string, any>();

    // Get all unique column names from all records
    const allColumns = new Set<string>();
    records.forEach(record => {
      Object.keys(record).forEach(key => allColumns.add(key));
    });

    const columns = Array.from(allColumns).sort();
    this.app?.debug(`📋 Columns found: [${columns.join(', ')}]`);

    const schemaFields: { [key: string]: ParquetField } = {};

    // Analyze each column to determine the best Parquet type
    for (const colName of columns) {
      this.app?.debug(`🔎 Analyzing column: ${colName}`);
      // Guideline 1 & 2: Force timestamps, JSON, metadata, and source columns to UTF8
      if (colName === 'received_timestamp' || colName === 'signalk_timestamp' || colName === 'value_json' || colName === 'meta' || colName.startsWith('source') || colName === 'context' || colName === 'path') {
        this.app?.debug(`  ⏰ ${colName}: Forced to UTF8 (timestamp/json/meta/source/context/path rule)`);
        schemaFields[colName] = { type: 'UTF8', optional: true };
        continue;
      }

      const values = records
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .map(r => (r as any)[colName])
        .filter(v => v !== null && v !== undefined);

      this.app?.debug(`  📊 ${colName}: ${values.length}/${records.length} non-null values`);

      if (values.length === 0) {
        // Guideline 3: Handle empty values with intelligent fallback
        this.app?.debug(`  🔍 ${colName}: All null values, using intelligent fallback`);
        const fallbackType = this.getTypeForEmptyColumn(colName, currentPath, outputDirectory, metadataCache, filenamePrefix);
        this.app?.debug(`  ✅ ${colName}: ${fallbackType} (from intelligent fallback)`);
        schemaFields[colName] = { type: fallbackType, optional: true };
        continue;
      }

      // Guideline 4: Special handling for value_ fields (exploded values)
      if (colName.startsWith('value_')) {
        this.app?.debug(`  🧩 ${colName}: Exploded value field, analyzing across files`);
        const explodedType = this.getTypeForExplodedField(colName, currentPath, outputDirectory, filenamePrefix);
        this.app?.debug(`  ✅ ${colName}: ${explodedType} (from exploded field analysis)`);
        schemaFields[colName] = { type: explodedType, optional: true };
        continue;
      }

      const hasNumbers = values.some(v => typeof v === 'number');
      const hasStrings = values.some(v => typeof v === 'string');
      const hasBooleans = values.some(v => typeof v === 'boolean');
      const hasBigInts = values.some(v => typeof v === 'bigint');

      this.app?.debug(`  🧮 ${colName}: Types present - numbers:${hasNumbers}, strings:${hasStrings}, booleans:${hasBooleans}, bigints:${hasBigInts}`);

      // Guideline 5: Never use BIGINT, convert to UTF8
      if (hasBigInts && !hasNumbers && !hasStrings && !hasBooleans) {
        this.app?.debug(`  ✅ ${colName}: UTF8 (BigInt conversion)`);
        schemaFields[colName] = { type: 'UTF8', optional: true };
      } else if (hasNumbers && !hasStrings && !hasBooleans && !hasBigInts) {
        // All numbers - check if integers or floats
        const allIntegers = values.every(v => Number.isInteger(v));
        const finalType = allIntegers ? 'INT64' : 'DOUBLE';
        this.app?.debug(`  ✅ ${colName}: ${finalType} (all numbers, integers:${allIntegers})`);
        schemaFields[colName] = {
          type: finalType,
          optional: true,
        };
      } else if (hasBooleans && !hasNumbers && !hasStrings && !hasBigInts) {
        this.app?.debug(`  ✅ ${colName}: BOOLEAN (all booleans)`);
        schemaFields[colName] = { type: 'BOOLEAN', optional: true };
      } else {
        // Mixed types or strings - use UTF8
        this.app?.debug(`  ✅ ${colName}: UTF8 (mixed types or strings)`);
        schemaFields[colName] = { type: 'UTF8', optional: true };
      }
    }

    const finalSchema = new parquet.ParquetSchema(schemaFields);
    this.app?.debug(`🎯 Schema Detection: Complete. Final schema has ${Object.keys(schemaFields).length} fields`);
    this.app?.debug(`📋 Final schema object: ${finalSchema ? 'SUCCESS' : 'FAILED'}`);

    return finalSchema;
  }

  // Guideline 3: Get type for empty columns using SignalK metadata and other files
  private getTypeForEmptyColumn(
    colName: string,
    currentPath?: string,
    outputDirectory?: string,
    metadataCache?: Map<string, any>,
    filenamePrefix?: string
  ): string {
    this.app?.debug(`    🔍 Empty column fallback for: ${colName} (path: ${currentPath || 'unknown'})`);

    // For non-value columns, default to UTF8
    if (colName !== 'value') {
      this.app?.debug(`    ↪️ Non-value column, defaulting to UTF8`);
      return 'UTF8';
    }

    // Try SignalK metadata first
    if (currentPath && this.app && metadataCache) {
      this.app?.debug(`    🔎 Checking SignalK metadata for path: ${currentPath}`);

      if (!metadataCache.has(currentPath)) {
        try {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const metadata = (this.app as any).getMetadata?.(currentPath);
          metadataCache.set(currentPath, metadata);
          this.app?.debug(`    📡 Retrieved metadata: ${metadata ? JSON.stringify(metadata) : 'null'}`);
        } catch (error) {
          this.app?.debug(`    ❌ Metadata API call failed: ${(error as Error).message}`);
          metadataCache.set(currentPath, null);
        }
      } else {
        this.app?.debug(`    💾 Using cached metadata for ${currentPath}`);
      }

      const metadata = metadataCache.get(currentPath);
      if (metadata && metadata.units) {
        // If metadata suggests numeric units (m/s, degrees, etc.), assume numeric
        const numericUnits = ['m/s', 'm', 'deg', 'rad', 'Pa', 'K', 'Hz', 'V', 'A', 'W'];
        const matchedUnit = numericUnits.find(unit => metadata.units.includes(unit));
        if (matchedUnit) {
          this.app?.debug(`    ✅ Metadata indicates numeric unit '${matchedUnit}', using DOUBLE`);
          return 'DOUBLE';
        } else {
          this.app?.debug(`    ↪️ Metadata has units '${metadata.units}' but not recognized as numeric`);
        }
      } else {
        this.app?.debug(`    ↪️ No useful metadata found (metadata: ${!!metadata}, units: ${metadata?.units})`);
      }
    }

    // Fallback to other consolidated files for the same path
    // Disabled to prevent errors from corrupted parquet files
    // if (currentPath && outputDirectory) {
    //   this.app?.debug(`    🔎 Searching other files for path: ${currentPath}`);
    //   const typeFromOtherFiles = this.getTypeFromOtherFiles(currentPath, outputDirectory, undefined, filenamePrefix);
    //   if (typeFromOtherFiles) {
    //     this.app?.debug(`    ✅ Found type ${typeFromOtherFiles} from other files`);
    //     return typeFromOtherFiles;
    //   } else {
    //     this.app?.debug(`    ↪️ No type information found in other files`);
    //   }
    // }

    // Final fallback to UTF8
    this.app?.debug(`    ✅ Final fallback to UTF8`);
    return 'UTF8';
  }

  // Guideline 4: Get type for exploded value_ fields by examining other files
  private getTypeForExplodedField(colName: string, currentPath?: string, outputDirectory?: string, filenamePrefix?: string): string {
    this.app?.debug(`    🧩 Exploded field analysis for: ${colName} (path: ${currentPath || 'unknown'})`);

    if (!currentPath || !outputDirectory) {
      this.app?.debug(`    ↪️ Missing path/directory info, using field name inference`);
      return this.inferTypeFromFieldName(colName);
    }

    // Disabled cross-file analysis to prevent parquet read errors
    // const typeFromOtherFiles = this.getTypeFromOtherFiles(currentPath, outputDirectory, colName, filenamePrefix);
    // if (typeFromOtherFiles) {
    //   this.app?.debug(`    ✅ Found type ${typeFromOtherFiles} from other files for ${colName}`);
    //   return typeFromOtherFiles;
    // }

    // Fallback to field name inference
    this.app?.debug(`    ↪️ No type found in other files, using field name inference`);
    return this.inferTypeFromFieldName(colName);
  }

  // Helper: Search other consolidated files for type information
  private getTypeFromOtherFiles(currentPath: string, outputDirectory: string, specificColumn?: string, filenamePrefix?: string): string | null {
    const targetColumn = specificColumn || 'value';
    this.app?.debug(`      🔍 Searching files for column '${targetColumn}' in path '${currentPath}'`);

    try {
      const glob = require('glob');
      const prefix = filenamePrefix || 'signalk_data';
      const pathPattern = path.join(outputDirectory, 'vessels', '*', currentPath.replace(/\./g, '/'), `${prefix}_*.parquet`);
      this.app?.debug(`      📁 Search pattern: ${pathPattern}`);

      const allFiles = glob.sync(pathPattern);
      // Filter out consolidated files
      const files = allFiles.filter((file: string) => !file.includes('_consolidated.parquet'));
      this.app?.debug(`      📄 Found ${files.length} regular files to check (excluding consolidated)`);

      for (const filePath of files) {
        try {
          this.app?.debug(`      🔎 Checking file: ${path.basename(filePath)}`);

          if (!parquet) {
            this.app?.debug(`      ❌ Parquet library not available`);
            continue;
          }

          // Skip corrupted parquet files to prevent crashes
          if (path.basename(filePath).includes('corrupted') || path.basename(filePath).includes('quarantine')) {
            this.app?.debug(`      ⚠️ Skipping quarantined file: ${path.basename(filePath)}`);
            continue;
          }

          try {
            const reader = parquet.ParquetReader.openFile(filePath);
            const schema = reader.schema;

            if (schema && schema.schema && schema.schema[targetColumn]) {
              const columnType = schema.schema[targetColumn].type;
              this.app?.debug(`      ✅ Found type ${columnType} for column '${targetColumn}' in ${path.basename(filePath)}`);
              if (typeof reader.close === 'function') reader.close();
              return columnType;
            } else {
              this.app?.debug(`      ↪️ Column '${targetColumn}' not found in ${path.basename(filePath)}`);
            }
            if (typeof reader.close === 'function') reader.close();
          } catch (fileError) {
            this.app?.debug(`      ⚠️ Corrupted file, skipping: ${path.basename(filePath)} - ${(fileError as Error).message}`);
            continue;
          }
        } catch (error) {
          this.app?.debug(`      ❌ Error reading file ${path.basename(filePath)}: ${(error as Error).message}`);
          continue;
        }
      }
    } catch (error) {
      this.app?.debug(`      ❌ File search error: ${(error as Error).message}`);
    }

    this.app?.debug(`      ❌ No type information found in any files`);
    return null;
  }

  // Helper: Infer type from field name patterns
  private inferTypeFromFieldName(fieldName: string): string {
    this.app?.debug(`      🏷️ Inferring type from field name: ${fieldName}`);
    const field = fieldName.toLowerCase();

    // Coordinate fields
    if (field.includes('latitude') || field.includes('longitude') ||
        field.includes('lat') || field.includes('lon')) {
      this.app?.debug(`      ✅ Coordinate field detected, using DOUBLE`);
      return 'DOUBLE';
    }

    // Numeric measurements
    if (field.includes('speed') || field.includes('distance') || field.includes('depth') ||
        field.includes('temperature') || field.includes('pressure') || field.includes('angle') ||
        field.includes('bearing') || field.includes('course') || field.includes('heading')) {
      this.app?.debug(`      ✅ Numeric measurement field detected, using DOUBLE`);
      return 'DOUBLE';
    }

    // Time/duration fields
    if (field.includes('time') || field.includes('duration') || field.includes('age')) {
      const isTimestamp = field.includes('timestamp');
      const resultType = isTimestamp ? 'UTF8' : 'DOUBLE';
      this.app?.debug(`      ✅ Time field detected, using ${resultType} (timestamp: ${isTimestamp})`);
      return resultType;
    }

    // Default to UTF8 for unknown patterns
    this.app?.debug(`      ✅ Unknown pattern, defaulting to UTF8`);
    return 'UTF8';
  }

  // Helper: Extract output directory from filepath
  private extractOutputDirectory(filepath: string): string {
    // filepath format: /path/to/outputDir/vessels/context/path/filename.parquet
    // We want to extract up to the outputDir part
    const parts = filepath.split(path.sep);
    const vesselIndex = parts.findIndex(part => part === 'vessels');

    if (vesselIndex > 0) {
      // Return everything up to but not including 'vessels'
      return parts.slice(0, vesselIndex).join(path.sep);
    }

    // Fallback: assume current directory structure
    return path.dirname(path.dirname(path.dirname(filepath)));
  }

  // Prepare a record for typed Parquet writing
  prepareRecordForParquet(
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
      } else if (typeof value === 'bigint') {
        // Handle BigInt values by converting to appropriate type
        switch (fieldType) {
          case 'DOUBLE':
          case 'FLOAT':
            cleanRecord[fieldName] = Number(value);
            break;
          case 'INT64':
          case 'INT32':
            // Convert BigInt to number if it fits in safe integer range
            if (
              value <= Number.MAX_SAFE_INTEGER &&
              value >= Number.MIN_SAFE_INTEGER
            ) {
              cleanRecord[fieldName] = Number(value);
            } else {
              cleanRecord[fieldName] = value.toString();
            }
            break;
          case 'UTF8':
          default:
            cleanRecord[fieldName] = value.toString();
            break;
        }
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
          return String(timeA).localeCompare(String(timeB));
        });

        await this.writeRecords(targetFile, allRecords);
        return allRecords.length;
      }

      return 0;
    } catch (error) {
      throw new Error(`Failed to merge files: ${(error as Error).message}`);
    }
  }

  // Validate parquet file for corruption
  private async validateParquetFile(filepath: string): Promise<boolean> {
    try {
      if (!parquet || !(await fs.pathExists(filepath))) {
        return false;
      }

      // Check file size (must be > 100 bytes as per existing logic)
      const stats = await fs.stat(filepath);
      const fileSize = stats.size;
      
      if (fileSize < 100) {
        this.app?.debug(`❌ Parquet file too small: ${filepath} (${fileSize} bytes)`);
        return false;
      }

      // Try to open and read the parquet file
      try {
        const reader = await parquet.ParquetReader.openFile(filepath);
        const cursor = reader.getCursor();
        
        // Try to read first record to verify file structure
        const firstRecord = await cursor.next();
        await reader.close();
        
        // Log file size for debugging (matches your stat command format)
        this.app?.debug(`✅ Valid parquet file: ${fileSize.toString().padStart(12, ' ')}  ${filepath}`);
        
        return firstRecord !== null;
      } catch (readError) {
        this.app?.debug(`❌ Parquet file read failed: ${filepath} - ${(readError as Error).message}`);
        return false;
      }
    } catch (error) {
      this.app?.debug(`❌ Parquet validation error: ${filepath} - ${(error as Error).message}`);
      return false;
    }
  }

  // Log quarantined files
  private async logQuarantine(filepath: string, operation: string, reason: string): Promise<void> {
    try {
      const stats = await fs.stat(filepath);
      const logEntry = {
        timestamp: new Date().toISOString(),
        filepath,
        fileSize: stats.size,
        operation,
        reason,
        formattedSize: `${stats.size.toString().padStart(12, ' ')}  ${filepath}`
      };

      const quarantineDir = path.dirname(filepath);
      const logFile = path.join(quarantineDir, 'quarantine.log');
      
      // Append to log file
      const logLine = `${logEntry.timestamp} | ${logEntry.operation} | ${logEntry.fileSize} bytes | ${logEntry.reason} | ${filepath}\n`;
      await fs.appendFile(logFile, logLine);
      
      this.app?.debug(`📋 Quarantine logged: ${logEntry.formattedSize}`);
    } catch (error) {
      this.app?.debug(`Failed to log quarantine entry: ${(error as Error).message}`);
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

          if (stat.isDirectory() && item !== 'processed' && item !== 'claude-schemas' && item !== 'quarantine' && item !== 'failed') {
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

        // Validate consolidated parquet file
        const isValid = await this.validateParquetFile(entry.target);
        if (!isValid) {
          // Move corrupt file to quarantine
          const quarantineDir = path.join(path.dirname(entry.target), 'quarantine');
          await fs.ensureDir(quarantineDir);
          const quarantineFile = path.join(quarantineDir, path.basename(entry.target));
          await fs.move(entry.target, quarantineFile);
          
          // Log to quarantine log
          await this.logQuarantine(quarantineFile, 'consolidation', 'File failed validation after consolidation');
          
          this.app?.debug(`⚠️ Moved corrupt file to quarantine: ${quarantineFile}`);
          continue; // Skip moving source files since consolidation failed
        }

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
