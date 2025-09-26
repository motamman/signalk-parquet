import * as fs from 'fs-extra';
import * as path from 'path';
import { ServerAPI } from '@signalk/server-api';
import { DataRecord } from './types';

// Import parquet dynamically
let parquet: any;
try {
  parquet = require('@dsnp/parquetjs');
} catch (error) {
  console.warn('ParquetJS not available, some features will be disabled');
}

interface ParquetField {
  type: string;
  optional: boolean;
}

export interface SchemaDetectionResult {
  schema: any;
  isExplodedFile: boolean;
  fieldCount: number;
}

export interface ValidationResult {
  isValid: boolean;
  violations: string[];
  isExplodedFile: boolean;
  hasSchema: boolean;
}

export interface RepairResult {
  needsRepair: boolean;
  violations: string[];
  repairedFilePath?: string;
  backupFilePath?: string;
}

/**
 * Centralized schema detection, validation, and repair service
 * Consolidates logic from parquet-writer.ts and api-routes.ts
 */
export class SchemaService {
  private app: ServerAPI;

  constructor(app: ServerAPI) {
    this.app = app;
  }

  /**
   * CORE SCHEMA DETECTION LOGIC
   * Extracted and consolidated from createParquetSchema() in parquet-writer.ts
   */
  async detectOptimalSchema(records: DataRecord[], currentPath?: string): Promise<SchemaDetectionResult> {
    if (!parquet || records.length === 0) {
      this.app?.debug('SchemaService: No parquet lib or empty records, throwing error');
      throw new Error('Cannot create Parquet schema');
    }

    this.app?.debug(`🔍 Schema Detection: Starting for ${records.length} records`);
    this.app?.debug(`📍 Current Path: ${currentPath || 'unknown'}`);

    // Find all unique column names
    const allColumns = new Set<string>();
    records.forEach(record => {
      Object.keys(record).forEach(key => allColumns.add(key));
    });

    const columns = Array.from(allColumns).sort();
    this.app?.debug(`📋 Columns found: [${columns.join(', ')}]`);

    const schemaFields: { [key: string]: ParquetField } = {};

    // Determine if this is an exploded file
    const hasExplodedFields = columns.some(colName =>
      colName.startsWith('value_') && colName !== 'value' && colName !== 'value_json'
    );
    const isExplodedFile = hasExplodedFields;
    this.app?.debug(`🔍 Schema Detection: isExplodedFile = ${isExplodedFile}`);

    // Process each column
    for (const colName of columns) {
      this.app?.debug(`🔎 Analyzing column: ${colName}`);

      // Always skip value_json
      if (colName === 'value_json') {
        this.app?.debug(`  ⏭️ ${colName}: Skipped entirely (always ignored)`);
        continue;
      }

      // Skip value field in exploded files
      if (isExplodedFile && colName === 'value') {
        this.app?.debug(`  ⏭️ ${colName}: Skipped in exploded file (always empty)`);
        continue;
      }

      // Force timestamps, metadata, and source columns to UTF8
      if (colName === 'received_timestamp' || colName === 'signalk_timestamp' ||
          colName === 'meta' || colName.startsWith('source') ||
          colName === 'context' || colName === 'path') {
        this.app?.debug(`  ⏰ ${colName}: Forced to UTF8 (timestamp/meta/source/context/path rule)`);
        schemaFields[colName] = { type: 'UTF8', optional: true };
        continue;
      }

      // Extract values for this column
      const values = records
        .map(r => (r as any)[colName])
        .filter(v => v !== null && v !== undefined);

      this.app?.debug(`  📊 ${colName}: ${values.length}/${records.length} non-null values`);

      // Handle BIGINT fields (BIGINT -> DOUBLE)
      const hasBigInts = values.some(v => typeof v === 'bigint');
      if (hasBigInts) {
        this.app?.debug(`  ✅ ${colName}: DOUBLE (BIGINT converted to DOUBLE)`);
        schemaFields[colName] = { type: 'DOUBLE', optional: true };
        continue;
      }

      // STEP 1: LOOK AT THE STRING AND SEE WHAT IT IS
      let typeDetected = false;
      let schemaType = 'UTF8'; // default

      if (values.length > 0) {
        let allNumeric = true;
        let allBoolean = true;

        for (const value of values) {
          const str = String(value).trim();
          if (str === 'true' || str === 'false') {
            allNumeric = false;
          } else if (!isNaN(Number(str)) && str !== '') {
            allBoolean = false;
          } else {
            allNumeric = false;
            allBoolean = false;
            break;
          }
        }

        if (allNumeric && values.length > 0) {
          schemaType = 'DOUBLE';
          typeDetected = true;
          this.app?.debug(`  ✅ ${colName}: DOUBLE (contains numbers)`);
        } else if (allBoolean && values.length > 0) {
          schemaType = 'BOOLEAN';
          typeDetected = true;
          this.app?.debug(`  ✅ ${colName}: BOOLEAN (contains booleans)`);
        } else if (values.length > 0) {
          schemaType = 'UTF8';
          typeDetected = true;
          this.app?.debug(`  ✅ ${colName}: UTF8 (contains strings)`);
        }
      }

      // STEP 2: LOOK AT METADATA (SKIP IF EXPLODED) - only if step 1 can't determine
      if (!typeDetected) {
        const isExplodedField = colName.startsWith('value_');

        if (!isExplodedField && currentPath) {
          this.app?.debug(`  🔍 ${colName}: Using metadata fallback`);
          try {
            const response = await fetch(`http://localhost:3000/signalk/v1/api/vessels/self/${currentPath.replace(/\./g, '/')}/meta`);
            if (response.ok) {
              const metadata = await response.json() as any;
              if (metadata && metadata.units &&
                  (metadata.units === 'm' || metadata.units === 'deg' || metadata.units === 'm/s' ||
                   metadata.units === 'rad' || metadata.units === 'K' || metadata.units === 'Pa' ||
                   metadata.units === 'V' || metadata.units === 'A' || metadata.units === 'Hz' ||
                   metadata.units === 'ratio' || metadata.units === 'kg' || metadata.units === 'J')) {
                schemaType = 'DOUBLE';
                this.app?.debug(`  ✅ ${colName}: DOUBLE (from metadata units: ${metadata.units})`);
              } else {
                schemaType = 'UTF8';
                this.app?.debug(`  ✅ ${colName}: UTF8 (metadata has no numeric units)`);
              }
            } else {
              schemaType = 'UTF8';
              this.app?.debug(`  ✅ ${colName}: UTF8 (metadata request failed)`);
            }
          } catch (metadataError) {
            schemaType = 'UTF8';
            this.app?.debug(`  ✅ ${colName}: UTF8 (metadata error)`);
          }
        } else {
          schemaType = 'UTF8';
          this.app?.debug(`  ✅ ${colName}: UTF8 (exploded field or no path)`);
        }
      }

      schemaFields[colName] = { type: schemaType, optional: true };
    }

    const finalSchema = new parquet.ParquetSchema(schemaFields);
    this.app?.debug(`🎯 Schema Detection: Complete. Final schema has ${Object.keys(schemaFields).length} fields`);

    return {
      schema: finalSchema,
      isExplodedFile,
      fieldCount: Object.keys(schemaFields).length
    };
  }

  /**
   * SCHEMA VALIDATION LOGIC
   * Extracted and consolidated from validation logic in api-routes.ts
   */
  async validateFileSchema(filePath: string): Promise<ValidationResult> {
    try {
      if (!parquet) {
        throw new Error('ParquetJS not available');
      }

      const reader = await parquet.ParquetReader.openFile(filePath);
      const cursor = reader.getCursor();
      const schema = cursor.schema;

      if (!schema || !schema.schema) {
        if (typeof reader.close === 'function') reader.close();
        return {
          isValid: false,
          violations: ['No schema found'],
          isExplodedFile: false,
          hasSchema: false
        };
      }

      const fields = schema.schema;
      const violations: string[] = [];

      // Check timestamps
      const receivedTimestamp = fields.received_timestamp ? fields.received_timestamp.type : 'MISSING';
      const signalkTimestamp = fields.signalk_timestamp ? fields.signalk_timestamp.type : 'MISSING';

      // Rule 1: Timestamps should be UTF8/VARCHAR
      if (receivedTimestamp !== 'UTF8' && receivedTimestamp !== 'MISSING') {
        violations.push(`received_timestamp should be UTF8, got ${receivedTimestamp}`);
      }
      if (signalkTimestamp !== 'UTF8' && signalkTimestamp !== 'MISSING') {
        violations.push(`signalk_timestamp should be UTF8, got ${signalkTimestamp}`);
      }

      // Find all value fields
      const valueFields: { [key: string]: string } = {};
      Object.keys(fields).forEach(fieldName => {
        if (fieldName.startsWith('value_') || fieldName === 'value') {
          valueFields[fieldName] = fields[fieldName].type;
        }
      });

      // Determine if this is an exploded file
      const isExplodedFile = Object.keys(valueFields).some(fieldName =>
        fieldName.startsWith('value_') && fieldName !== 'value' && fieldName !== 'value_json'
      );

      // Extract SignalK path for metadata lookup
      const relativePath = path.relative(path.dirname(path.dirname(filePath)), filePath);
      const pathMatch = relativePath.match(/vessels\/[^/]+\/(.+?)\/[^/]*\.parquet$/);
      const signalkPath = pathMatch ? pathMatch[1].replace(/\//g, '.') : '';

      // Read sample data for content analysis
      let sampleRecords = [];
      try {
        const sampleReader = await parquet.ParquetReader.openFile(filePath);
        const sampleCursor = sampleReader.getCursor();
        let record: any;
        let count = 0;
        while ((record = await sampleCursor.next()) && count < 100) {
          sampleRecords.push(record);
          count++;
        }
        await sampleReader.close();
      } catch (error) {
        this.app?.debug(`⚠️ Could not read sample data for validation: ${(error as Error).message}`);
        sampleRecords = [];
      }

      // Rule 2: Check value fields using TWO-STEP PROCESS
      for (const [fieldName, fieldType] of Object.entries(valueFields)) {
        // Always skip value_json
        if (fieldName === 'value_json') {
          continue;
        }

        // Skip value field in exploded files
        if (isExplodedFile && fieldName === 'value') {
          continue;
        }

        if (fieldType === 'UTF8' || fieldType === 'VARCHAR') {
          let shouldBeNumeric = false;

          // STEP 1: LOOK AT THE STRING AND SEE WHAT IT IS
          if (sampleRecords.length > 0) {
            const values = sampleRecords
              .map(r => r[fieldName])
              .filter(v => v !== null && v !== undefined);

            if (values.length > 0) {
              let allNumeric = true;
              let allBoolean = true;

              for (const value of values) {
                const str = String(value).trim();
                if (str === 'true' || str === 'false') {
                  allNumeric = false;
                } else if (!isNaN(Number(str)) && str !== '') {
                  allBoolean = false;
                } else {
                  allNumeric = false;
                  allBoolean = false;
                  break;
                }
              }

              if (allNumeric && values.length > 0) {
                shouldBeNumeric = true;
                violations.push(`${fieldName} contains numbers but is ${fieldType}, should be DOUBLE`);
              } else if (allBoolean && values.length > 0) {
                violations.push(`${fieldName} contains booleans but is ${fieldType}, should be BOOLEAN`);
              }
            }
          }

          // STEP 2: LOOK AT METADATA (SKIP IF EXPLODED) - only if step 1 can't determine
          if (!shouldBeNumeric && sampleRecords.length === 0) {
            const isExplodedField = fieldName.startsWith('value_');

            if (!isExplodedField && signalkPath) {
              try {
                const response = await fetch(`http://localhost:3000/signalk/v1/api/vessels/self/${signalkPath.replace(/\./g, '/')}/meta`);
                if (response.ok) {
                  const metadata = await response.json() as any;
                  if (metadata && metadata.units &&
                      (metadata.units === 'm' || metadata.units === 'deg' || metadata.units === 'm/s' ||
                       metadata.units === 'rad' || metadata.units === 'K' || metadata.units === 'Pa' ||
                       metadata.units === 'V' || metadata.units === 'A' || metadata.units === 'Hz' ||
                       metadata.units === 'ratio' || metadata.units === 'kg' || metadata.units === 'J')) {
                    violations.push(`${fieldName} has numeric units (${metadata.units}) but is ${fieldType}, should be DOUBLE`);
                  }
                }
              } catch (metadataError) {
                // Metadata lookup failed, no violation flagged
              }
            }
          }
        } else if (fieldType === 'BIGINT') {
          // BIGINT fields are always violations
          violations.push(`${fieldName} is BIGINT, should be DOUBLE`);
        }
      }

      if (typeof reader.close === 'function') reader.close();

      return {
        isValid: violations.length === 0,
        violations,
        isExplodedFile,
        hasSchema: true
      };

    } catch (error) {
      this.app?.debug(`Error validating ${filePath}: ${(error as Error).message}`);
      return {
        isValid: false,
        violations: [`ERROR - ${(error as Error).message}`],
        isExplodedFile: false,
        hasSchema: false
      };
    }
  }

  /**
   * SCHEMA REPAIR LOGIC
   * Extracted and consolidated from repair logic in api-routes.ts
   */
  async repairFileSchema(filePath: string, filenamePrefix: string = 'signalk_data'): Promise<RepairResult> {
    try {
      if (!parquet) {
        throw new Error('ParquetJS not available');
      }

      // First validate to see if repair is needed
      const validation = await this.validateFileSchema(filePath);

      if (validation.isValid) {
        return {
          needsRepair: false,
          violations: []
        };
      }

      // File needs repair - create backup and repair
      const backupDir = path.join(path.dirname(filePath), 'repaired');
      await fs.mkdir(backupDir, { recursive: true });

      const originalFilename = path.basename(filePath);
      const backupFilename = originalFilename.replace('.parquet', '_BACKUP.parquet');
      const backupPath = path.join(backupDir, backupFilename);

      // Create backup
      await fs.copy(filePath, backupPath);

      // Read all data from original file
      const reader = await parquet.ParquetReader.openFile(filePath);
      const cursor = reader.getCursor();
      const records = [];
      let record;
      while ((record = await cursor.next())) {
        records.push(record);
      }
      await reader.close();

      if (records.length === 0) {
        return {
          needsRepair: false,
          violations: ['File contains no data']
        };
      }

      // Extract SignalK path for schema detection
      const relativePath = path.relative(path.dirname(path.dirname(filePath)), filePath);
      const pathMatch = relativePath.match(/vessels\/[^/]+\/(.+?)\/[^/]*\.parquet$/);
      const signalkPath = pathMatch ? pathMatch[1].replace(/\//g, '.') : '';

      // Generate optimal schema for the data
      const schemaResult = await this.detectOptimalSchema(records, signalkPath);

      // Write repaired file with correct schema
      const repairedFilename = originalFilename.replace('.parquet', '_REPAIRED.parquet');
      const repairedPath = path.join(backupDir, repairedFilename);

      const writer = await parquet.ParquetWriter.openFile(schemaResult.schema, repairedPath);

      for (const record of records) {
        // Prepare record for typed Parquet schema
        const cleanRecord: { [key: string]: any } = {};
        const schemaFields = schemaResult.schema.schema;

        Object.keys(schemaFields).forEach(fieldName => {
          const value = (record as any)[fieldName];
          const fieldType = schemaFields[fieldName].type;

          if (value === null || value === undefined) {
            cleanRecord[fieldName] = null;
          } else if (typeof value === 'bigint') {
            // Handle BigInt values by converting to appropriate type
            if (fieldType === 'DOUBLE') {
              cleanRecord[fieldName] = Number(value);
            } else {
              cleanRecord[fieldName] = value.toString();
            }
          } else if (fieldType === 'DOUBLE' && typeof value === 'string') {
            // Convert string to number for DOUBLE fields
            const numValue = Number(value);
            cleanRecord[fieldName] = isNaN(numValue) ? null : numValue;
          } else if (fieldType === 'BOOLEAN' && typeof value === 'string') {
            // Convert string to boolean for BOOLEAN fields
            cleanRecord[fieldName] = value.toLowerCase() === 'true';
          } else {
            cleanRecord[fieldName] = value;
          }
        });

        await writer.appendRow(cleanRecord);
      }

      await writer.close();

      return {
        needsRepair: true,
        violations: validation.violations,
        repairedFilePath: repairedPath,
        backupFilePath: backupPath
      };

    } catch (error) {
      this.app?.debug(`Error repairing ${filePath}: ${(error as Error).message}`);
      return {
        needsRepair: false,
        violations: [`REPAIR ERROR - ${(error as Error).message}`]
      };
    }
  }
}