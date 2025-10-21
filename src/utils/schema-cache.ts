import { Context, Path } from '@signalk/server-api';
import { DuckDBInstance } from '@duckdb/node-api';
import * as path from 'path';
import * as fs from 'fs-extra';

/**
 * Schema information for an object-valued path
 */
export interface PathComponentSchema {
  components: Map<string, ComponentInfo>; // component name -> type info
  timestamp: number;
}

export interface ComponentInfo {
  name: string; // e.g., "latitude", "longitude", "altitude"
  columnName: string; // e.g., "value_latitude", "value_longitude"
  dataType: 'numeric' | 'string' | 'boolean' | 'unknown';
}

/**
 * Cache for path component schemas
 * Key: `${context}:${path}`
 */
const schemaCache = new Map<string, PathComponentSchema>();
const SCHEMA_CACHE_TTL_MS = 2 * 60 * 1000; // 2 minutes

/**
 * Get the component schema for an object-valued path across all parquet files
 * Returns the union of all value_* columns found in any file for this path
 */
export async function getPathComponentSchema(
  dataDir: string,
  context: Context,
  pathStr: Path
): Promise<PathComponentSchema | null> {
  const cacheKey = `${context}:${pathStr}`;
  const now = Date.now();

  // Check cache first
  const cached = schemaCache.get(cacheKey);
  if (cached && now - cached.timestamp < SCHEMA_CACHE_TTL_MS) {
    return cached;
  }

  try {
    // Find all parquet files for this path
    const contextPath = toContextFilePath(context);
    const pathDirParts = pathStr.split('.');
    const pathDir = path.join(dataDir, contextPath, ...pathDirParts);

    if (!(await fs.pathExists(pathDir))) {
      return null;
    }

    // Recursively find all .parquet files
    const parquetFiles = await findParquetFiles(pathDir);

    if (parquetFiles.length === 0) {
      return null;
    }

    // Query schemas from all files to get union of components
    const allComponents = new Map<string, ComponentInfo>();

    const duckDB = await DuckDBInstance.create();
    const connection = await duckDB.connect();

    try {
      for (const filePath of parquetFiles) {
        try {
          // First check if this file has a 'value' column
          const valueColQuery = `
            SELECT name
            FROM parquet_schema('${filePath.replace(/'/g, "''")}')
            WHERE name = 'value'
          `;
          const valueColResult = await connection.runAndReadAll(valueColQuery);
          const hasValueColumn = valueColResult.getRowObjects().length > 0;

          // If 'value' column exists, skip this file - it's a scalar path
          if (hasValueColumn) {
            continue;
          }

          // Query the parquet schema for data component columns
          // Exclude metadata columns like value_units, value_description, value_json
          const schemaQuery = `
            SELECT name, type
            FROM parquet_schema('${filePath.replace(/'/g, "''")}')
            WHERE name LIKE 'value_%'
              AND name NOT IN ('value_json', 'value_units', 'value_description')
          `;

          const result = await connection.runAndReadAll(schemaQuery);
          const rows = result.getRowObjects();

          rows.forEach(row => {
            const columnName = row.name as string;
            const columnType = row.type as string;
            const componentName = columnName.replace(/^value_/, '');

            // Skip if we already have this component
            if (allComponents.has(componentName)) {
              return;
            }

            // Determine data type category
            const dataType = inferDataTypeCategory(columnType);

            allComponents.set(componentName, {
              name: componentName,
              columnName: columnName,
              dataType: dataType,
            });
          });
        } catch (error) {
          // Skip files with errors (corrupted, etc.)
          console.warn(
            `[Schema Cache] Error reading schema from ${filePath}:`,
            error
          );
        }
      }
    } finally {
      connection.disconnectSync();
    }

    if (allComponents.size === 0) {
      // No value_* columns found - this is a simple scalar path
      return null;
    }

    const schema: PathComponentSchema = {
      components: allComponents,
      timestamp: now,
    };

    // Cache it
    schemaCache.set(cacheKey, schema);

    return schema;
  } catch (error) {
    console.error(`[Schema Cache] Error getting schema for ${pathStr}:`, error);
    return null;
  }
}

/**
 * Clear the schema cache (useful for testing or when data structure changes)
 */
export function clearSchemaCache(): void {
  schemaCache.clear();
  console.log('[Schema Cache] Schema cache cleared');
}

/**
 * Infer data type category from DuckDB type string
 */
function inferDataTypeCategory(duckdbType: string): ComponentInfo['dataType'] {
  const typeUpper = duckdbType.toUpperCase();

  // Numeric types
  if (
    typeUpper.includes('INT') ||
    typeUpper.includes('DOUBLE') ||
    typeUpper.includes('FLOAT') ||
    typeUpper.includes('DECIMAL') ||
    typeUpper.includes('NUMERIC') ||
    typeUpper.includes('REAL') ||
    typeUpper.includes('BIGINT') ||
    typeUpper.includes('SMALLINT') ||
    typeUpper.includes('TINYINT')
  ) {
    return 'numeric';
  }

  // String types
  if (
    typeUpper.includes('VARCHAR') ||
    typeUpper.includes('CHAR') ||
    typeUpper.includes('TEXT') ||
    typeUpper.includes('STRING') ||
    typeUpper.includes('UTF8')
  ) {
    return 'string';
  }

  // Boolean
  if (typeUpper.includes('BOOL')) {
    return 'boolean';
  }

  return 'unknown';
}

/**
 * Convert context to filesystem path
 */
function toContextFilePath(context: Context): string {
  const parts = context.split('.');
  if (parts.length === 2) {
    // e.g., "vessels.urn:mrn:imo:mmsi:368396230" -> "vessels/urn_mrn_imo_mmsi_368396230"
    return `${parts[0]}/${parts[1].replace(/:/g, '_')}`;
  }
  return context.replace(/\./g, '/').replace(/:/g, '_');
}

/**
 * Recursively find all .parquet files in a directory
 */
async function findParquetFiles(dir: string): Promise<string[]> {
  const files: string[] = [];

  async function scan(currentDir: string) {
    try {
      const entries = await fs.readdir(currentDir, { withFileTypes: true });

      for (const entry of entries) {
        // Skip special directories
        if (
          entry.name === 'quarantine' ||
          entry.name === 'processed' ||
          entry.name === 'failed' ||
          entry.name === 'repaired' ||
          entry.name === 'claude-schemas'
        ) {
          continue;
        }

        const fullPath = path.join(currentDir, entry.name);

        if (entry.isFile() && entry.name.endsWith('.parquet')) {
          files.push(fullPath);
        } else if (entry.isDirectory()) {
          await scan(fullPath);
        }
      }
    } catch (error) {
      // Skip directories we can't read
    }
  }

  await scan(dir);
  return files;
}
