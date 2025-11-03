import { Context, Path } from '@signalk/server-api';
import { DuckDBPool } from './duckdb-pool';
import * as path from 'path';
import * as fs from 'fs-extra';
import { toContextFilePath } from './path-helpers';
import { debugLogger } from './debug-logger';
import { CACHE_TTL } from '../config/cache-defaults';
import { DirectoryScanner } from './directory-scanner';

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

/**
 * Directory scanner for finding parquet files
 * Reused across multiple schema discovery operations
 */
const directoryScanner = new DirectoryScanner();

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
  if (cached && now - cached.timestamp < CACHE_TTL.SCHEMA) {
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

    // Get connection from pool
    const connection = await DuckDBPool.getConnection();

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
          const rows = result.getRowObjects() as Array<{
            name: string;
            type: string;
          }>;

          rows.forEach(row => {
            const columnName = row.name;
            const columnType = row.type;
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
          debugLogger.warn(
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
    debugLogger.error(
      `[Schema Cache] Error getting schema for ${pathStr}:`,
      error
    );
    return null;
  }
}

/**
 * Clear the schema cache (useful for testing or when data structure changes)
 */
export function clearSchemaCache(): void {
  schemaCache.clear();
  debugLogger.log('[Schema Cache] Schema cache cleared');
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
 * Recursively find all .parquet files in a directory
 * Uses DirectoryScanner for cached, efficient file discovery
 */
async function findParquetFiles(dir: string): Promise<string[]> {
  // Use DirectoryScanner with pattern matching for .parquet files
  const fileInfos = await directoryScanner.scanDirectory(dir, /\.parquet$/);

  // Convert FileInfo[] to string[] for compatibility
  return fileInfos.map(f => f.path);
}
