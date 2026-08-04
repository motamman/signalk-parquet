import { Context, Path } from '@signalk/server-api';
import { DuckDBPool } from './duckdb-pool';
import * as path from 'path';
import * as fs from 'fs-extra';
import { debugLogger } from './debug-logger';
import { CACHE_TTL } from '../config/cache-defaults';
import { HivePathBuilder } from './hive-path-builder';

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
 * Key: `${dataDir}:${context}:${path}` — the data directory is part of the
 * key so a runtime reconfigure to a different store can't serve schemas
 * discovered in the old one.
 */
const schemaCache = new Map<string, PathComponentSchema>();

/**
 * Hive path builder for constructing Hive-style paths
 */
const hivePathBuilder = new HivePathBuilder();

/**
 * Get the component schema for an object-valued path across all parquet files
 * Returns the union of all value_* columns found in any file for this path
 * Uses Hive-partitioned directory structure: tier=raw/context=.../path=.../
 */
export async function getPathComponentSchema(
  dataDir: string,
  context: Context,
  pathStr: Path
): Promise<PathComponentSchema | null> {
  const cacheKey = `${dataDir}:${context}:${pathStr}`;
  const now = Date.now();

  // Check cache first
  const cached = schemaCache.get(cacheKey);
  if (cached && now - cached.timestamp < CACHE_TTL.SCHEMA) {
    return cached;
  }

  try {
    // Build Hive-style path for this context and path
    // Default to 'raw' tier for schema discovery
    const sanitizedContext = hivePathBuilder.sanitizeContext(context);
    const sanitizedPath = hivePathBuilder.sanitizePath(pathStr);
    const pathDir = path.join(
      dataDir,
      'tier=raw',
      `context=${sanitizedContext}`,
      `path=${sanitizedPath}`
    );

    if (!(await fs.pathExists(pathDir))) {
      return null;
    }

    // Read the column union straight from DuckDB rather than listing every
    // file in JS and querying each one. `year=*/day=*` matches only real
    // day-partition files (never quarantine/failed/processed siblings), and
    // union_by_name reconciles schema evolution across the day-files — the
    // same component union the old per-file loop produced, in one native
    // query, with no retained filename list to leak.
    const glob = path
      .join(pathDir, 'year=*', 'day=*', '*.parquet')
      .replace(/'/g, "''");

    // Metadata columns that are not object components.
    const EXCLUDED = new Set([
      'value_json',
      'value_units',
      'value_description',
      'value_age',
    ]);

    const allComponents = new Map<string, ComponentInfo>();
    const connection = await DuckDBPool.getConnection();

    try {
      let rows: Array<{ column_name: string; column_type: string }>;
      try {
        const result = await connection.runAndReadAll(
          `DESCRIBE SELECT * FROM read_parquet('${glob}', union_by_name=true)`
        );
        rows = result.getRowObjects() as Array<{
          column_name: string;
          column_type: string;
        }>;
      } catch (describeErr) {
        // No day-partition parquet files under this path (e.g. only
        // quarantined files) — read_parquet raises "No files found". Treat as
        // "no schema", matching the old empty-file-list behaviour. Any other
        // failure (binding, corruption, permissions) is rethrown so the
        // outer handler logs it and propagates it to the caller — null is
        // reserved for "path has no object schema", so corruption can't be
        // silently treated as a scalar path.
        const message = (describeErr as Error).message ?? '';
        if (!message.includes('No files found')) {
          throw describeErr;
        }
        debugLogger.warn(`[Schema Cache] No schema for ${pathStr}: ${message}`);
        return null;
      }

      for (const row of rows) {
        const columnName = row.column_name;
        // Object components are the flattened value_* columns; a bare `value`
        // column (scalar files) is skipped because it lacks the `value_`
        // prefix, so a purely scalar path yields no components (returns null).
        if (!columnName.startsWith('value_') || EXCLUDED.has(columnName)) {
          continue;
        }
        const componentName = columnName.replace(/^value_/, '');
        if (allComponents.has(componentName)) {
          continue;
        }
        allComponents.set(componentName, {
          name: componentName,
          columnName,
          dataType: inferDataTypeCategory(row.column_type),
        });
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
    // Null means "no object schema"; real failures (corruption, permissions,
    // binding) propagate so callers' per-path error handling can act instead
    // of misreading the path as scalar.
    throw error;
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
export function inferDataTypeCategory(
  duckdbType: string
): ComponentInfo['dataType'] {
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
    typeUpper.includes('UTF8') ||
    typeUpper.includes('BYTE_ARRAY')
  ) {
    return 'string';
  }

  // Boolean
  if (typeUpper.includes('BOOL')) {
    return 'boolean';
  }

  return 'unknown';
}
