import * as fs from 'fs-extra';
import * as path from 'path';
import { ServerAPI, Context, Path } from '@signalk/server-api';
import { PathInfo } from '../types';
import { ZonedDateTime } from '@js-joda/core';
import { HivePathBuilder } from './hive-path-builder';
import { DuckDBPool } from './duckdb-pool';

/**
 * Get available SignalK paths from Hive directory structure
 * Scans tier=raw/context={ctx}/path={path}/ and returns paths that contain data files
 */
export function getAvailablePaths(
  dataDir: string,
  app: ServerAPI,
  context?: string
): PathInfo[] {
  const paths: PathInfo[] = [];
  const hiveBuilder = new HivePathBuilder();

  // Scan Hive structure: tier=raw/context=*/path=*/
  const hiveRawDir = path.join(dataDir, 'tier=raw');

  if (!fs.existsSync(hiveRawDir)) {
    return paths;
  }

  try {
    // Get target context sanitized for matching
    const targetContext = context || app.selfContext;
    const sanitizedTargetContext = hiveBuilder.sanitizeContext(targetContext);

    // Iterate context= directories
    const contextDirs = fs.readdirSync(hiveRawDir);

    for (const contextDir of contextDirs) {
      if (!contextDir.startsWith('context=')) continue;

      const contextPath = path.join(hiveRawDir, contextDir);
      const stat = fs.statSync(contextPath);
      if (!stat.isDirectory()) continue;

      // Extract and unsanitize context name
      const sanitizedContext = contextDir.replace('context=', '');

      // Only include paths for target context
      if (sanitizedContext !== sanitizedTargetContext) continue;

      // Iterate path= directories within each context
      const pathDirs = fs.readdirSync(contextPath);

      for (const pathDir of pathDirs) {
        if (!pathDir.startsWith('path=')) continue;

        const pathPath = path.join(contextPath, pathDir);
        const pathStat = fs.statSync(pathPath);
        if (!pathStat.isDirectory()) continue;

        // Extract and unsanitize path name
        const sanitizedPath = pathDir.replace('path=', '');
        const unsanitizedPath = hiveBuilder.unsanitizePath(sanitizedPath);

        // Count parquet files across all year=/day=/ subdirs
        const fileCount = countParquetFilesRecursive(pathPath);

        if (fileCount > 0) {
          paths.push({
            path: unsanitizedPath,
            directory: pathPath,
            fileCount: fileCount,
          });
        }
      }
    }
  } catch (error) {
    // Error reading directory - skip
  }

  return paths;
}

/**
 * Count parquet files recursively in a directory
 */
function countParquetFilesRecursive(dir: string): number {
  let count = 0;

  try {
    const items = fs.readdirSync(dir);

    for (const item of items) {
      const fullPath = path.join(dir, item);
      const stat = fs.statSync(fullPath);

      if (stat.isDirectory()) {
        count += countParquetFilesRecursive(fullPath);
      } else if (item.endsWith('.parquet')) {
        count++;
      }
    }
  } catch (error) {
    // Error reading directory - skip
  }

  return count;
}

/**
 * Get available SignalK paths as simple string array
 * Useful for SignalK history API compliance
 */
export function getAvailablePathsArray(
  dataDir: string,
  app: ServerAPI,
  context?: string
): string[] {
  const pathInfos = getAvailablePaths(dataDir, app, context);
  return pathInfos.map(pathInfo => pathInfo.path);
}

/**
 * Get available SignalK paths that have data within a specific time range
 * This is compliant with SignalK History API specification
 * Uses Hive-partitioned directory structure
 */
export async function getAvailablePathsForTimeRange(
  dataDir: string,
  context: Context,
  from: ZonedDateTime,
  to: ZonedDateTime
): Promise<Path[]> {
  const hiveBuilder = new HivePathBuilder();
  const fromIso = from.toInstant().toString();
  const toIso = to.toInstant().toString();

  // Build Hive-style context directory
  const sanitizedContext = hiveBuilder.sanitizeContext(context);
  const contextDir = path.join(
    dataDir,
    'tier=raw',
    `context=${sanitizedContext}`
  );

  // Get all path= directories for this context
  const allPaths = await scanHivePathDirectories(contextDir, hiveBuilder);

  // Then, check each path to see if it has data in the time range
  const pathsWithData: Path[] = [];

  await Promise.all(
    allPaths.map(async pathStr => {
      const hasData = await checkPathHasDataInRangeHive(
        dataDir,
        context,
        pathStr,
        fromIso,
        toIso,
        hiveBuilder
      );
      if (hasData) {
        pathsWithData.push(pathStr as Path);
      }
    })
  );

  return pathsWithData.sort();
}

/**
 * Scan Hive-style path= directories to find all SignalK paths
 */
async function scanHivePathDirectories(
  contextDir: string,
  hiveBuilder: HivePathBuilder
): Promise<string[]> {
  const paths: string[] = [];

  try {
    if (!(await fs.pathExists(contextDir))) {
      return paths;
    }

    const entries = await fs.readdir(contextDir, { withFileTypes: true });

    for (const entry of entries) {
      // Only look at path= directories
      if (!entry.isDirectory() || !entry.name.startsWith('path=')) {
        continue;
      }

      const sanitizedPath = entry.name.replace('path=', '');
      const unsanitizedPath = hiveBuilder.unsanitizePath(sanitizedPath);

      // Check if this path directory has any parquet files (recursively in year/day subdirs)
      const pathDir = path.join(contextDir, entry.name);
      const hasParquet = await hasParquetFilesRecursive(pathDir);

      if (hasParquet) {
        paths.push(unsanitizedPath);
      }
    }
  } catch (error) {
    // Directory doesn't exist or not accessible - skip
  }

  return paths;
}

/**
 * Check if a directory (or its subdirectories) contains any parquet files
 */
async function hasParquetFilesRecursive(dir: string): Promise<boolean> {
  try {
    const entries = await fs.readdir(dir, { withFileTypes: true });

    for (const entry of entries) {
      if (entry.isFile() && entry.name.endsWith('.parquet')) {
        return true;
      }
      if (entry.isDirectory()) {
        // Skip special directories
        if (
          entry.name === 'processed' ||
          entry.name === 'failed' ||
          entry.name === 'quarantine' ||
          entry.name === 'repaired'
        ) {
          continue;
        }
        const hasFiles = await hasParquetFilesRecursive(
          path.join(dir, entry.name)
        );
        if (hasFiles) return true;
      }
    }
  } catch (error) {
    // Skip on error
  }
  return false;
}

/**
 * Check if a specific path has data within the given time range
 * Uses Hive-partitioned directory structure
 */
async function checkPathHasDataInRangeHive(
  dataDir: string,
  context: Context,
  pathStr: string,
  fromIso: string,
  toIso: string,
  hiveBuilder: HivePathBuilder
): Promise<boolean> {
  // Build Hive-style glob pattern for this path
  const filePath = hiveBuilder.getGlobPattern(dataDir, 'raw', context, pathStr);

  try {
    const connection = await DuckDBPool.getConnection();

    try {
      // Fast query: just check if ANY row exists in time range
      const query = `
        SELECT 1 as found
        FROM read_parquet('${filePath}', union_by_name=true, filename=true)
        WHERE signalk_timestamp >= '${fromIso}'
          AND signalk_timestamp < '${toIso}'
          AND filename NOT LIKE '%/processed/%'
          AND filename NOT LIKE '%/quarantine/%'
          AND filename NOT LIKE '%/failed/%'
          AND filename NOT LIKE '%/repaired/%'
        LIMIT 1
      `;

      const result = await connection.runAndReadAll(query);
      const rows = result.getRowObjects();

      return rows.length > 0;
    } finally {
      connection.disconnectSync();
    }
  } catch (error) {
    // If path doesn't exist or has no parquet files, return false
    return false;
  }
}
