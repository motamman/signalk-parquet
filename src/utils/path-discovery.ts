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
  context?: string,
  // When false, a path is included as soon as one parquet file is found rather
  // than counting every file. countParquetFilesRecursive descends every
  // year=/day= partition and stats every file — on a large store that is
  // millions of synchronous calls that block the event loop. Callers that
  // display the count (UI, analyzer) keep the default; the History API path
  // list, which discards the count, passes false. Both return the same paths.
  countFiles: boolean = true
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

        // Count parquet files across all year=/day=/ subdirs, or just confirm
        // at least one exists when the count is not needed (far cheaper, same
        // inclusion result).
        const fileCount = countFiles
          ? countParquetFilesRecursive(pathPath)
          : hasParquetFilesRecursiveSync(pathPath)
            ? 1
            : 0;

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
 * Return true as soon as a single parquet file is found anywhere under `dir`.
 * Mirrors countParquetFilesRecursive's traversal exactly (recurses every
 * subdirectory, no special-dir skipping) so it includes the same paths, but
 * short-circuits on the first hit and uses Dirent types (falling back to stat
 * only for symlinks/unknown entries) instead of a statSync per entry — turning
 * a full census into a handful of readdirs.
 */
function hasParquetFilesRecursiveSync(dir: string): boolean {
  try {
    const entries = fs.readdirSync(dir, { withFileTypes: true });

    for (const entry of entries) {
      let isDir = entry.isDirectory();
      let isParquet =
        !isDir && entry.isFile() && entry.name.endsWith('.parquet');

      // Dirent types are unreliable for symlinks (reported as the link, not its
      // target) and on filesystems that return unknown types. For those rare
      // entries fall back to stat (which follows the link) so traversal matches
      // countParquetFilesRecursive. The data tree has no symlinks in practice,
      // so the fast Dirent path covers essentially everything.
      if (!isDir && !isParquet && !entry.isFile()) {
        try {
          isDir = fs.statSync(path.join(dir, entry.name)).isDirectory();
          isParquet = !isDir && entry.name.endsWith('.parquet');
        } catch {
          // broken symlink / unreadable entry — skip
        }
      }

      if (isDir) {
        if (hasParquetFilesRecursiveSync(path.join(dir, entry.name))) {
          return true;
        }
      } else if (isParquet) {
        return true;
      }
    }
  } catch (error) {
    // Error reading directory - skip
  }
  return false;
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
  // Path names only — skip the per-file count so this stays cheap even on a
  // store with millions of parquet files (this is the History API hot path).
  const pathInfos = getAvailablePaths(dataDir, app, context, false);
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
