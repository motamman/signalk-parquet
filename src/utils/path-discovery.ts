import * as fs from 'fs-extra';
import * as path from 'path';
import { ServerAPI, Context, Path } from '@signalk/server-api';
import { PathInfo } from '../types';
import { DuckDBInstance } from '@duckdb/node-api';
import { ZonedDateTime } from '@js-joda/core';
import { toContextFilePath } from './path-helpers';

/**
 * Get available SignalK paths from directory structure
 * Scans the parquet data directory and returns paths that contain data files
 */
export function getAvailablePaths(dataDir: string, app: ServerAPI): PathInfo[] {
  const paths: PathInfo[] = [];

  // Clean the self context for filesystem usage (replace dots with slashes, colons with underscores)
  const selfContextPath = app.selfContext
    .replace(/\./g, '/')
    .replace(/:/g, '_');
  const vesselsDir = path.join(dataDir, selfContextPath);

  if (!fs.existsSync(vesselsDir)) {
    return paths;
  }

  function walkPaths(currentPath: string, relativePath: string = ''): void {
    try {
      const items = fs.readdirSync(currentPath);
      items.forEach((item: string) => {
        const fullPath = path.join(currentPath, item);
        const stat = fs.statSync(fullPath);

        if (
          stat.isDirectory() &&
          item !== 'processed' &&
          item !== 'failed' &&
          item !== 'quarantine' &&
          item !== 'claude-schemas' &&
          item !== 'repaired'
        ) {
          const newRelativePath = relativePath
            ? `${relativePath}.${item}`
            : item;

          // Check if this directory has parquet files
          const hasParquetFiles = fs
            .readdirSync(fullPath)
            .some((file: string) => file.endsWith('.parquet'));

          if (hasParquetFiles) {
            const fileCount = fs
              .readdirSync(fullPath)
              .filter((file: string) => file.endsWith('.parquet')).length;
            paths.push({
              path: newRelativePath,
              directory: fullPath,
              fileCount: fileCount,
            });
          }

          walkPaths(fullPath, newRelativePath);
        }
      });
    } catch (error) {
      // Error reading directory - skip
    }
  }

  if (fs.existsSync(vesselsDir)) {
    walkPaths(vesselsDir);
  }

  return paths;
}

/**
 * Get available SignalK paths as simple string array
 * Useful for SignalK history API compliance
 */
export function getAvailablePathsArray(
  dataDir: string,
  app: ServerAPI
): string[] {
  const pathInfos = getAvailablePaths(dataDir, app);
  return pathInfos.map(pathInfo => pathInfo.path);
}

/**
 * Get available SignalK paths that have data within a specific time range
 * This is compliant with SignalK History API specification
 */
export async function getAvailablePathsForTimeRange(
  dataDir: string,
  context: Context,
  from: ZonedDateTime,
  to: ZonedDateTime
): Promise<Path[]> {
  const contextPath = toContextFilePath(context);
  const fromIso = from.toInstant().toString();
  const toIso = to.toInstant().toString();

  // First, get all possible paths by scanning the directory structure
  const allPaths = await scanPathDirectories(path.join(dataDir, contextPath));

  // Then, check each path to see if it has data in the time range
  const pathsWithData: Path[] = [];

  await Promise.all(
    allPaths.map(async pathStr => {
      const hasData = await checkPathHasDataInRange(
        dataDir,
        contextPath,
        pathStr,
        fromIso,
        toIso
      );
      if (hasData) {
        pathsWithData.push(pathStr as Path);
      }
    })
  );

  return pathsWithData.sort();
}

/**
 * Recursively scan directories to find all paths with parquet files
 */
async function scanPathDirectories(contextDir: string): Promise<string[]> {
  const paths: string[] = [];

  async function scan(dir: string, prefix: string = '') {
    try {
      const entries = await fs.readdir(dir, { withFileTypes: true });

      for (const entry of entries) {
        // Skip special directories
        if (
          entry.name === 'processed' ||
          entry.name === 'failed' ||
          entry.name === 'quarantine' ||
          entry.name === 'claude-schemas' ||
          entry.name === 'repaired'
        ) {
          continue;
        }

        if (entry.isDirectory()) {
          const currentPath = prefix ? `${prefix}.${entry.name}` : entry.name;
          const fullPath = path.join(dir, entry.name);

          // Check if this directory contains parquet files
          const files = await fs.readdir(fullPath);
          const hasParquet = files.some(f => f.endsWith('.parquet'));

          if (hasParquet) {
            paths.push(currentPath);
          }

          // Recurse into subdirectories
          await scan(fullPath, currentPath);
        }
      }
    } catch (error) {
      // Directory doesn't exist or not accessible - skip
    }
  }

  await scan(contextDir);
  return paths;
}

/**
 * Check if a specific path has data within the given time range
 */
async function checkPathHasDataInRange(
  dataDir: string,
  contextPath: string,
  pathStr: string,
  fromIso: string,
  toIso: string
): Promise<boolean> {
  // Sanitize the path for filesystem use
  const sanitizedPath = pathStr
    .replace(/[^a-zA-Z0-9._]/g, '')
    .replace(/\./g, '/');

  const filePath = path.join(dataDir, contextPath, sanitizedPath, '*.parquet');

  try {
    const duckDB = await DuckDBInstance.create();
    const connection = await duckDB.connect();

    try {
      // Fast query: just check if ANY row exists in time range
      const query = `
        SELECT COUNT(*) as count
        FROM read_parquet('${filePath}', union_by_name=true)
        WHERE signalk_timestamp >= '${fromIso}'
          AND signalk_timestamp < '${toIso}'
        LIMIT 1
      `;

      const result = await connection.runAndReadAll(query);
      const rows = result.getRowObjects();

      return rows.length > 0 && (rows[0].count as number) > 0;
    } finally {
      connection.disconnectSync();
    }
  } catch (error) {
    // If path doesn't exist or has no parquet files, return false
    return false;
  }
}
