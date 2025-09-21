import * as fs from 'fs-extra';
import * as path from 'path';
import { ServerAPI } from '@signalk/server-api';
import { PathInfo } from '../types';

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
          item !== 'claude-schemas'
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
