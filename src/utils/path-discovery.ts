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

  app.debug(`🔍 Looking for paths in vessel directory: ${vesselsDir}`);
  app.debug(
    `📡 Using vessel context: ${app.selfContext} → ${selfContextPath}`
  );

  if (!fs.existsSync(vesselsDir)) {
    app.debug(`❌ Vessel directory does not exist: ${vesselsDir}`);
    return paths;
  }

  function walkPaths(currentPath: string, relativePath: string = ''): void {
    try {
      app.debug(
        `🚶 Walking path: ${currentPath} (relative: ${relativePath})`
      );
      const items = fs.readdirSync(currentPath);
      items.forEach((item: string) => {
        const fullPath = path.join(currentPath, item);
        const stat = fs.statSync(fullPath);

        if (
          stat.isDirectory() &&
          item !== 'processed' &&
          item !== 'failed'
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
            app.debug(
              `✅ Found SignalK path with data: ${newRelativePath} (${fileCount} files)`
            );
            paths.push({
              path: newRelativePath,
              directory: fullPath,
              fileCount: fileCount,
            });
          } else {
            app.debug(
              `📁 Directory ${newRelativePath} has no parquet files`
            );
          }

          walkPaths(fullPath, newRelativePath);
        }
      });
    } catch (error) {
      app.debug(
        `❌ Error reading directory ${currentPath}: ${(error as Error).message}`
      );
    }
  }

  if (fs.existsSync(vesselsDir)) {
    walkPaths(vesselsDir);
  }

  app.debug(
    `📊 Path discovery complete: found ${paths.length} paths with data`
  );
  return paths;
}

/**
 * Get available SignalK paths as simple string array
 * Useful for SignalK history API compliance
 */
export function getAvailablePathsArray(dataDir: string, app: ServerAPI): string[] {
  const pathInfos = getAvailablePaths(dataDir, app);
  return pathInfos.map(pathInfo => pathInfo.path);
}