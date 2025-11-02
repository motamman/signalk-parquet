import * as path from 'path';
import { Context } from '@signalk/server-api';

/**
 * Special directories that should be excluded during file scanning and data processing
 * These directories contain processed, failed, or metadata files that should not be included
 * in normal data operations.
 */
export const SPECIAL_DIRECTORIES = new Set([
  'processed',
  'failed',
  'quarantine',
  'claude-schemas',
  'repaired',
]);

/**
 * Check if a directory name should be skipped during scanning
 * @param dirName - The directory name to check
 * @returns true if the directory should be skipped
 */
export function shouldSkipDirectory(dirName: string): boolean {
  return SPECIAL_DIRECTORIES.has(dirName);
}

export function toContextFilePath(context: Context): string {
  return context.replace(/\./g, '/').replace(/:/g, '_');
}

export function toParquetFilePath(
  dataDir: string,
  selfContextPath: string,
  quotedPath: string
): string {
  return path.join(
    dataDir,
    selfContextPath,
    quotedPath.replace(/\./g, '/'),
    '*.parquet'
  );
}
