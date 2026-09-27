import * as fs from 'fs-extra';
import * as path from 'path';
import { ServerAPI, Context, Path } from '@signalk/server-api';
import { PathInfo } from '../types';
import { ZonedDateTime } from '@js-joda/core';
import { HivePathBuilder } from './hive-path-builder';
import { DuckDBPool } from './duckdb-pool';
import { escapeSqlString } from './sql-escape';
import { isoBound } from './iso-time';
import { filesFor, readParquetSql } from './parquet-files';
import {
  footerReaderAvailable,
  overlapsWindow,
  readFooters,
} from './parquet-footer';

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
 * The slice of the SQLite buffer a path listing needs: which paths it holds,
 * and whether one of them has a row for a context, in a window or at all.
 *
 * A path recorded since the last daily export has rows here and no files, so
 * a listing answered from files alone does not know it exists: a new path is
 * invisible until its first export, a buffer-only path is invisible for good,
 * and a window that lies inside today lists nothing at all. Each probe is one
 * index seek, and the predicate is the one the values federation reads with,
 * so a path listed from here is one a values query would answer for.
 */
export interface BufferPathSource {
  isOpen(): boolean;
  getKnownPaths(): Set<string>;
  hasRowsInWindow(
    signalkPath: string,
    context: string,
    fromIso: string,
    toIso: string
  ): boolean;
  hasRowsForContext(signalkPath: string, context: string): boolean;
}

/** `listed`, plus the buffer's paths for which `hasRows` says yes, sorted. */
function unionBufferPaths(
  listed: string[],
  buffer: BufferPathSource | undefined,
  hasRows: (signalkPath: string) => boolean
): string[] {
  if (!buffer || !buffer.isOpen()) return listed;
  const out = new Set(listed);
  for (const signalkPath of buffer.getKnownPaths()) {
    if (out.has(signalkPath)) continue;
    if (hasRows(signalkPath)) out.add(signalkPath);
  }
  return [...out].sort();
}

/**
 * Get available SignalK paths as simple string array
 * Useful for SignalK history API compliance
 *
 * Files first, then the buffer's paths with a row for the context, so a path
 * shows from its first recorded row rather than from its first export.
 */
export function getAvailablePathsArray(
  dataDir: string,
  app: ServerAPI,
  context?: string,
  buffer?: BufferPathSource
): string[] {
  // Path names only — skip the per-file count so this stays cheap even on a
  // store with millions of parquet files (this is the History API hot path).
  const pathInfos = getAvailablePaths(dataDir, app, context, false);
  const fromFiles = pathInfos.map(pathInfo => pathInfo.path);
  const target = context || app.selfContext;
  return unionBufferPaths(fromFiles, buffer, signalkPath =>
    buffer!.hasRowsForContext(signalkPath, target)
  );
}

/**
 * Get available SignalK paths that have data within a specific time range
 * This is compliant with SignalK History API specification
 *
 * One walk of the context's day partitions for the window gives every path's
 * files; each path is then answered from its files' footers (the
 * `signalk_timestamp` span of each row group, in JavaScript), stopping at the
 * first file that overlaps. Only a footer that cannot say sends that path to
 * a DuckDB row probe, and then only over the window's files.
 *
 * The previous shape was one unbounded DuckDB `SELECT 1 … LIMIT 1` per path
 * over the path's WHOLE history (`year=*\/day=*`), all in parallel: on a
 * vessel with ~2,000 paths a client's seven-day discovery call took 36 s and
 * permanently added 1.7 GB to the server (measured 2026-09-17).
 *
 * Then the buffer: any of its paths with an unexported row for the context
 * in the window is listed too, so the answer covers what a values query over
 * the same window would return, files and buffer alike.
 */
export async function getAvailablePathsForTimeRange(
  dataDir: string,
  context: Context,
  from: ZonedDateTime,
  to: ZonedDateTime,
  buffer?: BufferPathSource
): Promise<Path[]> {
  const fromIso = isoBound(from);
  const toIso = isoBound(to);

  const files = await filesFor({
    dataDir,
    contexts: [context],
    paths: 'all',
    fromIso,
    toIso,
  });
  const byPath = groupFilesByPath(files);

  const pathsWithData: string[] = [];
  for (const [signalkPath, pathFiles] of byPath) {
    if (await pathHasRowsInWindow(pathFiles, fromIso, toIso)) {
      pathsWithData.push(signalkPath);
    }
  }
  return unionBufferPaths(pathsWithData, buffer, signalkPath =>
    buffer!.hasRowsInWindow(signalkPath, context, fromIso, toIso)
  ) as Path[];
}

/** The window's files of a context, keyed by SignalK path, in listing order. */
function groupFilesByPath(files: string[]): Map<string, string[]> {
  const hiveBuilder = new HivePathBuilder();
  const byPath = new Map<string, string[]>();
  for (const file of files) {
    const parsed = hiveBuilder.detectPathStyle(file);
    if (!parsed.isHive || !parsed.signalkPath) continue;
    const list = byPath.get(parsed.signalkPath) ?? [];
    list.push(file);
    byPath.set(parsed.signalkPath, list);
  }
  return byPath;
}

/**
 * Whether any of a path's window files holds a row in [fromIso, toIso).
 * Footers first, stopping at the first that overlaps; files whose footers
 * cannot say (no timestamp statistics, unreadable) are asked through DuckDB,
 * which is the one thing it is kept for. A DuckDB failure means "no data",
 * as it did before.
 */
async function pathHasRowsInWindow(
  files: string[],
  fromIso: string,
  toIso: string
): Promise<boolean> {
  let found = false;
  let unreadable = !footerReaderAvailable();
  const undecided: string[] = [];
  if (!unreadable) {
    try {
      await readFooters(files, {
        until: footer => {
          const overlaps = overlapsWindow(footer, fromIso, toIso);
          if (overlaps === null) undecided.push(footer.file);
          if (overlaps === true) found = true;
          return found;
        },
      });
    } catch {
      // At least one file could not be read. A hit already found still
      // stands; otherwise DuckDB decides over all of them rather than a
      // guess at which one failed.
      unreadable = true;
    }
  }
  if (found) return true;
  const candidates = unreadable ? files : undecided;
  if (candidates.length === 0) return false;

  try {
    const connection = await DuckDBPool.getConnection();
    try {
      const result = await connection.runAndReadAll(
        `SELECT 1 AS found FROM ${readParquetSql(candidates)} WHERE signalk_timestamp >= '${escapeSqlString(fromIso)}' AND signalk_timestamp < '${escapeSqlString(toIso)}' LIMIT 1`
      );
      return result.getRowObjects().length > 0;
    } finally {
      connection.disconnectSync();
    }
  } catch {
    return false;
  }
}
