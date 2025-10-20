import * as fs from 'fs-extra';
import * as path from 'path';
import { Context } from '@signalk/server-api';
import { DuckDBInstance } from '@duckdb/node-api';
import { ZonedDateTime } from '@js-joda/core';

// Cache for parquet file list
interface FileListCache {
  files: Array<{ path: string; context: string }>;
  timestamp: number;
  dataDir: string;
}

let fileListCache: FileListCache | null = null;
const FILE_LIST_CACHE_TTL_MS = 2 * 60 * 1000; // 2 minutes

/**
 * Get available SignalK contexts that have data within a specific time range
 * This is compliant with SignalK History API specification
 *
 * OPTIMIZED: Uses a single SQL query across all vessels instead of checking each one individually
 * File list is cached for 2 minutes to avoid repeated filesystem scans
 */
export async function getAvailableContextsForTimeRange(
  dataDir: string,
  from: ZonedDateTime,
  to: ZonedDateTime
): Promise<Context[]> {
  const fromIso = from.toInstant().toString();
  const toIso = to.toInstant().toString();

  try {
    // Check cache first
    const now = Date.now();
    let allParquetFiles: Array<{ path: string; context: string }>;

    if (
      fileListCache &&
      fileListCache.dataDir === dataDir &&
      now - fileListCache.timestamp < FILE_LIST_CACHE_TTL_MS
    ) {
      // Cache hit
      allParquetFiles = fileListCache.files;
      console.log(`[Context Discovery] Using cached file list (${allParquetFiles.length} files, age: ${Math.round((now - fileListCache.timestamp) / 1000)}s)`);
    } else {
      // Cache miss - scan filesystem
      console.log(`[Context Discovery] Scanning filesystem for parquet files...`);
      allParquetFiles = await findAllValidParquetFiles(dataDir);

      // Update cache
      fileListCache = {
        files: allParquetFiles,
        timestamp: now,
        dataDir
      };
      console.log(`[Context Discovery] Cached ${allParquetFiles.length} parquet files`);
    }

    if (allParquetFiles.length === 0) {
      return [];
    }

    console.log(`[Context Discovery] Querying ${allParquetFiles.length} parquet files for time range ${fromIso} to ${toIso}`);

    const duckDB = await DuckDBInstance.create();
    const connection = await duckDB.connect();

    try {
      // Create file list for DuckDB
      const fileList = allParquetFiles.map(f => `'${f.path.replace(/'/g, "''")}'`).join(', ');

      // Single query to find all contexts with data in the time range
      // Use filename column to extract context information
      const query = `
        SELECT DISTINCT filename
        FROM read_parquet([${fileList}], union_by_name=true, filename=true)
        WHERE signalk_timestamp >= '${fromIso}'
          AND signalk_timestamp < '${toIso}'
      `;

      const result = await connection.runAndReadAll(query);
      const rows = result.getRowObjects();

      // Extract unique contexts from filenames
      const contextSet = new Set<string>();
      rows.forEach((row: any) => {
        const filename = row.filename as string;
        // Extract context from path: data/vessels/urn_mrn_imo_mmsi_368396230/navigation/...
        const match = filename.match(/vessels\/([^\/]+)\//);
        if (match) {
          const entityName = match[1].replace(/_/g, ':');
          contextSet.add(`vessels.${entityName}`);
        }
      });

      const contexts = Array.from(contextSet).sort() as Context[];
      console.log(`[Context Discovery] Found ${contexts.length} contexts with data in time range`);

      return contexts;
    } finally {
      connection.disconnectSync();
    }
  } catch (error) {
    console.error('Error scanning contexts:', error);
    return [];
  }
}

/**
 * Clear the file list cache (useful for testing or when data structure changes)
 */
export function clearFileListCache(): void {
  fileListCache = null;
  console.log('[Context Discovery] File list cache cleared');
}

/**
 * Find all valid parquet files across all contexts
 * Returns file paths with their context information
 */
async function findAllValidParquetFiles(
  dataDir: string
): Promise<Array<{ path: string; context: string }>> {
  const files: Array<{ path: string; context: string }> = [];

  try {
    const topLevel = await fs.readdir(dataDir, { withFileTypes: true });

    for (const typeEntry of topLevel) {
      // Only look in vessels directory for now (could expand to shore, etc.)
      if (!typeEntry.isDirectory() || typeEntry.name !== 'vessels') {
        continue;
      }

      const typePath = path.join(dataDir, typeEntry.name);
      const entities = await fs.readdir(typePath, { withFileTypes: true });

      for (const entityEntry of entities) {
        if (!entityEntry.isDirectory()) continue;

        const contextDir = path.join(typePath, entityEntry.name);
        const contextFiles = await findValidParquetFilesInContext(contextDir);

        contextFiles.forEach(filePath => {
          files.push({
            path: filePath,
            context: `${typeEntry.name}.${entityEntry.name.replace(/_/g, ':')}`
          });
        });
      }
    }
  } catch (error) {
    console.error('Error finding parquet files:', error);
  }

  return files;
}

/**
 * Find valid parquet files in a single context directory
 */
async function findValidParquetFilesInContext(dir: string): Promise<string[]> {
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

