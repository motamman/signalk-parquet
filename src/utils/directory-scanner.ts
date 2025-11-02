import * as fs from 'fs-extra';
import * as path from 'path';

/**
 * Information about a scanned file
 */
export interface FileInfo {
  path: string;
  name: string;
  size: number;
  modifiedTime: number;
  directory: string;
}

/**
 * Cache entry for a directory scan
 */
interface DirectoryScanCache {
  files: FileInfo[];
  scannedAt: number;
  directories: string[];
}

/**
 * Cached directory scanner that reduces filesystem operations
 * Maintains an index of files and only rescans when needed
 */
export class DirectoryScanner {
  private cache: Map<string, DirectoryScanCache> = new Map();
  private cacheTTL: number;
  private excludedDirs: Set<string>;

  /**
   * @param cacheTTL - Cache time-to-live in milliseconds (default: 5 minutes)
   * @param excludedDirs - Directory names to skip during scanning
   */
  constructor(
    cacheTTL: number = 5 * 60 * 1000,
    excludedDirs: string[] = [
      'processed',
      'failed',
      'quarantine',
      'claude-schemas',
      'repaired',
    ]
  ) {
    this.cacheTTL = cacheTTL;
    this.excludedDirs = new Set(excludedDirs);
  }

  /**
   * Scan a directory and return all files matching the pattern
   * Uses cache when available and valid
   *
   * @param baseDir - Root directory to scan
   * @param filePattern - Optional regex pattern to match files (null = match all)
   * @param forceRefresh - Force cache refresh even if valid
   * @returns Array of file information
   */
  async scanDirectory(
    baseDir: string,
    filePattern: RegExp | null = null,
    forceRefresh: boolean = false
  ): Promise<FileInfo[]> {
    const cacheKey = baseDir;
    const now = Date.now();

    // Check cache validity
    const cached = this.cache.get(cacheKey);
    const isCacheValid =
      cached && !forceRefresh && now - cached.scannedAt < this.cacheTTL;

    if (isCacheValid) {
      // Filter cached results by pattern
      return filePattern
        ? cached!.files.filter(f => filePattern.test(f.name))
        : cached!.files;
    }

    // Perform scan
    const files: FileInfo[] = [];
    const directories: string[] = [];

    await this.walkDirectory(baseDir, baseDir, files, directories);

    // Update cache
    this.cache.set(cacheKey, {
      files,
      scannedAt: now,
      directories,
    });

    // Filter results by pattern
    return filePattern ? files.filter(f => filePattern.test(f.name)) : files;
  }

  /**
   * Find files by date pattern (e.g., "2025-11-02")
   * Optimized for consolidation use case
   *
   * @param baseDir - Root directory to scan
   * @param dateStr - Date string to match (YYYY-MM-DD format)
   * @param excludeConsolidated - Exclude already consolidated files
   * @returns Array of matching files
   */
  async findFilesByDate(
    baseDir: string,
    dateStr: string,
    excludeConsolidated: boolean = true
  ): Promise<FileInfo[]> {
    const allFiles = await this.scanDirectory(baseDir);

    return allFiles.filter(file => {
      // Must include the date string
      if (!file.name.includes(dateStr)) return false;

      // Optionally exclude consolidated files
      if (excludeConsolidated && file.name.includes('_consolidated')) {
        return false;
      }

      return true;
    });
  }

  /**
   * Recursive directory walker
   * Private method used by scanDirectory
   */
  private async walkDirectory(
    baseDir: string,
    currentDir: string,
    files: FileInfo[],
    directories: string[]
  ): Promise<void> {
    try {
      const items = await fs.readdir(currentDir);

      for (const item of items) {
        const itemPath = path.join(currentDir, item);

        try {
          const stat = await fs.stat(itemPath);

          if (stat.isDirectory()) {
            // Skip excluded directories
            if (this.excludedDirs.has(item)) {
              continue;
            }

            directories.push(itemPath);
            // Recurse into subdirectory
            await this.walkDirectory(baseDir, itemPath, files, directories);
          } else if (stat.isFile()) {
            // Add file to results
            files.push({
              path: itemPath,
              name: item,
              size: stat.size,
              modifiedTime: stat.mtimeMs,
              directory: currentDir,
            });
          }
        } catch (statError) {
          // Skip files we can't stat (permissions, etc.)
          continue;
        }
      }
    } catch (readError) {
      // Skip directories we can't read
      return;
    }
  }

  /**
   * Invalidate cache for a specific directory
   * Call this when files are added/removed in that directory
   */
  invalidateCache(directory: string): void {
    this.cache.delete(directory);
  }

  /**
   * Clear entire cache
   * Useful for testing or manual cache refresh
   */
  clearCache(): void {
    this.cache.clear();
  }

  /**
   * Get cache statistics
   */
  getCacheStats(): {
    entries: number;
    totalFiles: number;
    totalDirectories: number;
  } {
    let totalFiles = 0;
    let totalDirectories = 0;

    this.cache.forEach(entry => {
      totalFiles += entry.files.length;
      totalDirectories += entry.directories.length;
    });

    return {
      entries: this.cache.size,
      totalFiles,
      totalDirectories,
    };
  }

  /**
   * Get list of cached directories
   */
  getCachedDirectories(): string[] {
    return Array.from(this.cache.keys());
  }
}
