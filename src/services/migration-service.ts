/**
 * Migration Service
 *
 * Handles migration of existing flat-structure Parquet files to
 * the new Hive-style partitioned structure.
 */

import * as fs from 'fs-extra';
import * as path from 'path';
import { glob } from 'glob';
import { ServerAPI } from '@signalk/server-api';
import { HivePathBuilder, AggregationTier } from '../utils/hive-path-builder';
import { DuckDBPool } from '../utils/duckdb-pool';

export interface MigrationConfig {
  sourceDirectory: string;
  targetDirectory: string;
  targetTier: AggregationTier;
  deleteSourceAfterMigration: boolean;
}

export interface MigrationProgress {
  jobId: string;
  status: 'scanning' | 'running' | 'completed' | 'cancelled' | 'error';
  phase: 'scan' | 'migrate' | 'cleanup';
  processed: number;
  total: number;
  percent: number;
  currentFile?: string;
  startTime: Date;
  completedAt?: Date;
  error?: string;
  bytesProcessed: number;
  filesMigrated: number;
  filesSkipped: number;
  errors: string[];
}

export interface ScanResult {
  totalFiles: number;
  totalSize: number;
  byPath: Map<string, { count: number; size: number }>;
  estimatedTime: number;
  sourceStyle: 'flat' | 'mixed' | 'unknown';
}

const migrationJobs = new Map<string, MigrationProgress>();
const MIGRATION_JOB_TTL_MS = 30 * 60 * 1000; // 30 minutes

function scheduleMigrationJobCleanup(jobId: string) {
  setTimeout(() => {
    const job = migrationJobs.get(jobId);
    if (job && job.status !== 'running') {
      migrationJobs.delete(jobId);
    }
  }, MIGRATION_JOB_TTL_MS);
}

export class MigrationService {
  private readonly app: ServerAPI;
  private readonly hivePathBuilder: HivePathBuilder;
  private cancelRequested: boolean = false;

  constructor(app: ServerAPI) {
    this.app = app;
    this.hivePathBuilder = new HivePathBuilder();
  }

  /**
   * Scan source directory for files to migrate
   */
  async scan(sourceDirectory: string): Promise<ScanResult> {
    const pattern = path.join(sourceDirectory, '**', '*.parquet');
    const files = await glob(pattern);

    let totalSize = 0;
    let flatCount = 0;
    let hiveCount = 0;
    const byPath = new Map<string, { count: number; size: number }>();

    for (const file of files) {
      try {
        const stats = await fs.stat(file);
        totalSize += stats.size;

        const parsed = this.hivePathBuilder.detectPathStyle(file);

        if (parsed.isHive) {
          hiveCount++;
        } else if (parsed.isFlat) {
          flatCount++;

          // Group by SignalK path
          const signalkPath = parsed.signalkPath || 'unknown';
          if (!byPath.has(signalkPath)) {
            byPath.set(signalkPath, { count: 0, size: 0 });
          }
          const pathStats = byPath.get(signalkPath)!;
          pathStats.count++;
          pathStats.size += stats.size;
        }
      } catch (error) {
        this.app.debug(`Failed to stat ${file}: ${(error as Error).message}`);
      }
    }

    // Determine source style
    let sourceStyle: 'flat' | 'mixed' | 'unknown' = 'unknown';
    if (flatCount > 0 && hiveCount === 0) {
      sourceStyle = 'flat';
    } else if (flatCount > 0 && hiveCount > 0) {
      sourceStyle = 'mixed';
    }

    // Estimate time based on file count and average processing rate
    // Assuming ~100 files per second
    const estimatedTime = Math.ceil(flatCount / 100);

    return {
      totalFiles: flatCount,
      totalSize,
      byPath,
      estimatedTime,
      sourceStyle,
    };
  }

  /**
   * Start migration job
   */
  async migrate(config: MigrationConfig): Promise<string> {
    const jobId = `migration_${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;

    const progress: MigrationProgress = {
      jobId,
      status: 'scanning',
      phase: 'scan',
      processed: 0,
      total: 0,
      percent: 0,
      startTime: new Date(),
      bytesProcessed: 0,
      filesMigrated: 0,
      filesSkipped: 0,
      errors: [],
    };

    migrationJobs.set(jobId, progress);
    this.cancelRequested = false;

    // Run migration in background
    this.runMigration(jobId, config).catch(error => {
      const job = migrationJobs.get(jobId);
      if (job) {
        job.status = 'error';
        job.error = (error as Error).message;
        job.completedAt = new Date();
      }
    });

    return jobId;
  }

  /**
   * Run the migration process
   */
  private async runMigration(jobId: string, config: MigrationConfig): Promise<void> {
    const progress = migrationJobs.get(jobId);
    if (!progress) return;

    try {
      // Phase 1: Scan
      progress.phase = 'scan';
      progress.status = 'scanning';

      const pattern = path.join(config.sourceDirectory, '**', '*.parquet');
      const files = await glob(pattern);

      // Filter to only flat-style files
      const flatFiles: string[] = [];
      for (const file of files) {
        const parsed = this.hivePathBuilder.detectPathStyle(file);
        if (parsed.isFlat) {
          flatFiles.push(file);
        }
      }

      progress.total = flatFiles.length;

      if (flatFiles.length === 0) {
        progress.status = 'completed';
        progress.completedAt = new Date();
        scheduleMigrationJobCleanup(jobId);
        return;
      }

      // Phase 2: Migrate
      progress.phase = 'migrate';
      progress.status = 'running';

      for (let i = 0; i < flatFiles.length; i++) {
        if (this.cancelRequested) {
          progress.status = 'cancelled';
          progress.completedAt = new Date();
          scheduleMigrationJobCleanup(jobId);
          return;
        }

        const file = flatFiles[i];
        progress.currentFile = path.basename(file);
        progress.processed = i + 1;
        progress.percent = Math.round(((i + 1) / flatFiles.length) * 100);

        try {
          const stats = await fs.stat(file);
          const targetPath = await this.migrateFile(file, config);

          if (targetPath) {
            progress.filesMigrated++;
            progress.bytesProcessed += stats.size;

            // Delete source if configured
            if (config.deleteSourceAfterMigration) {
              await fs.remove(file);
            }
          } else {
            progress.filesSkipped++;
          }
        } catch (error) {
          const errorMsg = `Failed to migrate ${file}: ${(error as Error).message}`;
          this.app.debug(errorMsg);
          progress.errors.push(errorMsg);
        }
      }

      // Phase 3: Cleanup empty directories
      if (config.deleteSourceAfterMigration) {
        progress.phase = 'cleanup';
        await this.cleanupEmptyDirectories(config.sourceDirectory);
      }

      progress.status = 'completed';
      progress.completedAt = new Date();
      scheduleMigrationJobCleanup(jobId);

    } catch (error) {
      progress.status = 'error';
      progress.error = (error as Error).message;
      progress.completedAt = new Date();
      scheduleMigrationJobCleanup(jobId);
    }
  }

  /**
   * Migrate a single file
   */
  private async migrateFile(
    sourcePath: string,
    config: MigrationConfig
  ): Promise<string | null> {
    const parsed = this.hivePathBuilder.detectPathStyle(sourcePath);

    if (!parsed.isFlat || !parsed.context || !parsed.signalkPath) {
      return null;
    }

    // Extract timestamp from source file
    const timestamp = await this.extractTimestampFromFile(sourcePath);
    if (!timestamp) {
      return null;
    }

    // Build target path
    const targetPath = this.hivePathBuilder.buildFilePath(
      config.targetDirectory,
      config.targetTier,
      parsed.context,
      parsed.signalkPath,
      timestamp,
      path.basename(sourcePath, '.parquet')
    );

    // Ensure target directory exists
    await fs.ensureDir(path.dirname(targetPath));

    // Copy or transform file
    // For simple migration, we can just copy. For complex cases, we might need DuckDB
    await fs.copy(sourcePath, targetPath);

    return targetPath;
  }

  /**
   * Extract the earliest timestamp from a parquet file
   */
  private async extractTimestampFromFile(filePath: string): Promise<Date | null> {
    try {
      const connection = await DuckDBPool.getConnection();
      try {
        const query = `
          SELECT MIN(received_timestamp) as min_ts
          FROM read_parquet('${filePath}')
        `;
        const result = await connection.runAndReadAll(query);
        const rows = result.getRowObjects();

        if (rows[0]?.min_ts) {
          return new Date(String(rows[0].min_ts));
        }
      } finally {
        connection.disconnectSync();
      }
    } catch (error) {
      // Fall back to filename parsing
      const filename = path.basename(filePath);
      const match = filename.match(/(\d{4})-?(\d{2})-?(\d{2})T?(\d{2})(\d{2})(\d{2})/);
      if (match) {
        return new Date(
          `${match[1]}-${match[2]}-${match[3]}T${match[4]}:${match[5]}:${match[6]}Z`
        );
      }
    }

    return null;
  }

  /**
   * Clean up empty directories after migration
   */
  private async cleanupEmptyDirectories(directory: string): Promise<void> {
    const entries = await fs.readdir(directory, { withFileTypes: true });

    for (const entry of entries) {
      if (entry.isDirectory()) {
        const subdir = path.join(directory, entry.name);
        await this.cleanupEmptyDirectories(subdir);

        // Check if directory is now empty
        const subEntries = await fs.readdir(subdir);
        if (subEntries.length === 0) {
          await fs.rmdir(subdir);
        }
      }
    }
  }

  /**
   * Get migration job progress
   */
  getProgress(jobId: string): MigrationProgress | null {
    return migrationJobs.get(jobId) || null;
  }

  /**
   * Cancel a running migration job
   */
  cancel(jobId: string): boolean {
    const job = migrationJobs.get(jobId);
    if (job && job.status === 'running') {
      this.cancelRequested = true;
      return true;
    }
    return false;
  }

  /**
   * Get all job IDs
   */
  getJobIds(): string[] {
    return Array.from(migrationJobs.keys());
  }
}
