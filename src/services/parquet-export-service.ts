/**
 * Parquet Export Service
 *
 * Handles periodic export of data from SQLite buffer to Parquet files.
 * Provides crash recovery by checking for pending records on startup.
 */

import * as fs from 'fs-extra';
import * as path from 'path';
import { SQLiteBuffer } from '../utils/sqlite-buffer';
import { DataRecord, ParquetWriter } from '../types';
import { ServerAPI } from '@signalk/server-api';
import { HivePathBuilder } from '../utils/hive-path-builder';

export interface ExportServiceConfig {
  exportIntervalMinutes: number;
  outputDirectory: string;
  filenamePrefix: string;
  useHivePartitioning: boolean;
  s3Upload?: {
    enabled: boolean;
    timing?: 'realtime' | 'consolidation';
  };
}

export interface ExportResult {
  batchId: string;
  recordsExported: number;
  filesCreated: string[];
  duration: number;
  errors: string[];
}

export class ParquetExportService {
  private readonly sqliteBuffer: SQLiteBuffer;
  private readonly parquetWriter: ParquetWriter;
  private readonly config: ExportServiceConfig;
  private readonly app: ServerAPI;
  private readonly hivePathBuilder: HivePathBuilder;
  private exportInterval: NodeJS.Timeout | null = null;
  private isExporting: boolean = false;
  private lastExportTime: Date | null = null;
  private totalExported: number = 0;
  private lastBatchExported: number = 0;

  constructor(
    sqliteBuffer: SQLiteBuffer,
    parquetWriter: ParquetWriter,
    config: ExportServiceConfig,
    app: ServerAPI
  ) {
    this.sqliteBuffer = sqliteBuffer;
    this.parquetWriter = parquetWriter;
    this.config = config;
    this.app = app;
    this.hivePathBuilder = new HivePathBuilder();
  }

  /**
   * Start the export service
   *
   * Note: The periodic every-N-minutes export has been removed. Daily exports
   * are now scheduled from index.ts using exportDayToParquet(). This method
   * only performs initial crash recovery export.
   */
  start(): void {
    // Export any pending records from crash recovery (for records that weren't
    // exported before a crash or restart)
    this.exportPending().catch(err => {
      this.app.error(`Initial export failed: ${err.message}`);
    });

    // Note: Periodic export removed - daily export now handles this
    this.app.debug(
      `ParquetExportService started (daily export mode, interval export disabled)`
    );
  }

  /**
   * Stop the export service
   */
  stop(): void {
    if (this.exportInterval) {
      clearInterval(this.exportInterval);
      this.exportInterval = null;
    }
    this.app.debug('ParquetExportService stopped');
  }

  /**
   * Force an immediate export of pending records
   */
  async forceExport(): Promise<ExportResult> {
    return this.exportPending();
  }

  /**
   * Export all pending records from the SQLite buffer to Parquet files
   */
  async exportPending(): Promise<ExportResult> {
    if (this.isExporting) {
      this.app.debug('Export already in progress, skipping');
      return {
        batchId: '',
        recordsExported: 0,
        filesCreated: [],
        duration: 0,
        errors: ['Export already in progress'],
      };
    }

    this.isExporting = true;
    const startTime = Date.now();
    const batchId = this.generateBatchId();
    const filesCreated: string[] = [];
    const errors: string[] = [];
    let recordsExported = 0;

    try {
      // Get pending records grouped by context:path, WITH their IDs
      const grouped = this.sqliteBuffer.getPendingRecordsGroupedWithIds();

      if (grouped.size === 0) {
        this.app.debug('No pending records to export');
        return {
          batchId,
          recordsExported: 0,
          filesCreated: [],
          duration: Date.now() - startTime,
          errors: [],
        };
      }

      this.app.debug(`Exporting ${grouped.size} path groups to Parquet`);

      // Export each group to a separate file
      for (const [key, { records, ids }] of grouped) {
        try {
          const [context, signalkPath] = this.parseBufferKey(key);
          const filePath = await this.exportGroup(
            context,
            signalkPath,
            records,
            batchId
          );

          if (filePath) {
            filesCreated.push(filePath);
            recordsExported += records.length;

            // Mark records as exported using the IDs we already have
            if (ids.length > 0) {
              this.sqliteBuffer.markAsExported(ids, batchId);
            }
          }
        } catch (error) {
          const errorMsg = `Failed to export ${key}: ${(error as Error).message}`;
          this.app.error(errorMsg);
          errors.push(errorMsg);
        }
      }

      // Cleanup old exported records
      const cleaned = this.sqliteBuffer.cleanup();
      if (cleaned > 0) {
        this.app.debug(`Cleaned up ${cleaned} old exported records`);
      }

      this.lastExportTime = new Date();
      this.lastBatchExported = recordsExported;
      this.totalExported += recordsExported;

      this.app.debug(
        `Export complete: ${recordsExported} records to ${filesCreated.length} files in ${Date.now() - startTime}ms`
      );

      return {
        batchId,
        recordsExported,
        filesCreated,
        duration: Date.now() - startTime,
        errors,
      };
    } finally {
      this.isExporting = false;
    }
  }

  /**
   * Export a group of records for a specific context/path
   */
  private async exportGroup(
    context: string,
    signalkPath: string,
    records: DataRecord[],
    _batchId: string
  ): Promise<string | null> {
    if (records.length === 0) return null;

    // Build file path
    let filePath: string;

    if (this.config.useHivePartitioning) {
      // Resolve vessels.self to actual vessel context
      const resolvedContext =
        context === 'vessels.self' ? this.app.selfContext : context;
      // Use Hive-style partitioning with CURRENT time for unique filename
      // (not records[0].received_timestamp which could cause overwrites)
      const timestamp = new Date();
      filePath = this.hivePathBuilder.buildFilePath(
        this.config.outputDirectory,
        'raw',
        resolvedContext,
        signalkPath,
        timestamp,
        this.config.filenamePrefix
      );
    } else {
      // Use legacy flat structure
      filePath = this.buildFlatFilePath(context, signalkPath);
    }

    // Ensure directory exists
    await fs.ensureDir(path.dirname(filePath));

    // Write to temp file first for atomic operation
    const tempFilePath = filePath + '.tmp';

    try {
      // Write records to temp file
      await this.parquetWriter.writeRecords(tempFilePath, records);

      // Validate the written file
      const stats = await fs.stat(tempFilePath);
      if (stats.size < 100) {
        throw new Error('Written file is too small, likely corrupt');
      }

      // Atomic rename
      await fs.rename(tempFilePath, filePath);

      return filePath;
    } catch (error) {
      // Clean up temp file on failure
      try {
        await fs.remove(tempFilePath);
      } catch {
        // Ignore cleanup errors
      }
      throw error;
    }
  }

  /**
   * Build a flat-structure file path (legacy compatibility)
   */
  private buildFlatFilePath(context: string, signalkPath: string): string {
    // Clean context for filesystem
    let contextPath: string;
    if (context === 'vessels.self') {
      contextPath = this.app.selfContext.replace(/\./g, '/').replace(/:/g, '_');
    } else if (context.startsWith('vessels.')) {
      const vesselId = context.replace('vessels.', '').replace(/:/g, '_');
      contextPath = `vessels/${vesselId}`;
    } else {
      contextPath = context.replace(/:/g, '_').replace(/\./g, '/');
    }

    const dirPath = path.join(
      this.config.outputDirectory,
      contextPath,
      signalkPath.replace(/\./g, '/')
    );

    const timestamp = new Date()
      .toISOString()
      .replace(/[:.]/g, '')
      .slice(0, 15);

    return path.join(
      dirPath,
      `${this.config.filenamePrefix}_${timestamp}.parquet`
    );
  }

  /**
   * Parse a buffer key into context and path
   */
  private parseBufferKey(key: string): [string, string] {
    // Buffer key format: "context:path"
    // Need to handle cases like "vessels.urn:mrn:signalk:uuid:xxx:navigation.speedOverGround"
    const pathMatch = key.match(/^(.+):([a-zA-Z][a-zA-Z0-9._]*)$/);
    if (pathMatch) {
      return [pathMatch[1], pathMatch[2]];
    }
    // Fallback: split on last colon
    const lastColon = key.lastIndexOf(':');
    if (lastColon > 0) {
      return [key.substring(0, lastColon), key.substring(lastColon + 1)];
    }
    return ['vessels.self', key];
  }

  /**
   * Generate a unique batch ID
   */
  private generateBatchId(): string {
    const timestamp = new Date()
      .toISOString()
      .replace(/[:.]/g, '')
      .slice(0, 15);
    const random = Math.random().toString(36).substring(2, 8);
    return `batch_${timestamp}_${random}`;
  }

  /**
   * Get service status
   */
  getStatus(): {
    isRunning: boolean;
    isExporting: boolean;
    lastExportTime: Date | null;
    lastBatchExported: number;
    totalExported: number;
    pendingRecords: number;
    exportIntervalMinutes: number;
    mode: 'daily' | 'interval';
  } {
    return {
      isRunning: true, // Always running in daily mode (scheduled from index.ts)
      isExporting: this.isExporting,
      lastExportTime: this.lastExportTime,
      lastBatchExported: this.lastBatchExported,
      totalExported: this.totalExported,
      pendingRecords: this.sqliteBuffer.getPendingCount(),
      exportIntervalMinutes: this.config.exportIntervalMinutes,
      mode: 'daily', // New simplified pipeline uses daily exports
    };
  }

  /**
   * Get health check information
   */
  getHealth(): {
    healthy: boolean;
    lastExportTime: Date | null;
    pendingRecords: number;
    bufferStats: ReturnType<SQLiteBuffer['getStats']>;
  } {
    const stats = this.sqliteBuffer.getStats();
    const healthy = this.exportInterval !== null && !this.isExporting;

    return {
      healthy,
      lastExportTime: this.lastExportTime,
      pendingRecords: stats.pendingRecords,
      bufferStats: stats,
    };
  }

  /**
   * Export all unexported data from SQLite to Parquet (complete days only)
   * Used at startup to catch up on any missed exports.
   * Excludes today's data to avoid creating partial day files that would
   * conflict with the daily export.
   */
  async exportAllUnexported(): Promise<ExportResult> {
    const startTime = Date.now();
    const batchId = this.generateBatchId();
    let totalRecordsExported = 0;
    const allFilesCreated: string[] = [];
    const allErrors: string[] = [];

    // Get dates with unexported records (excludes today to avoid partial files)
    const dates = this.sqliteBuffer.getDatesWithUnexportedRecords(true);

    if (dates.length === 0) {
      this.app.debug('[StartupExport] No unexported records found');
      return {
        batchId,
        recordsExported: 0,
        filesCreated: [],
        duration: Date.now() - startTime,
        errors: [],
      };
    }

    this.app.debug(
      `[StartupExport] Found unexported records for ${dates.length} dates: ${dates.join(', ')}`
    );

    // Export each date
    for (const dateStr of dates) {
      const targetDate = new Date(dateStr + 'T00:00:00.000Z');

      try {
        const result = await this.exportDayToParquet(targetDate);
        totalRecordsExported += result.recordsExported;
        allFilesCreated.push(...result.filesCreated);
        allErrors.push(...result.errors);

        if (result.recordsExported > 0) {
          this.app.debug(
            `[StartupExport] Exported ${result.recordsExported} records for ${dateStr}`
          );
        }
      } catch (error) {
        const errorMsg = `[StartupExport] Failed to export ${dateStr}: ${(error as Error).message}`;
        this.app.error(errorMsg);
        allErrors.push(errorMsg);
      }
    }

    this.app.debug(
      `[StartupExport] Complete: ${totalRecordsExported} records to ${allFilesCreated.length} files in ${Date.now() - startTime}ms`
    );

    return {
      batchId,
      recordsExported: totalRecordsExported,
      filesCreated: allFilesCreated,
      duration: Date.now() - startTime,
      errors: allErrors,
    };
  }

  /**
   * Export a full day's data to Parquet files (one file per context/path)
   * This creates consolidated daily files directly, without needing a separate
   * consolidation step.
   *
   * @param targetDate The date to export (UTC). Typically yesterday.
   * @returns Export result with details about files created
   */
  async exportDayToParquet(targetDate: Date): Promise<ExportResult> {
    if (this.isExporting) {
      this.app.debug('Export already in progress, skipping daily export');
      return {
        batchId: '',
        recordsExported: 0,
        filesCreated: [],
        duration: 0,
        errors: ['Export already in progress'],
      };
    }

    this.isExporting = true;
    const startTime = Date.now();
    const batchId = this.generateBatchId();
    const filesCreated: string[] = [];
    const errors: string[] = [];
    let recordsExported = 0;

    const dateStr = targetDate.toISOString().slice(0, 10);
    this.app.debug(`[DailyExport] Starting daily export for ${dateStr}`);

    try {
      // Get all distinct context/path combinations for this date
      const pathsForDate = this.sqliteBuffer.getPathsForDate(targetDate);

      if (pathsForDate.length === 0) {
        this.app.debug(`[DailyExport] No data found for ${dateStr}`);
        return {
          batchId,
          recordsExported: 0,
          filesCreated: [],
          duration: Date.now() - startTime,
          errors: [],
        };
      }

      this.app.debug(
        `[DailyExport] Found ${pathsForDate.length} paths with data for ${dateStr}`
      );

      // Export each context/path to its own file
      for (const { context, path: signalkPath } of pathsForDate) {
        try {
          const { records, ids } = this.sqliteBuffer.getRecordsForPathAndDate(
            context,
            signalkPath,
            targetDate
          );

          if (records.length === 0) {
            continue;
          }

          // Build file path for daily file
          const filePath = await this.exportDailyGroup(
            context,
            signalkPath,
            records,
            targetDate,
            batchId
          );

          if (filePath) {
            filesCreated.push(filePath);
            recordsExported += records.length;

            // Mark records as exported using the IDs we already have
            if (ids.length > 0) {
              this.sqliteBuffer.markAsExported(ids, batchId);
            }

            this.app.debug(
              `[DailyExport] Exported ${records.length} records for ${context}:${signalkPath}`
            );
          }
        } catch (error) {
          const errorMsg = `[DailyExport] Failed to export ${context}:${signalkPath}: ${(error as Error).message}`;
          this.app.error(errorMsg);
          errors.push(errorMsg);
        }
      }

      // Cleanup old exported records
      const cleaned = this.sqliteBuffer.cleanup();
      if (cleaned > 0) {
        this.app.debug(
          `[DailyExport] Cleaned up ${cleaned} old exported records`
        );
      }

      this.lastExportTime = new Date();
      this.lastBatchExported = recordsExported;
      this.totalExported += recordsExported;

      this.app.debug(
        `[DailyExport] Complete: ${recordsExported} records to ${filesCreated.length} files in ${Date.now() - startTime}ms`
      );

      return {
        batchId,
        recordsExported,
        filesCreated,
        duration: Date.now() - startTime,
        errors,
      };
    } finally {
      this.isExporting = false;
    }
  }

  /**
   * Export a group of records as a timestamped file
   * Uses consistent timestamped naming (same as startup exports) for simplicity
   */
  private async exportDailyGroup(
    context: string,
    signalkPath: string,
    records: DataRecord[],
    targetDate: Date,
    _batchId: string
  ): Promise<string | null> {
    if (records.length === 0) return null;

    // Build file path with timestamp (consistent with exportGroup)
    let filePath: string;

    if (this.config.useHivePartitioning) {
      // Resolve vessels.self to actual vessel context
      const resolvedContext =
        context === 'vessels.self' ? this.app.selfContext : context;

      // Use Hive-style partitioning
      // Directory is based on targetDate (for correct day partition)
      // Filename uses CURRENT time for uniqueness (avoid overwrites)
      const dirPath = this.hivePathBuilder.buildPath(
        this.config.outputDirectory,
        'raw',
        resolvedContext,
        signalkPath,
        targetDate
      );
      const timestampStr = new Date()
        .toISOString()
        .replace(/[:.]/g, '')
        .slice(0, 15);
      filePath = path.join(
        dirPath,
        `${this.config.filenamePrefix}_${timestampStr}.parquet`
      );
    } else {
      // Use legacy flat structure with timestamp
      filePath = this.buildFlatFilePath(context, signalkPath);
    }

    // Ensure directory exists
    await fs.ensureDir(path.dirname(filePath));

    // Write to temp file first for atomic operation
    const tempFilePath = filePath + '.tmp';

    try {
      // Write records to temp file
      await this.parquetWriter.writeRecords(tempFilePath, records);

      // Validate the written file
      const stats = await fs.stat(tempFilePath);
      if (stats.size < 100) {
        throw new Error('Written file is too small, likely corrupt');
      }

      // Atomic rename
      await fs.rename(tempFilePath, filePath);

      return filePath;
    } catch (error) {
      // Clean up temp file on failure
      try {
        await fs.remove(tempFilePath);
      } catch {
        // Ignore cleanup errors
      }
      throw error;
    }
  }
}
