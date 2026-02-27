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
   * Start the periodic export service
   */
  start(): void {
    // Export any pending records from crash recovery
    this.exportPending().catch(err => {
      this.app.error(`Initial export failed: ${err.message}`);
    });

    // Start periodic export
    const intervalMs = this.config.exportIntervalMinutes * 60 * 1000;
    this.exportInterval = setInterval(() => {
      this.exportPending().catch(err => {
        this.app.error(`Periodic export failed: ${err.message}`);
      });
    }, intervalMs);

    this.app.debug(`ParquetExportService started with ${this.config.exportIntervalMinutes} minute interval`);
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
      // Get pending records grouped by context:path
      const grouped = this.sqliteBuffer.getPendingRecordsGrouped();

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
      for (const [key, records] of grouped) {
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

            // Mark records as exported
            // We need to get the record IDs from the buffer
            const pendingRecords = this.sqliteBuffer.getPendingRecords(records.length);
            const recordIds = pendingRecords
              .filter(r => r.context === context && r.path === signalkPath)
              .map(r => r.id);

            if (recordIds.length > 0) {
              this.sqliteBuffer.markAsExported(recordIds, batchId);
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
    batchId: string
  ): Promise<string | null> {
    if (records.length === 0) return null;

    // Build file path
    let filePath: string;

    if (this.config.useHivePartitioning) {
      // Use Hive-style partitioning
      const timestamp = new Date(records[0].received_timestamp);
      filePath = this.hivePathBuilder.buildFilePath(
        this.config.outputDirectory,
        'raw',
        context,
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

    return path.join(dirPath, `${this.config.filenamePrefix}_${timestamp}.parquet`);
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
    const timestamp = new Date().toISOString().replace(/[:.]/g, '').slice(0, 15);
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
    totalExported: number;
    pendingRecords: number;
    exportIntervalMinutes: number;
  } {
    return {
      isRunning: this.exportInterval !== null,
      isExporting: this.isExporting,
      lastExportTime: this.lastExportTime,
      totalExported: this.totalExported,
      pendingRecords: this.sqliteBuffer.getPendingCount(),
      exportIntervalMinutes: this.config.exportIntervalMinutes,
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
}
