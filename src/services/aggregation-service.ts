/**
 * Aggregation Service
 *
 * Handles multi-tier data aggregation from raw data to progressively
 * downsampled tiers for efficient long-term storage and querying.
 *
 * Tiers:
 * - raw: Original data (~1s resolution)
 * - 5s: 5-second aggregates
 * - 60s: 1-minute aggregates
 * - 1h: Hourly aggregates
 */

import * as fs from 'fs-extra';
import * as path from 'path';
import { glob } from 'glob';
import { ServerAPI } from '@signalk/server-api';
import { DuckDBPool } from '../utils/duckdb-pool';
import { HivePathBuilder, AggregationTier } from '../utils/hive-path-builder';

export interface AggregationConfig {
  outputDirectory: string;
  filenamePrefix: string;
  retentionDays: {
    raw: number;
    '5s': number;
    '60s': number;
    '1h': number;
  };
}

export interface AggregationProgress {
  jobId: string;
  status: 'running' | 'completed' | 'cancelled' | 'error';
  tier: AggregationTier;
  processed: number;
  total: number;
  currentFile?: string;
  startTime: Date;
  completedAt?: Date;
  error?: string;
}

export interface AggregationResult {
  sourceTier: AggregationTier;
  targetTier: AggregationTier;
  filesProcessed: number;
  recordsAggregated: number;
  filesCreated: number;
  duration: number;
  errors: string[];
}

const TIER_INTERVALS: Record<AggregationTier, number> = {
  raw: 1,
  '5s': 5,
  '60s': 60,
  '1h': 3600,
};

const TIER_HIERARCHY: AggregationTier[] = ['raw', '5s', '60s', '1h'];

export class AggregationService {
  private readonly config: AggregationConfig;
  private readonly app: ServerAPI;
  private readonly hivePathBuilder: HivePathBuilder;
  private currentJob: AggregationProgress | null = null;
  private cancelRequested: boolean = false;

  constructor(config: AggregationConfig, app: ServerAPI) {
    this.config = config;
    this.app = app;
    this.hivePathBuilder = new HivePathBuilder();
  }

  /**
   * Run aggregation for a specific date
   */
  async aggregateDate(date: Date): Promise<AggregationResult[]> {
    const results: AggregationResult[] = [];

    // Aggregate through the hierarchy: raw -> 5s -> 60s -> 1h
    for (let i = 0; i < TIER_HIERARCHY.length - 1; i++) {
      const sourceTier = TIER_HIERARCHY[i];
      const targetTier = TIER_HIERARCHY[i + 1];

      try {
        const result = await this.aggregateTier(sourceTier, targetTier, date);
        results.push(result);
      } catch (error) {
        this.app.error(
          `Aggregation ${sourceTier} -> ${targetTier} failed: ${(error as Error).message}`
        );
        results.push({
          sourceTier,
          targetTier,
          filesProcessed: 0,
          recordsAggregated: 0,
          filesCreated: 0,
          duration: 0,
          errors: [(error as Error).message],
        });
      }
    }

    return results;
  }

  /**
   * Aggregate from one tier to the next
   */
  async aggregateTier(
    sourceTier: AggregationTier,
    targetTier: AggregationTier,
    date: Date
  ): Promise<AggregationResult> {
    const startTime = Date.now();
    const errors: string[] = [];
    let filesProcessed = 0;
    let recordsAggregated = 0;
    let filesCreated = 0;

    const year = date.getUTCFullYear();
    const dayOfYear = this.hivePathBuilder.getDayOfYear(date);

    // Find all source files for this date
    const sourcePattern = path.join(
      this.config.outputDirectory,
      `tier=${sourceTier}`,
      'context=*',
      'path=*',
      `year=${year}`,
      `day=${String(dayOfYear).padStart(3, '0')}`,
      '*.parquet'
    );

    const sourceFiles = await glob(sourcePattern);

    if (sourceFiles.length === 0) {
      this.app.debug(
        `No source files found for ${sourceTier} on ${date.toISOString().slice(0, 10)}`
      );
      return {
        sourceTier,
        targetTier,
        filesProcessed: 0,
        recordsAggregated: 0,
        filesCreated: 0,
        duration: Date.now() - startTime,
        errors: [],
      };
    }

    // Group files by context and path
    const fileGroups = this.groupFilesByContextPath(sourceFiles);

    // Process each group
    for (const [key, files] of fileGroups) {
      if (this.cancelRequested) break;

      try {
        const { context, signalkPath } = this.parseGroupKey(key);
        const result = await this.aggregateGroup(
          files,
          context,
          signalkPath,
          sourceTier,
          targetTier,
          date
        );

        filesProcessed += files.length;
        recordsAggregated += result.recordsAggregated;
        if (result.outputFile) {
          filesCreated++;
        }
      } catch (error) {
        const errorMsg = `Failed to aggregate ${key}: ${(error as Error).message}`;
        this.app.error(errorMsg);
        errors.push(errorMsg);
      }
    }

    return {
      sourceTier,
      targetTier,
      filesProcessed,
      recordsAggregated,
      filesCreated,
      duration: Date.now() - startTime,
      errors,
    };
  }

  /**
   * Aggregate a group of files for a specific context/path
   */
  private async aggregateGroup(
    files: string[],
    context: string,
    signalkPath: string,
    sourceTier: AggregationTier,
    targetTier: AggregationTier,
    date: Date
  ): Promise<{ recordsAggregated: number; outputFile: string | null }> {
    const intervalSeconds = TIER_INTERVALS[targetTier];
    const isSourceRaw = sourceTier === 'raw';
    const fileListStr = files.map(f => `'${f}'`).join(', ');

    // Check schema compatibility - skip object-type paths (like position with value_latitude/value_longitude)
    const connection = await DuckDBPool.getConnection();
    try {
      const schemaQuery = `SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet([${fileListStr}], union_by_name=true))`;
      const schemaResult = await connection.runAndReadAll(schemaQuery);
      const columns = schemaResult
        .getRowObjects()
        .map((r: Record<string, unknown>) => r.column_name as string);

      // For raw tier, we need 'value' column; for aggregated tiers, we need 'bucket_time' and 'value_avg'
      const requiredColumn = isSourceRaw ? 'value' : 'bucket_time';
      if (!columns.includes(requiredColumn)) {
        this.app.debug(
          `Skipping ${signalkPath}: no '${requiredColumn}' column (object-type data stays in raw tier)`
        );
        connection.disconnectSync();
        return { recordsAggregated: 0, outputFile: null };
      }
    } catch (error) {
      connection.disconnectSync();
      throw error;
    }

    // Build output path
    const outputDir = this.hivePathBuilder.buildPath(
      this.config.outputDirectory,
      targetTier,
      context,
      signalkPath,
      date
    );

    await fs.ensureDir(outputDir);

    const outputFile = path.join(
      outputDir,
      `${this.config.filenamePrefix}_${date.toISOString().slice(0, 10)}_aggregated.parquet`
    );

    // Use different query depending on source tier schema
    // Raw tier has: received_timestamp, value
    // Aggregated tiers have: bucket_time, value_avg, value_min, value_max, sample_count, first_timestamp, last_timestamp
    const query = isSourceRaw
      ? `
        COPY (
          SELECT
            time_bucket(INTERVAL '${intervalSeconds} seconds', received_timestamp::TIMESTAMP) as bucket_time,
            context,
            path,
            AVG(CASE WHEN value IS NOT NULL AND TRY_CAST(value AS DOUBLE) IS NOT NULL THEN CAST(value AS DOUBLE) END) as value_avg,
            MIN(CASE WHEN value IS NOT NULL AND TRY_CAST(value AS DOUBLE) IS NOT NULL THEN CAST(value AS DOUBLE) END) as value_min,
            MAX(CASE WHEN value IS NOT NULL AND TRY_CAST(value AS DOUBLE) IS NOT NULL THEN CAST(value AS DOUBLE) END) as value_max,
            COUNT(*) as sample_count,
            MIN(received_timestamp) as first_timestamp,
            MAX(received_timestamp) as last_timestamp
          FROM read_parquet([${fileListStr}], union_by_name=true)
          GROUP BY bucket_time, context, path
          ORDER BY bucket_time
        ) TO '${outputFile}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
      `
      : `
        COPY (
          SELECT
            time_bucket(INTERVAL '${intervalSeconds} seconds', src_bucket_time::TIMESTAMP) as bucket_time,
            context,
            path,
            SUM(value_avg * sample_count) / SUM(sample_count) as value_avg,
            MIN(value_min) as value_min,
            MAX(value_max) as value_max,
            SUM(sample_count)::BIGINT as sample_count,
            MIN(first_timestamp) as first_timestamp,
            MAX(last_timestamp) as last_timestamp
          FROM (
            SELECT bucket_time as src_bucket_time, context, path, value_avg, value_min, value_max, sample_count, first_timestamp, last_timestamp
            FROM read_parquet([${fileListStr}], union_by_name=true)
          ) src
          GROUP BY time_bucket(INTERVAL '${intervalSeconds} seconds', src_bucket_time::TIMESTAMP), context, path
          ORDER BY 1
        ) TO '${outputFile}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
      `;

    try {
      await connection.runAndReadAll(query);

      // Get record count from output
      const countQuery = `SELECT COUNT(*) as cnt FROM read_parquet('${outputFile}')`;
      const countResult = await connection.runAndReadAll(countQuery);
      const rows = countResult.getRowObjects();
      const recordCount = rows[0]?.cnt || 0;

      return {
        recordsAggregated: Number(recordCount),
        outputFile,
      };
    } finally {
      connection.disconnectSync();
    }
  }

  /**
   * Group files by context and path
   */
  private groupFilesByContextPath(files: string[]): Map<string, string[]> {
    const groups = new Map<string, string[]>();

    for (const file of files) {
      const parsed = this.hivePathBuilder.detectPathStyle(file);
      if (parsed.isHive && parsed.context && parsed.signalkPath) {
        const key = `${parsed.context}:${parsed.signalkPath}`;
        if (!groups.has(key)) {
          groups.set(key, []);
        }
        groups.get(key)!.push(file);
      }
    }

    return groups;
  }

  /**
   * Parse a group key back to context and path
   */
  private parseGroupKey(key: string): { context: string; signalkPath: string } {
    const lastColon = key.lastIndexOf(':');
    if (lastColon > 0) {
      return {
        context: key.substring(0, lastColon),
        signalkPath: key.substring(lastColon + 1),
      };
    }
    return { context: 'vessels.self', signalkPath: key };
  }

  /**
   * Clean up old data based on retention settings
   */
  async cleanupOldData(): Promise<{
    deletedFiles: number;
    freedBytes: number;
  }> {
    let deletedFiles = 0;
    let freedBytes = 0;

    const now = new Date();

    for (const tier of TIER_HIERARCHY) {
      const retentionDays = this.config.retentionDays[tier];
      const cutoffDate = new Date(now);
      cutoffDate.setUTCDate(cutoffDate.getUTCDate() - retentionDays);

      // Find files older than retention period
      const pattern = path.join(
        this.config.outputDirectory,
        `tier=${tier}`,
        '**',
        '*.parquet'
      );

      const files = await glob(pattern);

      for (const file of files) {
        try {
          const parsed = this.hivePathBuilder.detectPathStyle(file);
          if (parsed.isHive && parsed.year && parsed.dayOfYear) {
            const fileDate = this.hivePathBuilder.dateFromDayOfYear(
              parsed.year,
              parsed.dayOfYear
            );

            if (fileDate < cutoffDate) {
              const stats = await fs.stat(file);
              freedBytes += stats.size;
              await fs.remove(file);
              deletedFiles++;
            }
          }
        } catch (error) {
          this.app.debug(
            `Failed to check/delete ${file}: ${(error as Error).message}`
          );
        }
      }
    }

    this.app.debug(
      `Cleanup: deleted ${deletedFiles} files, freed ${(freedBytes / 1024 / 1024).toFixed(2)} MB`
    );

    return { deletedFiles, freedBytes };
  }

  /**
   * Get current job progress
   */
  getProgress(): AggregationProgress | null {
    return this.currentJob;
  }

  /**
   * Cancel current job
   */
  cancel(): void {
    this.cancelRequested = true;
  }

  /**
   * Schedule daily aggregation (call from consolidation timer)
   */
  async runDailyAggregation(): Promise<AggregationResult[]> {
    const yesterday = new Date();
    yesterday.setUTCDate(yesterday.getUTCDate() - 1);

    this.app.debug(
      `Running daily aggregation for ${yesterday.toISOString().slice(0, 10)}`
    );

    const results = await this.aggregateDate(yesterday);

    // Also run cleanup
    await this.cleanupOldData();

    return results;
  }
}
