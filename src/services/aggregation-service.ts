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
import { isAngularPath } from '../utils/angular-paths';
import { CLEANUP_YIELD_INTERVAL, POSITION_MAX_SPEED_MPS } from '../constants';
import { PathRetentionRule, RetentionRuleSet } from '../utils/retention-rules';

export interface AggregationConfig {
  outputDirectory: string;
  filenamePrefix: string;
  // Per-tier retention in days. 0 means "keep forever". Tiers above raw
  // are multiples of the raw value by convention (5s=2x, 60s=4x,
  // 1h=12x), but they are passed in independently so the route layer
  // can override.
  retentionDays: {
    raw: number;
    '5s': number;
    '60s': number;
    '1h': number;
  };
  // Optional per-path overrides applied on top of the tier defaults.
  // The override's `days` value is the raw retention; the same tier
  // multipliers (2/4/12x) scale it to upper tiers. `skipAggregation`
  // removes a path from the rollup pipeline entirely.
  pathRetentionOverrides?: PathRetentionRule[];
}

// Tier multipliers used when scaling a raw-tier retention to upper
// tiers. The same convention applies to the global default and to
// per-path overrides; exported so callers in index.ts and the route
// layer build identical AggregationConfig.retentionDays without
// drifting from this file.
export const TIER_RETENTION_MULTIPLIER: Record<AggregationTier, number> = {
  raw: 1,
  '5s': 2,
  '60s': 4,
  '1h': 12,
};

/**
 * Build the per-tier retention block from a raw-tier value. 0 stays
 * 0 in every tier (= keep forever).
 */
export function buildPerTierRetention(
  rawDays: number
): AggregationConfig['retentionDays'] {
  return {
    raw: rawDays * TIER_RETENTION_MULTIPLIER.raw,
    '5s': rawDays * TIER_RETENTION_MULTIPLIER['5s'],
    '60s': rawDays * TIER_RETENTION_MULTIPLIER['60s'],
    '1h': rawDays * TIER_RETENTION_MULTIPLIER['1h'],
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

export interface BulkAggregationProgress {
  jobId: string;
  status: 'scanning' | 'running' | 'completed' | 'cancelled' | 'error';
  phase: 'scan' | 'aggregation';
  currentDate?: string;
  datesProcessed: number;
  datesTotal: number;
  percent: number;
  filesCreated: number;
  recordsAggregated: number;
  startTime: Date;
  completedAt?: Date;
  error?: string;
  errors: string[];
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

const bulkAggregationJobs = new Map<string, BulkAggregationProgress>();
const BULK_JOB_TTL_MS = 60 * 60 * 1000; // 1 hour

function scheduleBulkJobCleanup(jobId: string) {
  setTimeout(() => {
    const job = bulkAggregationJobs.get(jobId);
    if (job && job.status !== 'running' && job.status !== 'scanning') {
      bulkAggregationJobs.delete(jobId);
    }
  }, BULK_JOB_TTL_MS);
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
  private readonly retentionRules: RetentionRuleSet;
  private currentJob: AggregationProgress | null = null;
  private cancelRequested: boolean = false;

  constructor(config: AggregationConfig, app: ServerAPI) {
    this.config = config;
    this.app = app;
    this.hivePathBuilder = new HivePathBuilder();
    this.retentionRules = new RetentionRuleSet(
      config.pathRetentionOverrides || [],
      (rule, err) => {
        // A pattern that fails to compile is dropped; log so the
        // operator sees the offending rule rather than silently losing
        // it. Doesn't block plugin start.
        this.app.error(
          `[Retention] Dropping rule with invalid pattern '${rule.pattern}': ${err.message}`
        );
      }
    );
  }

  /**
   * Run aggregation for a specific date.
   * Optional pathFilter restricts which signalk paths are aggregated (used by
   * targeted migrations like position re-aggregation).
   */
  async aggregateDate(
    date: Date,
    pathFilter?: (signalkPath: string) => boolean
  ): Promise<AggregationResult[]> {
    const results: AggregationResult[] = [];

    // Aggregate through the hierarchy: raw -> 5s -> 60s -> 1h
    for (let i = 0; i < TIER_HIERARCHY.length - 1; i++) {
      const sourceTier = TIER_HIERARCHY[i];
      const targetTier = TIER_HIERARCHY[i + 1];

      try {
        const result = await this.aggregateTier(
          sourceTier,
          targetTier,
          date,
          pathFilter
        );
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
    date: Date,
    pathFilter?: (signalkPath: string) => boolean
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

    const allSourceFiles = await glob(sourcePattern);
    // Exclude files in processed, quarantine, failed, repaired directories
    const sourceFiles = allSourceFiles.filter(
      f =>
        !f.includes('/processed/') &&
        !f.includes('/quarantine/') &&
        !f.includes('/failed/') &&
        !f.includes('/repaired/')
    );

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
        if (pathFilter && !pathFilter(signalkPath)) continue;
        // Honour per-path skipAggregation. These paths live only in
        // tier=raw and are cleaned up directly via cleanupOldData.
        if (this.retentionRules.shouldSkipAggregation(signalkPath)) {
          this.app.debug(
            `Skipping aggregation for ${signalkPath}: matched a retention rule with skipAggregation=true`
          );
          continue;
        }
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

    // Detect schema: scalar ('value' / 'value_avg'), position ('value_latitude'/'value_longitude'),
    // or unsupported object-type (skip).
    const connection = await DuckDBPool.getConnection();
    let isPosition = false;
    try {
      const schemaQuery = `SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet([${fileListStr}], union_by_name=true))`;
      const schemaResult = await connection.runAndReadAll(schemaQuery);
      const columns = schemaResult
        .getRowObjects()
        .map((r: Record<string, unknown>) => r.column_name as string);

      const hasLatLon =
        columns.includes('value_latitude') &&
        columns.includes('value_longitude');
      const requiredColumn = isSourceRaw ? 'value' : 'bucket_time';
      const hasScalarColumn = columns.includes(requiredColumn);

      if (hasLatLon) {
        isPosition = true;
      } else if (!hasScalarColumn) {
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

    // Use different query depending on source tier schema and whether the path is angular/position
    // Raw tier has: received_timestamp, value (or value_latitude/value_longitude for position)
    // Aggregated tiers have: bucket_time, value_avg/value_latitude, value_min, value_max, sample_count, first_timestamp, last_timestamp
    const angular =
      !isPosition && isAngularPath(signalkPath, this.app, context);
    const query = isPosition
      ? this.buildPositionAggregationQuery(
          fileListStr,
          intervalSeconds,
          isSourceRaw,
          outputFile
        )
      : this.buildAggregationQuery(
          fileListStr,
          intervalSeconds,
          isSourceRaw,
          angular,
          outputFile
        );

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
   * Build the aggregation SQL query, branching on angular vs scalar paths
   */
  private buildAggregationQuery(
    fileListStr: string,
    intervalSeconds: number,
    isSourceRaw: boolean,
    isAngular: boolean,
    outputFile: string
  ): string {
    if (isAngular) {
      return this.buildAngularAggregationQuery(
        fileListStr,
        intervalSeconds,
        isSourceRaw,
        outputFile
      );
    }

    // Standard scalar aggregation
    if (isSourceRaw) {
      return `
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
      `;
    }

    return `
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
  }

  /**
   * Build angular-specific aggregation query using vector decomposition:
   * ATAN2(AVG(SIN(value)), AVG(COS(value)))
   */
  private buildAngularAggregationQuery(
    fileListStr: string,
    intervalSeconds: number,
    isSourceRaw: boolean,
    outputFile: string
  ): string {
    if (isSourceRaw) {
      // Vector average from raw radian values
      return `
        COPY (
          SELECT
            time_bucket(INTERVAL '${intervalSeconds} seconds', received_timestamp::TIMESTAMP) as bucket_time,
            context,
            path,
            ATAN2(
              AVG(SIN(CAST(value AS DOUBLE))),
              AVG(COS(CAST(value AS DOUBLE)))
            ) as value_avg,
            NULL::DOUBLE as value_min,
            NULL::DOUBLE as value_max,
            COUNT(*) as sample_count,
            AVG(SIN(CAST(value AS DOUBLE))) as value_sin_avg,
            AVG(COS(CAST(value AS DOUBLE))) as value_cos_avg,
            MIN(received_timestamp) as first_timestamp,
            MAX(received_timestamp) as last_timestamp
          FROM read_parquet([${fileListStr}], union_by_name=true)
          WHERE value IS NOT NULL AND TRY_CAST(value AS DOUBLE) IS NOT NULL
          GROUP BY bucket_time, context, path
          ORDER BY bucket_time
        ) TO '${outputFile}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
      `;
    }

    // Re-aggregate from already-aggregated data using the stored sin/cos
    // averages, weighted by sample_count. COALESCE falls back to deriving
    // sin/cos from value_avg when an upstream tier predates the sin/cos columns,
    // or metadata flapped so the angular columns are absent: without it a
    // missing value_sin_avg/value_cos_avg makes the whole bucket NULL. This
    // mirrors the query-time fallback in HistoryAPI's tier aggregation.
    // Example: two source buckets at 10deg (0.1745 rad) and 350deg (6.1087 rad)
    // with NULL sin/cos -> COALESCE uses SIN/COS(value_avg) ->
    // ATAN2(~0, ~0.985) -> ~0 rad (0/360deg), not a 180deg arithmetic mean.
    return `
      COPY (
        SELECT
          time_bucket(INTERVAL '${intervalSeconds} seconds', src_bucket_time::TIMESTAMP) as bucket_time,
          context,
          path,
          ATAN2(
            SUM(COALESCE(value_sin_avg, SIN(value_avg)) * sample_count) / SUM(sample_count),
            SUM(COALESCE(value_cos_avg, COS(value_avg)) * sample_count) / SUM(sample_count)
          ) as value_avg,
          NULL::DOUBLE as value_min,
          NULL::DOUBLE as value_max,
          SUM(sample_count)::BIGINT as sample_count,
          SUM(COALESCE(value_sin_avg, SIN(value_avg)) * sample_count) / SUM(sample_count) as value_sin_avg,
          SUM(COALESCE(value_cos_avg, COS(value_avg)) * sample_count) / SUM(sample_count) as value_cos_avg,
          MIN(first_timestamp) as first_timestamp,
          MAX(last_timestamp) as last_timestamp
        FROM (
          SELECT bucket_time as src_bucket_time, context, path, value_avg, value_sin_avg, value_cos_avg, sample_count, first_timestamp, last_timestamp
          FROM read_parquet([${fileListStr}], union_by_name=true)
        ) src
        GROUP BY time_bucket(INTERVAL '${intervalSeconds} seconds', src_bucket_time::TIMESTAMP), context, path
        ORDER BY 1
      ) TO '${outputFile}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
    `;
  }

  /**
   * Build position aggregation query.
   *
   * Selects one representative point per bucket by ranking candidates:
   *   1. Fewest glitchy neighbors — a neighbor is glitchy when the implied
   *      speed between it and the candidate exceeds POSITION_MAX_SPEED_MPS.
   *      Both neighbors clean beats one; one beats none.
   *   2. Timestamp at or after the bucket midpoint.
   *   3. Closest to the bucket midpoint.
   *
   * A bucket always produces a row if any candidate exists; buckets made
   * entirely of glitches still emit their least-bad candidate.
   */
  private buildPositionAggregationQuery(
    fileListStr: string,
    intervalSeconds: number,
    isSourceRaw: boolean,
    outputFile: string
  ): string {
    // Haversine distance in meters between two lat/lon pairs.
    const haversine = (
      lat1: string,
      lon1: string,
      lat2: string,
      lon2: string
    ) => `
      (2 * 6371000 * ASIN(SQRT(
        POWER(SIN(RADIANS(${lat2} - ${lat1}) / 2), 2) +
        COS(RADIANS(${lat1})) * COS(RADIANS(${lat2})) *
        POWER(SIN(RADIANS(${lon2} - ${lon1}) / 2), 2)
      )))
    `;

    // Source columns differ between raw and aggregated tiers.
    // Raw: received_timestamp, value_latitude, value_longitude
    // Aggregated: bucket_time (as timestamp), value_latitude, value_longitude, sample_count, first_timestamp, last_timestamp
    const tsCol = isSourceRaw ? 'received_timestamp' : 'bucket_time';
    const srcSampleCount = isSourceRaw ? '1::BIGINT' : 'sample_count';
    const srcFirstTs = isSourceRaw ? 'received_timestamp' : 'first_timestamp';
    const srcLastTs = isSourceRaw ? 'received_timestamp' : 'last_timestamp';

    return `
      COPY (
        WITH src AS (
          SELECT
            ${tsCol}::TIMESTAMP AS ts,
            context,
            path,
            TRY_CAST(value_latitude AS DOUBLE) AS lat,
            TRY_CAST(value_longitude AS DOUBLE) AS lon,
            ${srcSampleCount} AS src_sample_count,
            ${srcFirstTs}::TIMESTAMP AS src_first_ts,
            ${srcLastTs}::TIMESTAMP AS src_last_ts
          FROM read_parquet([${fileListStr}], union_by_name=true)
          WHERE TRY_CAST(value_latitude AS DOUBLE) BETWEEN -90 AND 90
            AND TRY_CAST(value_longitude AS DOUBLE) BETWEEN -180 AND 180
        ),
        bucketed AS (
          SELECT
            time_bucket(INTERVAL '${intervalSeconds} seconds', ts) AS bucket_time,
            *
          FROM src
        ),
        with_neighbors AS (
          SELECT
            *,
            LAG(lat) OVER w AS prev_lat,
            LAG(lon) OVER w AS prev_lon,
            LAG(ts)  OVER w AS prev_ts,
            LEAD(lat) OVER w AS next_lat,
            LEAD(lon) OVER w AS next_lon,
            LEAD(ts)  OVER w AS next_ts
          FROM bucketed
          WINDOW w AS (PARTITION BY context, path, bucket_time ORDER BY ts)
        ),
        scored AS (
          SELECT
            *,
            (bucket_time + INTERVAL '${intervalSeconds * 500} milliseconds') AS bucket_mid,
            CASE
              WHEN prev_ts IS NULL THEN 1
              WHEN ${haversine('prev_lat', 'prev_lon', 'lat', 'lon')} /
                   GREATEST(EXTRACT(EPOCH FROM (ts - prev_ts)), 0.001) <= ${POSITION_MAX_SPEED_MPS}
                THEN 1
              ELSE 0
            END AS prev_ok,
            CASE
              WHEN next_ts IS NULL THEN 1
              WHEN ${haversine('lat', 'lon', 'next_lat', 'next_lon')} /
                   GREATEST(EXTRACT(EPOCH FROM (next_ts - ts)), 0.001) <= ${POSITION_MAX_SPEED_MPS}
                THEN 1
              ELSE 0
            END AS next_ok
          FROM with_neighbors
        ),
        ranked AS (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY context, path, bucket_time
              ORDER BY
                (prev_ok + next_ok) DESC,
                CASE WHEN ts >= bucket_mid THEN 0 ELSE 1 END,
                ABS(EXTRACT(EPOCH FROM (ts - bucket_mid)))
            ) AS rn
          FROM scored
        ),
        bucket_stats AS (
          SELECT
            bucket_time,
            context,
            path,
            SUM(src_sample_count)::BIGINT AS sample_count,
            MIN(src_first_ts) AS first_timestamp,
            MAX(src_last_ts) AS last_timestamp
          FROM bucketed
          GROUP BY bucket_time, context, path
        )
        SELECT
          r.bucket_time,
          r.context,
          r.path,
          r.lat AS value_latitude,
          r.lon AS value_longitude,
          NULL::DOUBLE AS value_min,
          NULL::DOUBLE AS value_max,
          s.sample_count,
          s.first_timestamp,
          s.last_timestamp
        FROM ranked r
        JOIN bucket_stats s
          ON r.bucket_time = s.bucket_time
         AND r.context = s.context
         AND r.path = s.path
        WHERE r.rn = 1
        ORDER BY r.bucket_time
      ) TO '${outputFile}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
    `;
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
   * Clean up old data based on retention settings.
   *
   * Per-tier retention defaults come from `config.retentionDays`. A
   * value of 0 at any tier means "keep forever" — that tier is skipped
   * entirely. Per-path overrides take precedence: when a file's path
   * matches a retention rule, the rule's `days` (scaled by the tier
   * multiplier) is used instead of the global tier default.
   *
   * Files outside the Hive layout (legacy flat storage) are not
   * touched: there is no path/year/day to anchor a retention decision
   * on.
   */
  async cleanupOldData(): Promise<{
    deletedFiles: number;
    failedFiles: number;
    freedBytes: number;
  }> {
    let deletedFiles = 0;
    let failedFiles = 0;
    let freedBytes = 0;

    const now = new Date();
    const hasPathRules = !this.retentionRules.isEmpty();
    let processedSinceYield = 0;

    for (const tier of TIER_HIERARCHY) {
      if (this.cancelRequested) break;

      const tierDefaultDays = this.config.retentionDays[tier];
      // No tier default and no per-path overrides → nothing to do for
      // this tier. With overrides present we still walk the tier so
      // path-specific rules can act even when the global is infinite.
      if (tierDefaultDays <= 0 && !hasPathRules) continue;

      const tierMultiplier = TIER_RETENTION_MULTIPLIER[tier];

      const pattern = path.join(
        this.config.outputDirectory,
        `tier=${tier}`,
        '**',
        '*.parquet'
      );

      const files = await glob(pattern);

      for (const file of files) {
        if (this.cancelRequested) break;
        if (++processedSinceYield >= CLEANUP_YIELD_INTERVAL) {
          processedSinceYield = 0;
          await new Promise(resolve => setImmediate(resolve));
        }

        try {
          const parsed = this.hivePathBuilder.detectPathStyle(file);
          if (!parsed.isHive || !parsed.year || !parsed.dayOfYear) continue;

          const effectiveDays = this.resolveEffectiveRetentionDays(
            parsed.signalkPath,
            tierDefaultDays,
            tierMultiplier
          );
          // null means keep forever for this (path, tier) pair.
          if (effectiveDays === null) continue;

          // Compare at day granularity (midnight UTC). The partition's
          // fileDate is midnight UTC; if cutoffDate kept the current
          // time-of-day, a `days: 1` cleanup running mid-afternoon
          // would already see "yesterday" as older than cutoff and
          // delete it ~16 hours before its day was actually complete.
          const cutoffDate = new Date(now);
          cutoffDate.setUTCHours(0, 0, 0, 0);
          cutoffDate.setUTCDate(cutoffDate.getUTCDate() - effectiveDays);

          const fileDate = this.hivePathBuilder.dateFromDayOfYear(
            parsed.year,
            parsed.dayOfYear
          );

          if (fileDate < cutoffDate) {
            const stats = await fs.stat(file);
            await fs.remove(file);
            freedBytes += stats.size;
            deletedFiles++;
          }
        } catch (error) {
          this.app.error(
            `Failed to check/delete ${file}: ${(error as Error).message}`
          );
          failedFiles++;
        }
      }
    }

    this.app.debug(
      `Cleanup: deleted ${deletedFiles} files, ${failedFiles} failed, freed ${(freedBytes / 1024 / 1024).toFixed(2)} MB`
    );

    return { deletedFiles, failedFiles, freedBytes };
  }

  /**
   * Pick the retention to apply for one (path, tier) pair.
   *
   * - If a path-rule matches and `skipAggregation` is set: use rule.days
   *   for every tier (no multiplier). The path lives only in raw, so
   *   any stale rows in upper tiers should sweep at the same horizon
   *   as raw rather than linger 2/4/12x longer.
   * - Else if a path-rule matches: use rule.days × tierMultiplier
   *   (rule.days is interpreted as raw-tier retention; upper tiers
   *   scale by the same convention as the global default).
   * - Else: use the global tier default.
   * - 0 (from either source) means infinite — return null so the caller
   *   skips deletion.
   */
  private resolveEffectiveRetentionDays(
    signalkPath: string | undefined,
    tierDefaultDays: number,
    tierMultiplier: number
  ): number | null {
    if (signalkPath) {
      const matched = this.retentionRules.match(signalkPath);
      if (matched) {
        if (matched.days <= 0) return null;
        return matched.skipAggregation
          ? matched.days
          : matched.days * tierMultiplier;
      }
    }
    return tierDefaultDays > 0 ? tierDefaultDays : null;
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

    return results;
  }

  /**
   * Discover all unique dates in tier=raw across all contexts and paths
   */
  async discoverRawDates(startDate?: Date, endDate?: Date): Promise<Date[]> {
    const rawDir = path.join(this.config.outputDirectory, 'tier=raw');
    if (!(await fs.pathExists(rawDir))) return [];

    const dayDirs = await glob(
      path.join(rawDir, 'context=*', 'path=*', 'year=*', 'day=*')
    );

    const dateSet = new Set<string>();
    for (const dir of dayDirs) {
      const yearMatch = dir.match(/year=(\d{4})/);
      const dayMatch = dir.match(/day=(\d{1,3})/);
      if (yearMatch && dayMatch) {
        const year = parseInt(yearMatch[1]);
        const day = parseInt(dayMatch[1]);
        const date = this.hivePathBuilder.dateFromDayOfYear(year, day);
        const dateStr = date.toISOString().slice(0, 10);

        if (startDate && date < startDate) continue;
        if (endDate && date > endDate) continue;

        dateSet.add(dateStr);
      }
    }

    return Array.from(dateSet)
      .sort()
      .map(d => new Date(d + 'T00:00:00Z'));
  }

  /**
   * Start bulk aggregation as a background job
   */
  startBulkAggregation(startDate?: Date, endDate?: Date): string {
    const jobId = `bulk_agg_${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;

    const progress: BulkAggregationProgress = {
      jobId,
      status: 'scanning',
      phase: 'scan',
      datesProcessed: 0,
      datesTotal: 0,
      percent: 0,
      filesCreated: 0,
      recordsAggregated: 0,
      startTime: new Date(),
      errors: [],
    };

    bulkAggregationJobs.set(jobId, progress);
    this.cancelRequested = false;

    this.runBulkAggregation(jobId, startDate, endDate).catch(error => {
      const job = bulkAggregationJobs.get(jobId);
      if (job) {
        job.status = 'error';
        job.error = (error as Error).message;
        job.completedAt = new Date();
      }
    });

    return jobId;
  }

  /**
   * Run bulk aggregation across all dates in tier=raw
   */
  private async runBulkAggregation(
    jobId: string,
    startDate?: Date,
    endDate?: Date
  ): Promise<void> {
    const progress = bulkAggregationJobs.get(jobId);
    if (!progress) return;

    try {
      // Phase 1: Discover dates
      progress.phase = 'scan';
      progress.status = 'scanning';

      const dates = await this.discoverRawDates(startDate, endDate);
      progress.datesTotal = dates.length;

      if (dates.length === 0) {
        progress.status = 'completed';
        progress.completedAt = new Date();
        scheduleBulkJobCleanup(jobId);
        return;
      }

      this.app.debug(
        `[BulkAggregation] Found ${dates.length} dates to aggregate (${dates[0].toISOString().slice(0, 10)} to ${dates[dates.length - 1].toISOString().slice(0, 10)})`
      );

      // Phase 2: Aggregate each date
      progress.phase = 'aggregation';
      progress.status = 'running';

      for (let i = 0; i < dates.length; i++) {
        if (this.cancelRequested) {
          progress.status = 'cancelled';
          progress.completedAt = new Date();
          scheduleBulkJobCleanup(jobId);
          return;
        }

        const date = dates[i];
        const dateStr = date.toISOString().slice(0, 10);
        progress.currentDate = dateStr;
        progress.datesProcessed = i;
        progress.percent = Math.round((i / dates.length) * 100);

        try {
          const results = await this.aggregateDate(date);

          for (const r of results) {
            progress.filesCreated += r.filesCreated;
            progress.recordsAggregated += r.recordsAggregated;
            if (r.errors.length > 0) {
              progress.errors.push(...r.errors.map(e => `[${dateStr}] ${e}`));
            }
          }
        } catch (error) {
          const errorMsg = `[${dateStr}] ${(error as Error).message}`;
          this.app.error(`[BulkAggregation] ${errorMsg}`);
          progress.errors.push(errorMsg);
        }
      }

      progress.datesProcessed = dates.length;
      progress.percent = 100;
      progress.status = 'completed';
      progress.completedAt = new Date();
      scheduleBulkJobCleanup(jobId);

      this.app.debug(
        `[BulkAggregation] Complete: ${dates.length} dates, ${progress.filesCreated} files created, ${progress.recordsAggregated} records aggregated`
      );
    } catch (error) {
      progress.status = 'error';
      progress.error = (error as Error).message;
      progress.completedAt = new Date();
      scheduleBulkJobCleanup(jobId);
    }
  }

  /**
   * Get bulk aggregation job progress
   */
  getBulkProgress(jobId: string): BulkAggregationProgress | null {
    return bulkAggregationJobs.get(jobId) || null;
  }

  /**
   * Cancel a bulk aggregation job
   */
  cancelBulk(jobId: string): boolean {
    const job = bulkAggregationJobs.get(jobId);
    if (job && (job.status === 'running' || job.status === 'scanning')) {
      this.cancelRequested = true;
      return true;
    }
    return false;
  }
}
