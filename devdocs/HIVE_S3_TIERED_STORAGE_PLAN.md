# Hive-Style Partitioning, S3 Storage & Aggregation Tiers Plan

> **Status**: Planning
> **Created**: 2026-02-19
> **Goal**: Restructure parquet storage with Hive partitioning, add S3 support, and implement aggregation tiers for optimal query performance

---

## Current State

### Directory Structure
```
data/
  vessels/
    urn_mrn_imo_mmsi_368396230/
      navigation/
        position/
          signalk_data_2025-08-20T1116.parquet
```

### Problems
1. Not Hive-style → no automatic partition pruning
2. Local only → no cloud backup/access
3. Raw data only → slow long-range queries
4. No time-based partitioning → must scan all files for date ranges

---

## Target Architecture

### New Directory Structure (Hive-Style)

```
data/
  tier=raw/
    context=vessels.urn_mrn_imo_mmsi_368396230/
      path=navigation.position/
        year=2025/
          day=232/
            001_1724184960000.parquet
            002_1724185020000.parquet
  tier=5s/
    context=vessels.urn_mrn_imo_mmsi_368396230/
      path=navigation.position/
        year=2025/
          day=232/
            navigation.position_2025_232_5s.parquet
  tier=60s/
    context=vessels.urn_mrn_imo_mmsi_368396230/
      path=navigation.position/
        year=2025/
          month=08/
            navigation.position_2025_08_60s.parquet
  tier=1h/
    context=vessels.urn_mrn_imo_mmsi_368396230/
      path=navigation.position/
        year=2025/
          navigation.position_2025_1h.parquet
```

### Partition Hierarchy

```
tier=<raw|5s|60s|1h>/
  context=<vessel_id>/
    path=<signalk.path>/
      year=<YYYY>/
        [day=<DDD>/ or month=<MM>/]
          <filename>.parquet
```

| Level | Purpose |
|-------|---------|
| `tier` | First! Enables different storage/retention per tier |
| `context` | Vessel identifier |
| `path` | SignalK path (dots replaced with underscores) |
| `year` | Year partition |
| `day` or `month` | Time granularity (day for raw/5s, month for 60s/1h) |

### Aggregation Tiers

| Tier | Resolution | Granularity | Retention | Storage |
|------|------------|-------------|-----------|---------|
| `raw` | 1s | Daily files | 7 days | Local SSD / S3 Standard |
| `5s` | 5 seconds | Daily files | 30 days | Local / S3 Standard |
| `60s` | 1 minute | Monthly files | 2 years | S3 Infrequent Access |
| `1h` | 1 hour | Yearly files | Forever | S3 Glacier |

---

## Phase 1: Hive-Style Path Restructure

### 1.1 Path Utilities

Create: `src/utils/hive-paths.ts`

```typescript
import path from 'path';
import { Context, Path } from '@signalk/server-api';

export type AggregationTier = 'raw' | '5s' | '60s' | '1h';

export interface HivePartition {
  tier: AggregationTier;
  context: string;
  path: string;
  year: number;
  day?: number;   // Day of year (1-366) for raw/5s
  month?: number; // Month (1-12) for 60s/1h
}

/**
 * Convert SignalK context to Hive-safe string
 * vessels.urn:mrn:imo:mmsi:368396230 → vessels.urn_mrn_imo_mmsi_368396230
 */
export function contextToHive(context: Context): string {
  return context.replace(/:/g, '_');
}

/**
 * Convert SignalK path to Hive-safe string
 * navigation.position → navigation.position (dots OK in partition values)
 */
export function pathToHive(signalkPath: Path): string {
  return signalkPath;
}

/**
 * Build Hive-style directory path
 */
export function buildHivePath(
  baseDir: string,
  partition: HivePartition
): string {
  const parts = [
    baseDir,
    `tier=${partition.tier}`,
    `context=${partition.context}`,
    `path=${partition.path}`,
    `year=${partition.year}`,
  ];

  if (partition.day !== undefined) {
    parts.push(`day=${partition.day.toString().padStart(3, '0')}`);
  } else if (partition.month !== undefined) {
    parts.push(`month=${partition.month.toString().padStart(2, '0')}`);
  }

  return path.join(...parts);
}

/**
 * Build glob pattern for Hive queries
 */
export function buildHiveGlob(
  baseDir: string,
  options: {
    tier?: AggregationTier | '*';
    context?: string | '*';
    path?: string | '*';
    year?: number | '*';
    day?: number | '*';
    month?: number | '*';
  }
): string {
  const tier = options.tier ?? '*';
  const context = options.context ?? '*';
  const signalkPath = options.path ?? '*';
  const year = options.year ?? '*';

  let timePart = '*';
  if (options.day !== undefined) {
    timePart = `day=${options.day === '*' ? '*' : options.day.toString().padStart(3, '0')}`;
  } else if (options.month !== undefined) {
    timePart = `month=${options.month === '*' ? '*' : options.month.toString().padStart(2, '0')}`;
  }

  return path.join(
    baseDir,
    `tier=${tier}`,
    `context=${context}`,
    `path=${signalkPath}`,
    `year=${year}`,
    timePart,
    '*.parquet'
  );
}

/**
 * Get day of year (1-366)
 */
export function getDayOfYear(date: Date): number {
  const start = new Date(date.getFullYear(), 0, 0);
  const diff = date.getTime() - start.getTime();
  const oneDay = 1000 * 60 * 60 * 24;
  return Math.floor(diff / oneDay);
}

/**
 * Parse Hive path back to partition info
 */
export function parseHivePath(filePath: string): HivePartition | null {
  const tierMatch = filePath.match(/tier=(\w+)/);
  const contextMatch = filePath.match(/context=([^/]+)/);
  const pathMatch = filePath.match(/path=([^/]+)/);
  const yearMatch = filePath.match(/year=(\d{4})/);
  const dayMatch = filePath.match(/day=(\d{3})/);
  const monthMatch = filePath.match(/month=(\d{2})/);

  if (!tierMatch || !contextMatch || !pathMatch || !yearMatch) {
    return null;
  }

  return {
    tier: tierMatch[1] as AggregationTier,
    context: contextMatch[1],
    path: pathMatch[1],
    year: parseInt(yearMatch[1]),
    day: dayMatch ? parseInt(dayMatch[1]) : undefined,
    month: monthMatch ? parseInt(monthMatch[1]) : undefined,
  };
}
```

### 1.2 Update Parquet Writer

Modify: `src/parquet-writer.ts`

```typescript
import { buildHivePath, contextToHive, pathToHive, getDayOfYear, AggregationTier } from './utils/hive-paths';

async writeParquet(
  context: Context,
  signalkPath: Path,
  data: DataPoint[],
  tier: AggregationTier = 'raw'
): Promise<string> {
  const now = new Date();

  const partition = {
    tier,
    context: contextToHive(context),
    path: pathToHive(signalkPath),
    year: now.getFullYear(),
    day: tier === 'raw' || tier === '5s' ? getDayOfYear(now) : undefined,
    month: tier === '60s' || tier === '1h' ? now.getMonth() + 1 : undefined,
  };

  const outputDir = buildHivePath(this.dataDir, partition);
  await fs.ensureDir(outputDir);

  const filename = `${Date.now()}.parquet`;
  const filePath = path.join(outputDir, filename);

  // Write parquet file...

  return filePath;
}
```

### 1.3 Update Query Paths

Modify: `src/HistoryAPI.ts`

```typescript
import { buildHiveGlob } from './utils/hive-paths';

async getNumericValues(...) {
  // Build Hive-aware glob
  const parquetGlob = buildHiveGlob(this.dataDir, {
    tier: this.selectTierForQuery(from, to),  // Auto-select best tier
    context: contextToHive(context),
    path: pathToHive(signalkPath),
    year: '*',  // Or specific years based on query range
  });

  const query = `
    SELECT * FROM read_parquet('${parquetGlob}', hive_partitioning=true)
    WHERE signalk_timestamp >= '${fromIso}'
      AND signalk_timestamp < '${toIso}'
  `;
}

/**
 * Select optimal tier based on query time range
 */
private selectTierForQuery(from: ZonedDateTime, to: ZonedDateTime): AggregationTier {
  const durationHours = (to.toEpochSecond() - from.toEpochSecond()) / 3600;

  if (durationHours <= 1) return 'raw';
  if (durationHours <= 24) return '5s';
  if (durationHours <= 24 * 30) return '60s';
  return '1h';
}
```

---

## Phase 2: Aggregation Pipeline

### 2.1 Aggregation Service

Create: `src/services/aggregation-service.ts`

```typescript
import { DuckDBPool } from '../utils/duckdb-pool';
import { buildHivePath, buildHiveGlob, AggregationTier, getDayOfYear } from '../utils/hive-paths';

interface AggregationConfig {
  sourceTier: AggregationTier;
  targetTier: AggregationTier;
  resolutionMs: number;
  schedule: 'hourly' | 'daily' | 'weekly';
}

const AGGREGATION_CONFIGS: AggregationConfig[] = [
  { sourceTier: 'raw', targetTier: '5s', resolutionMs: 5000, schedule: 'hourly' },
  { sourceTier: '5s', targetTier: '60s', resolutionMs: 60000, schedule: 'daily' },
  { sourceTier: '60s', targetTier: '1h', resolutionMs: 3600000, schedule: 'weekly' },
];

export class AggregationService {
  constructor(private dataDir: string) {}

  /**
   * Run aggregation for a specific tier transition
   */
  async aggregate(
    config: AggregationConfig,
    context: string,
    signalkPath: string,
    date: Date
  ): Promise<number> {
    const connection = await DuckDBPool.getConnection();

    try {
      // Source glob
      const sourceGlob = buildHiveGlob(this.dataDir, {
        tier: config.sourceTier,
        context,
        path: signalkPath,
        year: date.getFullYear(),
        day: getDayOfYear(date),
      });

      // Target path
      const targetPartition = {
        tier: config.targetTier,
        context,
        path: signalkPath,
        year: date.getFullYear(),
        day: config.targetTier === '5s' ? getDayOfYear(date) : undefined,
        month: config.targetTier === '60s' || config.targetTier === '1h'
          ? date.getMonth() + 1 : undefined,
      };
      const targetDir = buildHivePath(this.dataDir, targetPartition);
      const targetFile = path.join(targetDir, `${signalkPath.replace(/\./g, '_')}_${config.targetTier}.parquet`);

      await fs.ensureDir(targetDir);

      // Aggregation query
      const query = `
        COPY (
          SELECT
            -- Bucket timestamp to resolution
            EPOCH_MS(
              CAST(FLOOR(EPOCH_MS(signalk_timestamp) / ${config.resolutionMs}) * ${config.resolutionMs} AS BIGINT)
            )::TIMESTAMP as signalk_timestamp,

            -- Aggregations for numeric values
            AVG(TRY_CAST(value AS DOUBLE)) as value,
            MIN(TRY_CAST(value AS DOUBLE)) as value_min,
            MAX(TRY_CAST(value AS DOUBLE)) as value_max,
            COUNT(*) as sample_count,

            -- For position data, average lat/lon separately
            AVG(value_latitude) as value_latitude,
            AVG(value_longitude) as value_longitude

          FROM read_parquet('${sourceGlob}', hive_partitioning=true, union_by_name=true)
          GROUP BY 1
          ORDER BY 1
        ) TO '${targetFile}' (FORMAT PARQUET, COMPRESSION ZSTD)
      `;

      const result = await connection.runAndReadAll(query);

      // Return row count
      const countResult = await connection.runAndReadAll(
        `SELECT COUNT(*) as cnt FROM read_parquet('${targetFile}')`
      );
      return (countResult.getRowObjects()[0] as any).cnt;

    } finally {
      connection.disconnectSync();
    }
  }

  /**
   * Run all pending aggregations
   */
  async runScheduledAggregations(): Promise<void> {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);

    // Get all contexts and paths
    const paths = await this.discoverPaths('raw');

    for (const { context, signalkPath } of paths) {
      for (const config of AGGREGATION_CONFIGS) {
        if (this.shouldRunAggregation(config, yesterday)) {
          try {
            const count = await this.aggregate(config, context, signalkPath, yesterday);
            console.log(`[Aggregation] ${config.sourceTier}→${config.targetTier} for ${signalkPath}: ${count} rows`);
          } catch (err) {
            console.error(`[Aggregation] Failed: ${err}`);
          }
        }
      }
    }
  }

  private shouldRunAggregation(config: AggregationConfig, date: Date): boolean {
    const hour = new Date().getHours();
    const dayOfWeek = date.getDay();

    switch (config.schedule) {
      case 'hourly': return true;
      case 'daily': return hour === 2;  // Run at 2 AM
      case 'weekly': return hour === 3 && dayOfWeek === 0;  // Sunday 3 AM
      default: return false;
    }
  }

  private async discoverPaths(tier: AggregationTier): Promise<Array<{context: string; signalkPath: string}>> {
    // Discover all context/path combinations in the tier
    const glob = buildHiveGlob(this.dataDir, { tier });
    // ... implementation
    return [];
  }
}
```

### 2.2 Schedule Aggregations

Add to plugin start:

```typescript
import { AggregationService } from './services/aggregation-service';

// In plugin.start()
const aggregationService = new AggregationService(config.outputDirectory);

// Run aggregations every hour
setInterval(() => {
  aggregationService.runScheduledAggregations().catch(err => {
    console.error('[Aggregation] Scheduled run failed:', err);
  });
}, 60 * 60 * 1000);
```

---

## Phase 3: S3 Integration

### 3.1 S3 Configuration

Add to `src/types.ts`:

```typescript
export interface S3Config {
  enabled: boolean;
  bucket: string;
  region: string;
  prefix?: string;  // e.g., "signalk-data/"
  accessKeyId?: string;  // Optional if using IAM roles
  secretAccessKey?: string;

  // Tier-specific settings
  tierSettings?: {
    raw?: { storageClass: 'STANDARD' | 'STANDARD_IA'; sync: boolean };
    '5s'?: { storageClass: 'STANDARD' | 'STANDARD_IA'; sync: boolean };
    '60s'?: { storageClass: 'STANDARD_IA' | 'GLACIER_IR'; sync: boolean };
    '1h'?: { storageClass: 'GLACIER_IR' | 'DEEP_ARCHIVE'; sync: boolean };
  };
}

export interface PluginConfig {
  // ... existing config
  s3?: S3Config;
}
```

### 3.2 S3 Sync Service

Create: `src/services/s3-sync.ts`

```typescript
import { S3Client, PutObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import fs from 'fs-extra';
import path from 'path';
import { glob } from 'glob';
import { S3Config, AggregationTier } from '../types';
import { parseHivePath } from '../utils/hive-paths';

export class S3SyncService {
  private s3: S3Client;
  private syncedFiles: Set<string> = new Set();

  constructor(private config: S3Config, private localDataDir: string) {
    this.s3 = new S3Client({
      region: config.region,
      credentials: config.accessKeyId ? {
        accessKeyId: config.accessKeyId,
        secretAccessKey: config.secretAccessKey!,
      } : undefined,  // Use IAM role if no credentials
    });
  }

  /**
   * Sync local parquet files to S3
   */
  async syncToS3(): Promise<{ synced: number; skipped: number }> {
    const localFiles = await glob(`${this.localDataDir}/tier=*/**/*.parquet`);
    let synced = 0;
    let skipped = 0;

    for (const localPath of localFiles) {
      const relativePath = path.relative(this.localDataDir, localPath);

      // Check if already synced
      if (this.syncedFiles.has(relativePath)) {
        skipped++;
        continue;
      }

      // Parse Hive path to determine tier
      const partition = parseHivePath(localPath);
      if (!partition) continue;

      // Check if tier should be synced
      const tierSettings = this.config.tierSettings?.[partition.tier];
      if (tierSettings && !tierSettings.sync) {
        skipped++;
        continue;
      }

      try {
        await this.uploadFile(localPath, relativePath, partition.tier);
        this.syncedFiles.add(relativePath);
        synced++;
      } catch (err) {
        console.error(`[S3Sync] Failed to upload ${relativePath}:`, err);
      }
    }

    return { synced, skipped };
  }

  private async uploadFile(
    localPath: string,
    s3Key: string,
    tier: AggregationTier
  ): Promise<void> {
    const fileStream = fs.createReadStream(localPath);
    const tierSettings = this.config.tierSettings?.[tier];
    const storageClass = tierSettings?.storageClass ?? 'STANDARD';

    const key = this.config.prefix
      ? `${this.config.prefix}${s3Key}`
      : s3Key;

    const upload = new Upload({
      client: this.s3,
      params: {
        Bucket: this.config.bucket,
        Key: key,
        Body: fileStream,
        StorageClass: storageClass,
        ContentType: 'application/octet-stream',
      },
    });

    await upload.done();
    console.log(`[S3Sync] Uploaded ${s3Key} (${storageClass})`);
  }

  /**
   * Build S3 URI for DuckDB queries
   */
  getS3Uri(globPattern: string): string {
    const prefix = this.config.prefix ?? '';
    return `s3://${this.config.bucket}/${prefix}${globPattern}`;
  }
}
```

### 3.3 DuckDB S3 Configuration

Create: `src/utils/duckdb-s3.ts`

```typescript
import { DuckDBConnection } from '@duckdb/node-api';
import { S3Config } from '../types';

/**
 * Configure DuckDB connection for S3 access
 */
export async function configureS3(
  connection: DuckDBConnection,
  config: S3Config
): Promise<void> {
  // Install and load httpfs extension
  await connection.run('INSTALL httpfs; LOAD httpfs;');

  // Configure S3 credentials
  await connection.run(`SET s3_region = '${config.region}';`);

  if (config.accessKeyId) {
    await connection.run(`SET s3_access_key_id = '${config.accessKeyId}';`);
    await connection.run(`SET s3_secret_access_key = '${config.secretAccessKey}';`);
  } else {
    // Use IAM role / instance profile
    await connection.run(`SET s3_use_ssl = true;`);
  }

  // Performance tuning for S3
  await connection.run(`SET s3_uploader_max_parts_per_file = 10000;`);
  await connection.run(`SET s3_uploader_thread_limit = 8;`);
}

/**
 * Build query that spans local and S3 storage
 */
export function buildHybridQuery(
  localGlob: string,
  s3Uri: string,
  whereClause: string
): string {
  return `
    SELECT * FROM (
      -- Local data (recent/raw)
      SELECT *, 'local' as _source
      FROM read_parquet('${localGlob}', hive_partitioning=true, union_by_name=true)

      UNION ALL

      -- S3 data (historical/aggregated)
      SELECT *, 's3' as _source
      FROM read_parquet('${s3Uri}', hive_partitioning=true, union_by_name=true)
    )
    WHERE ${whereClause}
    ORDER BY signalk_timestamp
  `;
}
```

### 3.4 Hybrid Query Strategy

```typescript
/**
 * Determine where to query based on time range and tier
 */
function getQuerySources(
  from: ZonedDateTime,
  to: ZonedDateTime,
  tier: AggregationTier,
  config: PluginConfig
): { local: boolean; s3: boolean } {
  const now = ZonedDateTime.now(ZoneOffset.UTC);
  const daysAgo = (now.toEpochSecond() - from.toEpochSecond()) / 86400;

  // Raw/5s: local only (short retention)
  if (tier === 'raw' || tier === '5s') {
    return { local: true, s3: false };
  }

  // 60s/1h: S3 for older data
  if (daysAgo > 30) {
    return { local: false, s3: true };
  }

  // Recent aggregates might be in both
  return { local: true, s3: config.s3?.enabled ?? false };
}
```

---

## Phase 4: Retention & Lifecycle

### 4.1 Local Retention Policy

Create: `src/services/retention-service.ts`

```typescript
import fs from 'fs-extra';
import { glob } from 'glob';
import { parseHivePath, AggregationTier } from '../utils/hive-paths';

interface RetentionPolicy {
  tier: AggregationTier;
  localRetentionDays: number;
  deleteAfterS3Sync: boolean;
}

const DEFAULT_POLICIES: RetentionPolicy[] = [
  { tier: 'raw', localRetentionDays: 7, deleteAfterS3Sync: true },
  { tier: '5s', localRetentionDays: 30, deleteAfterS3Sync: true },
  { tier: '60s', localRetentionDays: 90, deleteAfterS3Sync: false },
  { tier: '1h', localRetentionDays: 365, deleteAfterS3Sync: false },
];

export class RetentionService {
  constructor(
    private dataDir: string,
    private policies: RetentionPolicy[] = DEFAULT_POLICIES
  ) {}

  /**
   * Clean up old local files according to retention policy
   */
  async enforceRetention(syncedFiles: Set<string>): Promise<{ deleted: number; freed: number }> {
    let deleted = 0;
    let freedBytes = 0;

    for (const policy of this.policies) {
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - policy.localRetentionDays);

      const files = await glob(`${this.dataDir}/tier=${policy.tier}/**/*.parquet`);

      for (const filePath of files) {
        const stat = await fs.stat(filePath);
        const isOld = stat.mtime < cutoffDate;
        const isSynced = syncedFiles.has(filePath.replace(this.dataDir + '/', ''));

        const shouldDelete = isOld && (!policy.deleteAfterS3Sync || isSynced);

        if (shouldDelete) {
          freedBytes += stat.size;
          await fs.remove(filePath);
          deleted++;
        }
      }

      // Clean up empty directories
      await this.removeEmptyDirs(`${this.dataDir}/tier=${policy.tier}`);
    }

    return { deleted, freed: freedBytes };
  }

  private async removeEmptyDirs(dir: string): Promise<void> {
    // Recursively remove empty directories
    const entries = await fs.readdir(dir, { withFileTypes: true });

    for (const entry of entries) {
      if (entry.isDirectory()) {
        await this.removeEmptyDirs(path.join(dir, entry.name));
      }
    }

    const remaining = await fs.readdir(dir);
    if (remaining.length === 0) {
      await fs.rmdir(dir);
    }
  }
}
```

### 4.2 S3 Lifecycle Rules

Apply via AWS CLI or Terraform:

```json
{
  "Rules": [
    {
      "ID": "raw-tier-expiration",
      "Filter": { "Prefix": "tier=raw/" },
      "Status": "Enabled",
      "Expiration": { "Days": 7 }
    },
    {
      "ID": "5s-tier-transition",
      "Filter": { "Prefix": "tier=5s/" },
      "Status": "Enabled",
      "Transitions": [
        { "Days": 30, "StorageClass": "STANDARD_IA" }
      ],
      "Expiration": { "Days": 90 }
    },
    {
      "ID": "60s-tier-transition",
      "Filter": { "Prefix": "tier=60s/" },
      "Status": "Enabled",
      "Transitions": [
        { "Days": 90, "StorageClass": "STANDARD_IA" },
        { "Days": 365, "StorageClass": "GLACIER_IR" }
      ]
    },
    {
      "ID": "1h-tier-transition",
      "Filter": { "Prefix": "tier=1h/" },
      "Status": "Enabled",
      "Transitions": [
        { "Days": 365, "StorageClass": "GLACIER_IR" },
        { "Days": 730, "StorageClass": "DEEP_ARCHIVE" }
      ]
    }
  ]
}
```

---

## Phase 5: Migration Tool (Integrated into Plugin UI)

Migration is built into the plugin's web interface, following the same pattern as validation/repair jobs.

### 5.1 Architecture

Uses the existing plugin patterns:
- **API routes** in `api-routes.ts` with job tracking (like `validationJobs`)
- **Progress polling** from frontend
- **Cancel support** for long-running migrations
- **New tab** in web UI for migration

### 5.2 Supported Migration Paths

| Source | Target | Use Case |
|--------|--------|----------|
| Local → Local | Restructure existing local data |
| Local → S3 | Move local data to cloud with new structure |
| S3 → S3 | Restructure existing S3 data |
| S3 → Local | Pull cloud data locally with new structure |

### 5.3 Backend: Migration Service

Create: `src/services/migration-service.ts`

```typescript
import fs from 'fs-extra';
import path from 'path';
import { glob } from 'glob';
import { S3Client, ListObjectsV2Command, CopyObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { pipeline } from 'stream/promises';
import { createWriteStream, createReadStream } from 'fs';

// Types
export interface StorageLocation {
  type: 'local' | 's3';
  path: string;
  prefix?: string;
}

export interface MigrationProgress {
  jobId: string;
  status: 'scanning' | 'running' | 'cancelling' | 'completed' | 'cancelled' | 'error';
  phase: 'scan' | 'migrate' | 'verify' | 'cleanup';
  processed: number;
  total: number;
  percent: number;
  bytesTransferred: number;
  currentFile?: string;
  cancelRequested?: boolean;
  error?: string;
  result?: { success: number; failed: number; totalBytes: number };
}

export interface ScanResult {
  files: string[];
  totalSize: number;
  contexts: string[];
  paths: string[];
}

// Service class designed to be called from API routes
export class MigrationService {
  private s3?: S3Client;

  constructor(private s3Config?: { region: string }) {
    if (s3Config) {
      this.s3 = new S3Client({ region: s3Config.region });
    }
  }

  /**
   * Scan source location and return file list + stats
   * Called by POST /api/migrate/scan
   */
  async scanSource(source: StorageLocation): Promise<ScanResult> {
    const files = await this.discoverFiles(source);
    const contexts = new Set<string>();
    const paths = new Set<string>();
    let totalSize = 0;

    for (const file of files) {
      const parsed = this.parseOldPath(file, source);
      if (parsed) {
        contexts.add(parsed.context);
        paths.add(parsed.signalkPath);
      }
      // Get size (simplified - actual impl would batch this)
      if (source.type === 'local') {
        const stat = await fs.stat(file);
        totalSize += stat.size;
      }
    }

    return {
      files,
      totalSize,
      contexts: Array.from(contexts),
      paths: Array.from(paths),
    };
  }

  /**
   * Run migration with progress callback
   * Called by POST /api/migrate (runs async)
   */
  async migrate(
    source: StorageLocation,
    target: StorageLocation,
    tier: string,
    deleteSource: boolean,
    progress: MigrationProgress,
    onProgress: (p: MigrationProgress) => void
  ): Promise<void> {
    try {
      // Phase 1: Scan
      progress.phase = 'scan';
      onProgress(progress);

      const files = await this.discoverFiles(source);
      const plan = this.planMigration(files, source, target, tier);

      progress.total = plan.length;
      progress.status = 'running';
      progress.phase = 'migrate';
      onProgress(progress);

      // Phase 2: Migrate
      for (let i = 0; i < plan.length; i++) {
        if (progress.cancelRequested) {
          progress.status = 'cancelled';
          onProgress(progress);
          return;
        }

        const file = plan[i];
        progress.currentFile = file.sourcePath;
        onProgress(progress);

        try {
          const bytes = await this.migrateFile(file, source, target);
          progress.bytesTransferred += bytes;
          progress.processed++;
          progress.percent = Math.round((progress.processed / progress.total) * 100);
        } catch (err) {
          // Log error but continue
          console.error(`Migration failed for ${file.sourcePath}:`, err);
        }

        onProgress(progress);
      }

      // Phase 3: Cleanup (if requested)
      if (deleteSource && progress.processed === progress.total) {
        progress.phase = 'cleanup';
        onProgress(progress);
        await this.cleanup(plan, source);
      }

      progress.status = 'completed';
      progress.result = {
        success: progress.processed,
        failed: progress.total - progress.processed,
        totalBytes: progress.bytesTransferred,
      };
      onProgress(progress);

    } catch (err) {
      progress.status = 'error';
      progress.error = String(err);
      onProgress(progress);
    }
  }

  // ... helper methods: discoverFiles, planMigration, migrateFile, cleanup, parseOldPath
  // (Same implementation as before, but without CLI-specific code)
}
```

### 5.4 API Routes

Add to `src/api-routes.ts` (following existing patterns like `validationJobs`):

```typescript
// Migration job tracking (same pattern as validationJobs)
interface MigrationProgress {
  jobId: string;
  status: 'scanning' | 'running' | 'cancelling' | 'completed' | 'cancelled' | 'error';

      try {
        const size = await this.migrateFile(file);
        success++;
        bytes += size;
      } catch (err) {
        failed++;
        console.error(`\n      Failed: ${file.sourcePath} - ${err}`);
      }
    }
    console.log(''); // newline

    return { success, failed, bytes };
  }

  private async migrateFile(file: MigrationFile): Promise<number> {
    const srcType = this.config.source.type;
    const tgtType = this.config.target.type;

    // Local → Local
    if (srcType === 'local' && tgtType === 'local') {
      await fs.ensureDir(path.dirname(file.targetPath));
      await fs.copy(file.sourcePath, file.targetPath);
      const stat = await fs.stat(file.targetPath);
      return stat.size;
    }

    // Local → S3
    if (srcType === 'local' && tgtType === 's3') {
      const stat = await fs.stat(file.sourcePath);
      const stream = createReadStream(file.sourcePath);

      const upload = new Upload({
        client: this.s3!,
        params: {
          Bucket: this.config.target.path,
          Key: file.targetPath,
          Body: stream,
        },
      });
      await upload.done();
      return stat.size;
    }

    // S3 → S3
    if (srcType === 's3' && tgtType === 's3') {
      const copySource = `${this.config.source.path}/${file.sourcePath}`;
      await this.s3!.send(new CopyObjectCommand({
        Bucket: this.config.target.path,
        Key: file.targetPath,
        CopySource: encodeURIComponent(copySource),
      }));
      return 0; // Size not easily available for S3→S3
    }

    // S3 → Local
    if (srcType === 's3' && tgtType === 'local') {
      await fs.ensureDir(path.dirname(file.targetPath));

      const resp = await this.s3!.send(new GetObjectCommand({
        Bucket: this.config.source.path,
        Key: file.sourcePath,
      }));

      await pipeline(
        resp.Body as NodeJS.ReadableStream,
        createWriteStream(file.targetPath)
      );

      const stat = await fs.stat(file.targetPath);
      return stat.size;
    }

    throw new Error(`Unsupported migration: ${srcType} → ${tgtType}`);
  }

  // --------------------------------------------------------------------------
  // Verification
  // --------------------------------------------------------------------------

  private async verifyMigration(plan: MigrationFile[]): Promise<void> {
    // Verify a sample of files
    const sample = plan.slice(0, Math.min(5, plan.length));

    for (const file of sample) {
      const sourceExists = await this.fileExists(file.sourcePath, this.config.source);
      const targetExists = await this.fileExists(file.targetPath, this.config.target);

      if (targetExists) {
        console.log(`      ✓ ${path.basename(file.targetPath)}`);
      } else {
        console.log(`      ✗ ${path.basename(file.targetPath)} - TARGET MISSING`);
      }
    }
  }

  private async fileExists(filePath: string, location: StorageLocation): Promise<boolean> {
    if (location.type === 'local') {
      return fs.pathExists(filePath);
    }

    try {
      await this.s3!.send(new GetObjectCommand({
        Bucket: location.path,
        Key: filePath,
      }));
      return true;
    } catch {
      return false;
    }
  }

  // --------------------------------------------------------------------------
  // Cleanup
  // --------------------------------------------------------------------------

  private async cleanup(plan: MigrationFile[]): Promise<void> {
    for (const file of plan) {
      try {
        if (this.config.source.type === 'local') {
          await fs.remove(file.sourcePath);
        } else {
          await this.s3!.send(new DeleteObjectCommand({
            Bucket: this.config.source.path,
            Key: file.sourcePath,
          }));
        }
      } catch (err) {
        console.warn(`      Failed to delete: ${file.sourcePath}`);
      }
    }

    // Remove empty directories (local only)
    if (this.config.source.type === 'local') {
      await this.removeEmptyDirs(this.config.source.path);
    }

    console.log(`      Deleted ${plan.length} source files`);
  }

  private async removeEmptyDirs(dir: string): Promise<void> {
    const entries = await fs.readdir(dir, { withFileTypes: true });

    for (const entry of entries) {
      if (entry.isDirectory()) {
        const subDir = path.join(dir, entry.name);
        await this.removeEmptyDirs(subDir);

        const remaining = await fs.readdir(subDir);
        if (remaining.length === 0) {
          await fs.rmdir(subDir);
        }
      }
    }
  }

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------

  private formatLocation(loc: StorageLocation): string {
    if (loc.type === 'local') {
      return loc.path;
    }
    return `s3://${loc.path}/${loc.prefix || ''}`;
  }

  private printSummary(result: { success: number; failed: number; bytes: number }): void {
    console.log('\n' + '='.repeat(60));
    console.log('MIGRATION COMPLETE');
    console.log('='.repeat(60));
    console.log(`Success: ${result.success}`);
    console.log(`Failed:  ${result.failed}`);
    console.log(`Size:    ${(result.bytes / 1024 / 1024).toFixed(1)} MB`);
    console.log('='.repeat(60) + '\n');

    if (result.failed === 0) {
      console.log('Next steps:');
      console.log('  1. Verify data with: duckdb -c "SELECT COUNT(*) FROM read_parquet(\'<target>/tier=raw/**/*.parquet\', hive_partitioning=true)"');
      console.log('  2. Update plugin config to use new data directory');
      console.log('  3. Start the plugin');
      console.log('  4. Delete old data (if not already done with --delete-source)\n');
    }
  }
}

// ============================================================================
// CLI
// ============================================================================

function parseArgs(): MigrationConfig {
  const args = process.argv.slice(2);

  const getArg = (name: string): string | undefined => {
    const arg = args.find(a => a.startsWith(`--${name}=`));
    return arg?.split('=')[1];
  };

  const hasFlag = (name: string): boolean => {
    return args.includes(`--${name}`);
  };

  const parseLocation = (value: string | undefined, defaultPath: string): StorageLocation => {
    if (!value) {
      return { type: 'local', path: defaultPath };
    }

    if (value.startsWith('s3://')) {
      const parts = value.replace('s3://', '').split('/');
      return {
        type: 's3',
        path: parts[0],
        prefix: parts.slice(1).join('/') || undefined,
      };
    }

    return { type: 'local', path: value };
  };

  return {
    source: parseLocation(getArg('source'), './data'),
    target: parseLocation(getArg('target'), './data-hive'),
    dryRun: hasFlag('dry-run'),
    verify: hasFlag('verify') || !hasFlag('no-verify'),
    deleteSource: hasFlag('delete-source'),
    tier: (getArg('tier') as any) || 'raw',
  };
}

function printUsage(): void {
  console.log(`
Hive Migration Tool - Migrate parquet files to Hive-style partitioning

Usage:
  npx ts-node migrate-to-hive.ts [options]

Options:
  --source=<path>       Source location (default: ./data)
                        Local: --source=./data
                        S3:    --source=s3://bucket/prefix/

  --target=<path>       Target location (default: ./data-hive)
                        Local: --target=./data-hive
                        S3:    --target=s3://bucket/prefix/

  --tier=<tier>         Tier to assign (default: raw)
                        Options: raw, 5s, 60s, 1h

  --dry-run             Preview migration without making changes
  --verify              Verify files after migration (default: true)
  --no-verify           Skip verification
  --delete-source       Delete source files after successful migration

Examples:
  # Preview local migration
  npx ts-node migrate-to-hive.ts --dry-run

  # Migrate local to local
  npx ts-node migrate-to-hive.ts --source=./data --target=./data-hive --verify

  # Migrate local to S3
  npx ts-node migrate-to-hive.ts --source=./data --target=s3://my-bucket/signalk/

  # Migrate S3 to S3 (restructure)
  npx ts-node migrate-to-hive.ts --source=s3://bucket/old/ --target=s3://bucket/hive/

  # Migrate with cleanup
  npx ts-node migrate-to-hive.ts --source=./data --target=./data-hive --delete-source
`);
}

// Main
async function main(): Promise<void> {
  if (process.argv.includes('--help') || process.argv.includes('-h')) {
    printUsage();
    return;
  }

  const config = parseArgs();
  const tool = new MigrationTool(config);
  await tool.run();
}

main().catch(err => {
  console.error('Migration failed:', err);
  process.exit(1);
});
```

### 5.4 API Routes

Add to `src/api-routes.ts` (following existing patterns like `validationJobs`):

```typescript
// Migration job tracking (same pattern as validationJobs)
interface MigrationProgress {
  jobId: string;
  status: 'scanning' | 'running' | 'cancelling' | 'completed' | 'cancelled' | 'error';
  phase: 'scan' | 'migrate' | 'verify' | 'cleanup';
  processed: number;
  total: number;
  percent: number;
  bytesTransferred: number;
  currentFile?: string;
  startTime: Date;
  cancelRequested?: boolean;
  error?: string;
  result?: {
    success: number;
    failed: number;
    skipped: number;
    totalBytes: number;
  };
}

const migrationJobs = new Map<string, MigrationProgress>();

// POST /api/migrate/scan - Scan source and return file count/size
router.post('/api/migrate/scan', async (req, res) => {
  const { sourceType, sourcePath, sourcePrefix } = req.body;

  const migrationService = new MigrationService(/* config from state */);
  const scanResult = await migrationService.scanSource({
    type: sourceType,
    path: sourcePath,
    prefix: sourcePrefix,
  });

  res.json({
    success: true,
    fileCount: scanResult.files.length,
    totalSize: scanResult.totalSize,
    contexts: scanResult.contexts,
    paths: scanResult.paths,
  });
});

// POST /api/migrate - Start migration job
router.post('/api/migrate', async (req, res) => {
  const { source, target, tier, deleteSource } = req.body;

  const jobId = `mig_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

  const progress: MigrationProgress = {
    jobId,
    status: 'scanning',
    phase: 'scan',
    processed: 0,
    total: 0,
    percent: 0,
    bytesTransferred: 0,
    startTime: new Date(),
  };

  migrationJobs.set(jobId, progress);

  // Run migration async
  runMigrationJob(jobId, source, target, tier, deleteSource, state);

  res.json({ success: true, jobId });
});

// GET /api/migrate/progress/:jobId - Get migration progress
router.get('/api/migrate/progress/:jobId', (req, res) => {
  const progress = migrationJobs.get(req.params.jobId);
  if (!progress) {
    return res.status(404).json({ success: false, error: 'Job not found' });
  }
  res.json(progress);
});

// POST /api/migrate/cancel/:jobId - Cancel migration
router.post('/api/migrate/cancel/:jobId', (req, res) => {
  const job = migrationJobs.get(req.params.jobId);
  if (!job) {
    return res.status(404).json({ success: false, error: 'Job not found' });
  }
  job.cancelRequested = true;
  job.status = 'cancelling';
  res.json({ success: true, status: job.status });
});
```

### 5.5 Frontend: Migration Tab

Add to `public/index.html` (new tab):

```html
<button class="tab-button" onclick="showTab('migration')">Migration</button>

<!-- Migration Tab Panel -->
<div id="migration" class="tab-panel">
  <div class="section">
    <h2>Migrate to Hive Structure</h2>
    <p>Convert existing parquet files to Hive-style partitioning for better query performance.</p>

    <div class="form-group">
      <label>Source Type</label>
      <select id="migrationSourceType" onchange="updateMigrationSourceFields()">
        <option value="local">Local Filesystem</option>
        <option value="s3">Amazon S3</option>
      </select>
    </div>

    <div class="form-group">
      <label>Source Path</label>
      <input type="text" id="migrationSourcePath" placeholder="./data" />
    </div>

    <div class="form-group" id="sourceS3Fields" style="display:none;">
      <label>S3 Prefix</label>
      <input type="text" id="migrationSourcePrefix" placeholder="signalk/" />
    </div>

    <div class="form-group">
      <label>Target Type</label>
      <select id="migrationTargetType" onchange="updateMigrationTargetFields()">
        <option value="local">Local Filesystem</option>
        <option value="s3">Amazon S3</option>
      </select>
    </div>

    <div class="form-group">
      <label>Target Path</label>
      <input type="text" id="migrationTargetPath" placeholder="./data-hive" />
    </div>

    <div class="form-group">
      <label>Tier</label>
      <select id="migrationTier">
        <option value="raw">Raw (1s resolution)</option>
        <option value="5s">5s aggregation</option>
        <option value="60s">60s aggregation</option>
        <option value="1h">1h aggregation</option>
      </select>
    </div>

    <div class="form-group">
      <label>
        <input type="checkbox" id="migrationDeleteSource" />
        Delete source files after successful migration
      </label>
    </div>

    <div class="button-group">
      <button onclick="scanMigrationSource()">Scan Source</button>
      <button onclick="startMigration()" id="startMigrationBtn" disabled>Start Migration</button>
      <button onclick="cancelMigration()" id="cancelMigrationBtn" style="display:none;">Cancel</button>
    </div>

    <div id="migrationScanResults" style="display:none;">
      <h3>Scan Results</h3>
      <p>Files found: <span id="migrationFileCount">0</span></p>
      <p>Total size: <span id="migrationTotalSize">0 MB</span></p>
      <p>Contexts: <span id="migrationContexts">0</span></p>
      <p>Paths: <span id="migrationPaths">0</span></p>
    </div>

    <div id="migrationProgress" style="display:none;">
      <h3>Migration Progress</h3>
      <div class="progress-bar">
        <div class="progress-fill" id="migrationProgressBar" style="width: 0%"></div>
      </div>
      <p id="migrationProgressText">0%</p>
      <p id="migrationCurrentFile"></p>
      <p id="migrationBytesTransferred"></p>
    </div>

    <div id="migrationResults" style="display:none;">
      <h3>Migration Complete</h3>
      <p>Success: <span id="migrationSuccessCount">0</span></p>
      <p>Failed: <span id="migrationFailedCount">0</span></p>
      <p>Total transferred: <span id="migrationTotalTransferred">0 MB</span></p>
    </div>
  </div>
</div>
```

### 5.6 Frontend: Migration JavaScript

Create `public/js/migration.js`:

```javascript
let currentMigrationJobId = null;
let migrationPollInterval = null;

export async function scanMigrationSource() {
  const sourceType = document.getElementById('migrationSourceType').value;
  const sourcePath = document.getElementById('migrationSourcePath').value;
  const sourcePrefix = document.getElementById('migrationSourcePrefix')?.value;

  try {
    const response = await fetch('/plugins/signalk-parquet/api/migrate/scan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sourceType, sourcePath, sourcePrefix }),
    });

    const result = await response.json();

    if (result.success) {
      document.getElementById('migrationScanResults').style.display = 'block';
      document.getElementById('migrationFileCount').textContent = result.fileCount;
      document.getElementById('migrationTotalSize').textContent = formatBytes(result.totalSize);
      document.getElementById('migrationContexts').textContent = result.contexts.length;
      document.getElementById('migrationPaths').textContent = result.paths.length;
      document.getElementById('startMigrationBtn').disabled = false;
    } else {
      alert('Scan failed: ' + result.error);
    }
  } catch (err) {
    alert('Scan error: ' + err.message);
  }
}

export async function startMigration() {
  const source = {
    type: document.getElementById('migrationSourceType').value,
    path: document.getElementById('migrationSourcePath').value,
    prefix: document.getElementById('migrationSourcePrefix')?.value,
  };

  const target = {
    type: document.getElementById('migrationTargetType').value,
    path: document.getElementById('migrationTargetPath').value,
  };

  const tier = document.getElementById('migrationTier').value;
  const deleteSource = document.getElementById('migrationDeleteSource').checked;

  try {
    const response = await fetch('/plugins/signalk-parquet/api/migrate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ source, target, tier, deleteSource }),
    });

    const result = await response.json();

    if (result.success) {
      currentMigrationJobId = result.jobId;
      document.getElementById('migrationProgress').style.display = 'block';
      document.getElementById('startMigrationBtn').style.display = 'none';
      document.getElementById('cancelMigrationBtn').style.display = 'inline-block';
      startProgressPolling();
    } else {
      alert('Migration failed to start: ' + result.error);
    }
  } catch (err) {
    alert('Migration error: ' + err.message);
  }
}

function startProgressPolling() {
  migrationPollInterval = setInterval(async () => {
    try {
      const response = await fetch(
        `/plugins/signalk-parquet/api/migrate/progress/${currentMigrationJobId}`
      );
      const progress = await response.json();

      updateProgressUI(progress);

      if (progress.status === 'completed' || progress.status === 'error' || progress.status === 'cancelled') {
        clearInterval(migrationPollInterval);
        showMigrationResults(progress);
      }
    } catch (err) {
      console.error('Progress poll error:', err);
    }
  }, 1000);
}

function updateProgressUI(progress) {
  document.getElementById('migrationProgressBar').style.width = progress.percent + '%';
  document.getElementById('migrationProgressText').textContent =
    `${progress.percent}% (${progress.processed}/${progress.total})`;
  document.getElementById('migrationCurrentFile').textContent =
    progress.currentFile || '';
  document.getElementById('migrationBytesTransferred').textContent =
    `Transferred: ${formatBytes(progress.bytesTransferred)}`;
}

function showMigrationResults(progress) {
  document.getElementById('migrationProgress').style.display = 'none';
  document.getElementById('migrationResults').style.display = 'block';
  document.getElementById('cancelMigrationBtn').style.display = 'none';
  document.getElementById('startMigrationBtn').style.display = 'inline-block';

  if (progress.result) {
    document.getElementById('migrationSuccessCount').textContent = progress.result.success;
    document.getElementById('migrationFailedCount').textContent = progress.result.failed;
    document.getElementById('migrationTotalTransferred').textContent =
      formatBytes(progress.result.totalBytes);
  }
}

export async function cancelMigration() {
  if (!currentMigrationJobId) return;

  try {
    await fetch(`/plugins/signalk-parquet/api/migrate/cancel/${currentMigrationJobId}`, {
      method: 'POST',
    });
  } catch (err) {
    console.error('Cancel error:', err);
  }
}

function formatBytes(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  if (bytes < 1024 * 1024 * 1024) return (bytes / 1024 / 1024).toFixed(1) + ' MB';
  return (bytes / 1024 / 1024 / 1024).toFixed(1) + ' GB';
}
```

### 5.7 Verification After Migration

After migration completes, verify with DuckDB queries in the "Data Paths" tab:

```sql
-- Count rows in new structure
SELECT COUNT(*) as rows,
       COUNT(DISTINCT tier) as tiers,
       COUNT(DISTINCT context) as contexts
FROM read_parquet('data-hive/tier=*/**/*.parquet', hive_partitioning=true);

-- Check partition distribution
SELECT tier, year, COUNT(*) as files
FROM read_parquet('data-hive/tier=*/**/*.parquet', hive_partitioning=true, filename=true)
GROUP BY tier, year
ORDER BY tier, year;
```

### 5.8 Post-Migration Steps

After successful migration via the UI:

1. **Update plugin config** - Change `outputDirectory` to point to new Hive location
2. **Restart plugin** - New writes will use Hive structure
3. **Delete old data** - Remove legacy structure (or use "Delete source" checkbox)

---

## Implementation Checklist

### Phase 1: Hive-Style Paths
- [ ] Create `src/utils/hive-paths.ts`
- [ ] Update parquet writer for Hive paths
- [ ] Update HistoryAPI queries with `hive_partitioning=true`
- [ ] Update path discovery utilities
- [ ] Test queries with new structure

### Phase 2: Aggregation Pipeline
- [ ] Create aggregation service
- [ ] Implement raw → 5s aggregation
- [ ] Implement 5s → 60s aggregation
- [ ] Implement 60s → 1h aggregation
- [ ] Schedule aggregation jobs
- [ ] Add aggregation status endpoint

### Phase 3: S3 Integration
- [ ] Add S3 configuration to plugin config
- [ ] Create S3 sync service
- [ ] Configure DuckDB for S3 queries
- [ ] Implement hybrid local/S3 queries
- [ ] Test S3 uploads and queries

### Phase 4: Retention & Lifecycle
- [ ] Create local retention service
- [ ] Configure S3 lifecycle rules
- [ ] Test retention enforcement
- [ ] Monitor storage usage

### Phase 5: Migration
- [ ] Create `src/scripts/migrate-to-hive.ts`
- [ ] Test local → local migration on copy of data
- [ ] Test local → S3 migration
- [ ] Test S3 → S3 migration (if applicable)
- [ ] Run production migration with `--dry-run` first
- [ ] Verify row counts match
- [ ] Update plugin config to new directory
- [ ] Delete old data after validation

---

## Configuration Example

```json
{
  "outputDirectory": "./data",
  "aggregation": {
    "enabled": true,
    "schedule": {
      "5s": "0 * * * *",
      "60s": "0 2 * * *",
      "1h": "0 3 * * 0"
    }
  },
  "retention": {
    "raw": { "days": 7 },
    "5s": { "days": 30 },
    "60s": { "days": 730 },
    "1h": { "days": null }
  },
  "s3": {
    "enabled": true,
    "bucket": "my-boat-data",
    "region": "us-east-1",
    "prefix": "signalk/",
    "tierSettings": {
      "raw": { "storageClass": "STANDARD", "sync": true },
      "5s": { "storageClass": "STANDARD", "sync": true },
      "60s": { "storageClass": "STANDARD_IA", "sync": true },
      "1h": { "storageClass": "GLACIER_IR", "sync": true }
    }
  }
}
```

---

## Query Examples

### Auto-Select Best Tier
```sql
-- DuckDB will prune partitions automatically
SELECT * FROM read_parquet(
  's3://bucket/signalk/tier=60s/context=*/path=navigation.position/year=2025/**/*.parquet',
  hive_partitioning=true
)
WHERE signalk_timestamp BETWEEN '2025-01-01' AND '2025-12-31'
```

### Query Specific Tier
```sql
-- Force raw tier for precise data
SELECT * FROM read_parquet(
  'data/tier=raw/context=*/path=navigation.position/year=2025/day=232/*.parquet',
  hive_partitioning=true
)
```

### Cross-Tier Query (Rare)
```sql
-- Combine multiple tiers if needed
SELECT * FROM (
  SELECT *, 'raw' as source_tier FROM read_parquet('data/tier=raw/**/*.parquet', hive_partitioning=true)
  UNION ALL
  SELECT *, '5s' as source_tier FROM read_parquet('data/tier=5s/**/*.parquet', hive_partitioning=true)
)
WHERE path = 'navigation.position'
  AND signalk_timestamp BETWEEN '...' AND '...'
```

---

## References

- [DuckDB Hive Partitioning](https://duckdb.org/docs/data/partitioning/hive_partitioning)
- [DuckDB S3 Support](https://duckdb.org/docs/extensions/httpfs/s3api)
- [AWS S3 Lifecycle Policies](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-configuration-examples.html)
- [Parquet Best Practices](https://parquet.apache.org/docs/file-format/configurations/)
