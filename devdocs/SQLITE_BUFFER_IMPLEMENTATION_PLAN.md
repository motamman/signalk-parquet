# SQLite Write-Ahead Buffer Implementation Plan

> **Status**: Planning
> **Created**: 2026-02-19
> **Goal**: Replace in-memory buffer with SQLite for crash-safe data ingestion before parquet export

---

## Problem Statement

### Current Pipeline
```
SignalK stream → In-memory buffer → Parquet file → Daily consolidation
```

### Failure Modes
| Failure Point | Consequence |
|---------------|-------------|
| Crash during buffering | All buffered data lost |
| Crash during parquet write | Corrupted parquet file |
| Process restart | Gap in recorded data |

### Why Parquet Writes Corrupt
- Parquet files have a footer written last
- If write is interrupted, footer is missing/invalid
- Entire file becomes unreadable
- No partial recovery possible

---

## Proposed Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  SignalK Data   │────▶│  SQLite Buffer   │────▶│  Parquet Files  │
│  (streaming)    │     │  (WAL mode)      │     │  (long-term)    │
└─────────────────┘     └──────────────────┘     └─────────────────┘
        │                       │                        │
   continuous              crash-safe               immutable
   high-frequency          recoverable              queryable
```

### Data Flow
1. **Ingest**: SignalK deltas written immediately to SQLite
2. **Buffer**: Data accumulates in SQLite (configurable duration)
3. **Export**: Periodic job exports buffered data to parquet
4. **Cleanup**: Exported rows marked/deleted from SQLite
5. **Consolidate**: Existing daily consolidation continues

### Crash Recovery
- On startup, check for unexported data in SQLite
- Resume export from last successful point
- No data loss, no corruption

---

## Phase 1: SQLite Buffer Implementation

### 1.1 Add SQLite Dependency

```bash
npm install better-sqlite3
npm install -D @types/better-sqlite3
```

**Why better-sqlite3**: Synchronous API, fastest SQLite binding for Node.js, perfect for high-throughput writes.

### 1.2 Create Buffer Database Module

Create: `src/utils/sqlite-buffer.ts`

```typescript
import Database from 'better-sqlite3';
import path from 'path';
import { SignalKDelta } from '../types';

export interface BufferedDataPoint {
  id: number;
  context: string;
  path: string;
  timestamp: string;
  value: string;  // JSON stringified
  value_type: string;
  exported: number;  // 0 = pending, 1 = exported
  created_at: string;
}

export class SQLiteBuffer {
  private db: Database.Database;
  private insertStmt: Database.Statement;
  private batchInsertTransaction: Database.Transaction;

  constructor(dataDir: string) {
    const dbPath = path.join(dataDir, 'buffer.sqlite');

    this.db = new Database(dbPath);

    // Enable WAL mode for crash safety and better write performance
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');  // Good balance of safety/speed
    this.db.pragma('cache_size = -64000');   // 64MB cache
    this.db.pragma('temp_store = MEMORY');

    this.initSchema();
    this.prepareStatements();
  }

  private initSchema(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS data_buffer (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        context TEXT NOT NULL,
        path TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        value TEXT,
        value_type TEXT NOT NULL,
        exported INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now')),

        -- Index for efficient export queries
        CONSTRAINT idx_export UNIQUE (context, path, timestamp)
      );

      CREATE INDEX IF NOT EXISTS idx_unexported
        ON data_buffer(exported, created_at)
        WHERE exported = 0;

      CREATE INDEX IF NOT EXISTS idx_context_path_time
        ON data_buffer(context, path, timestamp);
    `);
  }

  private prepareStatements(): void {
    this.insertStmt = this.db.prepare(`
      INSERT OR REPLACE INTO data_buffer
        (context, path, timestamp, value, value_type)
      VALUES (?, ?, ?, ?, ?)
    `);

    // Batch insert for better performance
    this.batchInsertTransaction = this.db.transaction(
      (dataPoints: Array<{
        context: string;
        path: string;
        timestamp: string;
        value: string;
        valueType: string;
      }>) => {
        for (const dp of dataPoints) {
          this.insertStmt.run(
            dp.context,
            dp.path,
            dp.timestamp,
            dp.value,
            dp.valueType
          );
        }
      }
    );
  }

  /**
   * Insert a single data point
   */
  insert(
    context: string,
    path: string,
    timestamp: string,
    value: unknown,
    valueType: string
  ): void {
    const valueStr = typeof value === 'object'
      ? JSON.stringify(value)
      : String(value);

    this.insertStmt.run(context, path, timestamp, valueStr, valueType);
  }

  /**
   * Insert multiple data points in a transaction (much faster)
   */
  insertBatch(
    dataPoints: Array<{
      context: string;
      path: string;
      timestamp: string;
      value: unknown;
      valueType: string;
    }>
  ): void {
    const prepared = dataPoints.map(dp => ({
      context: dp.context,
      path: dp.path,
      timestamp: dp.timestamp,
      value: typeof dp.value === 'object'
        ? JSON.stringify(dp.value)
        : String(dp.value),
      valueType: dp.valueType,
    }));

    this.batchInsertTransaction(prepared);
  }

  /**
   * Get unexported data for a specific context/path, ready for parquet export
   */
  getUnexportedData(
    context: string,
    path: string,
    limit: number = 10000
  ): BufferedDataPoint[] {
    const stmt = this.db.prepare(`
      SELECT * FROM data_buffer
      WHERE context = ? AND path = ? AND exported = 0
      ORDER BY timestamp
      LIMIT ?
    `);

    return stmt.all(context, path, limit) as BufferedDataPoint[];
  }

  /**
   * Get all unexported data grouped by context/path
   */
  getUnexportedPaths(): Array<{ context: string; path: string; count: number }> {
    const stmt = this.db.prepare(`
      SELECT context, path, COUNT(*) as count
      FROM data_buffer
      WHERE exported = 0
      GROUP BY context, path
      ORDER BY context, path
    `);

    return stmt.all() as Array<{ context: string; path: string; count: number }>;
  }

  /**
   * Mark rows as exported after successful parquet write
   */
  markExported(ids: number[]): void {
    if (ids.length === 0) return;

    const placeholders = ids.map(() => '?').join(',');
    const stmt = this.db.prepare(`
      UPDATE data_buffer
      SET exported = 1
      WHERE id IN (${placeholders})
    `);

    stmt.run(...ids);
  }

  /**
   * Delete exported data older than retention period
   */
  cleanupExported(retentionHours: number = 24): number {
    const stmt = this.db.prepare(`
      DELETE FROM data_buffer
      WHERE exported = 1
        AND created_at < datetime('now', '-' || ? || ' hours')
    `);

    const result = stmt.run(retentionHours);
    return result.changes;
  }

  /**
   * Get buffer statistics
   */
  getStats(): {
    totalRows: number;
    unexportedRows: number;
    oldestUnexported: string | null;
    dbSizeBytes: number;
  } {
    const stats = this.db.prepare(`
      SELECT
        (SELECT COUNT(*) FROM data_buffer) as totalRows,
        (SELECT COUNT(*) FROM data_buffer WHERE exported = 0) as unexportedRows,
        (SELECT MIN(created_at) FROM data_buffer WHERE exported = 0) as oldestUnexported
    `).get() as { totalRows: number; unexportedRows: number; oldestUnexported: string | null };

    // Get database file size
    const pageCount = this.db.pragma('page_count', { simple: true }) as number;
    const pageSize = this.db.pragma('page_size', { simple: true }) as number;

    return {
      ...stats,
      dbSizeBytes: pageCount * pageSize,
    };
  }

  /**
   * Checkpoint WAL file (call periodically or on graceful shutdown)
   */
  checkpoint(): void {
    this.db.pragma('wal_checkpoint(TRUNCATE)');
  }

  /**
   * Close database connection
   */
  close(): void {
    this.checkpoint();
    this.db.close();
  }
}
```

### 1.3 Modify Parquet Writer to Use Buffer

Location: `src/parquet-writer.ts`

```typescript
import { SQLiteBuffer } from './utils/sqlite-buffer';

export class ParquetWriter {
  private sqliteBuffer: SQLiteBuffer;
  private memoryBuffer: Map<string, DataPoint[]>;  // Small in-memory batch
  private flushThreshold = 100;  // Flush to SQLite every N points per path

  constructor(dataDir: string) {
    this.sqliteBuffer = new SQLiteBuffer(dataDir);
    this.memoryBuffer = new Map();
  }

  /**
   * Buffer incoming data point
   */
  bufferDataPoint(
    context: string,
    path: string,
    timestamp: string,
    value: unknown,
    valueType: string
  ): void {
    const key = `${context}:${path}`;

    if (!this.memoryBuffer.has(key)) {
      this.memoryBuffer.set(key, []);
    }

    this.memoryBuffer.get(key)!.push({
      context,
      path,
      timestamp,
      value,
      valueType,
    });

    // Flush to SQLite when threshold reached
    if (this.memoryBuffer.get(key)!.length >= this.flushThreshold) {
      this.flushToSQLite(key);
    }
  }

  /**
   * Flush in-memory batch to SQLite
   */
  private flushToSQLite(key?: string): void {
    const keys = key ? [key] : Array.from(this.memoryBuffer.keys());

    for (const k of keys) {
      const batch = this.memoryBuffer.get(k);
      if (batch && batch.length > 0) {
        this.sqliteBuffer.insertBatch(batch);
        this.memoryBuffer.set(k, []);
      }
    }
  }

  /**
   * Flush all pending data to SQLite (call on shutdown)
   */
  flushAll(): void {
    this.flushToSQLite();
  }
}
```

### 1.4 Export Service

Create: `src/services/parquet-export.ts`

```typescript
import { SQLiteBuffer } from '../utils/sqlite-buffer';
import { DuckDBPool } from '../utils/duckdb-pool';
import path from 'path';
import fs from 'fs-extra';

export class ParquetExportService {
  private sqliteBuffer: SQLiteBuffer;
  private dataDir: string;
  private exportIntervalMs: number;
  private exportTimer: NodeJS.Timer | null = null;

  constructor(
    sqliteBuffer: SQLiteBuffer,
    dataDir: string,
    exportIntervalMinutes: number = 5
  ) {
    this.sqliteBuffer = sqliteBuffer;
    this.dataDir = dataDir;
    this.exportIntervalMs = exportIntervalMinutes * 60 * 1000;
  }

  /**
   * Start periodic export
   */
  start(): void {
    // Export any unexported data from previous session (crash recovery)
    this.exportAll().catch(err => {
      console.error('[ParquetExport] Error during startup export:', err);
    });

    // Schedule periodic exports
    this.exportTimer = setInterval(() => {
      this.exportAll().catch(err => {
        console.error('[ParquetExport] Error during scheduled export:', err);
      });
    }, this.exportIntervalMs);
  }

  /**
   * Stop periodic export
   */
  stop(): void {
    if (this.exportTimer) {
      clearInterval(this.exportTimer);
      this.exportTimer = null;
    }
  }

  /**
   * Export all unexported data to parquet files
   */
  async exportAll(): Promise<void> {
    const paths = this.sqliteBuffer.getUnexportedPaths();

    if (paths.length === 0) {
      return;
    }

    console.log(`[ParquetExport] Exporting ${paths.length} paths...`);

    for (const { context, path: signalkPath, count } of paths) {
      try {
        await this.exportPath(context, signalkPath);
        console.log(`[ParquetExport] Exported ${count} rows for ${context}/${signalkPath}`);
      } catch (err) {
        console.error(`[ParquetExport] Failed to export ${context}/${signalkPath}:`, err);
        // Continue with other paths
      }
    }
  }

  /**
   * Export a single context/path to parquet
   */
  private async exportPath(context: string, signalkPath: string): Promise<void> {
    const data = this.sqliteBuffer.getUnexportedData(context, signalkPath);

    if (data.length === 0) {
      return;
    }

    // Determine output file path
    const contextDir = context.replace(/\./g, '/');
    const pathDir = signalkPath.replace(/\./g, '/');
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const outputDir = path.join(this.dataDir, contextDir, pathDir);
    const outputFile = path.join(outputDir, `${timestamp}.parquet`);
    const tempFile = path.join(outputDir, `${timestamp}.parquet.tmp`);

    await fs.ensureDir(outputDir);

    // Write to temp file first (atomic write pattern)
    const connection = await DuckDBPool.getConnection();

    try {
      // Create temp table with data
      await connection.run(`
        CREATE TEMP TABLE export_data (
          signalk_timestamp TIMESTAMP,
          value VARCHAR,
          value_type VARCHAR
        )
      `);

      // Insert data
      const insertStmt = await connection.prepare(
        'INSERT INTO export_data VALUES (?, ?, ?)'
      );

      for (const row of data) {
        await insertStmt.run(row.timestamp, row.value, row.value_type);
      }

      // Export to parquet
      await connection.run(`
        COPY export_data TO '${tempFile}' (FORMAT PARQUET, COMPRESSION ZSTD)
      `);

      // Atomic rename
      await fs.rename(tempFile, outputFile);

      // Mark as exported only after successful write
      const ids = data.map(d => d.id);
      this.sqliteBuffer.markExported(ids);

    } finally {
      // Cleanup
      await connection.run('DROP TABLE IF EXISTS export_data');
      connection.disconnectSync();

      // Remove temp file if it exists (failed write)
      if (await fs.pathExists(tempFile)) {
        await fs.remove(tempFile);
      }
    }
  }
}
```

---

## Phase 2: Integration with Existing System

### 2.1 Modify Plugin Initialization

Location: `src/index.ts`

```typescript
import { SQLiteBuffer } from './utils/sqlite-buffer';
import { ParquetExportService } from './services/parquet-export';

// In plugin.start()
const sqliteBuffer = new SQLiteBuffer(config.outputDirectory);
const exportService = new ParquetExportService(
  sqliteBuffer,
  config.outputDirectory,
  config.exportIntervalMinutes || 5
);

// Start export service
exportService.start();

// Store references for cleanup
state.sqliteBuffer = sqliteBuffer;
state.exportService = exportService;

// In plugin.stop()
state.exportService?.stop();
state.sqliteBuffer?.flushAll();
state.sqliteBuffer?.close();
```

### 2.2 Add Configuration Options

Location: `src/types.ts` (PluginConfig)

```typescript
export interface PluginConfig {
  // ... existing config ...

  // Buffer settings
  bufferFlushThreshold?: number;      // Rows before flush to SQLite (default: 100)
  exportIntervalMinutes?: number;      // Parquet export interval (default: 5)
  bufferRetentionHours?: number;       // Keep exported rows for N hours (default: 24)
}
```

### 2.3 Buffer Statistics Endpoint

Add to `src/api-routes.ts`:

```typescript
router.get('/api/buffer/stats', (req, res) => {
  const stats = sqliteBuffer.getStats();
  res.json({
    ...stats,
    dbSizeMB: (stats.dbSizeBytes / 1024 / 1024).toFixed(2),
    status: stats.unexportedRows > 10000 ? 'warning' : 'healthy',
  });
});
```

---

## Phase 3: Crash Recovery

### 3.1 Startup Recovery Logic

```typescript
async function recoverFromCrash(sqliteBuffer: SQLiteBuffer): Promise<void> {
  const stats = sqliteBuffer.getStats();

  if (stats.unexportedRows > 0) {
    console.log(`[Recovery] Found ${stats.unexportedRows} unexported rows`);
    console.log(`[Recovery] Oldest unexported: ${stats.oldestUnexported}`);

    // Export will happen automatically via exportService.start()
  }

  // Clean up old exported data
  const cleaned = sqliteBuffer.cleanupExported(24);
  if (cleaned > 0) {
    console.log(`[Recovery] Cleaned up ${cleaned} old exported rows`);
  }
}
```

### 3.2 Graceful Shutdown

```typescript
// Handle process signals
process.on('SIGTERM', async () => {
  console.log('[Shutdown] Received SIGTERM, flushing buffers...');

  // Flush in-memory to SQLite
  parquetWriter.flushAll();

  // Final export attempt
  await exportService.exportAll();

  // Close SQLite (checkpoints WAL)
  sqliteBuffer.close();

  process.exit(0);
});

process.on('SIGINT', async () => {
  // Same as SIGTERM
});
```

---

## Phase 4: Monitoring & Maintenance

### 4.1 Health Checks

```typescript
interface BufferHealth {
  status: 'healthy' | 'warning' | 'critical';
  unexportedRows: number;
  oldestUnexportedMinutes: number | null;
  dbSizeMB: number;
  lastExportTime: string | null;
}

function checkBufferHealth(sqliteBuffer: SQLiteBuffer): BufferHealth {
  const stats = sqliteBuffer.getStats();

  let status: 'healthy' | 'warning' | 'critical' = 'healthy';
  let oldestMinutes: number | null = null;

  if (stats.oldestUnexported) {
    const oldest = new Date(stats.oldestUnexported);
    oldestMinutes = (Date.now() - oldest.getTime()) / 1000 / 60;

    if (oldestMinutes > 30) status = 'warning';
    if (oldestMinutes > 60) status = 'critical';
  }

  if (stats.unexportedRows > 50000) status = 'warning';
  if (stats.unexportedRows > 100000) status = 'critical';

  return {
    status,
    unexportedRows: stats.unexportedRows,
    oldestUnexportedMinutes: oldestMinutes,
    dbSizeMB: stats.dbSizeBytes / 1024 / 1024,
    lastExportTime: null,  // Track separately
  };
}
```

### 4.2 Periodic Maintenance

```typescript
// Run daily
async function runMaintenance(sqliteBuffer: SQLiteBuffer): Promise<void> {
  // Clean up old exported data
  const cleaned = sqliteBuffer.cleanupExported(24);
  console.log(`[Maintenance] Cleaned ${cleaned} exported rows`);

  // Vacuum to reclaim space (do this sparingly)
  // sqliteBuffer.db.exec('VACUUM');

  // Checkpoint WAL
  sqliteBuffer.checkpoint();
}
```

---

## Phase 5: Federated Queries (SQLite + Parquet)

### The Problem

When a query spans both buffered (SQLite) and exported (parquet) data:

```
Timeline:  [-------- Parquet (exported) --------][---- SQLite (buffered) ----]
                                                  ↑
Query:     [===========================================]
                                            spans both!
```

Without handling this, recent data would be missing from query results.

### Solution: DuckDB Federated Queries

DuckDB can attach SQLite databases and query both sources in a single query.

### 5.1 Attach SQLite on Pool Initialization

Location: `src/utils/duckdb-pool.ts`

```typescript
// During pool initialization or connection setup
async function initConnection(connection: DuckDBConnection, bufferDbPath: string): Promise<void> {
  // Load required extensions
  await connection.run('INSTALL sqlite; LOAD sqlite;');

  // Attach SQLite buffer database
  await connection.run(`
    ATTACH IF NOT EXISTS '${bufferDbPath}' AS buffer (TYPE sqlite)
  `);
}
```

### 5.2 Modify HistoryAPI for Federated Queries

Location: `src/HistoryAPI.ts`

```typescript
async getNumericValues(
  context: Context,
  from: ZonedDateTime,
  to: ZonedDateTime,
  timeResolutionMillis: number,
  pathSpecs: PathSpec[],
  // ... other params
): Promise<DataResult> {
  const connection = await DuckDBPool.getConnection();

  try {
    // Build federated query for each path
    for (const pathSpec of pathSpecs) {
      const parquetPath = this.getParquetPath(context, pathSpec.path);
      const fromIso = from.toInstant().toString();
      const toIso = to.toInstant().toString();

      const query = `
        WITH combined_data AS (
          -- Historical data from parquet files
          SELECT
            signalk_timestamp as timestamp,
            value,
            'parquet' as source
          FROM read_parquet('${parquetPath}/*.parquet', union_by_name=true)
          WHERE signalk_timestamp >= '${fromIso}'
            AND signalk_timestamp < '${toIso}'

          UNION ALL

          -- Recent buffered data from SQLite (not yet exported)
          SELECT
            timestamp,
            value,
            'buffer' as source
          FROM buffer.data_buffer
          WHERE context = '${context}'
            AND path = '${pathSpec.path}'
            AND exported = 0
            AND timestamp >= '${fromIso}'
            AND timestamp < '${toIso}'
        )
        SELECT
          strftime(
            DATE_TRUNC('seconds',
              EPOCH_MS(CAST(
                FLOOR(EPOCH_MS(timestamp::TIMESTAMP) / ${timeResolutionMillis}) * ${timeResolutionMillis}
              AS BIGINT))
            ),
            '%Y-%m-%dT%H:%M:%SZ'
          ) as bucket_timestamp,
          ${getAggregateFunction(pathSpec.aggregateMethod)}(TRY_CAST(value AS DOUBLE)) as value
        FROM combined_data
        GROUP BY bucket_timestamp
        ORDER BY bucket_timestamp
      `;

      const result = await connection.runAndReadAll(query);
      // ... process results
    }
  } finally {
    connection.disconnectSync();
  }
}
```

### 5.3 Helper Function for Parquet Path

```typescript
private getParquetPath(context: Context, signalkPath: string): string {
  const contextDir = toContextFilePath(context);
  const pathDir = signalkPath.replace(/\./g, '/');
  return path.join(this.dataDir, contextDir, pathDir);
}
```

### 5.4 Handle Edge Cases

#### No Parquet Files Yet (Fresh Install)

```typescript
const query = `
  WITH combined_data AS (
    -- Parquet data (may not exist yet)
    SELECT timestamp, value FROM (
      SELECT
        signalk_timestamp as timestamp,
        value
      FROM read_parquet('${parquetPath}/*.parquet', union_by_name=true)
      WHERE signalk_timestamp >= '${fromIso}'
        AND signalk_timestamp < '${toIso}'
    ) parquet_data
    WHERE EXISTS (
      SELECT 1 FROM glob('${parquetPath}/*.parquet')
    )

    UNION ALL

    -- SQLite buffer (always check)
    SELECT timestamp, value
    FROM buffer.data_buffer
    WHERE context = '${context}'
      AND path = '${pathSpec.path}'
      AND exported = 0
      AND timestamp >= '${fromIso}'
      AND timestamp < '${toIso}'
  )
  -- ... rest of query
`;
```

#### Alternative: Check File Existence First

```typescript
import { glob } from 'glob';

async function queryPath(context: string, signalkPath: string, from: string, to: string) {
  const parquetPath = this.getParquetPath(context, signalkPath);
  const parquetFiles = await glob(`${parquetPath}/*.parquet`);
  const hasParquetData = parquetFiles.length > 0;

  let query: string;

  if (hasParquetData) {
    // Query both sources
    query = buildFederatedQuery(parquetPath, context, signalkPath, from, to);
  } else {
    // Query only SQLite buffer
    query = buildBufferOnlyQuery(context, signalkPath, from, to);
  }

  return await connection.runAndReadAll(query);
}
```

### 5.5 Query for Real-Time Dashboard (Buffer Priority)

For dashboards showing "last N minutes", prioritize buffer data:

```typescript
// Real-time query: last 5 minutes, buffer likely has everything
const recentQuery = `
  SELECT timestamp, value
  FROM buffer.data_buffer
  WHERE context = '${context}'
    AND path = '${signalkPath}'
    AND timestamp >= datetime('now', '-5 minutes')
  ORDER BY timestamp DESC
  LIMIT 100
`;
```

### 5.6 Consistency Guarantees

| Scenario | Behavior |
|----------|----------|
| Query during export | Data in SQLite until marked exported, no duplicates |
| Query spans buffer + parquet | UNION ALL merges seamlessly |
| Crash mid-export | Data stays in SQLite, re-exported on restart |
| Overlapping timestamps | `exported = 0` filter prevents duplicates |

### 5.7 Performance Optimization

For frequently-queried recent data, skip parquet entirely:

```typescript
function shouldQueryParquet(from: ZonedDateTime, exportIntervalMinutes: number): boolean {
  const bufferWindow = exportIntervalMinutes * 2; // Safety margin
  const cutoff = ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(bufferWindow);

  // If query starts after cutoff, all data is in buffer
  return from.isBefore(cutoff);
}

// Usage
if (shouldQueryParquet(from, config.exportIntervalMinutes)) {
  // Full federated query (parquet + buffer)
  return executeFederatedQuery(...);
} else {
  // Buffer-only query (faster)
  return executeBufferQuery(...);
}
```

---

## Implementation Checklist

### Phase 1 (Core Buffer)
- [ ] Install `better-sqlite3` dependency
- [ ] Create `src/utils/sqlite-buffer.ts`
- [ ] Write unit tests for SQLiteBuffer
- [ ] Modify parquet-writer to use SQLite buffer
- [ ] Create export service

### Phase 2 (Integration)
- [ ] Integrate with plugin initialization
- [ ] Add configuration options
- [ ] Add buffer stats endpoint
- [ ] Update plugin stop to flush buffers

### Phase 3 (Crash Recovery)
- [ ] Add startup recovery logic
- [ ] Implement graceful shutdown handlers
- [ ] Test crash scenarios

### Phase 4 (Monitoring)
- [ ] Add health check endpoint
- [ ] Add maintenance scheduling
- [ ] Add alerting for buffer backlog

### Phase 5 (Federated Queries)
- [ ] Install DuckDB sqlite extension
- [ ] Attach SQLite buffer in DuckDB pool
- [ ] Modify HistoryAPI to query both sources
- [ ] Handle edge case: no parquet files yet
- [ ] Add buffer-only fast path for recent queries
- [ ] Test query consistency during exports

---

## Testing Crash Scenarios

### Test 1: Kill During Buffer Write
```bash
# Start plugin, send data, kill -9
kill -9 $(pgrep -f signalk)

# Restart and verify data recovered
```

### Test 2: Kill During Parquet Export
```bash
# Monitor export timing, kill during write
# Verify temp file cleaned up
# Verify data still in SQLite buffer
# Verify re-export succeeds
```

### Test 3: Disk Full
```bash
# Fill disk, verify graceful error handling
# Verify SQLite buffer still works
# Free space, verify export resumes
```

---

## Performance Considerations

### Write Throughput
- SQLite WAL mode: ~50,000+ inserts/second
- Batch inserts (transactions): 10x faster than individual
- In-memory micro-batch before SQLite: reduces syscalls

### Memory Usage
- SQLite cache: 64MB (configurable)
- In-memory micro-batch: ~100 rows per path × N paths

### Disk Usage
- SQLite WAL file can grow during heavy writes
- Periodic checkpoint keeps it bounded
- Cleanup exported rows after 24h

---

## Rollback Plan

If issues arise, can revert to direct parquet writes:

1. Stop export service
2. Export all remaining SQLite data
3. Remove SQLite buffer from write path
4. Keep SQLite module for future use

---

## References

- [better-sqlite3 Documentation](https://github.com/WiseLibs/better-sqlite3)
- [SQLite WAL Mode](https://www.sqlite.org/wal.html)
- [Atomic File Writes](https://lwn.net/Articles/457667/)
