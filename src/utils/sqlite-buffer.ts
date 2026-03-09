/**
 * SQLite Write-Ahead Buffer for crash-safe data ingestion
 *
 * Replaces the in-memory LRU cache with a WAL-mode SQLite database
 * that provides crash recovery and durability guarantees.
 */

import Database = require('better-sqlite3');
import * as path from 'path';
import * as fs from 'fs-extra';
import { DataRecord } from '../types';

export interface BufferRecord {
  id: number;
  context: string;
  path: string;
  received_timestamp: string;
  signalk_timestamp: string;
  value: number | string | boolean | null;
  value_json: string | null;
  source: string | null;
  source_label: string | null;
  source_type: string | null;
  source_pgn: number | null;
  source_src: string | null;
  meta: string | null;
  exported: number;
  export_batch_id: string | null;
  created_at: string;
}

export interface BufferStats {
  totalRecords: number;
  pendingRecords: number;
  exportedRecords: number;
  oldestPendingTimestamp: string | null;
  newestRecordTimestamp: string | null;
  dbSizeBytes: number;
  walSizeBytes: number;
}

export interface SQLiteBufferConfig {
  dbPath: string;
  maxBatchSize?: number;
  retentionHours?: number;
}

export class SQLiteBuffer {
  private db: Database.Database;
  private readonly dbPath: string;
  private readonly maxBatchSize: number;
  private readonly retentionHours: number;
  private insertStmt: Database.Statement;
  private getPendingStmt: Database.Statement;
  private markExportedStmt: Database.Statement;
  private cleanupStmt: Database.Statement;
  private getStatsStmt: Database.Statement;

  constructor(config: SQLiteBufferConfig) {
    this.dbPath = config.dbPath;
    this.maxBatchSize = config.maxBatchSize || 10000;
    this.retentionHours = config.retentionHours || 24;

    // Ensure directory exists
    fs.ensureDirSync(path.dirname(this.dbPath));

    // Open database with WAL mode for crash safety and better concurrency
    this.db = new Database(this.dbPath);

    // Configure for performance and crash safety
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL'); // Good balance of safety and performance
    this.db.pragma('cache_size = -64000'); // 64MB cache
    this.db.pragma('temp_store = MEMORY');
    this.db.pragma('mmap_size = 268435456'); // 256MB memory-mapped I/O

    // Create schema
    this.createSchema();

    // Prepare statements for performance
    this.insertStmt = this.db.prepare(`
      INSERT INTO buffer_records (
        context, path, received_timestamp, signalk_timestamp,
        value, value_json, source, source_label, source_type,
        source_pgn, source_src, meta, exported, export_batch_id, created_at
      ) VALUES (
        @context, @path, @received_timestamp, @signalk_timestamp,
        @value, @value_json, @source, @source_label, @source_type,
        @source_pgn, @source_src, @meta, 0, NULL, datetime('now')
      )
    `);

    this.getPendingStmt = this.db.prepare(`
      SELECT * FROM buffer_records
      WHERE exported = 0
      ORDER BY created_at ASC
      LIMIT ?
    `);

    this.markExportedStmt = this.db.prepare(`
      UPDATE buffer_records
      SET exported = 1, export_batch_id = ?
      WHERE id IN (SELECT value FROM json_each(?))
    `);

    this.cleanupStmt = this.db.prepare(`
      DELETE FROM buffer_records
      WHERE exported = 1
        AND created_at < datetime('now', '-' || ? || ' hours')
    `);

    this.getStatsStmt = this.db.prepare(`
      SELECT
        COUNT(*) as totalRecords,
        SUM(CASE WHEN exported = 0 THEN 1 ELSE 0 END) as pendingRecords,
        SUM(CASE WHEN exported = 1 THEN 1 ELSE 0 END) as exportedRecords,
        MIN(CASE WHEN exported = 0 THEN received_timestamp END) as oldestPendingTimestamp,
        MAX(received_timestamp) as newestRecordTimestamp
      FROM buffer_records
    `);
  }

  private createSchema(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS buffer_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        context TEXT NOT NULL,
        path TEXT NOT NULL,
        received_timestamp TEXT NOT NULL,
        signalk_timestamp TEXT NOT NULL,
        value TEXT,
        value_json TEXT,
        source TEXT,
        source_label TEXT,
        source_type TEXT,
        source_pgn INTEGER,
        source_src TEXT,
        meta TEXT,
        exported INTEGER NOT NULL DEFAULT 0,
        export_batch_id TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );

      CREATE INDEX IF NOT EXISTS idx_buffer_context_path_exported
        ON buffer_records (context, path, exported);

      CREATE INDEX IF NOT EXISTS idx_buffer_exported_created
        ON buffer_records (exported, created_at);

      CREATE INDEX IF NOT EXISTS idx_buffer_received_timestamp
        ON buffer_records (received_timestamp);
    `);
  }

  /**
   * Check if the database connection is open
   */
  isOpen(): boolean {
    return this.db.open;
  }

  /**
   * Insert a single record into the buffer
   */
  insert(record: DataRecord): void {
    if (!this.db.open) {
      throw new Error('SQLite buffer is closed');
    }
    const params = this.prepareRecord(record);
    this.insertStmt.run(params);
  }

  /**
   * Insert multiple records in a single transaction (much faster)
   */
  insertBatch(records: DataRecord[]): void {
    if (records.length === 0) return;

    const insertMany = this.db.transaction((recs: DataRecord[]) => {
      for (const record of recs) {
        const params = this.prepareRecord(record);
        this.insertStmt.run(params);
      }
    });

    insertMany(records);
  }

  private prepareRecord(record: DataRecord): Record<string, unknown> {
    // Serialize value based on type
    let valueStr: string | null = null;
    let valueJson: string | null = null;

    if (record.value !== null && record.value !== undefined) {
      if (typeof record.value === 'object') {
        valueJson = JSON.stringify(record.value);
      } else {
        valueStr = String(record.value);
      }
    }

    // Handle value_json if present
    if (record.value_json !== undefined && record.value_json !== null) {
      valueJson =
        typeof record.value_json === 'string'
          ? record.value_json
          : JSON.stringify(record.value_json);
    }

    // Serialize source if object
    const sourceStr = record.source
      ? typeof record.source === 'object'
        ? JSON.stringify(record.source)
        : String(record.source)
      : null;

    // Serialize meta if object
    const metaStr = record.meta
      ? typeof record.meta === 'object'
        ? JSON.stringify(record.meta)
        : String(record.meta)
      : null;

    return {
      context: record.context,
      path: record.path,
      received_timestamp: record.received_timestamp,
      signalk_timestamp: record.signalk_timestamp,
      value: valueStr,
      value_json: valueJson,
      source: sourceStr,
      source_label: record.source_label || null,
      source_type: record.source_type || null,
      source_pgn: record.source_pgn || null,
      source_src: record.source_src || null,
      meta: metaStr,
    };
  }

  /**
   * Get pending records that need to be exported to Parquet
   */
  getPendingRecords(limit?: number): BufferRecord[] {
    const actualLimit = limit || this.maxBatchSize;
    return this.getPendingStmt.all(actualLimit) as BufferRecord[];
  }

  /**
   * Get pending records grouped by context and path
   */
  getPendingRecordsGrouped(limit?: number): Map<string, DataRecord[]> {
    const records = this.getPendingRecords(limit);
    const grouped = new Map<string, DataRecord[]>();

    for (const record of records) {
      const key = `${record.context}:${record.path}`;
      if (!grouped.has(key)) {
        grouped.set(key, []);
      }
      grouped.get(key)!.push(this.bufferRecordToDataRecord(record));
    }

    return grouped;
  }

  /**
   * Convert a BufferRecord back to a DataRecord
   */
  private bufferRecordToDataRecord(record: BufferRecord): DataRecord {
    let value: unknown = record.value;
    let valueJson: unknown = undefined;

    // Parse value_json if present
    if (record.value_json) {
      try {
        valueJson = JSON.parse(record.value_json);
      } catch {
        valueJson = record.value_json;
      }
    }

    // Parse numeric values
    if (value !== null && !isNaN(Number(value))) {
      value = Number(value);
    } else if (value === 'true') {
      value = true;
    } else if (value === 'false') {
      value = false;
    }

    // Parse source if JSON
    let source: unknown = record.source;
    if (record.source) {
      try {
        source = JSON.parse(record.source);
      } catch {
        source = record.source;
      }
    }

    // Parse meta if JSON
    let meta: unknown = record.meta;
    if (record.meta) {
      try {
        meta = JSON.parse(record.meta);
      } catch {
        meta = record.meta;
      }
    }

    return {
      received_timestamp: record.received_timestamp,
      signalk_timestamp: record.signalk_timestamp,
      context: record.context,
      path: record.path,
      value: value,
      value_json: valueJson as string | object | undefined,
      source: source as string | object | undefined,
      source_label: record.source_label || undefined,
      source_type: record.source_type || undefined,
      source_pgn: record.source_pgn || undefined,
      source_src: record.source_src || undefined,
      meta: meta as string | object | undefined,
    };
  }

  /**
   * Mark records as exported with a batch ID
   */
  markAsExported(recordIds: number[], batchId: string): void {
    if (recordIds.length === 0) return;
    this.markExportedStmt.run(batchId, JSON.stringify(recordIds));
  }

  /**
   * Clean up old exported records
   */
  cleanup(): number {
    const result = this.cleanupStmt.run(this.retentionHours);
    return result.changes;
  }

  /**
   * Get buffer statistics
   */
  getStats(): BufferStats {
    const row = this.getStatsStmt.get() as {
      totalRecords: number;
      pendingRecords: number;
      exportedRecords: number;
      oldestPendingTimestamp: string | null;
      newestRecordTimestamp: string | null;
    };

    // Get file sizes
    let dbSizeBytes = 0;
    let walSizeBytes = 0;

    try {
      const dbStats = fs.statSync(this.dbPath);
      dbSizeBytes = dbStats.size;
    } catch {
      // File may not exist yet
    }

    try {
      const walPath = this.dbPath + '-wal';
      if (fs.existsSync(walPath)) {
        const walStats = fs.statSync(walPath);
        walSizeBytes = walStats.size;
      }
    } catch {
      // WAL file may not exist
    }

    return {
      totalRecords: row.totalRecords || 0,
      pendingRecords: row.pendingRecords || 0,
      exportedRecords: row.exportedRecords || 0,
      oldestPendingTimestamp: row.oldestPendingTimestamp,
      newestRecordTimestamp: row.newestRecordTimestamp,
      dbSizeBytes,
      walSizeBytes,
    };
  }

  /**
   * Get count of pending records (faster than full stats)
   */
  getPendingCount(): number {
    if (!this.db.open) {
      return 0;
    }
    const row = this.db
      .prepare(
        'SELECT COUNT(*) as count FROM buffer_records WHERE exported = 0'
      )
      .get() as { count: number };
    return row.count;
  }

  /**
   * Get records for a specific context and path (for federated queries)
   */
  getRecordsForPath(
    context: string,
    signalkPath: string,
    from?: string,
    to?: string
  ): DataRecord[] {
    if (!this.db.open) {
      return []; // Return empty array if database is closed
    }
    let query = `
      SELECT * FROM buffer_records
      WHERE context = ? AND path = ?
    `;
    const params: (string | number)[] = [context, signalkPath];

    if (from) {
      query += ` AND received_timestamp >= ?`;
      params.push(from);
    }

    if (to) {
      query += ` AND received_timestamp <= ?`;
      params.push(to);
    }

    query += ` ORDER BY received_timestamp ASC`;

    const stmt = this.db.prepare(query);
    const records = stmt.all(...params) as BufferRecord[];
    return records.map(r => this.bufferRecordToDataRecord(r));
  }

  /**
   * Checkpoint WAL file (useful before backup or to reduce WAL size)
   */
  checkpoint(): void {
    this.db.pragma('wal_checkpoint(TRUNCATE)');
  }

  /**
   * Get distinct dates that have unexported records, excluding today
   * Used at startup to catch up on missed exports without creating partial day files
   *
   * @param excludeToday If true (default), excludes today's date to avoid partial exports
   */
  getDatesWithUnexportedRecords(excludeToday: boolean = true): string[] {
    if (!this.db.open) {
      return [];
    }

    let query = `
      SELECT DISTINCT date(received_timestamp) as record_date
      FROM buffer_records
      WHERE exported = 0
    `;

    if (excludeToday) {
      query += ` AND date(received_timestamp) < date('now')`;
    }

    query += ` ORDER BY record_date ASC`;

    const stmt = this.db.prepare(query);
    const rows = stmt.all() as Array<{ record_date: string }>;
    return rows.map(r => r.record_date);
  }

  /**
   * Get distinct context/path combinations for a specific date (UTC)
   * Used for daily export to determine which paths have data for a given day
   */
  getPathsForDate(date: Date): Array<{ context: string; path: string }> {
    if (!this.db.open) {
      return [];
    }

    const dateStr = date.toISOString().slice(0, 10); // YYYY-MM-DD
    const startOfDay = `${dateStr}T00:00:00.000Z`;
    const endOfDay = `${dateStr}T23:59:59.999Z`;

    const stmt = this.db.prepare(`
      SELECT DISTINCT context, path
      FROM buffer_records
      WHERE received_timestamp >= ? AND received_timestamp <= ?
        AND exported = 0
      ORDER BY context, path
    `);

    return stmt.all(startOfDay, endOfDay) as Array<{
      context: string;
      path: string;
    }>;
  }

  /**
   * Get all records for a specific context, path, and date (UTC)
   * Returns records with their IDs for marking as exported
   */
  getRecordsForPathAndDate(
    context: string,
    signalkPath: string,
    date: Date
  ): { records: DataRecord[]; ids: number[] } {
    if (!this.db.open) {
      return { records: [], ids: [] };
    }

    const dateStr = date.toISOString().slice(0, 10); // YYYY-MM-DD
    const startOfDay = `${dateStr}T00:00:00.000Z`;
    const endOfDay = `${dateStr}T23:59:59.999Z`;

    const stmt = this.db.prepare(`
      SELECT * FROM buffer_records
      WHERE context = ? AND path = ?
        AND received_timestamp >= ? AND received_timestamp <= ?
        AND exported = 0
      ORDER BY received_timestamp ASC
    `);

    const bufferRecords = stmt.all(
      context,
      signalkPath,
      startOfDay,
      endOfDay
    ) as BufferRecord[];

    const records: DataRecord[] = [];
    const ids: number[] = [];

    for (const record of bufferRecords) {
      records.push(this.bufferRecordToDataRecord(record));
      ids.push(record.id);
    }

    return { records, ids };
  }

  /**
   * Mark records for a specific date as exported
   * Used after successful daily export
   */
  markDateExported(
    context: string,
    signalkPath: string,
    date: Date,
    batchId: string
  ): void {
    if (!this.db.open) {
      return;
    }

    const dateStr = date.toISOString().slice(0, 10); // YYYY-MM-DD
    const startOfDay = `${dateStr}T00:00:00.000Z`;
    const endOfDay = `${dateStr}T23:59:59.999Z`;

    const stmt = this.db.prepare(`
      UPDATE buffer_records
      SET exported = 1, export_batch_id = ?
      WHERE context = ? AND path = ?
        AND received_timestamp >= ? AND received_timestamp <= ?
        AND exported = 0
    `);

    stmt.run(batchId, context, signalkPath, startOfDay, endOfDay);
  }

  /**
   * Get the database path
   */
  getDbPath(): string {
    return this.dbPath;
  }

  /**
   * Close the database connection
   */
  close(): void {
    // Checkpoint WAL before closing
    try {
      this.checkpoint();
    } catch {
      // Ignore checkpoint errors during shutdown
    }
    this.db.close();
  }
}
