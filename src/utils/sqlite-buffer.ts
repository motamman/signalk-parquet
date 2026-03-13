/**
 * SQLite Write-Ahead Buffer for crash-safe data ingestion
 *
 * Per-path table architecture: each SignalK path gets its own table in buffer.db.
 * Scalar paths have a `value` column; object paths have `value_json` + flattened `value_*` columns.
 * This eliminates column pollution from ALTER TABLE ADD COLUMN on a shared table.
 */

import { DatabaseSync, StatementSync } from 'node:sqlite';
import * as path from 'path';
import * as fs from 'fs-extra';
import { DataRecord } from '../types';

export interface BufferRecord {
  id: number;
  context: string;
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
  [key: string]: unknown; // Dynamic value_* columns
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

interface TableInfo {
  tableName: string;
  isObject: boolean;
  columns: Set<string>;
  insertStmt: StatementSync;
}

/**
 * Convert a SignalK path to a SQLite table name.
 * Dots become underscores, any non-alphanumeric/underscore chars are stripped,
 * prefixed with `buffer_`.
 */
export function pathToTableName(signalkPath: string): string {
  return `buffer_${signalkPath.replace(/\./g, '_').replace(/[^a-zA-Z0-9_]/g, '_')}`;
}

export class SQLiteBuffer {
  private db: DatabaseSync;
  private _open: boolean;
  private readonly dbPath: string;
  private readonly retentionHours: number;
  private tableMap: Map<string, TableInfo>; // keyed by SignalK path

  constructor(config: SQLiteBufferConfig) {
    this.dbPath = config.dbPath;
    this.retentionHours = config.retentionHours || 24;

    // Ensure directory exists
    fs.ensureDirSync(path.dirname(this.dbPath));

    // Open database with WAL mode for crash safety and better concurrency
    this.db = new DatabaseSync(this.dbPath);
    this._open = true;

    // Configure for performance and crash safety
    this.db.exec('PRAGMA journal_mode = WAL');
    this.db.exec('PRAGMA synchronous = NORMAL');
    this.db.exec('PRAGMA cache_size = -64000'); // 64MB cache
    this.db.exec('PRAGMA temp_store = MEMORY');
    this.db.exec('PRAGMA mmap_size = 268435456'); // 256MB memory-mapped I/O

    // Create metadata table
    this.createMetadataSchema();

    // Migrate from old single-table layout if needed
    this.migrateFromLegacy();

    // Rebuild tableMap from buffer_tables metadata
    this.tableMap = new Map();
    this.loadExistingTables();
  }

  private createMetadataSchema(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS buffer_tables (
        path TEXT PRIMARY KEY,
        table_name TEXT NOT NULL,
        is_object INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
    `);
  }

  /**
   * Migrate from the legacy single buffer_records table to per-path tables.
   * Runs once automatically if buffer_records exists but buffer_tables is empty.
   */
  private migrateFromLegacy(): void {
    // Check if old table exists
    const oldTableExists = this.db
      .prepare(
        `SELECT name FROM sqlite_master WHERE type='table' AND name='buffer_records'`
      )
      .get();

    if (!oldTableExists) return;

    // Check if we already migrated (buffer_tables has entries)
    const tableCount = (
      this.db.prepare(`SELECT COUNT(*) as cnt FROM buffer_tables`).get() as {
        cnt: number;
      }
    ).cnt;

    if (tableCount > 0) {
      // Already migrated — drop legacy table if it still exists
      this.db.exec(`DROP TABLE IF EXISTS buffer_records`);
      return;
    }

    // Get all columns from the old table
    const oldColumns = (
      this.db.prepare('PRAGMA table_info(buffer_records)').all() as Array<{
        name: string;
        type: string;
      }>
    ).map(c => c.name);

    // Discover all dynamic value_* columns (beyond base schema)
    const dynamicValueCols = oldColumns.filter(
      c => c.startsWith('value_') && c !== 'value_json'
    );

    // Get distinct paths
    const paths = (
      this.db
        .prepare(`SELECT DISTINCT path FROM buffer_records ORDER BY path`)
        .all() as Array<{ path: string }>
    ).map(r => r.path);

    if (paths.length === 0) {
      // No data — just drop the old table
      this.db.exec(`DROP TABLE IF EXISTS buffer_records`);
      return;
    }

    this.db.exec('BEGIN');
    try {
      for (const signalkPath of paths) {
        // Determine if this path is an object path
        const hasJson = (
          this.db
            .prepare(
              `SELECT COUNT(*) as cnt FROM buffer_records WHERE path = ? AND value_json IS NOT NULL`
            )
            .get(signalkPath) as { cnt: number }
        ).cnt;

        const isObject = hasJson > 0;

        // For object paths, discover which value_* columns have data for this path
        const pathValueCols: string[] = [];
        if (isObject && dynamicValueCols.length > 0) {
          // Check which dynamic columns have non-NULL data for this path
          for (const col of dynamicValueCols) {
            const hasData = (
              this.db
                .prepare(
                  `SELECT COUNT(*) as cnt FROM buffer_records WHERE path = ? AND ${col} IS NOT NULL`
                )
                .get(signalkPath) as { cnt: number }
            ).cnt;
            if (hasData > 0) {
              pathValueCols.push(col);
            }
          }
        }

        const tableName = pathToTableName(signalkPath);

        // Build CREATE TABLE
        const columns: string[] = [
          'id INTEGER PRIMARY KEY AUTOINCREMENT',
          'context TEXT NOT NULL',
          'received_timestamp TEXT NOT NULL',
          'signalk_timestamp TEXT NOT NULL',
        ];

        if (isObject) {
          columns.push('value_json TEXT');
          for (const col of pathValueCols) {
            columns.push(`${col} REAL`);
          }
        } else {
          columns.push('value TEXT');
        }

        columns.push(
          'source TEXT',
          'source_label TEXT',
          'source_type TEXT',
          'source_pgn INTEGER',
          'source_src TEXT',
          'meta TEXT',
          'exported INTEGER NOT NULL DEFAULT 0',
          'export_batch_id TEXT',
          `created_at TEXT NOT NULL DEFAULT (datetime('now'))`
        );

        this.db.exec(`CREATE TABLE ${tableName} (${columns.join(', ')})`);
        this.db.exec(
          `CREATE INDEX idx_${tableName}_ctx_exp ON ${tableName} (context, exported)`
        );
        this.db.exec(
          `CREATE INDEX idx_${tableName}_received ON ${tableName} (received_timestamp)`
        );

        // Copy data
        const selectCols = [
          'context',
          'received_timestamp',
          'signalk_timestamp',
        ];
        if (isObject) {
          selectCols.push('value_json');
          selectCols.push(...pathValueCols);
        } else {
          selectCols.push('value');
        }
        selectCols.push(
          'source',
          'source_label',
          'source_type',
          'source_pgn',
          'source_src',
          'meta',
          'exported',
          'export_batch_id',
          'created_at'
        );

        this.db.exec(
          `INSERT INTO ${tableName} (${selectCols.join(', ')}) SELECT ${selectCols.join(', ')} FROM buffer_records WHERE path = '${signalkPath.replace(/'/g, "''")}'`
        );

        // Register in metadata
        this.db
          .prepare(
            `INSERT INTO buffer_tables (path, table_name, is_object) VALUES (?, ?, ?)`
          )
          .run(signalkPath, tableName, isObject ? 1 : 0);
      }

      // Drop legacy table
      this.db.exec(`DROP TABLE buffer_records`);
      this.db.exec('COMMIT');
    } catch (e) {
      this.db.exec('ROLLBACK');
      throw e;
    }
  }

  /**
   * Load existing per-path tables from buffer_tables metadata and prepare INSERT statements.
   */
  private loadExistingTables(): void {
    const rows = this.db
      .prepare(`SELECT path, table_name, is_object FROM buffer_tables`)
      .all() as Array<{ path: string; table_name: string; is_object: number }>;

    for (const row of rows) {
      const columns = new Set<string>();
      const tableInfo = this.db
        .prepare(`PRAGMA table_info(${row.table_name})`)
        .all() as Array<{ name: string }>;
      for (const col of tableInfo) {
        columns.add(col.name);
      }

      const insertStmt = this.buildInsertStmt(
        row.table_name,
        columns,
        row.is_object === 1
      );

      this.tableMap.set(row.path, {
        tableName: row.table_name,
        isObject: row.is_object === 1,
        columns,
        insertStmt,
      });
    }
  }

  /**
   * Build an INSERT statement for a per-path table.
   */
  private buildInsertStmt(
    tableName: string,
    columns: Set<string>,
    isObject: boolean
  ): StatementSync {
    const insertCols: string[] = [];
    const placeholders: string[] = [];

    // Order: context, received_timestamp, signalk_timestamp, value/value_json+value_*, source*, meta, exported, export_batch_id, created_at
    const orderedCols = ['context', 'received_timestamp', 'signalk_timestamp'];

    if (isObject) {
      orderedCols.push('value_json');
      // Add any dynamic value_* columns
      for (const col of columns) {
        if (col.startsWith('value_') && col !== 'value_json') {
          orderedCols.push(col);
        }
      }
    } else {
      orderedCols.push('value');
    }

    orderedCols.push(
      'source',
      'source_label',
      'source_type',
      'source_pgn',
      'source_src',
      'meta'
    );

    for (const col of orderedCols) {
      if (columns.has(col)) {
        insertCols.push(col);
        placeholders.push(`@${col}`);
      }
    }

    // Automatic columns
    insertCols.push('exported', 'export_batch_id', 'created_at');
    placeholders.push('0', 'NULL', "datetime('now')");

    return this.db.prepare(
      `INSERT INTO ${tableName} (${insertCols.join(', ')}) VALUES (${placeholders.join(', ')})`
    );
  }

  /**
   * Ensure a per-path table exists. Creates it on first insert for a new path.
   */
  private ensureTable(signalkPath: string, record: DataRecord): TableInfo {
    const existing = this.tableMap.get(signalkPath);
    if (existing) return existing;

    const tableName = pathToTableName(signalkPath);

    // Detect object vs scalar from the record
    const valueKeys = Object.keys(record).filter(
      k =>
        k.startsWith('value_') &&
        k !== 'value_json' &&
        record[k] !== undefined &&
        record[k] !== null
    );
    const isObject =
      valueKeys.length > 0 ||
      (record.value_json !== undefined && record.value_json !== null);

    const columnDefs: string[] = [
      'id INTEGER PRIMARY KEY AUTOINCREMENT',
      'context TEXT NOT NULL',
      'received_timestamp TEXT NOT NULL',
      'signalk_timestamp TEXT NOT NULL',
    ];

    if (isObject) {
      columnDefs.push('value_json TEXT');
      for (const key of valueKeys) {
        const val = record[key];
        const colType = typeof val === 'number' ? 'REAL' : 'TEXT';
        columnDefs.push(`${key} ${colType}`);
      }
    } else {
      columnDefs.push('value TEXT');
    }

    columnDefs.push(
      'source TEXT',
      'source_label TEXT',
      'source_type TEXT',
      'source_pgn INTEGER',
      'source_src TEXT',
      'meta TEXT',
      'exported INTEGER NOT NULL DEFAULT 0',
      'export_batch_id TEXT',
      `created_at TEXT NOT NULL DEFAULT (datetime('now'))`
    );

    this.db.exec(`CREATE TABLE ${tableName} (${columnDefs.join(', ')})`);
    this.db.exec(
      `CREATE INDEX idx_${tableName}_ctx_exp ON ${tableName} (context, exported)`
    );
    this.db.exec(
      `CREATE INDEX idx_${tableName}_received ON ${tableName} (received_timestamp)`
    );

    // Register in metadata
    this.db
      .prepare(
        `INSERT INTO buffer_tables (path, table_name, is_object) VALUES (?, ?, ?)`
      )
      .run(signalkPath, tableName, isObject ? 1 : 0);

    const columns = new Set<string>();
    const tableInfoRows = this.db
      .prepare(`PRAGMA table_info(${tableName})`)
      .all() as Array<{ name: string }>;
    for (const col of tableInfoRows) {
      columns.add(col.name);
    }

    const insertStmt = this.buildInsertStmt(tableName, columns, isObject);

    const info: TableInfo = { tableName, isObject, columns, insertStmt };
    this.tableMap.set(signalkPath, info);
    return info;
  }

  /**
   * Ensure a dynamic value_* column exists on a per-path table.
   * Only affects the single table for that path.
   */
  private ensureColumn(
    tableInfo: TableInfo,
    columnName: string,
    value: unknown
  ): void {
    if (tableInfo.columns.has(columnName)) return;

    const colType = typeof value === 'number' ? 'REAL' : 'TEXT';
    this.db.exec(
      `ALTER TABLE ${tableInfo.tableName} ADD COLUMN ${columnName} ${colType}`
    );
    tableInfo.columns.add(columnName);
    tableInfo.insertStmt = this.buildInsertStmt(
      tableInfo.tableName,
      tableInfo.columns,
      tableInfo.isObject
    );
  }

  /**
   * Check if the database connection is open
   */
  isOpen(): boolean {
    return this._open;
  }

  /**
   * Insert a single record into the buffer
   */
  insert(record: DataRecord): void {
    if (!this._open) {
      throw new Error('SQLite buffer is closed');
    }
    const tableInfo = this.ensureTable(record.path, record);
    const params = this.prepareRecord(record, tableInfo);
    tableInfo.insertStmt.run(params);
  }

  /**
   * Insert multiple records in a single transaction (much faster)
   */
  insertBatch(records: DataRecord[]): void {
    if (records.length === 0) return;

    this.db.exec('BEGIN');
    try {
      for (const record of records) {
        const tableInfo = this.ensureTable(record.path, record);
        const params = this.prepareRecord(record, tableInfo);
        tableInfo.insertStmt.run(params);
      }
      this.db.exec('COMMIT');
    } catch (e) {
      this.db.exec('ROLLBACK');
      throw e;
    }
  }

  private prepareRecord(
    record: DataRecord,
    tableInfo: TableInfo
  ): Record<string, unknown> {
    const params: Record<string, unknown> = {
      context: record.context,
      received_timestamp: record.received_timestamp,
      signalk_timestamp: record.signalk_timestamp,
    };

    if (tableInfo.isObject) {
      // Object path: value_json + flattened value_* columns
      let valueJson: string | null = null;
      if (
        record.value !== null &&
        record.value !== undefined &&
        typeof record.value === 'object'
      ) {
        valueJson = JSON.stringify(record.value);
      }
      if (record.value_json !== undefined && record.value_json !== null) {
        valueJson =
          typeof record.value_json === 'string'
            ? record.value_json
            : JSON.stringify(record.value_json);
      }
      params.value_json = valueJson;

      // Extract dynamic value_* columns
      for (const key of Object.keys(record)) {
        if (
          key.startsWith('value_') &&
          key !== 'value_json' &&
          record[key] !== undefined &&
          record[key] !== null
        ) {
          this.ensureColumn(tableInfo, key, record[key]);
          const val = record[key];
          if (typeof val === 'number') {
            params[key] = val;
          } else if (typeof val === 'boolean') {
            params[key] = val ? 1 : 0;
          } else {
            params[key] = String(val);
          }
        }
      }

      // NULL-fill any known value_* columns not in this record
      for (const col of tableInfo.columns) {
        if (
          col.startsWith('value_') &&
          col !== 'value_json' &&
          !(col in params)
        ) {
          params[col] = null;
        }
      }
    } else {
      // Scalar path: value column
      let valueStr: string | null = null;
      if (record.value !== null && record.value !== undefined) {
        valueStr = String(record.value);
      }
      params.value = valueStr;
    }

    // Serialize source
    params.source = record.source
      ? typeof record.source === 'object'
        ? JSON.stringify(record.source)
        : String(record.source)
      : null;

    params.source_label = record.source_label || null;
    params.source_type = record.source_type || null;
    params.source_pgn = record.source_pgn || null;
    params.source_src = record.source_src || null;

    // Serialize meta
    params.meta = record.meta
      ? typeof record.meta === 'object'
        ? JSON.stringify(record.meta)
        : String(record.meta)
      : null;

    return params;
  }

  /**
   * Convert a BufferRecord back to a DataRecord.
   * Path must be passed in since per-path tables have no path column.
   */
  private bufferRecordToDataRecord(
    record: BufferRecord,
    signalkPath: string
  ): DataRecord {
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
    if (value !== null && value !== undefined && !isNaN(Number(value))) {
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

    const dataRecord: DataRecord = {
      received_timestamp: record.received_timestamp,
      signalk_timestamp: record.signalk_timestamp,
      context: record.context,
      path: signalkPath,
      value: value,
      value_json: valueJson as string | object | undefined,
      source: source as string | object | undefined,
      source_label: record.source_label || undefined,
      source_type: record.source_type || undefined,
      source_pgn: record.source_pgn || undefined,
      source_src: record.source_src || undefined,
      meta: meta as string | object | undefined,
    };

    // Restore dynamic value_* columns from the record
    const rec = record as Record<string, unknown>;
    for (const key of Object.keys(rec)) {
      if (
        key.startsWith('value_') &&
        key !== 'value_json' &&
        rec[key] !== null &&
        rec[key] !== undefined
      ) {
        dataRecord[key] = rec[key];
      }
    }

    return dataRecord;
  }

  /**
   * Clean up old exported records from all per-path tables
   */
  cleanup(): number {
    let totalCleaned = 0;
    for (const [, info] of this.tableMap) {
      const result = this.db
        .prepare(
          `DELETE FROM ${info.tableName} WHERE exported = 1 AND created_at < datetime('now', '-' || ? || ' hours')`
        )
        .run(this.retentionHours);
      totalCleaned += result.changes;
    }
    return totalCleaned;
  }

  /**
   * Get buffer statistics aggregated across all per-path tables
   */
  getStats(): BufferStats {
    let totalRecords = 0;
    let pendingRecords = 0;
    let exportedRecords = 0;
    let oldestPendingTimestamp: string | null = null;
    let newestRecordTimestamp: string | null = null;

    for (const [, info] of this.tableMap) {
      const row = this.db
        .prepare(
          `
        SELECT
          COUNT(*) as totalRecords,
          SUM(CASE WHEN exported = 0 THEN 1 ELSE 0 END) as pendingRecords,
          SUM(CASE WHEN exported = 1 THEN 1 ELSE 0 END) as exportedRecords,
          MIN(CASE WHEN exported = 0 THEN received_timestamp END) as oldestPendingTimestamp,
          MAX(received_timestamp) as newestRecordTimestamp
        FROM ${info.tableName}
      `
        )
        .get() as {
        totalRecords: number;
        pendingRecords: number;
        exportedRecords: number;
        oldestPendingTimestamp: string | null;
        newestRecordTimestamp: string | null;
      };

      totalRecords += row.totalRecords || 0;
      pendingRecords += row.pendingRecords || 0;
      exportedRecords += row.exportedRecords || 0;

      if (row.oldestPendingTimestamp) {
        if (
          !oldestPendingTimestamp ||
          row.oldestPendingTimestamp < oldestPendingTimestamp
        ) {
          oldestPendingTimestamp = row.oldestPendingTimestamp;
        }
      }
      if (row.newestRecordTimestamp) {
        if (
          !newestRecordTimestamp ||
          row.newestRecordTimestamp > newestRecordTimestamp
        ) {
          newestRecordTimestamp = row.newestRecordTimestamp;
        }
      }
    }

    // Get file sizes
    let dbSizeBytes = 0;
    let walSizeBytes = 0;
    try {
      dbSizeBytes = fs.statSync(this.dbPath).size;
    } catch {
      // File may not exist yet
    }
    try {
      const walPath = this.dbPath + '-wal';
      if (fs.existsSync(walPath)) {
        walSizeBytes = fs.statSync(walPath).size;
      }
    } catch {
      // WAL file may not exist
    }

    return {
      totalRecords,
      pendingRecords,
      exportedRecords,
      oldestPendingTimestamp,
      newestRecordTimestamp,
      dbSizeBytes,
      walSizeBytes,
    };
  }

  /**
   * Get count of pending records (faster than full stats)
   */
  getPendingCount(): number {
    if (!this._open) {
      return 0;
    }
    let total = 0;
    for (const [, info] of this.tableMap) {
      const row = this.db
        .prepare(
          `SELECT COUNT(*) as count FROM ${info.tableName} WHERE exported = 0`
        )
        .get() as { count: number };
      total += row.count;
    }
    return total;
  }

  /**
   * Checkpoint WAL file (useful before backup or to reduce WAL size)
   */
  checkpoint(): void {
    this.db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
  }

  /**
   * Get distinct dates that have unexported records, excluding today.
   * Scans all per-path tables.
   */
  getDatesWithUnexportedRecords(excludeToday: boolean = true): string[] {
    if (!this._open) {
      return [];
    }

    const allDates = new Set<string>();
    for (const [, info] of this.tableMap) {
      let query = `
        SELECT DISTINCT date(received_timestamp) as record_date
        FROM ${info.tableName}
        WHERE exported = 0
      `;
      if (excludeToday) {
        query += ` AND date(received_timestamp) < date('now')`;
      }

      const rows = this.db.prepare(query).all() as Array<{
        record_date: string;
      }>;
      for (const r of rows) {
        allDates.add(r.record_date);
      }
    }

    return Array.from(allDates).sort();
  }

  /**
   * Get distinct context/path combinations for a specific date (UTC).
   * Scans all per-path tables.
   */
  getPathsForDate(date: Date): Array<{ context: string; path: string }> {
    if (!this._open) {
      return [];
    }

    const dateStr = date.toISOString().slice(0, 10);
    const startOfDay = `${dateStr}T00:00:00.000Z`;
    const endOfDay = `${dateStr}T23:59:59.999Z`;

    const results: Array<{ context: string; path: string }> = [];

    for (const [signalkPath, info] of this.tableMap) {
      const rows = this.db
        .prepare(
          `
        SELECT DISTINCT context
        FROM ${info.tableName}
        WHERE received_timestamp >= ? AND received_timestamp <= ?
          AND exported = 0
      `
        )
        .all(startOfDay, endOfDay) as Array<{ context: string }>;

      for (const row of rows) {
        results.push({ context: row.context, path: signalkPath });
      }
    }

    return results.sort((a, b) =>
      a.context < b.context
        ? -1
        : a.context > b.context
          ? 1
          : a.path < b.path
            ? -1
            : a.path > b.path
              ? 1
              : 0
    );
  }

  /**
   * Get all records for a specific context, path, and date (UTC).
   * Returns just DataRecord[] — no IDs needed since markDateExported() handles marking.
   */
  getRecordsForPathAndDate(
    context: string,
    signalkPath: string,
    date: Date
  ): DataRecord[] {
    if (!this._open) {
      return [];
    }

    const tableInfo = this.tableMap.get(signalkPath);
    if (!tableInfo) return [];

    const dateStr = date.toISOString().slice(0, 10);
    const startOfDay = `${dateStr}T00:00:00.000Z`;
    const endOfDay = `${dateStr}T23:59:59.999Z`;

    const bufferRecords = this.db
      .prepare(
        `
      SELECT * FROM ${tableInfo.tableName}
      WHERE context = ?
        AND received_timestamp >= ? AND received_timestamp <= ?
        AND exported = 0
      ORDER BY received_timestamp ASC
    `
      )
      .all(context, startOfDay, endOfDay) as BufferRecord[];

    return bufferRecords.map(r =>
      this.bufferRecordToDataRecord(r, signalkPath)
    );
  }

  /**
   * Mark records for a specific date as exported.
   * Queries the specific path's table.
   */
  markDateExported(
    context: string,
    signalkPath: string,
    date: Date,
    batchId: string
  ): void {
    if (!this._open) return;

    const tableInfo = this.tableMap.get(signalkPath);
    if (!tableInfo) return;

    const dateStr = date.toISOString().slice(0, 10);
    const startOfDay = `${dateStr}T00:00:00.000Z`;
    const endOfDay = `${dateStr}T23:59:59.999Z`;

    this.db
      .prepare(
        `
      UPDATE ${tableInfo.tableName}
      SET exported = 1, export_batch_id = ?
      WHERE context = ?
        AND received_timestamp >= ? AND received_timestamp <= ?
        AND exported = 0
    `
      )
      .run(batchId, context, startOfDay, endOfDay);
  }

  /**
   * Get the set of known SignalK paths that have buffer tables.
   * Used by SQL builders to check if a buffer table exists for federation.
   */
  getKnownPaths(): Set<string> {
    return new Set(this.tableMap.keys());
  }

  /**
   * Check if a buffer table exists for a given SignalK path.
   */
  hasTable(signalkPath: string): boolean {
    return this.tableMap.has(signalkPath);
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
    try {
      this.checkpoint();
    } catch {
      // Ignore checkpoint errors during shutdown
    }
    this.db.close();
    this._open = false;
  }
}
