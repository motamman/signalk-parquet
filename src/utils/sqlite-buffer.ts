/**
 * SQLite Write-Ahead Buffer for crash-safe data ingestion
 *
 * Per-path table architecture: each SignalK path gets its own table in buffer.db.
 * Scalar paths have a `value` column; object paths have `value_json` + flattened `value_*` columns.
 * This eliminates column pollution from ALTER TABLE ADD COLUMN on a shared table.
 */

import * as path from 'path';
import * as fs from 'fs-extra';
import { DataRecord, FederationCursor } from '../types';

// Lazy-loaded: node:sqlite requires Node 22.5+
let DatabaseSync: typeof import('node:sqlite').DatabaseSync;
type StatementSync = import('node:sqlite').StatementSync;
type SQLInputValue = import('node:sqlite').SQLInputValue;
try {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  ({ DatabaseSync } = require('node:sqlite'));
} catch {
  // node:sqlite not available — SQLiteBuffer constructor will throw a clear error
}

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
  /** Rows on short-term paths: never exported, aged out by retention. */
  bufferOnlyRecords: number;
  oldestPendingTimestamp: string | null;
  newestRecordTimestamp: string | null;
  dbSizeBytes: number;
  walSizeBytes: number;
}

export interface SQLiteBufferConfig {
  dbPath: string;
  maxBatchSize?: number;
  retentionHours?: number;
  /**
   * Open for reading only: no schema creation, no legacy migration, no index
   * building, and every write method refuses.
   *
   * Nothing opens one today; the buffer worker did while it served reads, and
   * would again (see devdocs/BUFFER_WORKER_REMOVED.md). Two node:sqlite
   * connections to one database are safe — they share a library, so
   * their POSIX locks are coherent — but a second *writer* is not free: the
   * connection that owns ingestion sets no `busy_timeout`, because waiting for
   * a lock on the server's event loop is the very thing the worker exists to
   * avoid, so it would see SQLITE_BUSY and drop a record rather than wait.
   * A reader takes no write lock and creates no such window.
   */
  readOnly?: boolean;
}

/**
 * States of the `exported` flag.
 *
 * A row's fate is decided when it is written and never re-stamped: changing a
 * path's retention mode affects only rows inserted from that point on.
 *
 *   PENDING (0)      tracked path, owes a Parquet write
 *   EXPORTED (1)     written to Parquet, now eligible for retention cleanup
 *   BUFFER_ONLY (-1) short-term path, will never be written to Parquet, and is
 *                    eligible for retention cleanup immediately
 *
 * Three predicates follow from this. They are written as IN lists rather than
 * `<>`, because an inequality cannot seek an index and would turn retention
 * and the playback probes back into full table scans:
 *   `exported IN (0, -1)`  "not in Parquet, so a query must include it" —
 *     every federation and playback read, since BUFFER_ONLY rows exist
 *     nowhere else and would otherwise be invisible.
 *   `exported IN (1, -1)`  "settled, so retention may age it out".
 *   `exported = 0`         "owes an export" — only to select rows to write.
 */
export const EXPORTED_PENDING = 0;
export const EXPORTED_DONE = 1;
export const EXPORTED_BUFFER_ONLY = -1;

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

/**
 * The plain object a `value_json` column text holds, or null when the text is
 * missing, not JSON, or not an object. Both sides of `completes` go through
 * this, so a value that cannot be read is never taken for one that can.
 */
function parseObjectJson(json: unknown): Record<string, unknown> | null {
  if (typeof json !== 'string' || json.length === 0) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(json);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return null;
  }
  return parsed as Record<string, unknown>;
}

/**
 * True when `next` carries every key `stored` had, unchanged — so the stored
 * row is completed rather than changed and can be extended in place.
 *
 * Both are `value_json` texts: `stored` the row's, `next` the new record's as
 * `prepareRecord` normalised it. Unparseable or empty JSON on either side
 * returns false: a row whose content cannot be read is never overwritten, it
 * is superseded by a new row, and a value that cannot be read completes
 * nothing.
 *
 * Values are compared by their JSON form, not by `===`. Both sides come from
 * `JSON.parse`, so two structurally identical objects or arrays are never the
 * same reference: comparing with `===` would report "changed" for a nested
 * value that is in fact unchanged, and the row would be superseded by a new one
 * every time. Today's only caller records primitives, so it could not have been
 * seen there — but the operation is documented as being about object rows in
 * general, and this makes that true.
 */
function completes(stored: string | null, next: unknown): boolean {
  const previous = parseObjectJson(stored);
  const after = parseObjectJson(next);
  if (!previous || !after) return false;
  const keys = Object.keys(previous);
  if (keys.length === 0) return false;
  return keys.every(key => sameJsonValue(after[key], previous[key]));
}

/**
 * Equality for two parsed-JSON values. Primitives compare directly; anything
 * structural compares by its serialised form, which is stable here because both
 * sides were produced by `JSON.parse` of an object whose keys therefore appear
 * in the same insertion order only when the JSON agreed — so this is a
 * conservative test: it can report "changed" for two objects that differ only
 * in key order, which supersedes the row rather than corrupting it.
 */
function sameJsonValue(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a === null || b === null) return false;
  if (typeof a !== 'object' || typeof b !== 'object') return false;
  return JSON.stringify(a) === JSON.stringify(b);
}

/**
 * The columns of a per-path table that a federated read returns: the cursor
 * (`id`, `signalk_timestamp`), what the federation SQL filters on (`context`,
 * `exported`, `source_label` for a source filter or source discovery), and the
 * value itself (`value`, or `value_json` and the `value_*` components). Every
 * reader of getRowsForFederation and stageBufferTable uses only these.
 *
 * The rest — `source`, `received_timestamp`, `created_at`, `source_type`,
 * `source_pgn`, `source_src`, `meta`, `export_batch_id` — is ingestion and
 * export bookkeeping. Reading it anyway was most of a values request's
 * garbage: on brain (2026-10-05) one request read 50,659 position rows as
 * about 66 MB of JS objects, `source` alone being the widest column.
 */
export function federationColumns(columns: Iterable<string>): string[] {
  const base = new Set([
    'id',
    'context',
    'signalk_timestamp',
    'exported',
    'source_label',
    'value',
    'value_json',
  ]);
  return [...columns].filter(c => base.has(c) || c.startsWith('value_'));
}

/**
 * The keyset cursor for the last row of a federation page — what the next call
 * to getRowsForFederation resumes after.
 */
export function federationCursor(
  row: Record<string, unknown>
): FederationCursor {
  return {
    signalkTimestamp: String(row.signalk_timestamp),
    id: Number(row.id),
  };
}

export class SQLiteBuffer {
  private db: InstanceType<typeof DatabaseSync>;
  private _open: boolean;
  private readonly dbPath: string;
  private readonly retentionHours: number;
  /** Reading only: writes refuse and no DDL was run at open. */
  private readonly readOnly: boolean;
  private tableMap: Map<string, TableInfo>; // keyed by SignalK path

  constructor(config: SQLiteBufferConfig) {
    if (!DatabaseSync) {
      throw new Error(
        'node:sqlite is not available (requires Node.js 22.5+). SQLite buffer disabled — falling back to in-memory LRU.'
      );
    }

    this.dbPath = config.dbPath;
    this.retentionHours = config.retentionHours || 24;
    this.readOnly = config.readOnly === true;

    // Ensure directory exists
    fs.ensureDirSync(path.dirname(this.dbPath));

    // Open database with WAL mode for crash safety and better concurrency
    this.db = this.readOnly
      ? new DatabaseSync(this.dbPath, { readOnly: true })
      : new DatabaseSync(this.dbPath);
    this._open = true;

    // Configure for performance and crash safety. journal_mode and synchronous
    // are properties of the database, not of a connection, and cannot be set
    // from a read-only one; the rest are per-connection and apply either way.
    if (!this.readOnly) {
      this.db.exec('PRAGMA journal_mode = WAL');
      this.db.exec('PRAGMA synchronous = NORMAL');
    }
    this.db.exec('PRAGMA cache_size = -64000'); // 64MB cache
    this.db.exec('PRAGMA temp_store = MEMORY');
    this.db.exec('PRAGMA mmap_size = 268435456'); // 256MB memory-mapped I/O

    if (!this.readOnly) {
      // Create metadata table
      this.createMetadataSchema();

      // Migrate from old single-table layout if needed
      this.migrateFromLegacy();
    }

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
        this.ensureIndexes(tableName);

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
   * Load per-path tables from buffer_tables metadata and prepare INSERT
   * statements. Paths already in the table map are left as they are, so this
   * can run again later to pick up tables that appeared since.
   */
  private loadExistingTables(): void {
    const rows = this.db
      .prepare(`SELECT path, table_name, is_object FROM buffer_tables`)
      .all() as Array<{ path: string; table_name: string; is_object: number }>;

    for (const row of rows) {
      if (this.tableMap.has(row.path)) continue;
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

      // Tables created before an index was introduced get it here. A read-only
      // connection cannot, and does not need to: the writing connection has
      // already done it, or will when it opens.
      if (!this.readOnly) {
        this.ensureIndexes(row.table_name);
      }

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

    // Automatic columns. `exported` is bound rather than literal so a row can
    // be stamped BUFFER_ONLY at insert; see the EXPORTED_* constants.
    insertCols.push('exported', 'export_batch_id', 'created_at');
    placeholders.push('@exported', 'NULL', "datetime('now')");

    return this.db.prepare(
      `INSERT INTO ${tableName} (${insertCols.join(', ')}) VALUES (${placeholders.join(', ')})`
    );
  }

  /**
   * Indexes every per-path table needs. Idempotent, so it doubles as the
   * migration for tables created before an index was added.
   *
   * `(exported, created_at)` serves retention cleanup, which would otherwise
   * scan the table: the `(context, exported)` index cannot answer a predicate
   * on `exported` alone. `(exported, signalk_timestamp)` serves the playback
   * probes, which ask for the earliest unexported row per table.
   * `(context, signalk_timestamp)` serves the federation read, which asks for
   * one context's rows in a time window in timestamp order: it is the only
   * index that answers the equality, the range and the ORDER BY together, so
   * the read is a seek rather than a search of the window plus a temp B-tree
   * sort. Measured locally (2026-09-26) on 32,000 rows in 32 pages, the plan
   * loses `USE TEMP B-TREE FOR ORDER BY` and the read goes 73 ms to 62 ms.
   */
  private ensureIndexes(tableName: string): void {
    this.db.exec(
      `CREATE INDEX IF NOT EXISTS idx_${tableName}_ctx_exp ON ${tableName} (context, exported)`
    );
    this.db.exec(
      `CREATE INDEX IF NOT EXISTS idx_${tableName}_ctx_sk ON ${tableName} (context, signalk_timestamp)`
    );
    this.db.exec(
      `CREATE INDEX IF NOT EXISTS idx_${tableName}_received ON ${tableName} (received_timestamp)`
    );
    this.db.exec(
      `CREATE INDEX IF NOT EXISTS idx_${tableName}_exp_created ON ${tableName} (exported, created_at)`
    );
    this.db.exec(
      `CREATE INDEX IF NOT EXISTS idx_${tableName}_exp_sk ON ${tableName} (exported, signalk_timestamp)`
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
    this.ensureIndexes(tableName);

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
   * Pick up a table the writing connection created after this one opened.
   *
   * The table map is read from `buffer_tables` once, at open. A writer adds
   * to it as it creates tables, so its map is always complete; a read-only
   * connection creates nothing and would otherwise never learn of a path
   * first recorded after it opened, and answer "no table" for rows that are
   * there. Reloads the metadata when `signalkPath` is unknown; a no-op for a
   * known path, and for a writer, whose map cannot be behind.
   */
  loadTableIfMissing(signalkPath: string): void {
    if (!this._open || !this.readOnly) return;
    if (this.tableMap.has(signalkPath)) return;
    this.loadExistingTables();
  }

  /**
   * Insert a single record into the buffer. Returns the new row's id.
   */
  insert(record: DataRecord): number {
    if (!this._open) {
      throw new Error('SQLite buffer is closed');
    }
    const tableInfo = this.ensureTable(record.path, record);
    const params = this.prepareRecord(record, tableInfo);
    const result = tableInfo.insertStmt.run(
      params as Record<string, SQLInputValue>
    );
    return Number(result.lastInsertRowid);
  }

  /**
   * Write an object row for one context, extending the context's newest row
   * in place when that row is still unexported and the new value only
   * *completes* it — every key the stored row carries is present in the new
   * value with the same value — and inserting a new row otherwise.
   *
   * It is the newest row that is examined, whatever its state: an older row
   * that is still pending behind a newer exported one is never extended,
   * because extending it would rewrite history that the exported row has
   * already superseded.
   *
   * This exists as one operation rather than as a read, a decision and a
   * write because the decision depends on what is stored: split across calls,
   * the caller has to carry a row id between them, and the row can be
   * exported or deleted in the gap. Both happen inside one transaction here,
   * so no caller ever holds a row id and the write cannot land against a row
   * that has since changed state. It is also the shape that survives the
   * buffer moving to a worker, where a caller-held row id would need a
   * promise chain per context to stay correct.
   *
   * "Completes" is a property of object rows, not of any one path's meaning:
   * a stored value of `{a:1}` is completed by `{a:1,b:2}` but not by `{a:9}`
   * (changed) and not by `{b:2}` (dropped a). A stored row with no value at
   * all is never extended, so the first real value starts a new row.
   *
   * Returns whether the latest row was extended. Scalar paths always insert.
   */
  insertOrExtendLatest(record: DataRecord): { extended: boolean } {
    if (!this._open) {
      throw new Error('SQLite buffer is closed');
    }
    const tableInfo = this.ensureTable(record.path, record);
    if (!tableInfo.isObject) {
      this.insert(record);
      return { extended: false };
    }

    const params = this.prepareRecord(record, tableInfo);
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const latest = this.db
        .prepare(
          `SELECT id, value_json, exported FROM ${tableInfo.tableName}
             WHERE context = ?
             ORDER BY id DESC LIMIT 1`
        )
        .get(record.context) as
        { id: number; value_json: string | null; exported: number } | undefined;

      const eligible =
        latest !== undefined &&
        (latest.exported === EXPORTED_PENDING ||
          latest.exported === EXPORTED_BUFFER_ONLY);
      let extended = false;
      // Compared on the normalised text, not on `record.value_json`: the
      // record may carry its value as an object under `value`, or as a JSON
      // string, and either lands in the column as this text.
      if (eligible && completes(latest.value_json, params.value_json)) {
        // A row's retention stamp is decided when it is written and never
        // re-stamped, so the overwrite leaves `exported` as it was:
        // re-stamping a BUFFER_ONLY row PENDING would export data recorded as
        // buffer-only, and the reverse would drop an export the row owes.
        const update = { ...params };
        delete update.exported;
        const sets = Object.keys(update).map(col => `${col} = @${col}`);
        const updated = this.db
          .prepare(
            `UPDATE ${tableInfo.tableName} SET ${sets.join(', ')} WHERE id = @id`
          )
          .run({ ...update, id: latest.id } as Record<string, SQLInputValue>);
        extended = Number(updated.changes) > 0;
      }
      if (!extended) {
        tableInfo.insertStmt.run(params as Record<string, SQLInputValue>);
      }
      this.db.exec('COMMIT');
      return { extended };
    } catch (e) {
      try {
        this.db.exec('ROLLBACK');
      } catch {
        // The transaction is already gone; the original error is what matters.
      }
      throw e;
    }
  }

  /**
   * The newest row per context for a path, exported or not: its exported flag
   * and its value_json. This is the record of what was actually written,
   * whatever happened to the process that wrote it. No row id is returned:
   * nothing outside the buffer may hold one.
   */
  getLatestRowPerContext(signalkPath: string): Array<{
    context: string;
    value_json: string | null;
    exported: number;
  }> {
    if (!this._open) return [];
    const tableInfo = this.tableMap.get(signalkPath);
    if (!tableInfo) return [];
    const valueJson = tableInfo.isObject ? 'value_json' : 'NULL AS value_json';
    return this.db
      .prepare(
        `SELECT context, ${valueJson}, exported FROM ${tableInfo.tableName}
         WHERE id IN (SELECT MAX(id) FROM ${tableInfo.tableName} GROUP BY context)`
      )
      .all() as Array<{
      context: string;
      value_json: string | null;
      exported: number;
    }>;
  }

  /**
   * Insert multiple records in a single transaction (much faster)
   */
  insertBatch(records: DataRecord[]): void {
    // Every other write path guards on _open; without the same check here a
    // batch could still open a transaction on a database close() has already
    // marked closed (and may have failed to actually close).
    if (!this._open) {
      throw new Error('SQLite buffer is closed');
    }
    if (records.length === 0) return;

    this.db.exec('BEGIN');
    try {
      for (const record of records) {
        const tableInfo = this.ensureTable(record.path, record);
        const params = this.prepareRecord(record, tableInfo);
        tableInfo.insertStmt.run(params as Record<string, SQLInputValue>);
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

    // Stamped once, here, from the mode in force at insert. Anything other
    // than an explicit BUFFER_ONLY is a tracked row that owes an export.
    params.exported =
      record.exported === EXPORTED_BUFFER_ONLY
        ? EXPORTED_BUFFER_ONLY
        : EXPORTED_PENDING;

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
   * Delete rows whose fate is settled and which are past the retention window:
   * exported ones, and buffer-only ones that were never going to be exported.
   * A PENDING row is never deleted, however old, so a path that failed to
   * export keeps its data for the next attempt.
   */
  cleanup(): number {
    let totalCleaned = 0;
    for (const [, info] of this.tableMap) {
      const result = this.db
        .prepare(
          `DELETE FROM ${info.tableName} WHERE exported IN (${EXPORTED_DONE}, ${EXPORTED_BUFFER_ONLY}) AND created_at < datetime('now', '-' || ? || ' hours')`
        )
        .run(this.retentionHours);
      totalCleaned += Number(result.changes);
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
    let bufferOnlyRecords = 0;
    let oldestPendingTimestamp: string | null = null;
    let newestRecordTimestamp: string | null = null;

    for (const [, info] of this.tableMap) {
      const row = this.db
        .prepare(
          `
        SELECT
          COUNT(*) as totalRecords,
          SUM(CASE WHEN exported = ${EXPORTED_PENDING} THEN 1 ELSE 0 END) as pendingRecords,
          SUM(CASE WHEN exported = ${EXPORTED_DONE} THEN 1 ELSE 0 END) as exportedRecords,
          SUM(CASE WHEN exported = ${EXPORTED_BUFFER_ONLY} THEN 1 ELSE 0 END) as bufferOnlyRecords,
          MIN(CASE WHEN exported = ${EXPORTED_PENDING} THEN received_timestamp END) as oldestPendingTimestamp,
          MAX(received_timestamp) as newestRecordTimestamp
        FROM ${info.tableName}
      `
        )
        .get() as {
        totalRecords: number;
        pendingRecords: number;
        exportedRecords: number;
        bufferOnlyRecords: number;
        oldestPendingTimestamp: string | null;
        newestRecordTimestamp: string | null;
      };

      totalRecords += row.totalRecords || 0;
      pendingRecords += row.pendingRecords || 0;
      exportedRecords += row.exportedRecords || 0;
      bufferOnlyRecords += row.bufferOnlyRecords || 0;

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
      bufferOnlyRecords,
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
   * Get distinct dates (UTC) that have unexported records, excluding the
   * current export window.
   *
   * `exportHourUtc` is the scheduler's daily export hour (UTC). A UTC day only
   * becomes eligible for the startup catch-up once its scheduled export time —
   * `exportHourUtc` on the following day — has passed. Shifting `now` back by
   * `exportHourUtc` hours aligns the catch-up with the scheduled daily export,
   * so a restart between UTC-midnight and `exportHourUtc` no longer exports the
   * current local day early. Multi-day backlogs are still fully vacuumed; only
   * the single most-recent day whose scheduled time hasn't yet arrived is
   * briefly deferred (and picked up by the scheduled run or the next boot).
   * Default `0` preserves the plain-UTC "before today" behaviour.
   * Scans all per-path tables.
   */
  getDatesWithUnexportedRecords(
    excludeToday: boolean = true,
    exportHourUtc: number = 0
  ): string[] {
    if (!this._open) {
      return [];
    }

    // Clamp to a safe integer 0–23 so it can be inlined into the SQL modifier.
    const h = Math.max(0, Math.min(23, Math.trunc(Number(exportHourUtc)) || 0));

    const allDates = new Set<string>();
    for (const [, info] of this.tableMap) {
      let query = `
        SELECT DISTINCT date(received_timestamp) as record_date
        FROM ${info.tableName}
        WHERE exported = 0
      `;
      if (excludeToday) {
        query += ` AND date(received_timestamp) < date('now', '-${h} hours')`;
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
   * Count unexported records for a specific context, path, and date (UTC).
   */
  getRecordCountForPathAndDate(
    context: string,
    signalkPath: string,
    date: Date
  ): number {
    if (!this._open) return 0;

    const tableInfo = this.tableMap.get(signalkPath);
    if (!tableInfo) return 0;

    const dateStr = date.toISOString().slice(0, 10);
    const startOfDay = `${dateStr}T00:00:00.000Z`;
    const endOfDay = `${dateStr}T23:59:59.999Z`;

    const row = this.db
      .prepare(
        `
      SELECT COUNT(*) as cnt FROM ${tableInfo.tableName}
      WHERE context = ?
        AND received_timestamp >= ? AND received_timestamp <= ?
        AND exported = 0
    `
      )
      .get(context, startOfDay, endOfDay) as { cnt: number } | undefined;

    return row?.cnt ?? 0;
  }

  /**
   * Get a batch of unexported records for a specific context, path, and date (UTC).
   * Uses LIMIT/OFFSET to avoid materializing all rows at once.
   */
  getRecordsForPathAndDateBatched(
    context: string,
    signalkPath: string,
    date: Date,
    limit: number,
    offset: number
  ): DataRecord[] {
    if (!this._open) return [];

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
      LIMIT ? OFFSET ?
    `
      )
      .all(context, startOfDay, endOfDay, limit, offset) as BufferRecord[];

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
   * All SignalK path names the buffer has a table for. Tables are keyed by
   * path (not context), so this is the small set of distinct recorded paths —
   * used to compute the angular-path set for the aggregation worker.
   */
  getPaths(): string[] {
    return Array.from(this.tableMap.keys());
  }

  /**
   * Get the set of columns for a given path's buffer table.
   * Returns undefined if no table exists for this path.
   */
  getTableColumns(signalkPath: string): Set<string> | undefined {
    const info = this.tableMap.get(signalkPath);
    return info?.columns;
  }

  /**
   * Get the column schema (name + declared SQLite type) for a path's buffer table.
   * Returns undefined if no table exists for this path.
   */
  getTableSchema(
    signalkPath: string
  ): Array<{ name: string; type: string }> | undefined {
    if (!this._open) return undefined;

    const info = this.tableMap.get(signalkPath);
    if (!info) return undefined;

    const rows = this.db
      .prepare(`PRAGMA table_info(${info.tableName})`)
      .all() as Array<{ name: string; type: string }>;
    return rows.map(r => ({ name: r.name, type: r.type }));
  }

  /**
   * Read a batch of unexported rows for federated history queries,
   * keyset-paginated by `(signalk_timestamp, id)` so callers can stream large
   * windows without materializing them all. Rows carry the table's
   * federationColumns, in timestamp order.
   *
   * Pass `null` for the first page and the last row of each page thereafter;
   * see FederationCursor for why the timestamp and not the id carries the
   * cursor. Callers that need a particular row order sort what they get: the
   * order here exists to make the pages resumable.
   */
  getRowsForFederation(
    signalkPath: string,
    context: string,
    fromIso: string,
    toIso: string,
    after: FederationCursor | null,
    limit: number
  ): Array<Record<string, unknown>> {
    if (!this._open) return [];

    const tableInfo = this.tableMap.get(signalkPath);
    if (!tableInfo) return [];
    // From the table as it is now, not the column set read at open: a
    // read-only connection's set does not learn of a `value_*` column the
    // writer added since, and stageBufferTable builds its table from this
    // same schema.
    const columns = federationColumns(
      (this.getTableSchema(signalkPath) ?? []).map(c => c.name)
    );

    // Resuming raises the window's lower bound to the cursor's timestamp,
    // and a separate predicate drops the rows already read at exactly that
    // timestamp. The obvious spelling — `ts > ? OR (ts = ? AND id > ?)` — is
    // a disjunction across two columns, which SQLite cannot turn into an
    // index range: the scan restarts at the window's beginning on every page
    // and walks forward to the cursor, so the read costs grow with the square
    // of the page count while the query plan looks identical either way.
    // Measured locally (2026-09-26) on 31,778 rows: the disjunction read
    // 83 ms in 32 pages and 160-201 ms in 128, this form 40 ms and 38 ms.
    // Both return every row exactly once (distinct ids equalled row count).
    const resume = after ? 'AND NOT (signalk_timestamp = ? AND id <= ?)' : '';
    const params: SQLInputValue[] = [
      context,
      after ? after.signalkTimestamp : fromIso,
      toIso,
    ];
    if (after) {
      params.push(after.signalkTimestamp, after.id);
    }
    params.push(limit);

    return this.db
      .prepare(
        `
      SELECT ${columns.join(', ')}
      FROM ${tableInfo.tableName}
      WHERE context = ?
        AND signalk_timestamp >= ? AND signalk_timestamp < ?
        AND exported IN (${EXPORTED_PENDING}, ${EXPORTED_BUFFER_ONLY})
        ${resume}
      ORDER BY signalk_timestamp ASC, id ASC
      LIMIT ?
    `
      )
      .all(...params) as Array<Record<string, unknown>>;
  }

  /**
   * Unexported rows of every path in a time window, for playback. Object
   * paths carry `value_json`, scalar paths the text `value`. `contexts`
   * narrows to those vessels; null means every vessel.
   *
   * `limit` caps the rows returned across all paths together, and what
   * comes back is the EARLIEST `limit` rows of the window, not the first
   * `limit` a path-by-path walk happened to reach: each path is read in
   * timestamp order, merged into the result, and the merge truncated, so
   * once the result is full its last timestamp becomes an upper bound that
   * the remaining paths are queried with. A path registered late can
   * therefore still contribute rows earlier than one registered first,
   * which a per-path or first-come budget would have dropped. Memory is
   * bounded by the result plus one path's read, never by the path count.
   * The caller shrinks its window when it gets `limit` rows back.
   */
  getRowsForPlayback(
    fromIso: string,
    toIso: string,
    contexts: string[] | null,
    limit: number
  ): Array<{
    path: string;
    context: string;
    signalk_timestamp: string;
    source_label: string | null;
    value: string | null;
    value_json: string | null;
  }> {
    if (!this._open) return [];
    const out: Array<{
      path: string;
      context: string;
      signalk_timestamp: string;
      source_label: string | null;
      value: string | null;
      value_json: string | null;
    }> = [];
    const contextClause =
      contexts && contexts.length > 0
        ? ` AND context IN (${contexts.map(() => '?').join(', ')})`
        : '';
    if (limit <= 0) return out;
    // Narrows to the result's last timestamp once it is full, so the paths
    // still to read are pruned in SQL rather than in memory. Inclusive, so a
    // tie at the boundary is still read and settled by the merge below.
    let cutoff = toIso;
    let cutoffInclusive = false;
    for (const [signalkPath, info] of this.tableMap) {
      const valueCols = info.isObject
        ? 'NULL AS value, value_json'
        : 'value, NULL AS value_json';
      const rows = this.db
        .prepare(
          `SELECT context, signalk_timestamp, source_label, ${valueCols}
           FROM ${info.tableName}
           WHERE signalk_timestamp >= ? AND signalk_timestamp ${cutoffInclusive ? '<=' : '<'} ?
             AND exported IN (${EXPORTED_PENDING}, ${EXPORTED_BUFFER_ONLY})${contextClause}
           ORDER BY signalk_timestamp ASC
           LIMIT ?`
        )
        .all(fromIso, cutoff, ...(contexts ?? []), limit) as Array<{
        context: string;
        signalk_timestamp: string;
        source_label: string | null;
        value: string | null;
        value_json: string | null;
      }>;
      if (rows.length === 0) continue;
      for (const row of rows) out.push({ path: signalkPath, ...row });
      // Stable: equal timestamps keep the order the paths were read in.
      out.sort((a, b) =>
        a.signalk_timestamp < b.signalk_timestamp
          ? -1
          : a.signalk_timestamp > b.signalk_timestamp
            ? 1
            : 0
      );
      if (out.length > limit) out.length = limit;
      if (out.length === limit) {
        cutoff = out[limit - 1].signalk_timestamp;
        cutoffInclusive = true;
      }
    }
    return out;
  }

  /**
   * True when one path has an unexported row for a context in
   * [fromIso, toIso). The predicate the values federation reads with, so a
   * path this says yes to is one a values query would answer for. One seek
   * on the `(context, signalk_timestamp)` index; it exists so the path
   * listings can include what is recorded but not yet exported.
   */
  hasRowsInWindow(
    signalkPath: string,
    context: string,
    fromIso: string,
    toIso: string
  ): boolean {
    if (!this._open) return false;
    const info = this.tableMap.get(signalkPath);
    if (!info) return false;
    const row = this.db
      .prepare(
        `SELECT 1 AS hit FROM ${info.tableName}
         WHERE context = ?
           AND signalk_timestamp >= ? AND signalk_timestamp < ?
           AND exported IN (${EXPORTED_PENDING}, ${EXPORTED_BUFFER_ONLY})
         LIMIT 1`
      )
      .get(context, fromIso, toIso);
    return row !== undefined;
  }

  /**
   * True when one path has an unexported row for a context at any time. One
   * seek on the `(context, exported)` index, for the listings that take no
   * window.
   */
  hasRowsForContext(signalkPath: string, context: string): boolean {
    if (!this._open) return false;
    const info = this.tableMap.get(signalkPath);
    if (!info) return false;
    const row = this.db
      .prepare(
        `SELECT 1 AS hit FROM ${info.tableName}
         WHERE context = ?
           AND exported IN (${EXPORTED_PENDING}, ${EXPORTED_BUFFER_ONLY})
         LIMIT 1`
      )
      .get(context);
    return row !== undefined;
  }

  /** True when any path has an unexported row at or after `fromIso`. */
  hasRowsSince(fromIso: string, contexts: string[] | null): boolean {
    if (!this._open) return false;
    const contextClause =
      contexts && contexts.length > 0
        ? ` AND context IN (${contexts.map(() => '?').join(', ')})`
        : '';
    for (const info of this.tableMap.values()) {
      const row = this.db
        .prepare(
          `SELECT 1 AS hit FROM ${info.tableName}
           WHERE signalk_timestamp >= ? AND exported IN (${EXPORTED_PENDING}, ${EXPORTED_BUFFER_ONLY})${contextClause}
           LIMIT 1`
        )
        .get(fromIso, ...(contexts ?? []));
      if (row) return true;
    }
    return false;
  }

  /**
   * The earliest unexported row timestamp at or after `fromIso` across every
   * path, or null when there is none. Lets playback jump over silence.
   */
  getNextRowTime(fromIso: string, contexts: string[] | null): string | null {
    if (!this._open) return null;
    const contextClause =
      contexts && contexts.length > 0
        ? ` AND context IN (${contexts.map(() => '?').join(', ')})`
        : '';
    let best: string | null = null;
    for (const info of this.tableMap.values()) {
      const row = this.db
        .prepare(
          `SELECT MIN(signalk_timestamp) AS t FROM ${info.tableName}
           WHERE signalk_timestamp >= ? AND exported IN (${EXPORTED_PENDING}, ${EXPORTED_BUFFER_ONLY})${contextClause}`
        )
        .get(fromIso, ...(contexts ?? [])) as { t: string | null } | undefined;
      const t = row?.t ?? null;
      if (t && (best === null || t < best)) best = t;
    }
    return best;
  }

  /**
   * The newest row of an object path for a vessel at or before `atIso`,
   * exported or not (an exported row is still the truth until retention
   * removes it). Used to look up a vessel's identity for playback.
   */
  getLatestObjectRowAt(
    signalkPath: string,
    context: string,
    atIso: string
  ):
    | {
        signalk_timestamp: string;
        source_label: string | null;
        value_json: string | null;
      }
    | undefined {
    if (!this._open) return undefined;
    const info = this.tableMap.get(signalkPath);
    if (!info || !info.isObject) return undefined;
    return this.db
      .prepare(
        `SELECT signalk_timestamp, source_label, value_json FROM ${info.tableName}
         WHERE context = ? AND signalk_timestamp <= ?
         ORDER BY signalk_timestamp DESC LIMIT 1`
      )
      .get(context, atIso) as
      | {
          signalk_timestamp: string;
          source_label: string | null;
          value_json: string | null;
        }
      | undefined;
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
    // Mark closed before db.close() so a throw there can't leave the buffer
    // reporting isOpen() === true, which would let writers keep inserting into
    // a half-closed database.
    this._open = false;
    this.db.close();
  }
}
