import { DuckDBInstance } from '@duckdb/node-api';
import * as path from 'path';
import * as fs from 'fs-extra';

/**
 * Singleton DuckDB instance with connection pooling
 * Loads spatial extension once on initialization
 *
 * This eliminates the performance overhead of creating new DuckDB instances
 * for every request and repeatedly loading the spatial extension.
 *
 * @example
 * ```typescript
 * // During plugin startup
 * await DuckDBPool.initialize();
 *
 * // In request handlers
 * const connection = await DuckDBPool.getConnection();
 * const result = await connection.runAndReadAll("SELECT * FROM ...");
 * // ... process result
 * // Note: DuckDB connections close automatically when no longer referenced
 *
 * // During plugin shutdown
 * await DuckDBPool.shutdown();
 * ```
 */
export interface S3Config {
  accessKeyId: string;
  secretAccessKey: string;
  region: string;
  endpoint?: string; // R2: '{accountId}.r2.cloudflarestorage.com', or host[:port] for self-hosted S3 (Garage, MinIO, etc.)
  useSSL?: boolean; // Set to false for self-hosted endpoints served over plain HTTP
  urlStyle?: 'path' | 'vhost'; // R2 and most self-hosted S3-compatible services require 'path'
}

export class DuckDBPool {
  private static instance: DuckDBInstance | null = null;
  private static initialized: boolean = false;
  private static s3Initialized: boolean = false;
  private static sqliteDbPath: string | null = null;
  private static sqliteInitialized: boolean = false;
  private static spatialAvailable: boolean = false;

  /**
   * Initialize the DuckDB instance and load extensions
   * Call this once during plugin startup
   *
   * @throws Error if initialization fails
   */
  static async initialize(
    homeBaseDir?: string,
    warn?: (message: string) => void
  ): Promise<void> {
    if (this.instance) {
      return; // Already initialized
    }

    // DuckDB defaults its extension/home directory to `$HOME/.duckdb`. On hosts
    // where $HOME is read-only — e.g. the Signal K App Store CI sandbox, which
    // fails activation with `IO Error: Failed to create directory
    // "$HOME/.duckdb": Read-only file system` — the `INSTALL spatial`
    // below then aborts. Point DuckDB at a writable dir under the plugin's own
    // data directory instead; this also caches downloaded extensions across
    // restarts. Falls back to DuckDB's default when no directory is provided.
    const config: Record<string, string> = {};
    if (homeBaseDir) {
      const duckdbHome = path.join(homeBaseDir, '.duckdb');
      await fs.ensureDir(duckdbHome);
      config.home_directory = duckdbHome;
      config.extension_directory = path.join(duckdbHome, 'extensions');
      config.temp_directory = path.join(duckdbHome, 'tmp');
    }

    // Fully set up on a local variable and only publish to this.instance once
    // the core (non-extension) setup succeeds, so a failure in instance
    // creation or the memory-limit PRAGMA leaves the pool uninitialized and a
    // later initialize() can retry cleanly.
    const instance = await DuckDBInstance.create(':memory:', config);

    const setupConn = await instance.connect();
    try {
      // Cap DuckDB memory to prevent OOM when combined with Node's heap
      await setupConn.runAndReadAll("SET memory_limit = '512MB';");

      // Spatial is a downloadable extension: the first load fetches it from
      // DuckDB's extension repo, then caches it under extension_directory. If
      // the plugin is first enabled offline (installed while online but never
      // started with connectivity), that download fails. Treat spatial as
      // best-effort — a failure here must NOT reject plugin.start(), which
      // would take down parquet writing and the history API with it. Warn and
      // continue; spatial-dependent queries fail individually until a later
      // start (a fresh process) with connectivity caches the extension.
      try {
        await setupConn.runAndReadAll('INSTALL spatial;');
        await setupConn.runAndReadAll('LOAD spatial;');
        this.spatialAvailable = true;
      } catch (err) {
        this.spatialAvailable = false;
        warn?.(
          `DuckDB spatial extension unavailable (likely no network on first ` +
            `start): ${(err as Error).message}. Position and bbox/radius ` +
            `history queries will be degraded until the plugin next starts ` +
            `with connectivity to cache the extension.`
        );
      }

      this.instance = instance;
      this.initialized = true;
    } finally {
      setupConn.disconnectSync();
    }
  }

  /**
   * Get a connection from the pool
   * The connection shares the same instance, so spatial extension is already loaded
   *
   * @returns A new connection (closes automatically when no longer referenced)
   * @throws Error if pool is not initialized
   */
  static async getConnection() {
    if (!this.instance) {
      throw new Error(
        'DuckDBPool not initialized. Call DuckDBPool.initialize() first.'
      );
    }

    return await this.instance.connect();
  }

  /**
   * Store the SQLite buffer database path for federated queries.
   * Call this after initialize() and after the SQLiteBuffer is created.
   *
   * The path is used only as a "buffer exists" signal and for diagnostics —
   * DuckDB must NEVER open buffer.db itself (see buffer-staging.ts for why
   * the old ATTACH approach crashed the server).
   *
   * @param dbPath Absolute path to the SQLite buffer.db file
   */
  static initializeSQLiteBuffer(dbPath: string): void {
    this.sqliteDbPath = dbPath;
    this.sqliteInitialized = true;
  }

  /**
   * Check if the SQLite buffer path has been configured
   */
  static isSQLiteBufferInitialized(): boolean {
    return this.sqliteInitialized && this.sqliteDbPath !== null;
  }

  /**
   * Get the configured SQLite buffer path (or null)
   */
  static getSQLiteBufferPath(): string | null {
    return this.sqliteDbPath;
  }

  /**
   * Cleanup on plugin shutdown
   * Sets the instance to null to allow garbage collection
   */
  static async shutdown(): Promise<void> {
    if (this.instance) {
      // DuckDB instances handle cleanup automatically
      this.instance = null;
      this.initialized = false;
      this.s3Initialized = false;
      this.sqliteDbPath = null;
      this.sqliteInitialized = false;
    }
  }

  /**
   * Check if pool is ready
   * @returns true if the pool has been initialized
   */
  static isInitialized(): boolean {
    return this.initialized;
  }

  /**
   * Whether the DuckDB spatial extension loaded successfully. False when the
   * plugin was first started with no network and could not download it, in
   * which case spatial queries fail until a later start caches the extension.
   */
  static isSpatialAvailable(): boolean {
    return this.spatialAvailable;
  }

  /**
   * Initialize S3 credentials for DuckDB
   * This allows DuckDB to query S3 parquet files directly
   *
   * @param config S3 configuration with credentials and region
   * @throws Error if pool is not initialized or S3 setup fails
   */
  static async initializeS3(config: S3Config): Promise<void> {
    if (!this.instance) {
      throw new Error(
        'DuckDBPool not initialized. Call DuckDBPool.initialize() first.'
      );
    }

    if (this.s3Initialized) {
      return; // Already initialized
    }

    const connection = await this.instance.connect();
    try {
      // Install and load httpfs extension for S3 support
      await connection.runAndReadAll('INSTALL httpfs;');
      await connection.runAndReadAll('LOAD httpfs;');

      // Create S3 secret — R2 and self-hosted S3-compatible services (Garage, MinIO)
      // typically need ENDPOINT and URL_STYLE 'path'
      let endpointClause = config.endpoint
        ? `,\n          ENDPOINT '${config.endpoint.replace(/'/g, "''")}'`
        : '';
      if (config.urlStyle) {
        endpointClause += `,\n          URL_STYLE '${config.urlStyle}'`;
      }
      if (config.endpoint && config.useSSL !== undefined) {
        endpointClause += `,\n          USE_SSL ${config.useSSL ? 'true' : 'false'}`;
      }
      const secretSql = `
        CREATE OR REPLACE SECRET s3_credentials (
          TYPE S3,
          KEY_ID '${config.accessKeyId.replace(/'/g, "''")}',
          SECRET '${config.secretAccessKey.replace(/'/g, "''")}',
          REGION '${config.region.replace(/'/g, "''")}'${endpointClause}
        )
      `;
      await connection.runAndReadAll(secretSql);
      this.s3Initialized = true;
    } finally {
      connection.disconnectSync();
    }
  }

  /**
   * Check if S3 credentials have been initialized
   * @returns true if S3 is ready for queries
   */
  static isS3Initialized(): boolean {
    return this.s3Initialized;
  }
}
