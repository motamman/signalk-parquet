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
  private static sandboxInstance: DuckDBInstance | null = null;

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
   * Get a connection to a hardened sandbox instance for executing UNTRUSTED SQL
   * (the raw-SQL `/api/query` endpoint and LLM-generated analysis SQL).
   *
   * The sandbox is a SEPARATE DuckDB instance scoped to the plugin data
   * directory: `allowed_directories=[dataDir]` followed by
   * `enable_external_access=false` confines all file access to the data dir, so
   * reading arbitrary host files (read_text/read_csv/read_parquet outside the
   * dir) and ATTACHing external databases are denied. It never loads httpfs and
   * never holds the S3 secret, so it cannot be used for SSRF or credential
   * exfiltration. Local parquet reads/globs under the data dir still work, which
   * is all the untrusted-SQL paths need. DuckDB settings are instance-global and
   * cannot be relaxed once the database is running, which is why the trusted
   * upload/export path keeps using the separate, fully-capable pool instance.
   *
   * @param dataDir Absolute path to the plugin data directory. Used on the first
   *   call to scope the sandbox; ignored thereafter while the instance lives.
   */
  static async getSandboxConnection(dataDir: string) {
    if (!this.sandboxInstance) {
      const instance = await DuckDBInstance.create();
      const setup = await instance.connect();
      // Cap memory on this untrusted-SQL instance, matching the main pool, so a
      // heavy query can't exhaust the Node process.
      await setup.runAndReadAll("SET memory_limit = '512MB';");
      // Spatial must be available before access is locked down (extensions
      // cannot load once external access is disabled). It is already installed
      // globally by the main instance, so LOAD normally succeeds without network.
      try {
        await setup.runAndReadAll('LOAD spatial;');
      } catch {
        await setup.runAndReadAll('INSTALL spatial;');
        await setup.runAndReadAll('LOAD spatial;');
      }
      // Order matters: allowed_directories can only be set while external access
      // is still enabled; disabling it afterwards confines file access to that
      // directory and cannot be re-enabled for the life of the instance.
      await setup.runAndReadAll(
        `SET allowed_directories=['${dataDir.replace(/'/g, "''")}'];`
      );
      await setup.runAndReadAll('SET enable_external_access=false;');
      // Freeze the configuration last. The sandbox never changes a setting
      // after this point (unlike the main pool, which loads httpfs later), so
      // locking it costs nothing and closes the one hole the settings above
      // leave open: untrusted SQL can otherwise raise memory_limit itself —
      // `EXPLAIN ANALYZE SET memory_limit='4GB'` lifts the 512MB cap — which
      // the SQL guard catches by keyword but the engine should refuse outright.
      await setup.runAndReadAll('SET lock_configuration=true;');
      setup.disconnectSync();
      this.sandboxInstance = instance;
    }
    return await this.sandboxInstance.connect();
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
   *
   * Closes the native instances rather than waiting for a finalizer: a plugin
   * disable/enable cycle would otherwise leave the previous DuckDB instances
   * (and their own memory budgets) alive on hardware that has little to spare.
   */
  static async shutdown(): Promise<void> {
    if (this.instance) {
      this.closeQuietly(this.instance);
      this.instance = null;
      this.initialized = false;
      this.s3Initialized = false;
      this.sqliteDbPath = null;
      this.sqliteInitialized = false;
    }
    // Drop the sandbox instance too so a reconfigure rebuilds it against the
    // (possibly changed) data directory.
    if (this.sandboxInstance) {
      this.closeQuietly(this.sandboxInstance);
      this.sandboxInstance = null;
    }
  }

  /**
   * Close a DuckDB instance, ignoring failures. Shutdown runs from
   * plugin.stop(), where a close error must not prevent the remaining
   * teardown; dropping the reference still allows the finalizer to reclaim
   * the instance.
   */
  private static closeQuietly(instance: DuckDBInstance): void {
    try {
      instance.closeSync();
    } catch {
      // Best-effort.
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
      // SECURITY: this secret lives on the main pool instance. Any code path that
      // executes untrusted (user- or LLM-supplied) SQL must use
      // getSandboxConnection() instead of getConnection(), so the secret and
      // httpfs are unreachable from that SQL.
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
