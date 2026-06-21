import { DuckDBInstance } from '@duckdb/node-api';

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
  private static sandboxInstance: DuckDBInstance | null = null;

  /**
   * Initialize the DuckDB instance and load extensions
   * Call this once during plugin startup
   *
   * @throws Error if initialization fails
   */
  static async initialize(): Promise<void> {
    if (this.instance) {
      return; // Already initialized
    }

    this.instance = await DuckDBInstance.create();

    // Load spatial extension once for all future connections
    const setupConn = await this.instance.connect();
    // Cap DuckDB memory to prevent OOM when combined with Node's heap
    await setupConn.runAndReadAll("SET memory_limit = '512MB';");
    await setupConn.runAndReadAll('INSTALL spatial;');
    await setupConn.runAndReadAll('LOAD spatial;');
    // sqlite extension is auto-loaded by ATTACH ... (TYPE SQLITE) in getConnectionWithBuffer()
    this.initialized = true;
    // Connection closes automatically when no longer referenced
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
      setup.disconnectSync();
      this.sandboxInstance = instance;
    }
    return await this.sandboxInstance.connect();
  }

  /**
   * Store the SQLite buffer database path for federated queries.
   * Call this after initialize() and after the SQLiteBuffer is created.
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
   * Get a connection with the SQLite buffer ATTACHed as 'buffer' (READ_ONLY).
   * Falls back to a plain connection if no buffer path is configured.
   *
   * @returns A DuckDB connection with buffer attached
   */
  static async getConnectionWithBuffer() {
    const connection = await this.getConnection();

    if (this.sqliteDbPath) {
      try {
        await connection.runAndReadAll(
          `ATTACH '${this.sqliteDbPath.replace(/'/g, "''")}' AS buffer (TYPE SQLITE, READ_ONLY)`
        );
      } catch (err: unknown) {
        // If already attached (e.g. connection reuse), ignore
        if (!(err instanceof Error && err.message.includes('already exists'))) {
          throw err;
        }
      }
    }

    return connection;
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
    // Drop the sandbox instance too so a reconfigure rebuilds it against the
    // (possibly changed) data directory.
    this.sandboxInstance = null;
  }

  /**
   * Check if pool is ready
   * @returns true if the pool has been initialized
   */
  static isInitialized(): boolean {
    return this.initialized;
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
