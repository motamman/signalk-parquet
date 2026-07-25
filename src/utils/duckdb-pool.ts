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
  private static sqliteAutoloadDisabled: boolean = false;
  private static sqliteExtensionRemoved: boolean = false;
  private static lockdownSummary: string = 'not applied';

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

      await this.lockDownSqliteExtension(setupConn, homeBaseDir, warn);

      this.instance = instance;
      this.initialized = true;
    } finally {
      setupConn.disconnectSync();
    }
  }

  /**
   * Raise the cost of DuckDB's bundled SQLite ever opening the live write
   * buffer: doing so truncates buffer.db-shm under node:sqlite's active mmap
   * and kills the server with SIGBUS (see buffer-staging.ts).
   *
   * What each measure actually buys, since the boundaries are easy to
   * overestimate:
   * - Removing a cached sqlite_scanner is what stops `ATTACH ... (TYPE
   *   SQLITE)`. A cached extension loads on ATTACH regardless of the autoload
   *   setting, and every installation that ran the old federation code has one
   *   cached here.
   * - Disabling autoinstall/autoload stops SQL that merely references
   *   sqlite_scan() from pulling the extension in, and stops a re-download.
   * - Locking the configuration stops a later query flipping those toggles
   *   back on. Explicit INSTALL/LOAD (spatial above, httpfs in initializeS3)
   *   and CREATE SECRET still work once the config is locked.
   *
   * These do NOT stop an explicit `INSTALL sqlite; LOAD sqlite;`, which
   * re-populates the cache and makes sqlite_scan() live again. Only the SQL
   * guard's read-only statement whitelist rejects that, so the two layers are
   * complementary rather than redundant: the guard blocks the statements, this
   * blocks the implicit loads the guard's function check might miss.
   *
   * Hardening failures must not take down the plugin — parquet writing and the
   * history API matter more. isSqliteLockedDown() reports the outcome so the
   * degraded state is visible rather than assumed.
   */
  private static async lockDownSqliteExtension(
    setupConn: Awaited<ReturnType<DuckDBInstance['connect']>>,
    homeBaseDir: string | undefined,
    warn?: (message: string) => void
  ): Promise<void> {
    let cachedExtensionsRemoved = 0;

    if (homeBaseDir) {
      const extensionDir = path.join(homeBaseDir, '.duckdb', 'extensions');
      try {
        const entries: string[] = (await fs.pathExists(extensionDir))
          ? await fs.readdir(extensionDir, {
              recursive: true,
              encoding: 'utf8',
            })
          : [];
        for (const entry of entries) {
          if (path.basename(entry).startsWith('sqlite_scanner')) {
            await fs.remove(path.join(extensionDir, entry));
            cachedExtensionsRemoved += 1;
          }
        }
        this.sqliteExtensionRemoved = true;
      } catch (err) {
        warn?.(
          `Could not remove a cached DuckDB sqlite extension from ` +
            `${extensionDir}: ${(err as Error).message}. Raw SQL and ` +
            `AI-generated queries remain guarded, but avoid enabling raw SQL ` +
            `until the file is gone.`
        );
      }
    } else {
      // No plugin-owned extension directory: DuckDB uses its default home and
      // there is nothing here that we own well enough to delete.
      this.sqliteExtensionRemoved = true;
    }

    // Applied one at a time: a failure on the first (e.g. a future DuckDB
    // renaming the option) must not skip the other two.
    const settings: [string, string][] = [
      ['autoinstall_known_extensions', 'false'],
      ['autoload_known_extensions', 'false'],
      // Must come last — it freezes every option above.
      ['lock_configuration', 'true'],
    ];
    const failed: string[] = [];
    for (const [name, value] of settings) {
      try {
        await setupConn.runAndReadAll(`SET GLOBAL ${name} = ${value};`);
      } catch (err) {
        failed.push(`${name} (${(err as Error).message})`);
      }
    }

    // Read back rather than trusting the SET calls, so a silently ineffective
    // option is reported as unlocked instead of assumed applied.
    let verified = false;
    try {
      const reader = await setupConn.runAndReadAll(
        `SELECT value FROM duckdb_settings() ` +
          `WHERE name = 'autoload_known_extensions';`
      );
      const rows = reader.getRowObjects();
      verified = String(rows[0]?.value).toLowerCase() === 'false';
    } catch (err) {
      failed.push(`read-back (${(err as Error).message})`);
    }

    this.sqliteAutoloadDisabled = verified;

    if (failed.length > 0 || !verified) {
      warn?.(
        `Could not fully lock down DuckDB extension loading ` +
          `(${failed.join('; ') || 'settings did not take effect'}). The SQL ` +
          `guard still rejects the statements that could open the write ` +
          `buffer, but avoid enabling raw SQL on this instance.`
      );
    }

    this.lockdownSummary =
      `removed ${cachedExtensionsRemoved} cached sqlite extension file(s), ` +
      `autoload disabled: ${verified}`;
  }

  /**
   * Whether the SQLite extension lockdown fully applied. False means DuckDB
   * could still load its sqlite extension on this instance, so raw SQL should
   * be treated as unsafe even though the SQL guard remains in force.
   */
  static isSqliteLockedDown(): boolean {
    return this.sqliteAutoloadDisabled && this.sqliteExtensionRemoved;
  }

  /**
   * One-line description of what the lockdown did, for startup logging.
   */
  static getLockdownSummary(): string {
    return this.lockdownSummary;
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
   *
   * Closes the native instance rather than waiting for a finalizer: a plugin
   * disable/enable cycle would otherwise leave the previous DuckDB instance
   * (and its own memory budget) alive on hardware that has little to spare.
   */
  static async shutdown(): Promise<void> {
    if (this.instance) {
      try {
        this.instance.closeSync();
      } catch {
        // Close is best-effort; dropping the reference still allows GC and a
        // failure here must not break plugin.stop().
      }
      this.instance = null;
      this.initialized = false;
      this.s3Initialized = false;
      this.sqliteDbPath = null;
      this.sqliteInitialized = false;
      this.sqliteAutoloadDisabled = false;
      this.sqliteExtensionRemoved = false;
      this.lockdownSummary = 'not applied';
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
