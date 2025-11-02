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
export class DuckDBPool {
  private static instance: DuckDBInstance | null = null;
  private static initialized: boolean = false;

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
    await setupConn.runAndReadAll("INSTALL spatial;");
    await setupConn.runAndReadAll("LOAD spatial;");
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
      throw new Error('DuckDBPool not initialized. Call DuckDBPool.initialize() first.');
    }

    return await this.instance.connect();
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
    }
  }

  /**
   * Check if pool is ready
   * @returns true if the pool has been initialized
   */
  static isInitialized(): boolean {
    return this.initialized;
  }
}
