/**
 * Centralized cache configuration defaults
 * These values can be adjusted based on system resources and usage patterns
 */

/**
 * Cache Time-To-Live (TTL) values in milliseconds
 */
export const CACHE_TTL = {
  /**
   * Schema cache TTL - schemas are relatively static
   * Increase if schema changes are rare, decrease if schema changes frequently
   * @default 30 minutes
   */
  SCHEMA: 30 * 60 * 1000,

  /**
   * File list cache TTL - used for context discovery
   * Increase for better performance, decrease for more real-time file discovery
   * @default 2 minutes
   */
  FILE_LIST: 2 * 60 * 1000,

  /**
   * Path and context cache TTL - used for query optimization
   * Increase for better cache hit rate, decrease for more up-to-date results
   * @default 1 minute
   */
  PATH_CONTEXT: 60 * 1000,

  /**
   * Directory scanner cache TTL - used during consolidation
   * Increase for better performance on large file systems
   * @default 5 minutes
   */
  DIRECTORY_SCAN: 5 * 60 * 1000,
} as const;

/**
 * Cache size limits
 */
export const CACHE_SIZE = {
  /**
   * Maximum number of path/context cache entries, per cache. An entry holds a
   * whole listing (every path of a context, or every context in a window), so
   * its size grows with the number of vessels or paths heard; it is not a
   * small fixed record. Expired entries are dropped on every insert
   * (utils/path-cache.ts).
   * @default 100 entries
   */
  PATH_CONTEXT_MAX: 100,

  /**
   * Maximum number of data buffer entries (LRU cache)
   * Each entry can be several KB depending on data volume
   * @default 1000 entries
   */
  DATA_BUFFER_MAX: 1000,

  /**
   * Maximum number of decoded parquet footers kept (parquet-footer.ts).
   * An entry is 2-5 KB (measured on brain, 2026-10-05: 5.3 KB for a raw
   * position file of 18 row groups, 1.9 KB for a scalar one), so this is a
   * budget of about 50 MB. Decoding the same footer again allocates about
   * 4.6 MB of short-lived heap. Too small a limit only means more files are
   * decoded again, as every file was before the cache.
   * @default 10000 entries
   */
  FOOTER_MAX: 10000,
} as const;

/**
 * Concurrency limits
 */
export const CONCURRENCY = {
  /**
   * Maximum concurrent DuckDB queries in History API
   * Increase for better throughput on powerful systems
   * Decrease to prevent resource exhaustion
   * @default 10 concurrent queries
   */
  MAX_QUERIES: 10,
} as const;

/**
 * Default query parameters
 */
export const QUERY_DEFAULTS = {
  /**
   * Default time resolution for queries (milliseconds)
   * @default 60000 (1 minute)
   */
  RESOLUTION_MS: 60000,
} as const;
