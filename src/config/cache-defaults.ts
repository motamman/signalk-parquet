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
   * Maximum number of path/context cache entries
   * Each entry is relatively small (~100 bytes)
   * @default 100 entries
   */
  PATH_CONTEXT_MAX: 100,

  /**
   * Maximum number of data buffer entries (LRU cache)
   * Each entry can be several KB depending on data volume
   * @default 1000 entries
   */
  DATA_BUFFER_MAX: 1000,
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
