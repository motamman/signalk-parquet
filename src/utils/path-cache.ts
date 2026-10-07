import { Context, Path } from '@signalk/server-api';
import { ZonedDateTime } from '@js-joda/core';
import { CACHE_TTL, CACHE_SIZE } from '../config/cache-defaults';

interface PathCacheEntry {
  timeRange: { from: string; to: string };
  paths: Path[];
  timestamp: number;
}

interface ContextCacheEntry {
  timeRange: { from: string; to: string };
  contexts: Context[];
  timestamp: number;
}

const pathCache = new Map<string, PathCacheEntry>();

/**
 * Bumped by every `clearPathCache()`, so a writer can tell whether the cache was
 * invalidated while it was computing.
 *
 * A read-through cache is filled by: miss, compute, store — and the compute is
 * an await. If the parquet tree changes during it (the daily export writes a
 * file and marks that path's buffer rows exported), the invalidation lands
 * *before* the store, and the store then puts the pre-export listing back into
 * the cache it was meant to drop. The path is by then absent from the buffer
 * probe too, so it disappears from the listing for the life of the entry. Seen
 * on macOS CI, 2026-09-27, where a mid-export observation listed one of two
 * exported paths; the same code passes six runs for six on a faster machine,
 * which is what a race looks like.
 */
let pathCacheEpoch = 0;
const contextCache = new Map<string, ContextCacheEntry>();

/**
 * Store an entry, dropping every expired one first and then the oldest past
 * the size limit. Keys carry the minute-rounded window, so a sliding
 * dashboard poll makes a new key every minute and never asks for the old one
 * again: expiring entries only when their own key is looked up left each map
 * full of stale result lists (#143). Insertion order is age order, because an
 * entry is deleted before it is set again.
 */
function store<T extends { timestamp: number }>(
  cache: Map<string, T>,
  key: string,
  entry: T
): void {
  const now = Date.now();
  for (const [k, v] of cache) {
    if (now - v.timestamp >= CACHE_TTL.PATH_CONTEXT) cache.delete(k);
  }
  cache.delete(key);
  cache.set(key, entry);
  while (cache.size > CACHE_SIZE.PATH_CONTEXT_MAX) {
    cache.delete(cache.keys().next().value as string);
  }
}

/**
 * Round timestamp to nearest minute for cache key generation
 * This allows queries within the same minute to share cache entries
 * @param dateTime - ZonedDateTime to round
 * @returns ISO string rounded to the minute (e.g., "2025-11-02T10:15:00Z")
 */
function roundToMinute(dateTime: ZonedDateTime): string {
  // Get the instant and convert to epoch milliseconds
  const epochMs = dateTime.toInstant().toEpochMilli();

  // Round to nearest minute (60000 ms)
  const roundedMs = Math.floor(epochMs / 60000) * 60000;

  // Convert back to ISO string
  return new Date(roundedMs).toISOString();
}

/**
 * Get cached paths for a specific data directory, context, and time range.
 * Keys include the data directory so entries cached before a setDataDir()
 * reconfigure (or by another HistoryAPI instance) are never served for a
 * different directory.
 */
export function getCachedPaths(
  dataDir: string,
  context: Context,
  from: ZonedDateTime,
  to: ZonedDateTime
): Path[] | null {
  // Use rounded timestamps for cache key to improve hit rate
  const key = `${dataDir}:${context}:${roundToMinute(from)}:${roundToMinute(to)}`;
  const cached = pathCache.get(key);

  if (cached && Date.now() - cached.timestamp < CACHE_TTL.PATH_CONTEXT) {
    return cached.paths;
  }

  // Clean up expired entry
  if (cached) {
    pathCache.delete(key);
  }

  return null;
}

/**
 * Cache paths for a specific data directory, context, and time range.
 * Pass the directory the query actually ran against (captured before the
 * query started), so a result that resolves after a setDataDir() is stored
 * under the directory it belongs to.
 */
export function setCachedPaths(
  dataDir: string,
  context: Context,
  from: ZonedDateTime,
  to: ZonedDateTime,
  paths: Path[],
  epoch: number
): void {
  // Drop a result computed before an invalidation rather than storing it over
  // the cleared cache. Required, not optional, so a caller cannot omit it and
  // silently reintroduce the race.
  if (epoch !== pathCacheEpoch) return;

  // Use rounded timestamps for cache key to improve hit rate
  const key = `${dataDir}:${context}:${roundToMinute(from)}:${roundToMinute(to)}`;

  store(pathCache, key, {
    timeRange: { from: from.toString(), to: to.toString() },
    paths,
    timestamp: Date.now(),
  });
}

/**
 * The current epoch, to be captured before computing a listing and handed to
 * `setCachedPaths`.
 */
export function currentPathCacheEpoch(): number {
  return pathCacheEpoch;
}

/**
 * Clear all cached paths, and invalidate any listing already being computed.
 */
export function clearPathCache(): void {
  pathCache.clear();
  pathCacheEpoch += 1;
}

/**
 * Get cache statistics for monitoring
 */
export function getPathCacheStats() {
  return {
    size: pathCache.size,
    maxSize: CACHE_SIZE.PATH_CONTEXT_MAX,
    ttlMs: CACHE_TTL.PATH_CONTEXT,
  };
}

// ============================================================================
// Context Caching Functions
// ============================================================================

/**
 * Get cached contexts for a specific data directory and time range.
 * Keyed by directory for the same reason as getCachedPaths.
 */
export function getCachedContexts(
  dataDir: string,
  from: ZonedDateTime,
  to: ZonedDateTime
): Context[] | null {
  // Use rounded timestamps for cache key to improve hit rate
  const key = `${dataDir}:${roundToMinute(from)}:${roundToMinute(to)}`;
  const cached = contextCache.get(key);

  if (cached && Date.now() - cached.timestamp < CACHE_TTL.PATH_CONTEXT) {
    return cached.contexts;
  }

  // Clean up expired entry
  if (cached) {
    contextCache.delete(key);
  }

  return null;
}

/**
 * Cache contexts for a specific data directory and time range.
 * Pass the directory the query actually ran against.
 */
export function setCachedContexts(
  dataDir: string,
  from: ZonedDateTime,
  to: ZonedDateTime,
  contexts: Context[]
): void {
  // Use rounded timestamps for cache key to improve hit rate
  const key = `${dataDir}:${roundToMinute(from)}:${roundToMinute(to)}`;

  store(contextCache, key, {
    timeRange: { from: from.toString(), to: to.toString() },
    contexts,
    timestamp: Date.now(),
  });
}

/**
 * Clear all cached contexts
 */
export function clearContextCache(): void {
  contextCache.clear();
}

/**
 * Clear all caches (paths and contexts)
 */
export function clearAllCaches(): void {
  pathCache.clear();
  contextCache.clear();
}

/**
 * Get all cache statistics
 */
export function getAllCacheStats() {
  return {
    paths: {
      size: pathCache.size,
      maxSize: CACHE_SIZE.PATH_CONTEXT_MAX,
      ttlMs: CACHE_TTL.PATH_CONTEXT,
    },
    contexts: {
      size: contextCache.size,
      maxSize: CACHE_SIZE.PATH_CONTEXT_MAX,
      ttlMs: CACHE_TTL.PATH_CONTEXT,
    },
  };
}
