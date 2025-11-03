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
const contextCache = new Map<string, ContextCacheEntry>();

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
 * Get cached paths for a specific context and time range
 */
export function getCachedPaths(
  context: Context,
  from: ZonedDateTime,
  to: ZonedDateTime
): Path[] | null {
  // Use rounded timestamps for cache key to improve hit rate
  const key = `${context}:${roundToMinute(from)}:${roundToMinute(to)}`;
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
 * Cache paths for a specific context and time range
 */
export function setCachedPaths(
  context: Context,
  from: ZonedDateTime,
  to: ZonedDateTime,
  paths: Path[]
): void {
  // Use rounded timestamps for cache key to improve hit rate
  const key = `${context}:${roundToMinute(from)}:${roundToMinute(to)}`;

  pathCache.set(key, {
    timeRange: { from: from.toString(), to: to.toString() },
    paths,
    timestamp: Date.now(),
  });

  // Clean up old entries if cache is too large
  if (pathCache.size > CACHE_SIZE.PATH_CONTEXT_MAX) {
    const oldestKey = Array.from(pathCache.entries()).sort(
      (a, b) => a[1].timestamp - b[1].timestamp
    )[0][0];
    pathCache.delete(oldestKey);
  }
}

/**
 * Clear all cached paths
 */
export function clearPathCache(): void {
  pathCache.clear();
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
 * Get cached contexts for a specific time range
 */
export function getCachedContexts(
  from: ZonedDateTime,
  to: ZonedDateTime
): Context[] | null {
  // Use rounded timestamps for cache key to improve hit rate
  const key = `${roundToMinute(from)}:${roundToMinute(to)}`;
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
 * Cache contexts for a specific time range
 */
export function setCachedContexts(
  from: ZonedDateTime,
  to: ZonedDateTime,
  contexts: Context[]
): void {
  // Use rounded timestamps for cache key to improve hit rate
  const key = `${roundToMinute(from)}:${roundToMinute(to)}`;

  contextCache.set(key, {
    timeRange: { from: from.toString(), to: to.toString() },
    contexts,
    timestamp: Date.now(),
  });

  // Clean up old entries if cache is too large
  if (contextCache.size > CACHE_SIZE.PATH_CONTEXT_MAX) {
    const oldestKey = Array.from(contextCache.entries()).sort(
      (a, b) => a[1].timestamp - b[1].timestamp
    )[0][0];
    contextCache.delete(oldestKey);
  }
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
