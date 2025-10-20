import { Context, Path } from '@signalk/server-api';
import { ZonedDateTime } from '@js-joda/core';

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
const CACHE_TTL_MS = 60 * 1000; // 1 minute
const MAX_CACHE_SIZE = 100;

/**
 * Get cached paths for a specific context and time range
 */
export function getCachedPaths(
  context: Context,
  from: ZonedDateTime,
  to: ZonedDateTime
): Path[] | null {
  const key = `${context}:${from.toString()}:${to.toString()}`;
  const cached = pathCache.get(key);

  if (cached && Date.now() - cached.timestamp < CACHE_TTL_MS) {
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
  const key = `${context}:${from.toString()}:${to.toString()}`;

  pathCache.set(key, {
    timeRange: { from: from.toString(), to: to.toString() },
    paths,
    timestamp: Date.now()
  });

  // Clean up old entries if cache is too large
  if (pathCache.size > MAX_CACHE_SIZE) {
    const oldestKey = Array.from(pathCache.entries())
      .sort((a, b) => a[1].timestamp - b[1].timestamp)[0][0];
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
    maxSize: MAX_CACHE_SIZE,
    ttlMs: CACHE_TTL_MS
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
  const key = `${from.toString()}:${to.toString()}`;
  const cached = contextCache.get(key);

  if (cached && Date.now() - cached.timestamp < CACHE_TTL_MS) {
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
  const key = `${from.toString()}:${to.toString()}`;

  contextCache.set(key, {
    timeRange: { from: from.toString(), to: to.toString() },
    contexts,
    timestamp: Date.now()
  });

  // Clean up old entries if cache is too large
  if (contextCache.size > MAX_CACHE_SIZE) {
    const oldestKey = Array.from(contextCache.entries())
      .sort((a, b) => a[1].timestamp - b[1].timestamp)[0][0];
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
      maxSize: MAX_CACHE_SIZE,
      ttlMs: CACHE_TTL_MS
    },
    contexts: {
      size: contextCache.size,
      maxSize: MAX_CACHE_SIZE,
      ttlMs: CACHE_TTL_MS
    }
  };
}
