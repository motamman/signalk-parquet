/**
 * Unit tests for the path/context query cache. The cache keys round the time
 * range to the minute so near-simultaneous History API queries share a result,
 * and entries expire after a TTL and are bounded in size. Time is controlled
 * by stubbing Date.now (saved/restored per test) so nothing depends on the
 * wall clock.
 */
import { expect } from 'chai';
import { Instant, ZoneOffset, ZonedDateTime } from '@js-joda/core';
import { Context, Path } from '@signalk/server-api';
import { CACHE_TTL, CACHE_SIZE } from '../../../src/config/cache-defaults';
import {
  getCachedPaths,
  setCachedPaths,
  clearPathCache,
  getPathCacheStats,
  getCachedContexts,
  setCachedContexts,
  clearContextCache,
  clearAllCaches,
  getAllCacheStats,
} from '../../../src/utils/path-cache';

/** Build a UTC ZonedDateTime from an ISO instant string. */
function zdt(iso: string): ZonedDateTime {
  return Instant.parse(iso).atZone(ZoneOffset.UTC);
}

const CTX = 'vessels.self' as Context;
const FROM = zdt('2025-11-02T10:15:30Z');
const TO = zdt('2025-11-02T11:15:30Z');
const PATHS = ['navigation.position', 'navigation.speedOverGround'] as Path[];

const realNow = Date.now;
let clock = 1_000_000;

beforeEach(() => {
  clock = 1_000_000;
  Date.now = () => clock;
  clearAllCaches();
});

afterEach(() => {
  Date.now = realNow;
  clearAllCaches();
});

describe('path cache', () => {
  it('returns null on a miss', () => {
    expect(getCachedPaths(CTX, FROM, TO)).to.equal(null);
  });

  it('round-trips a stored value within the TTL', () => {
    setCachedPaths(CTX, FROM, TO, PATHS);
    expect(getCachedPaths(CTX, FROM, TO)).to.deep.equal(PATHS);
  });

  it('keeps the entry just before the TTL boundary', () => {
    setCachedPaths(CTX, FROM, TO, PATHS);
    clock += CACHE_TTL.PATH_CONTEXT - 1;
    expect(getCachedPaths(CTX, FROM, TO)).to.deep.equal(PATHS);
  });

  it('expires the entry at exactly the TTL and removes it', () => {
    setCachedPaths(CTX, FROM, TO, PATHS);
    clock += CACHE_TTL.PATH_CONTEXT;
    expect(getCachedPaths(CTX, FROM, TO)).to.equal(null);
    // The expired entry is deleted on read, not merely skipped.
    expect(getPathCacheStats().size).to.equal(0);
  });

  it('shares an entry for queries in the same minute', () => {
    // 10:15:30 and 10:15:45 both round down to 10:15:00; likewise the upper
    // bound. The second query is a cache hit despite different seconds.
    setCachedPaths(CTX, FROM, TO, PATHS);
    const hit = getCachedPaths(
      CTX,
      zdt('2025-11-02T10:15:45Z'),
      zdt('2025-11-02T11:15:50Z')
    );
    expect(hit).to.deep.equal(PATHS);
  });

  it('does not share an entry across minute boundaries', () => {
    setCachedPaths(CTX, FROM, TO, PATHS);
    const miss = getCachedPaths(CTX, zdt('2025-11-02T10:16:30Z'), TO);
    expect(miss).to.equal(null);
  });

  it('keys on the context', () => {
    setCachedPaths(CTX, FROM, TO, PATHS);
    expect(getCachedPaths('vessels.other' as Context, FROM, TO)).to.equal(null);
  });

  it('evicts the oldest entry when over the size limit', () => {
    // Fill to capacity, advancing the clock so timestamps are distinct and
    // the oldest is unambiguous, then add one more to trigger eviction.
    for (let i = 0; i < CACHE_SIZE.PATH_CONTEXT_MAX; i++) {
      clock += 1;
      setCachedPaths(`vessels.v${i}` as Context, FROM, TO, [`p${i}`] as Path[]);
    }
    expect(getPathCacheStats().size).to.equal(CACHE_SIZE.PATH_CONTEXT_MAX);

    clock += 1;
    setCachedPaths('vessels.vNew' as Context, FROM, TO, ['pNew'] as Path[]);

    expect(getPathCacheStats().size).to.equal(CACHE_SIZE.PATH_CONTEXT_MAX);
    // The first-inserted (oldest) context was evicted; the newest is present.
    expect(getCachedPaths('vessels.v0' as Context, FROM, TO)).to.equal(null);
    expect(getCachedPaths('vessels.vNew' as Context, FROM, TO)).to.deep.equal([
      'pNew',
    ]);
  });

  it('clears only the path cache', () => {
    setCachedPaths(CTX, FROM, TO, PATHS);
    setCachedContexts(FROM, TO, [CTX]);
    clearPathCache();
    expect(getCachedPaths(CTX, FROM, TO)).to.equal(null);
    expect(getCachedContexts(FROM, TO)).to.deep.equal([CTX]);
  });

  it('reports stats', () => {
    setCachedPaths(CTX, FROM, TO, PATHS);
    expect(getPathCacheStats()).to.deep.equal({
      size: 1,
      maxSize: CACHE_SIZE.PATH_CONTEXT_MAX,
      ttlMs: CACHE_TTL.PATH_CONTEXT,
    });
  });
});

describe('context cache', () => {
  const CONTEXTS = [
    'vessels.self',
    'vessels.urn:mrn:imo:mmsi:123',
  ] as Context[];

  it('returns null on a miss', () => {
    expect(getCachedContexts(FROM, TO)).to.equal(null);
  });

  it('round-trips a stored value within the TTL', () => {
    setCachedContexts(FROM, TO, CONTEXTS);
    expect(getCachedContexts(FROM, TO)).to.deep.equal(CONTEXTS);
  });

  it('expires at the TTL and removes the entry', () => {
    setCachedContexts(FROM, TO, CONTEXTS);
    clock += CACHE_TTL.PATH_CONTEXT;
    expect(getCachedContexts(FROM, TO)).to.equal(null);
    expect(getAllCacheStats().contexts.size).to.equal(0);
  });

  it('shares an entry for queries in the same minute', () => {
    setCachedContexts(FROM, TO, CONTEXTS);
    const hit = getCachedContexts(
      zdt('2025-11-02T10:15:59Z'),
      zdt('2025-11-02T11:15:00Z')
    );
    expect(hit).to.deep.equal(CONTEXTS);
  });

  it('clears only the context cache', () => {
    setCachedPaths(CTX, FROM, TO, PATHS);
    setCachedContexts(FROM, TO, CONTEXTS);
    clearContextCache();
    expect(getCachedContexts(FROM, TO)).to.equal(null);
    expect(getCachedPaths(CTX, FROM, TO)).to.deep.equal(PATHS);
  });
});

describe('clearAllCaches', () => {
  it('empties both caches', () => {
    setCachedPaths(CTX, FROM, TO, PATHS);
    setCachedContexts(FROM, TO, [CTX]);
    clearAllCaches();
    const stats = getAllCacheStats();
    expect(stats.paths.size).to.equal(0);
    expect(stats.contexts.size).to.equal(0);
  });

  it('reports the configured limits and TTLs', () => {
    expect(getAllCacheStats()).to.deep.equal({
      paths: {
        size: 0,
        maxSize: CACHE_SIZE.PATH_CONTEXT_MAX,
        ttlMs: CACHE_TTL.PATH_CONTEXT,
      },
      contexts: {
        size: 0,
        maxSize: CACHE_SIZE.PATH_CONTEXT_MAX,
        ttlMs: CACHE_TTL.PATH_CONTEXT,
      },
    });
  });
});
