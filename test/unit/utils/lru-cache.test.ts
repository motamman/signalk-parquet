/**
 * Unit tests for the LRU cache used to bound in-memory data buffers. The
 * recency rules (get refreshes, set re-inserts, oldest evicted) are what keep
 * hot paths buffered while idle paths age out.
 */
import { expect } from 'chai';
import { LRUCache } from '../../../src/utils/lru-cache';

describe('LRUCache', () => {
  describe('constructor', () => {
    it('rejects a zero maxSize', () => {
      expect(() => new LRUCache<string, number>(0)).to.throw(
        'maxSize must be positive'
      );
    });

    it('rejects a negative maxSize', () => {
      expect(() => new LRUCache<string, number>(-1)).to.throw(
        'maxSize must be positive'
      );
    });

    it('allows a cache of size one', () => {
      const cache = new LRUCache<string, number>(1);
      cache.set('a', 1);
      cache.set('b', 2);
      expect(cache.has('a')).to.equal(false);
      expect(cache.get('b')).to.equal(2);
    });
  });

  describe('set / get', () => {
    it('stores and retrieves a value', () => {
      const cache = new LRUCache<string, number[]>(10);
      cache.set('k', [1, 2, 3]);
      expect(cache.get('k')).to.deep.equal([1, 2, 3]);
    });

    it('returns undefined for a missing key', () => {
      const cache = new LRUCache<string, number>(10);
      expect(cache.get('missing')).to.equal(undefined);
    });

    it('overwrites an existing key', () => {
      const cache = new LRUCache<string, number>(10);
      cache.set('k', 1);
      cache.set('k', 2);
      expect(cache.get('k')).to.equal(2);
      expect(cache.size).to.equal(1);
    });
  });

  describe('eviction', () => {
    it('evicts the oldest entry when capacity is exceeded', () => {
      const cache = new LRUCache<string, number>(2);
      cache.set('a', 1);
      cache.set('b', 2);
      cache.set('c', 3);
      expect(cache.has('a')).to.equal(false);
      expect(cache.has('b')).to.equal(true);
      expect(cache.has('c')).to.equal(true);
    });

    it('treats a got entry as recently used', () => {
      const cache = new LRUCache<string, number>(2);
      cache.set('a', 1);
      cache.set('b', 2);
      cache.get('a');
      cache.set('c', 3);
      expect(cache.has('a')).to.equal(true);
      expect(cache.has('b')).to.equal(false);
    });

    it('treats a re-set entry as recently used', () => {
      const cache = new LRUCache<string, number>(2);
      cache.set('a', 1);
      cache.set('b', 2);
      cache.set('a', 10);
      cache.set('c', 3);
      expect(cache.has('a')).to.equal(true);
      expect(cache.has('b')).to.equal(false);
    });

    it('never exceeds maxSize', () => {
      const cache = new LRUCache<number, number>(3);
      for (let i = 0; i < 10; i++) cache.set(i, i);
      expect(cache.size).to.equal(3);
      expect(cache.keys()).to.deep.equal([7, 8, 9]);
    });
  });

  describe('delete / clear', () => {
    it('deletes a key and reports whether it existed', () => {
      const cache = new LRUCache<string, number>(10);
      cache.set('a', 1);
      expect(cache.delete('a')).to.equal(true);
      expect(cache.delete('a')).to.equal(false);
      expect(cache.has('a')).to.equal(false);
    });

    it('clears all entries', () => {
      const cache = new LRUCache<string, number>(10);
      cache.set('a', 1);
      cache.set('b', 2);
      cache.clear();
      expect(cache.size).to.equal(0);
    });
  });

  describe('iteration', () => {
    it('lists keys and values oldest to newest', () => {
      const cache = new LRUCache<string, number>(10);
      cache.set('a', 1);
      cache.set('b', 2);
      cache.get('a');
      expect(cache.keys()).to.deep.equal(['b', 'a']);
      expect(cache.values()).to.deep.equal([2, 1]);
    });

    it('forEach visits every entry', () => {
      const cache = new LRUCache<string, number>(10);
      cache.set('a', 1);
      cache.set('b', 2);
      const seen: Array<[string, number]> = [];
      cache.forEach((value, key) => seen.push([key, value]));
      expect(seen).to.deep.equal([
        ['a', 1],
        ['b', 2],
      ]);
    });
  });

  describe('getStats', () => {
    it('reports size, capacity, and utilization', () => {
      const cache = new LRUCache<string, number>(4);
      cache.set('a', 1);
      expect(cache.getStats()).to.deep.equal({
        size: 1,
        maxSize: 4,
        utilizationPercent: 25,
      });
    });
  });
});
