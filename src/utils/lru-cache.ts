/**
 * Least Recently Used (LRU) cache with size limit
 * Automatically evicts oldest entries when size limit is reached
 *
 * This prevents memory leaks from unbounded Map growth while maintaining
 * a Map-compatible interface for easy drop-in replacement.
 *
 * @example
 * ```typescript
 * const cache = new LRUCache<string, number[]>(1000);
 * cache.set('key1', [1, 2, 3]);
 * const value = cache.get('key1'); // [1, 2, 3]
 * ```
 */
export class LRUCache<K, V> {
  private cache: Map<K, V>;
  private maxSize: number;

  constructor(maxSize: number = 1000) {
    if (maxSize <= 0) {
      throw new Error('maxSize must be positive');
    }
    this.cache = new Map();
    this.maxSize = maxSize;
  }

  /**
   * Set a value in the cache
   * Automatically evicts oldest entry if size limit is reached
   */
  set(key: K, value: V): void {
    // If key exists, delete it first (to re-insert at end, making it "most recent")
    if (this.cache.has(key)) {
      this.cache.delete(key);
    }

    // Evict oldest entry if at capacity
    if (this.cache.size >= this.maxSize) {
      const firstKey = this.cache.keys().next().value;
      if (firstKey !== undefined) {
        this.cache.delete(firstKey);
      }
    }

    // Insert at end (most recent)
    this.cache.set(key, value);
  }

  /**
   * Get a value from cache
   * Moves accessed entry to end (marks as recently used)
   */
  get(key: K): V | undefined {
    if (!this.cache.has(key)) {
      return undefined;
    }

    // Move to end (mark as recently used)
    const value = this.cache.get(key)!;
    this.cache.delete(key);
    this.cache.set(key, value);

    return value;
  }

  /**
   * Check if key exists
   */
  has(key: K): boolean {
    return this.cache.has(key);
  }

  /**
   * Delete an entry
   */
  delete(key: K): boolean {
    return this.cache.delete(key);
  }

  /**
   * Clear all entries
   */
  clear(): void {
    this.cache.clear();
  }

  /**
   * Get current size
   */
  get size(): number {
    return this.cache.size;
  }

  /**
   * Iterate over entries (oldest to newest)
   */
  forEach(callback: (value: V, key: K) => void): void {
    this.cache.forEach(callback);
  }

  /**
   * Get all keys
   */
  keys(): K[] {
    return Array.from(this.cache.keys());
  }

  /**
   * Get all values
   */
  values(): V[] {
    return Array.from(this.cache.values());
  }

  /**
   * Get cache statistics
   */
  getStats(): { size: number; maxSize: number; utilizationPercent: number } {
    return {
      size: this.cache.size,
      maxSize: this.maxSize,
      utilizationPercent: (this.cache.size / this.maxSize) * 100,
    };
  }
}
