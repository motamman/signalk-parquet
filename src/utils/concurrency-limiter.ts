/**
 * Limits the number of concurrent promise executions
 *
 * @example
 * const limiter = new ConcurrencyLimiter(5);
 * const results = await limiter.map(items, async (item) => {
 *   return await processItem(item);
 * });
 */
export class ConcurrencyLimiter {
  private maxConcurrent: number;
  private running: number = 0;
  private queue: Array<() => void> = [];

  constructor(maxConcurrent: number = 10) {
    if (maxConcurrent <= 0) {
      throw new Error('maxConcurrent must be positive');
    }
    this.maxConcurrent = maxConcurrent;
  }

  /**
   * Execute an async function with concurrency limiting
   */
  private async execute<T>(fn: () => Promise<T>): Promise<T> {
    // Wait if at capacity
    while (this.running >= this.maxConcurrent) {
      await new Promise<void>(resolve => this.queue.push(resolve));
    }

    this.running++;
    try {
      return await fn();
    } finally {
      this.running--;
      // Resume next queued operation
      const next = this.queue.shift();
      if (next) {
        next();
      }
    }
  }

  /**
   * Map over an array with concurrency limiting
   * Similar to Promise.all() but with controlled concurrency
   *
   * @param items - Array of items to process
   * @param fn - Async function to execute for each item
   * @returns Array of results in the same order as input
   */
  async map<T, R>(
    items: T[],
    fn: (item: T, index: number) => Promise<R>
  ): Promise<R[]> {
    return Promise.all(
      items.map((item, index) => this.execute(() => fn(item, index)))
    );
  }

  /**
   * Get current concurrency statistics
   */
  getStats(): { running: number; queued: number; maxConcurrent: number } {
    return {
      running: this.running,
      queued: this.queue.length,
      maxConcurrent: this.maxConcurrent,
    };
  }
}
