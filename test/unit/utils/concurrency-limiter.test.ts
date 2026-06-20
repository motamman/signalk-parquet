/**
 * Unit tests for ConcurrencyLimiter. The limiter gates parallel DuckDB/file
 * work, so the tests pin the contract precisely: results in input order, the
 * concurrency ceiling actually enforced, FIFO wake-up of queued tasks, and
 * capacity release on failure. All scheduling is driven by manually resolved
 * deferred promises plus a microtask drain — no timers, no wall-clock
 * dependence.
 */
import { expect } from 'chai';
import { ConcurrencyLimiter } from '../../../src/utils/concurrency-limiter';

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/**
 * Wait until the microtask queue is fully drained. setImmediate runs in the
 * event loop's check phase, after every pending promise continuation
 * (including continuations those continuations schedule) has executed, so a
 * single await is enough for any pure-promise cascade.
 */
function drain(): Promise<void> {
  return new Promise<void>(resolve => setImmediate(resolve));
}

describe('ConcurrencyLimiter', () => {
  describe('constructor', () => {
    it('throws for maxConcurrent = 0', () => {
      expect(() => new ConcurrencyLimiter(0)).to.throw(
        Error,
        'maxConcurrent must be positive'
      );
    });

    it('throws for maxConcurrent = -1', () => {
      expect(() => new ConcurrencyLimiter(-1)).to.throw(
        Error,
        'maxConcurrent must be positive'
      );
    });

    it('defaults to a maxConcurrent of 10', () => {
      expect(new ConcurrencyLimiter().getStats().maxConcurrent).to.equal(10);
    });

    it('accepts the boundary value 1', () => {
      expect(new ConcurrencyLimiter(1).getStats().maxConcurrent).to.equal(1);
    });
  });

  describe('map', () => {
    it('returns an empty array without invoking fn for empty input', async () => {
      const limiter = new ConcurrencyLimiter(2);
      let calls = 0;

      const results = await limiter.map([], async () => {
        calls++;
        return 1;
      });

      expect(results).to.deep.equal([]);
      expect(calls).to.equal(0);
    });

    it('passes each item with its index to fn', async () => {
      const limiter = new ConcurrencyLimiter(2);
      const seen: Array<[string, number]> = [];

      await limiter.map(['a', 'b', 'c'], async (item, index) => {
        seen.push([item, index]);
        return index;
      });

      expect(seen).to.deep.equal([
        ['a', 0],
        ['b', 1],
        ['c', 2],
      ]);
    });

    it('returns results in input order even when tasks finish in reverse', async () => {
      const limiter = new ConcurrencyLimiter(3);
      const deferreds = [
        deferred<string>(),
        deferred<string>(),
        deferred<string>(),
      ];

      const mapPromise = limiter.map(
        [0, 1, 2],
        (_item, index) => deferreds[index].promise
      );

      // Complete the tasks back to front.
      deferreds[2].resolve('two');
      deferreds[1].resolve('one');
      deferreds[0].resolve('zero');

      expect(await mapPromise).to.deep.equal(['zero', 'one', 'two']);
    });

    it('propagates a task rejection through map', async () => {
      const limiter = new ConcurrencyLimiter(2);
      let caught: unknown;

      try {
        await limiter.map([1, 2, 3], async n => {
          if (n === 2) {
            throw new Error('task failed');
          }
          return n;
        });
      } catch (err) {
        caught = err;
      }

      expect(caught).to.be.instanceOf(Error);
      expect((caught as Error).message).to.equal('task failed');
    });

    it('releases capacity after a rejection and stays usable', async () => {
      const limiter = new ConcurrencyLimiter(2);

      try {
        await limiter.map([1, 2, 3], async n => {
          if (n === 2) {
            throw new Error('boom');
          }
          return n;
        });
      } catch {
        // expected — only the cleanup matters here
      }
      await drain();

      expect(limiter.getStats()).to.deep.equal({
        running: 0,
        queued: 0,
        maxConcurrent: 2,
      });
      expect(await limiter.map([10, 20], async n => n * 2)).to.deep.equal([
        20, 40,
      ]);
    });
  });

  describe('concurrency enforcement', () => {
    it('never runs more than maxConcurrent tasks and wakes the queue FIFO', async () => {
      const limiter = new ConcurrencyLimiter(2);
      const deferreds = [0, 1, 2, 3, 4].map(() => deferred<number>());
      const started: number[] = [];
      let active = 0;
      let maxActive = 0;

      const mapPromise = limiter.map([0, 1, 2, 3, 4], async (_item, index) => {
        active++;
        if (active > maxActive) {
          maxActive = active;
        }
        started.push(index);
        try {
          return await deferreds[index].promise;
        } finally {
          active--;
        }
      });
      await drain();

      // Only the first two tasks start; the rest are queued.
      expect(started).to.deep.equal([0, 1]);
      expect(limiter.getStats()).to.deep.equal({
        running: 2,
        queued: 3,
        maxConcurrent: 2,
      });

      // Finishing task 0 wakes exactly the head of the queue (task 2).
      deferreds[0].resolve(0);
      await drain();
      expect(started).to.deep.equal([0, 1, 2]);
      expect(limiter.getStats().running).to.equal(2);
      expect(limiter.getStats().queued).to.equal(2);

      // Finishing task 2 (out of start order) still wakes FIFO: task 3 next.
      deferreds[2].resolve(2);
      await drain();
      expect(started).to.deep.equal([0, 1, 2, 3]);

      deferreds[1].resolve(1);
      await drain();
      expect(started).to.deep.equal([0, 1, 2, 3, 4]);

      deferreds[3].resolve(3);
      deferreds[4].resolve(4);
      expect(await mapPromise).to.deep.equal([0, 1, 2, 3, 4]);
      expect(maxActive).to.equal(2);
      expect(limiter.getStats()).to.deep.equal({
        running: 0,
        queued: 0,
        maxConcurrent: 2,
      });
    });

    it('runs strictly sequentially with maxConcurrent = 1', async () => {
      const limiter = new ConcurrencyLimiter(1);
      const deferreds = [0, 1, 2].map(() => deferred<string>());
      const started: number[] = [];

      const mapPromise = limiter.map([0, 1, 2], (_item, index) => {
        started.push(index);
        return deferreds[index].promise;
      });
      await drain();

      expect(started).to.deep.equal([0]);
      expect(limiter.getStats()).to.deep.equal({
        running: 1,
        queued: 2,
        maxConcurrent: 1,
      });

      deferreds[0].resolve('a');
      await drain();
      expect(started).to.deep.equal([0, 1]);

      deferreds[1].resolve('b');
      await drain();
      expect(started).to.deep.equal([0, 1, 2]);

      deferreds[2].resolve('c');
      expect(await mapPromise).to.deep.equal(['a', 'b', 'c']);
    });
  });

  describe('getStats', () => {
    it('reports an idle limiter before any work', () => {
      expect(new ConcurrencyLimiter(4).getStats()).to.deep.equal({
        running: 0,
        queued: 0,
        maxConcurrent: 4,
      });
    });

    it('returns to the idle shape after map completes', async () => {
      const limiter = new ConcurrencyLimiter(4);

      await limiter.map([1, 2, 3], async n => n);

      expect(limiter.getStats()).to.deep.equal({
        running: 0,
        queued: 0,
        maxConcurrent: 4,
      });
    });
  });
});
