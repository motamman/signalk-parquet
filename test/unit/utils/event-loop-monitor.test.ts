/**
 * The blocking monitor.
 *
 * It exists because client-side timings cannot tell a slow answer from a frozen
 * server, so these tests check the thing that matters: a synchronous block of a
 * known length shows up, and a reset forgets it.
 */
import { expect } from 'chai';
import {
  eventLoopDelay,
  eventLoopMonitorRunning,
  resetEventLoopDelay,
  startEventLoopMonitor,
  stopEventLoopMonitor,
} from '../../../src/utils/event-loop-monitor';

/** Hold the thread for `ms`, as a synchronous SQLite read or append would. */
function block(ms: number): void {
  const end = process.hrtime.bigint() + BigInt(Math.round(ms * 1e6));
  while (process.hrtime.bigint() < end) {
    // Spinning is the point: this is what a blocked event loop looks like.
  }
}

/** Let the histogram's timer fire a few times. */
const breathe = () => new Promise(r => setTimeout(r, 60));

describe('event loop monitor', () => {
  afterEach(() => {
    stopEventLoopMonitor();
  });

  it('reports nothing until started', async () => {
    expect(eventLoopMonitorRunning()).to.equal(false);
    expect(await eventLoopDelay()).to.equal(null);
  });

  it('sees a synchronous block of a known length', async () => {
    startEventLoopMonitor();
    expect(eventLoopMonitorRunning()).to.equal(true);
    await breathe();
    await resetEventLoopDelay();

    block(150);
    await breathe();

    const delay = await eventLoopDelay();
    expect(delay).to.not.equal(null);
    // The histogram's timer cannot fire during the block, so the delay it
    // records is at least the block minus one sampling interval.
    expect(delay!.max, `max was ${delay!.max}ms`).to.be.greaterThan(100);
    expect(delay!.resolutionMs).to.equal(10);
    expect(delay!.sinceResetSec).to.be.greaterThan(0);
  });

  it('includes a block done in the same loop turn as the read', async () => {
    startEventLoopMonitor();
    await breathe();
    await resetEventLoopDelay();

    // No breathe between the block and the read: the block is recorded when
    // the sampling timer next fires, and the read must wait for that.
    block(150);
    const delay = await eventLoopDelay();
    expect(delay!.max, `max was ${delay!.max}ms`).to.be.greaterThan(100);
  });

  it('forgets a block after a reset', async () => {
    startEventLoopMonitor();
    await breathe();
    block(150);
    await breathe();
    expect((await eventLoopDelay())!.max).to.be.greaterThan(100);

    await resetEventLoopDelay();
    await breathe();
    const after = await eventLoopDelay();
    expect(after!.max, `max after reset was ${after!.max}ms`).to.be.lessThan(
      100
    );
  });

  it('starting twice keeps the same histogram rather than restarting it', async () => {
    startEventLoopMonitor();
    await breathe();
    await resetEventLoopDelay();
    block(150);
    await breathe();

    startEventLoopMonitor(); // second call: must not discard what was recorded
    expect((await eventLoopDelay())!.max).to.be.greaterThan(100);
  });

  it('reports nothing again after stopping', async () => {
    startEventLoopMonitor();
    await breathe();
    stopEventLoopMonitor();
    expect(eventLoopMonitorRunning()).to.equal(false);
    expect(await eventLoopDelay()).to.equal(null);
  });

  it('answers null rather than hanging when stopped mid-read', async () => {
    startEventLoopMonitor();
    const reading = eventLoopDelay();
    stopEventLoopMonitor();
    expect(await reading).to.equal(null);
  });
});
