/**
 * plugin.start() and plugin.stop() against a fake server: a stop leaves
 * nothing of the run it stopped behind — no timer and no subscription to the
 * server's data buses (identity capture's among them) — including when it lands while
 * start() is still waiting on something, which Signal K allows because it
 * does not await start() (#139).
 *
 * start() is held at a real await: the legacy retention migration saves the
 * plugin's options and waits for the server's callback, which the fake
 * server here keeps until the test lets it go.
 */
import { expect } from 'chai';
import createPlugin from '../../src/index';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import type { SignalKPlugin } from '../../src/types';
import { DuckDBPool } from '../../src/utils/duckdb-pool';

/** Timers armed in this process, setTimeout and setInterval alike. */
function timers(): number {
  return process
    .getActiveResourcesInfo()
    .filter(resource => resource === 'Timeout').length;
}

/**
 * The timer count to come back to. Taken after a turn of the loop: mocha arms
 * its own timeout for an async test only once the test's synchronous start
 * has run, and that timer is not the plugin's.
 */
async function baselineTimers(): Promise<number> {
  await new Promise(resolve => setImmediate(resolve));
  return timers();
}

describe('plugin start and stop', function () {
  // A full start opens DuckDB, which may install the spatial extension.
  this.timeout(60000);

  let host: FakeSignalK;
  let plugin: SignalKPlugin;
  let releaseSave: (() => void) | undefined;

  beforeEach(() => {
    releaseSave = undefined;
    host = createFakeSignalK({
      savePluginOptions: (_opts, cb) => {
        releaseSave = () => cb?.();
      },
    });
    plugin = createPlugin(host.app);
  });

  afterEach(async () => {
    releaseSave?.();
    await plugin.stop();
    await host.cleanup();
  });

  /** Options that run the legacy retention migration, so start() waits. */
  const heldOptions = () => ({
    outputDirectory: host.dataDir,
    retentionDays: 7,
  });
  const options = () => ({
    outputDirectory: host.dataDir,
    configSchemaVersion: 1,
  });

  /**
   * Start the plugin on a DuckDB pool already open on DuckDB's default home,
   * which start() then keeps, rather than one it opens on the temp data
   * directory: an extension this process loads stays loaded after the pool
   * closes, and Windows refuses to delete a loaded DLL, so the directory
   * could not be removed afterwards (EPERM on spatial.duckdb_extension,
   * Windows CI). stop() still shuts the pool down as it always does.
   */
  async function start(opts: Record<string, unknown>): Promise<void> {
    await DuckDBPool.initialize();
    await plugin.start(opts);
  }

  /** Until start() is waiting on the save callback. */
  async function untilHeld(): Promise<void> {
    while (!releaseSave) await new Promise(r => setImmediate(r));
  }

  it('leaves nothing running after a full start and stop', async () => {
    const before = await baselineTimers();
    await start(options());
    expect(host.activeBusHandlers()).to.be.greaterThan(0);
    expect(timers()).to.be.greaterThan(before);

    await plugin.stop();
    expect(timers()).to.equal(before);
    expect(host.activeBusHandlers()).to.equal(0);
  });

  it('leaves nothing running when stopped while start is waiting', async () => {
    const before = await baselineTimers();
    const starting = start(heldOptions());
    await untilHeld();

    const stopping = plugin.stop();
    releaseSave!();
    await Promise.all([starting, stopping]);

    expect(timers()).to.equal(before);
    expect(host.activeBusHandlers()).to.equal(0);
  });

  it('runs one set of everything when started again after an interrupted start', async () => {
    const before = await baselineTimers();
    // What one run holds, from a run nothing interrupted.
    await start(options());
    const oneRun = { handlers: host.activeBusHandlers(), timers: timers() };
    await plugin.stop();

    const interrupted = start(heldOptions());
    await untilHeld();
    const stopping = plugin.stop();
    releaseSave!();
    await Promise.all([interrupted, stopping]);

    await start(options());
    expect(host.activeBusHandlers(), 'bus subscriptions').to.equal(
      oneRun.handlers
    );
    expect(timers(), 'timers').to.equal(oneRun.timers);

    await plugin.stop();
    expect(timers()).to.equal(before);
    expect(host.activeBusHandlers()).to.equal(0);
  });
});
