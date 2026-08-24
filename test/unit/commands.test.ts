/**
 * Unit tests for threshold-monitor lifecycle in the command registry.
 *
 * Every enabled threshold holds a live streambundle subscription, keyed by
 * buildThresholdMonitorKey() (command + watchPath + operator + comparison
 * values). Editing a command's thresholds must unsubscribe the monitors for
 * the *old* threshold set, otherwise each edit leaves an orphaned subscription
 * running for the lifetime of the server. These tests drive registerCommand /
 * updateCommand against a stub streambundle and assert on the number of live
 * subscriptions, which is the observable form of that leak.
 */
import { expect } from 'chai';
import type { ServerAPI } from '@signalk/server-api';
import {
  initializeCommandState,
  registerCommand,
  setCurrentCommands,
  stopThresholdMonitoring,
  updateCommand,
} from '../../src/commands';
import type { ThresholdConfig } from '../../src/types';

/** One subscription handed out by the stub streambundle. */
interface StubSubscription {
  path: string;
  active: boolean;
}

interface StubHost {
  app: ServerAPI;
  subscriptions: StubSubscription[];
  /** Subscriptions that have not been unsubscribed. */
  live(): StubSubscription[];
}

/**
 * Minimal ServerAPI stub covering only what the command registry touches:
 * logging, PUT-handler registration, delta publishing, self-path reads, and
 * the streambundle subscription whose teardown is under test.
 */
function createStubHost(): StubHost {
  const subscriptions: StubSubscription[] = [];

  const app = {
    debug: () => {},
    error: () => {},
    registerPutHandler: () => {},
    handleMessage: () => {},
    getSelfPath: () => undefined,
    streambundle: {
      getSelfStream: (path: string) => ({
        onValue: () => {
          const sub: StubSubscription = { path, active: true };
          subscriptions.push(sub);
          return () => {
            sub.active = false;
          };
        },
      }),
    },
  } as unknown as ServerAPI;

  return {
    app,
    subscriptions,
    live: () => subscriptions.filter(s => s.active),
  };
}

function threshold(overrides: Partial<ThresholdConfig> = {}): ThresholdConfig {
  return {
    enabled: true,
    watchPath: 'environment.depth.belowKeel',
    operator: 'lt',
    value: 2,
    activateOnMatch: true,
    ...overrides,
  };
}

describe('command threshold monitoring', () => {
  let host: StubHost;

  beforeEach(() => {
    host = createStubHost();
    setCurrentCommands([]);
    initializeCommandState([], host.app);
  });

  afterEach(() => {
    stopThresholdMonitoring();
    setCurrentCommands([]);
  });

  it('subscribes once per enabled threshold on registration', () => {
    registerCommand('bilgepump', undefined, undefined, false, [threshold()]);

    expect(host.subscriptions).to.have.lengthOf(1);
    expect(host.live()).to.have.lengthOf(1);
  });

  it('does not subscribe for a disabled threshold', () => {
    registerCommand('bilgepump', undefined, undefined, false, [
      threshold({ enabled: false }),
    ]);

    expect(host.subscriptions).to.be.empty;
  });

  // The regression this suite exists for: updateCommand used to rebuild the
  // monitor key ad-hoc as `${commandName}_${watchPath}`, which never matched
  // the key setup had stored, so the old monitor was never found and the
  // subscription outlived the threshold it belonged to.
  it('unsubscribes the old monitor when a threshold comparison value changes', () => {
    registerCommand('bilgepump', undefined, undefined, false, [
      threshold({ value: 2 }),
    ]);

    updateCommand('bilgepump', undefined, undefined, undefined, [
      threshold({ value: 5 }),
    ]);

    expect(host.subscriptions, 'a new monitor is subscribed').to.have.lengthOf(
      2
    );
    expect(host.live(), 'the superseded monitor is torn down').to.have.lengthOf(
      1
    );
    expect(host.subscriptions[0].active).to.be.false;
  });

  it('unsubscribes the old monitor when only the operator changes', () => {
    registerCommand('bilgepump', undefined, undefined, false, [
      threshold({ operator: 'lt' }),
    ]);

    updateCommand('bilgepump', undefined, undefined, undefined, [
      threshold({ operator: 'gt' }),
    ]);

    expect(host.live()).to.have.lengthOf(1);
    expect(host.subscriptions[0].active).to.be.false;
  });

  it('unsubscribes when a threshold is removed entirely', () => {
    registerCommand('bilgepump', undefined, undefined, false, [threshold()]);

    updateCommand('bilgepump', undefined, undefined, undefined, []);

    expect(host.live()).to.be.empty;
  });

  it('leaves no orphaned subscriptions across repeated edits', () => {
    registerCommand('bilgepump', undefined, undefined, false, [
      threshold({ value: 1 }),
    ]);

    for (const value of [2, 3, 4, 5]) {
      updateCommand('bilgepump', undefined, undefined, undefined, [
        threshold({ value }),
      ]);
    }

    expect(host.subscriptions).to.have.lengthOf(5);
    expect(host.live()).to.have.lengthOf(1);
  });

  // updateCommand replaces the whole monitor set rather than diffing it: every
  // old monitor is torn down and re-subscribed, including ones whose threshold
  // did not change. That is fine for correctness as long as nothing is left
  // behind, which is what this pins — one live monitor per threshold, and every
  // superseded subscription closed.
  it('replaces every monitor on edit, leaving one live per threshold', () => {
    const depth = threshold({ watchPath: 'environment.depth.belowKeel' });
    const wind = threshold({
      watchPath: 'environment.wind.speedApparent',
      operator: 'gt',
      value: 15,
    });

    registerCommand('bilgepump', undefined, undefined, false, [depth, wind]);
    const original = [...host.subscriptions];
    expect(original).to.have.lengthOf(2);

    // Edit only the wind threshold; the depth monitor is still recycled.
    updateCommand('bilgepump', undefined, undefined, undefined, [
      depth,
      { ...wind, value: 20 },
    ]);

    expect(
      original.every(s => !s.active),
      'every pre-edit subscription is torn down'
    ).to.be.true;

    const live = host.live();
    expect(live).to.have.lengthOf(2);
    expect(live.map(s => s.path).sort()).to.deep.equal([
      'environment.depth.belowKeel',
      'environment.wind.speedApparent',
    ]);
  });
});
