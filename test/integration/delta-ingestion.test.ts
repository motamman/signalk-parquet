/**
 * Integration of the delta ingestion path with a simulated Signal K host.
 *
 * Drives the real subscription wiring (subscribeToCommandPaths /
 * updateDataSubscriptions from data-handler) against a fake streambundle and
 * subscription manager, with a real SQLite buffer underneath. It verifies the
 * use cases an operator cares about:
 *   - an always-enabled path is subscribed and its deltas are buffered;
 *   - a regimen-gated path is NOT collected until its command turns on;
 *   - toggling the command via the command subscription starts/stops
 *     collection of the gated path;
 *   - context and source filters drop deltas that do not match.
 */
import { expect } from 'chai';
import * as path from 'path';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { LRUCache } from '../../src/utils/lru-cache';
import {
  subscribeToCommandPaths,
  updateDataSubscriptions,
} from '../../src/data-handler';
import type {
  DataRecord,
  PathConfig,
  PluginConfig,
  PluginState,
} from '../../src/types';
import {
  createFakeSignalK,
  makeTestConfig,
  FakeSignalK,
} from './helpers/fake-signalk';

// PathConfig.path/context are branded string types; tests author plain-string
// fixtures and cast them through this helper.
function asPaths(entries: Array<Record<string, unknown>>): PathConfig[] {
  return entries as unknown as PathConfig[];
}

const NOW = '2024-06-01T10:00:00.000Z';

// The suite freezes the wall clock (see beforeEach). handleStreamData stamps
// received_timestamp with new Date() at insert time and the buffer's
// date-range reads filter on that column, so without a frozen clock a UTC
// midnight tick falling between the insert and the read-back would put them on
// different days. Freezing pins both to FROZEN_NOW, making the date reads
// deterministic.
const RealDate = Date;
const FROZEN_NOW = new RealDate('2024-06-01T12:00:00.000Z');

// The UTC day the frozen clock falls on — used to read records back.
function frozenDay(): Date {
  return new RealDate(FROZEN_NOW);
}

/** Install a Date whose zero-arg form returns the frozen instant. */
function freezeClock(): void {
  class FrozenDate extends RealDate {
    constructor(value?: number | string | Date) {
      super(value === undefined ? FROZEN_NOW.getTime() : value);
    }
    static now(): number {
      return FROZEN_NOW.getTime();
    }
  }
  globalThis.Date = FrozenDate as DateConstructor;
}

function unfreezeClock(): void {
  globalThis.Date = RealDate;
}

/** Minimal NormalizedDelta as produced by the streambundle. */
function dataDelta(
  signalkPath: string,
  value: unknown,
  opts: { context?: string; source?: string } = {}
) {
  return {
    context: opts.context ?? 'vessels.self',
    path: signalkPath,
    value,
    timestamp: NOW,
    $source: opts.source ?? 'test.source',
    source: { label: opts.source ?? 'test.source', type: 'test' },
  };
}

/** A command delta as delivered by subscriptionmanager. */
function commandDelta(commandPath: string, value: boolean) {
  return {
    context: 'vessels.self',
    updates: [
      {
        $source: 'test.command',
        timestamp: NOW,
        values: [{ path: commandPath, value }],
      },
    ],
  };
}

function makeState(buffer: SQLiteBuffer, config: PluginConfig): PluginState {
  return {
    unsubscribes: [],
    streamSubscriptions: [],
    dataBuffers: new LRUCache<string, DataRecord[]>(1000),
    activeRegimens: new Set<string>(),
    subscribedPaths: new Set<string>(),
    parquetWriter: undefined,
    currentConfig: config,
    getDataDirPath: () => config.outputDirectory,
    commandState: {
      registeredCommands: new Map(),
      putHandlers: new Map(),
    },
    sqliteBuffer: buffer,
  };
}

describe('delta ingestion and regimen control', () => {
  let host: FakeSignalK;
  let buffer: SQLiteBuffer;
  let config: PluginConfig;
  let state: PluginState;

  const PATHS = asPaths([
    { path: 'commands.captureWeather', enabled: true, context: 'vessels.self' },
    {
      path: 'environment.wind.speedApparent',
      enabled: false,
      regimen: 'captureWeather',
      context: 'vessels.self',
    },
    {
      path: 'navigation.speedOverGround',
      enabled: true,
      context: 'vessels.self',
    },
  ]);

  beforeEach(() => {
    freezeClock();
    host = createFakeSignalK();
    buffer = new SQLiteBuffer({ dbPath: path.join(host.dataDir, 'buffer.db') });
    config = makeTestConfig(host.dataDir);
    state = makeState(buffer, config);
  });

  afterEach(async () => {
    // Guard each resource: if beforeEach failed partway, these may be unset,
    // and an unguarded teardown throw would mask the real setup error.
    unfreezeClock();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  it('subscribes only to enabled paths, not regimen-gated ones', () => {
    updateDataSubscriptions(PATHS, state, config, host.app);

    expect(state.subscribedPaths.has('navigation.speedOverGround')).to.equal(
      true
    );
    expect(
      state.subscribedPaths.has('environment.wind.speedApparent')
    ).to.equal(false);
  });

  it('buffers a delta arriving on an enabled path', () => {
    updateDataSubscriptions(PATHS, state, config, host.app);

    host.emitBus(
      'navigation.speedOverGround',
      dataDelta('navigation.speedOverGround', 5.2)
    );

    expect(buffer.getKnownPaths().has('navigation.speedOverGround')).to.equal(
      true
    );
    expect(buffer.getStats().pendingRecords).to.equal(1);
  });

  it('does not collect a regimen-gated path until its command turns on', () => {
    updateDataSubscriptions(PATHS, state, config, host.app);

    // Wind is gated and inactive: even if a delta somehow arrived, there is
    // no subscription, so nothing is buffered.
    host.emitBus(
      'environment.wind.speedApparent',
      dataDelta('environment.wind.speedApparent', 12)
    );
    expect(buffer.getStats().pendingRecords).to.equal(0);

    // Turn the regimen on via the command subscription.
    host.emitCommand(commandDelta('commands.captureWeather', true));

    expect(state.activeRegimens.has('captureWeather')).to.equal(true);
    expect(
      state.subscribedPaths.has('environment.wind.speedApparent')
    ).to.equal(true);

    // Now the gated path collects. Assert on the wind path specifically: the
    // command toggle itself also buffers a record (the plugin stores command
    // state as data), so the global pending count is not 1.
    host.emitBus(
      'environment.wind.speedApparent',
      dataDelta('environment.wind.speedApparent', 13)
    );
    expect(
      buffer.getKnownPaths().has('environment.wind.speedApparent')
    ).to.equal(true);
    const windRecords = buffer.getRecordsForPathAndDate(
      'vessels.self',
      'environment.wind.speedApparent',
      frozenDay()
    );
    expect(windRecords).to.have.lengthOf(1);
    expect(Number(windRecords[0].value)).to.equal(13);
  });

  it('stops collecting the gated path when the command turns off', () => {
    updateDataSubscriptions(PATHS, state, config, host.app);
    host.emitCommand(commandDelta('commands.captureWeather', true));
    expect(
      state.subscribedPaths.has('environment.wind.speedApparent')
    ).to.equal(true);

    host.emitCommand(commandDelta('commands.captureWeather', false));

    expect(state.activeRegimens.has('captureWeather')).to.equal(false);
    expect(
      state.subscribedPaths.has('environment.wind.speedApparent')
    ).to.equal(false);
  });

  it('persists the values and source of buffered records', () => {
    updateDataSubscriptions(PATHS, state, config, host.app);

    host.emitBus(
      'navigation.speedOverGround',
      dataDelta('navigation.speedOverGround', 7.5, { source: 'nmea.gps' })
    );

    const records = buffer.getRecordsForPathAndDate(
      'vessels.self',
      'navigation.speedOverGround',
      frozenDay()
    );
    expect(records).to.have.lengthOf(1);
    expect(Number(records[0].value)).to.equal(7.5);
    expect(records[0].source_label).to.equal('nmea.gps');
  });

  it('buffers object-valued deltas with flattened components', () => {
    const positionPaths = asPaths([
      { path: 'navigation.position', enabled: true, context: 'vessels.self' },
    ]);
    updateDataSubscriptions(positionPaths, state, config, host.app);

    host.emitBus(
      'navigation.position',
      dataDelta('navigation.position', {
        latitude: 47.5,
        longitude: 8.7,
      })
    );

    const records = buffer.getRecordsForPathAndDate(
      'vessels.self',
      'navigation.position',
      frozenDay()
    );
    expect(records).to.have.lengthOf(1);
    expect(Number(records[0].value_latitude)).to.be.closeTo(47.5, 1e-9);
    expect(Number(records[0].value_longitude)).to.be.closeTo(8.7, 1e-9);
  });

  it('drops deltas from a non-matching source when a source filter is set', () => {
    const filtered = asPaths([
      {
        path: 'navigation.speedOverGround',
        enabled: true,
        context: 'vessels.self',
        source: 'nmea.gps',
      },
    ]);
    updateDataSubscriptions(filtered, state, config, host.app);

    // Wrong source: filtered out before buffering.
    host.emitBus(
      'navigation.speedOverGround',
      dataDelta('navigation.speedOverGround', 5, { source: 'other.source' })
    );
    expect(buffer.getStats().pendingRecords).to.equal(0);

    // Matching source: buffered.
    host.emitBus(
      'navigation.speedOverGround',
      dataDelta('navigation.speedOverGround', 6, { source: 'nmea.gps' })
    );
    expect(buffer.getStats().pendingRecords).to.equal(1);
  });

  it('drops deltas whose context is a different vessel', () => {
    updateDataSubscriptions(PATHS, state, config, host.app);

    host.emitBus(
      'navigation.speedOverGround',
      dataDelta('navigation.speedOverGround', 5, {
        context: 'vessels.urn:mrn:imo:mmsi:999999999',
      })
    );

    expect(buffer.getStats().pendingRecords).to.equal(0);
  });

  it('re-evaluates subscriptions only when the regimen set changes', () => {
    updateDataSubscriptions(PATHS, state, config, host.app);

    host.emitCommand(commandDelta('commands.captureWeather', true));
    const afterOn = state.subscribedPaths.has('environment.wind.speedApparent');

    // Same command value again — still active, still subscribed.
    host.emitCommand(commandDelta('commands.captureWeather', true));
    const afterRepeat = state.subscribedPaths.has(
      'environment.wind.speedApparent'
    );

    expect(afterOn).to.equal(true);
    expect(afterRepeat).to.equal(true);
    expect(state.activeRegimens.has('captureWeather')).to.equal(true);
  });
});
