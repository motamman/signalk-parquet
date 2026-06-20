/**
 * Unit tests for SignalK path data-type detection.
 *
 * detectPathType resolves a type in this order: position-path shortcut,
 * server metadata (units, enum, type, description), live-value sampling via
 * getSelfPath, then a string fallback. detectPathTypeFromSample infers the
 * type purely from a sampled value plus path-name heuristics. A minimal fake
 * server pins the exact lookup and logging behaviour.
 */
import { expect } from 'chai';
import { ServerAPI } from '@signalk/server-api';
import {
  detectPathType,
  detectPathTypeFromSample,
} from '../../../src/utils/type-detector';

interface AppOverrides {
  getMetadata?: (path: string) => unknown;
  getSelfPath?: (path: string) => unknown;
}

interface StubApp {
  app: ServerAPI;
  debugMessages: string[];
  errorMessages: string[];
}

/**
 * Minimal stand-in for the SignalK server: only the four members the
 * detector touches. Debug and error calls are recorded so tests can pin
 * the logging side effects.
 */
function makeApp(overrides: AppOverrides = {}): StubApp {
  const debugMessages: string[] = [];
  const errorMessages: string[] = [];
  const app = {
    getMetadata: overrides.getMetadata ?? (() => undefined),
    getSelfPath: overrides.getSelfPath ?? (() => undefined),
    debug: (msg: string) => {
      debugMessages.push(msg);
    },
    error: (msg: string) => {
      errorMessages.push(msg);
    },
  } as unknown as ServerAPI;
  return { app, debugMessages, errorMessages };
}

describe('detectPathType', () => {
  describe('position paths', () => {
    it('detects navigation.position without consulting metadata', async () => {
      const result = await detectPathType('navigation.position');
      expect(result).to.deep.equal({
        dataType: 'position',
        description: 'Geographic position (latitude/longitude)',
      });
    });

    it('detects antennaPosition through the hardcoded list', async () => {
      // 'navigation.gnss.antennaPosition' ends with capital-P 'Position',
      // so the case-sensitive endsWith('position') check misses it; the
      // POSITION_PATHS list is what catches it.
      const result = await detectPathType('navigation.gnss.antennaPosition');
      expect(result.dataType).to.equal('position');
    });

    it('detects any path ending in lowercase "position"', async () => {
      const result = await detectPathType('sensors.ais.target.position');
      expect(result.dataType).to.equal('position');
    });

    it('wins over a metadata lookup that would throw', async () => {
      const { app, errorMessages } = makeApp({
        getMetadata: () => {
          throw new Error('boom');
        },
      });
      const result = await detectPathType('navigation.position', app);
      expect(result.dataType).to.equal('position');
      expect(errorMessages).to.deep.equal([]);
    });
  });

  describe('without a server connection', () => {
    it('returns unknown when no app is provided', async () => {
      const result = await detectPathType('navigation.speedOverGround');
      expect(result).to.deep.equal({ dataType: 'unknown' });
    });
  });

  describe('metadata lookup', () => {
    it('returns unknown and logs when the server has no metadata', async () => {
      const { app, debugMessages } = makeApp({ getMetadata: () => undefined });
      const result = await detectPathType('navigation.speedOverGround', app);
      expect(result).to.deep.equal({ dataType: 'unknown' });
      expect(debugMessages).to.have.length(1);
      expect(debugMessages[0]).to.include('navigation.speedOverGround');
    });

    it('pins that a throwing getMetadata is swallowed into unknown', async () => {
      const { app, errorMessages } = makeApp({
        getMetadata: () => {
          throw new Error('schema offline');
        },
      });
      const result = await detectPathType('navigation.headingTrue', app);
      expect(result).to.deep.equal({ dataType: 'unknown' });
      expect(errorMessages).to.have.length(1);
      expect(errorMessages[0]).to.include('schema offline');
    });
  });

  describe('unit-based detection', () => {
    it('classifies radians as angular', async () => {
      // Mimics the schema metadata for navigation.headingMagnetic:
      // { units: 'rad', description: 'Current magnetic heading ...' }
      const metadata = {
        units: 'rad',
        description: 'Current magnetic heading of the vessel',
      };
      const { app } = makeApp({ getMetadata: () => metadata });
      const result = await detectPathType('navigation.headingMagnetic', app);
      expect(result.dataType).to.equal('angular');
      expect(result.unit).to.equal('rad');
      expect(result.description).to.equal(metadata.description);
      expect(result.rawMetadata).to.equal(metadata);
    });

    it('classifies known numeric units as numeric', async () => {
      // Mimics navigation.speedOverGround: { units: 'm/s', ... }
      const metadata = { units: 'm/s', description: 'Speed over ground' };
      const { app } = makeApp({ getMetadata: () => metadata });
      const result = await detectPathType('navigation.speedOverGround', app);
      expect(result.dataType).to.equal('numeric');
      expect(result.unit).to.equal('m/s');
      expect(result.description).to.equal('Speed over ground');
      expect(result.rawMetadata).to.equal(metadata);
    });

    it('treats "deg" as numeric, not angular', async () => {
      // SignalK serves angles in radians; 'deg' only appears on
      // non-angular paths and sits on the numeric allow-list.
      const { app } = makeApp({ getMetadata: () => ({ units: 'deg' }) });
      const result = await detectPathType('some.custom.path', app);
      expect(result.dataType).to.equal('numeric');
      expect(result.unit).to.equal('deg');
    });

    it('defaults unknown units to numeric and logs', async () => {
      // Mimics a third-party sensor publishing Celsius, a unit SignalK
      // itself never uses (temperatures are Kelvin).
      const { app, debugMessages } = makeApp({
        getMetadata: () => ({ units: 'C' }),
      });
      const result = await detectPathType('environment.inside.custom', app);
      expect(result.dataType).to.equal('numeric');
      expect(result.unit).to.equal('C');
      expect(debugMessages).to.have.length(1);
      expect(debugMessages[0]).to.include('unknown unit');
    });

    it('prefers units over enum values when both are present', async () => {
      const metadata = { units: 'm', enum: ['short', 'long'] };
      const { app } = makeApp({ getMetadata: () => metadata });
      const result = await detectPathType('design.length', app);
      expect(result.dataType).to.equal('numeric');
      expect(result.enumValues).to.equal(undefined);
    });
  });

  describe('enum detection', () => {
    it('detects enums via the "enum" key', async () => {
      // Mimics navigation.state, whose schema metadata enumerates the
      // allowed vessel states.
      const metadata = {
        enum: ['anchored', 'sailing', 'motoring'],
        description: 'Current navigational state',
      };
      const { app } = makeApp({ getMetadata: () => metadata });
      const result = await detectPathType('navigation.state', app);
      expect(result.dataType).to.equal('enum');
      expect(result.enumValues).to.deep.equal([
        'anchored',
        'sailing',
        'motoring',
      ]);
      expect(result.description).to.equal('Current navigational state');
      expect(result.rawMetadata).to.equal(metadata);
    });

    it('detects enums via the alternate "values" key', async () => {
      const { app } = makeApp({
        getMetadata: () => ({ values: ['on', 'off'] }),
      });
      const result = await detectPathType('electrical.switches.mode', app);
      expect(result.dataType).to.equal('enum');
      expect(result.enumValues).to.deep.equal(['on', 'off']);
    });

    it('falls through to string for an empty enum list', async () => {
      const { app } = makeApp({ getMetadata: () => ({ enum: [] }) });
      const result = await detectPathType('navigation.state', app);
      expect(result.dataType).to.equal('string');
    });
  });

  describe('boolean detection from metadata', () => {
    it('detects an explicit boolean type field', async () => {
      // Mimics digital-switching style metadata such as
      // electrical.switches.<id>.state: { type: 'boolean', ... }
      const { app } = makeApp({
        getMetadata: () => ({ type: 'boolean', description: 'Switch state' }),
      });
      const result = await detectPathType('electrical.switches.nav.state', app);
      expect(result.dataType).to.equal('boolean');
      expect(result.description).to.equal('Switch state');
    });

    it('detects "Boolean" in the description, case-insensitively', async () => {
      const { app } = makeApp({
        getMetadata: () => ({
          description: 'Boolean flag set while the anchor alarm is armed',
        }),
      });
      const result = await detectPathType('notifications.anchorAlarm', app);
      expect(result.dataType).to.equal('boolean');
    });
  });

  describe('boolean detection from a sampled value', () => {
    it('detects a raw boolean sample', async () => {
      const { app } = makeApp({
        getMetadata: () => ({}),
        getSelfPath: () => true,
      });
      const result = await detectPathType('steering.autopilot.engaged', app);
      expect(result.dataType).to.equal('boolean');
      expect(result.description).to.equal(
        'Boolean value (detected from sample)'
      );
    });

    it('unwraps SignalK value objects when sampling', async () => {
      // getSelfPath commonly returns { value, timestamp, $source } rather
      // than the bare value.
      const { app } = makeApp({
        getMetadata: () => ({}),
        getSelfPath: () => ({
          value: false,
          timestamp: '2025-11-02T10:00:00Z',
        }),
      });
      const result = await detectPathType('steering.autopilot.engaged', app);
      expect(result.dataType).to.equal('boolean');
    });

    it('keeps the metadata description over the sample fallback', async () => {
      const { app } = makeApp({
        getMetadata: () => ({ description: 'Anchor alarm armed flag' }),
        getSelfPath: () => true,
      });
      const result = await detectPathType('navigation.anchor.armed', app);
      expect(result.dataType).to.equal('boolean');
      expect(result.description).to.equal('Anchor alarm armed flag');
    });

    it('swallows sampling errors and falls back to string', async () => {
      const { app } = makeApp({
        getMetadata: () => ({}),
        getSelfPath: () => {
          throw new Error('no data');
        },
      });
      const result = await detectPathType('some.flaky.path', app);
      expect(result.dataType).to.equal('string');
    });

    it('pins that a numeric sample still yields string', async () => {
      // Only booleans are sniffed from live values; a unitless metadata
      // entry with a numeric reading is reported as string.
      const { app } = makeApp({
        getMetadata: () => ({}),
        getSelfPath: () => 42,
      });
      const result = await detectPathType('sensors.custom.count', app);
      expect(result.dataType).to.equal('string');
    });
  });

  describe('string fallback', () => {
    it('defaults to string and keeps description and raw metadata', async () => {
      // Mimics a free-text path such as 'name':
      // { description: 'The name of the vessel' }
      const metadata = { description: 'The name of the vessel' };
      const { app, debugMessages } = makeApp({
        getMetadata: () => metadata,
        getSelfPath: () => 'Aurora',
      });
      const result = await detectPathType('name', app);
      expect(result.dataType).to.equal('string');
      expect(result.description).to.equal('The name of the vessel');
      expect(result.rawMetadata).to.equal(metadata);
      expect(debugMessages).to.have.length(1);
      expect(debugMessages[0]).to.include('no clear type');
    });
  });
});

describe('detectPathTypeFromSample', () => {
  it('detects a position object sample', async () => {
    // Shape of a SignalK position value: { latitude, longitude }.
    const result = await detectPathTypeFromSample('navigation.position', {
      latitude: 60.1699,
      longitude: 24.9384,
    });
    expect(result.dataType).to.equal('position');
  });

  it('pins that an object without both coordinates is unknown', async () => {
    // An attitude object has angles but no latitude/longitude pair, so it
    // falls through every branch to unknown rather than string.
    const result = await detectPathTypeFromSample('navigation.attitude', {
      roll: 0.02,
      pitch: 0.01,
    });
    expect(result.dataType).to.equal('unknown');
  });

  it('detects boolean samples', async () => {
    const yes = await detectPathTypeFromSample('autopilot.engaged', true);
    const no = await detectPathTypeFromSample('autopilot.engaged', false);
    expect(yes.dataType).to.equal('boolean');
    expect(no.dataType).to.equal('boolean');
  });

  // One real-world path per heuristic; each matches exactly one of the
  // /heading/, /course/, /bearing/, /angle/, /direction/ patterns.
  const angularPaths = [
    'navigation.headingTrue',
    'navigation.courseOverGroundTrue',
    'steering.autopilot.target.bearingMagnetic',
    'environment.wind.angleApparent',
    'environment.wind.directionTrue',
  ];

  angularPaths.forEach(skPath => {
    it(`detects angular for a numeric sample on ${skPath}`, async () => {
      const result = await detectPathTypeFromSample(skPath, 1.5708);
      expect(result.dataType).to.equal('angular');
    });
  });

  it('matches the angular patterns case-insensitively', async () => {
    const result = await detectPathTypeFromSample('sensors.imu.HEADING', 0.5);
    expect(result.dataType).to.equal('angular');
  });

  it('detects plain numeric samples on non-angular paths', async () => {
    const result = await detectPathTypeFromSample(
      'navigation.speedOverGround',
      3.6
    );
    expect(result.dataType).to.equal('numeric');
  });

  it('treats zero and negative numbers as numeric', async () => {
    const zero = await detectPathTypeFromSample(
      'environment.depth.belowKeel',
      0
    );
    const negative = await detectPathTypeFromSample(
      'environment.water.temperature',
      -1.5
    );
    expect(zero.dataType).to.equal('numeric');
    expect(negative.dataType).to.equal('numeric');
  });

  it('detects string samples', async () => {
    const result = await detectPathTypeFromSample('name', 'Aurora');
    expect(result.dataType).to.equal('string');
  });

  it('returns unknown for null and undefined samples', async () => {
    const fromNull = await detectPathTypeFromSample('mystery.path', null);
    const fromUndefined = await detectPathTypeFromSample(
      'mystery.path',
      undefined
    );
    expect(fromNull.dataType).to.equal('unknown');
    expect(fromUndefined.dataType).to.equal('unknown');
  });

  it('logs a debug message for undetectable samples when an app is given', async () => {
    const debugMessages: string[] = [];
    const app = {
      debug: (msg: string) => {
        debugMessages.push(msg);
      },
    } as unknown as ServerAPI;
    await detectPathTypeFromSample('mystery.path', null, app);
    expect(debugMessages).to.have.length(1);
    expect(debugMessages[0]).to.include('mystery.path');
  });
});
