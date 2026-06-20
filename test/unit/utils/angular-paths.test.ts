/**
 * Unit tests for the angular path helpers: the metadata-driven isAngularPath
 * check that decides whether a path needs vector averaging in SQL, and the
 * static angle-to-weight map used for magnitude-weighted averages (for
 * example wind direction weighted by wind speed).
 */
import { expect } from 'chai';
import { ServerAPI } from '@signalk/server-api';
import {
  isAngularPath,
  getWeightPath,
  WEIGHTED_ANGULAR_PATHS,
} from '../../../src/utils/angular-paths';

interface StubApp {
  app: ServerAPI;
  receivedPaths: string[];
}

/**
 * Fake server exposing only getMetadata. The paths it receives are recorded
 * so tests can pin the exact lookup key.
 */
function makeApp(getMetadata: (fullPath: string) => unknown): StubApp {
  const receivedPaths: string[] = [];
  const app = {
    getMetadata: (fullPath: string) => {
      receivedPaths.push(fullPath);
      return getMetadata(fullPath);
    },
  } as unknown as ServerAPI;
  return { app, receivedPaths };
}

describe('isAngularPath', () => {
  it('returns true when the schema reports radian units', () => {
    // Mimics navigation.headingTrue: { units: 'rad', description: ... }
    const { app, receivedPaths } = makeApp(() => ({
      units: 'rad',
      description: 'The current true north heading',
    }));
    const result = isAngularPath('navigation.headingTrue', app, 'vessels.self');
    expect(result).to.equal(true);
    // The lookup key is the dot-joined context and path; the schema regexes
    // accept any context prefix.
    expect(receivedPaths).to.deep.equal([
      'vessels.self.navigation.headingTrue',
    ]);
  });

  it('returns false for non-angular units', () => {
    // Mimics navigation.speedOverGround: { units: 'm/s' }
    const { app } = makeApp(() => ({ units: 'm/s' }));
    const result = isAngularPath(
      'navigation.speedOverGround',
      app,
      'vessels.self'
    );
    expect(result).to.equal(false);
  });

  it('returns false when the metadata has no units', () => {
    const { app } = makeApp(() => ({ description: 'no units here' }));
    expect(isAngularPath('navigation.state', app, 'vessels.self')).to.equal(
      false
    );
  });

  it('returns false when no metadata exists for the path', () => {
    const { app } = makeApp(() => undefined);
    expect(isAngularPath('custom.unknown.path', app, 'vessels.self')).to.equal(
      false
    );
  });

  it('returns false when the server lacks getMetadata', () => {
    // Older servers without the metadata API: the optional call resolves to
    // undefined instead of throwing.
    const app = {} as unknown as ServerAPI;
    expect(
      isAngularPath('navigation.headingTrue', app, 'vessels.self')
    ).to.equal(false);
  });

  it('pins that a throwing getMetadata is swallowed into false', () => {
    const { app } = makeApp(() => {
      throw new Error('schema unavailable');
    });
    expect(
      isAngularPath('navigation.headingTrue', app, 'vessels.self')
    ).to.equal(false);
  });

  it('builds the lookup key from non-self vessel contexts too', () => {
    const { app, receivedPaths } = makeApp(() => ({ units: 'rad' }));
    const result = isAngularPath(
      'navigation.courseOverGroundTrue',
      app,
      'vessels.urn:mrn:imo:mmsi:230099999'
    );
    expect(result).to.equal(true);
    expect(receivedPaths).to.deep.equal([
      'vessels.urn:mrn:imo:mmsi:230099999.navigation.courseOverGroundTrue',
    ]);
  });
});

describe('getWeightPath', () => {
  // The well-known SignalK angle/magnitude pairs from WEIGHTED_ANGULAR_PATHS.
  const knownPairs: Array<[string, string]> = [
    ['environment.wind.directionTrue', 'environment.wind.speedTrue'],
    ['environment.wind.directionMagnetic', 'environment.wind.speedOverGround'],
    ['environment.wind.angleApparent', 'environment.wind.speedApparent'],
    ['environment.current.setTrue', 'environment.current.drift'],
  ];

  knownPairs.forEach(([anglePath, weightPath]) => {
    it(`maps ${anglePath} to ${weightPath}`, () => {
      expect(getWeightPath(anglePath)).to.equal(weightPath);
    });
  });

  it('returns undefined for angular paths without a weight', () => {
    // Heading is angular but has no associated magnitude to weight by.
    expect(getWeightPath('navigation.headingTrue')).to.equal(undefined);
  });

  it('returns undefined when a weight path is used as the key', () => {
    // The map is one-directional: speed paths are values, not keys.
    expect(getWeightPath('environment.wind.speedTrue')).to.equal(undefined);
  });

  it('returns undefined for the empty string', () => {
    expect(getWeightPath('')).to.equal(undefined);
  });

  it('covers exactly the known pairs', () => {
    expect(WEIGHTED_ANGULAR_PATHS.size).to.equal(knownPairs.length);
  });
});
