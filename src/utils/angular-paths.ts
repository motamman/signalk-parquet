/**
 * Angular Path Detection and Helpers
 *
 * Detects angular paths (heading, COG, wind direction, etc.) via server metadata
 * and provides helpers for vector averaging in DuckDB SQL.
 *
 * Angular data requires vector decomposition for correct averaging:
 * AVG(10°, 350°) should = 0°, not 180°.
 * Solution: ATAN2(AVG(SIN(value)), AVG(COS(value)))
 */

import { ServerAPI } from '@signalk/server-api';

/**
 * Detect if a path contains angular data by checking the SignalK schema.
 * getMetadata() (from @signalk/signalk-schema) matches against schema
 * regexes like /vessels/<context>/<path>. Any valid context prefix works
 * since the schema uses wildcards for the context segment.
 */
export function isAngularPath(
  path: string,
  app: ServerAPI,
  context: string
): boolean {
  try {
    const metadata = (app as any).getMetadata?.(`${context}.${path}`);
    return metadata?.units === 'rad';
  } catch {
    return false;
  }
}

/**
 * Paths where angle should be weighted by an associated magnitude.
 * These are well-defined SignalK path pairs and unlikely to change,
 * so a static map is appropriate here.
 */
export const WEIGHTED_ANGULAR_PATHS: Map<string, string> = new Map([
  ['environment.wind.directionTrue', 'environment.wind.speedTrue'],
  ['environment.wind.directionMagnetic', 'environment.wind.speedOverGround'],
  ['environment.wind.angleApparent', 'environment.wind.speedApparent'],
  ['environment.current.setTrue', 'environment.current.drift'],
]);

export function getWeightPath(anglePath: string): string | undefined {
  return WEIGHTED_ANGULAR_PATHS.get(anglePath);
}
