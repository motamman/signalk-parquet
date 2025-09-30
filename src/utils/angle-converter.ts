/**
 * Angle conversion utilities for radian/degree conversions
 * Used for angular threshold values and display
 */

/**
 * Convert degrees to radians
 */
export function degreesToRadians(degrees: number): number {
  return degrees * (Math.PI / 180);
}

/**
 * Convert radians to degrees
 */
export function radiansToDegrees(radians: number): number {
  return radians * (180 / Math.PI);
}

/**
 * Normalize angle to 0-360 degree range
 */
export function normalizeDegrees(degrees: number): number {
  let normalized = degrees % 360;
  if (normalized < 0) {
    normalized += 360;
  }
  return normalized;
}

/**
 * Normalize angle to 0-2π radian range
 */
export function normalizeRadians(radians: number): number {
  let normalized = radians % (2 * Math.PI);
  if (normalized < 0) {
    normalized += 2 * Math.PI;
  }
  return normalized;
}

/**
 * Calculate the shortest angular difference between two angles in degrees
 * Result is in range [-180, 180]
 * Positive result means angle2 is clockwise from angle1
 */
export function angularDifferenceDegrees(
  angle1: number,
  angle2: number
): number {
  let diff = angle2 - angle1;

  // Normalize to [-180, 180]
  while (diff > 180) diff -= 360;
  while (diff < -180) diff += 360;

  return diff;
}

/**
 * Calculate the shortest angular difference between two angles in radians
 * Result is in range [-π, π]
 * Positive result means angle2 is clockwise from angle1
 */
export function angularDifferenceRadians(
  angle1: number,
  angle2: number
): number {
  let diff = angle2 - angle1;

  // Normalize to [-π, π]
  while (diff > Math.PI) diff -= 2 * Math.PI;
  while (diff < -Math.PI) diff += 2 * Math.PI;

  return diff;
}

/**
 * Check if an angle is within a range, accounting for wrap-around
 * All values in degrees
 * @param angle - The angle to check
 * @param center - The center of the range
 * @param tolerance - The tolerance in degrees (range is center ± tolerance)
 */
export function isAngleInRangeDegrees(
  angle: number,
  center: number,
  tolerance: number
): boolean {
  const diff = Math.abs(angularDifferenceDegrees(center, angle));
  return diff <= tolerance;
}

/**
 * Check if an angle is within a range, accounting for wrap-around
 * All values in radians
 * @param angle - The angle to check
 * @param center - The center of the range
 * @param tolerance - The tolerance in radians (range is center ± tolerance)
 */
export function isAngleInRangeRadians(
  angle: number,
  center: number,
  tolerance: number
): boolean {
  const diff = Math.abs(angularDifferenceRadians(center, angle));
  return diff <= tolerance;
}
