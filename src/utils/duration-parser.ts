import { Duration } from '@js-joda/core';

/**
 * Parse duration in multiple formats, return milliseconds.
 *
 * Formats (detection order):
 * 1. ISO 8601: PT1H, PT30M, P1D, PT1H30M
 * 2. Integer seconds: 3600, 60
 * 3. Shorthand: 1h, 30m, 5s, 2d (backward compatible)
 */
export function parseDurationToMillis(duration: string): number {
  const trimmed = duration.trim();

  // ISO 8601 (starts with P)
  if (trimmed.toUpperCase().startsWith('P')) {
    return Duration.parse(trimmed).toMillis();
  }

  // Integer seconds (pure digits)
  if (/^\d+$/.test(trimmed)) {
    return parseInt(trimmed, 10) * 1000;
  }

  // Shorthand (1h, 30m, 5s, 2d)
  const match = trimmed.match(/^(\d+)([smhd])$/i);
  if (match) {
    const value = parseInt(match[1]);
    const unit = match[2].toLowerCase();
    switch (unit) {
      case 's':
        return value * 1000;
      case 'm':
        return value * 60 * 1000;
      case 'h':
        return value * 60 * 60 * 1000;
      case 'd':
        return value * 24 * 60 * 60 * 1000;
    }
  }

  throw new Error(`Invalid duration: ${duration}. Use PT1H, 3600, or 1h`);
}

export class InvalidResolutionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidResolutionError';
  }
}

const RESOLUTION_UNIT_MILLIS: Record<string, number> = {
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
};

const RESOLUTION_TIME_EXPRESSION = /^(\d+)([smhd])$/i;

/**
 * Parse the History API `resolution` query parameter into milliseconds.
 *
 * Per the SignalK History API spec, resolution is a number of seconds or a
 * time expression of the form `<integer><unit>` where unit is s|m|h|d.
 */
export function parseResolutionToMillis(resolution: string | number): number {
  const reject = (): never => {
    throw new InvalidResolutionError(
      `resolution parameter must be a positive number of seconds or a time expression like '1s', '1m', '1h', '1d'`
    );
  };
  const toMillisOrReject = (seconds: number): number => {
    const millis = seconds * 1000;
    if (!Number.isFinite(millis) || millis <= 0) reject();
    return millis;
  };

  if (typeof resolution === 'number') {
    if (!Number.isFinite(resolution) || resolution <= 0) reject();
    return toMillisOrReject(resolution);
  }

  // Express can hand us an array (?resolution=1s&resolution=2s) typed as
  // string by the route layer's cast; reject anything that isn't actually
  // a string before calling .trim().
  if (typeof resolution !== 'string') reject();

  const trimmed = resolution.trim();
  const match = RESOLUTION_TIME_EXPRESSION.exec(trimmed);
  if (match) {
    const value = Number(match[1]);
    if (value <= 0) reject();
    const millis = value * RESOLUTION_UNIT_MILLIS[match[2].toLowerCase()];
    if (!Number.isFinite(millis) || millis <= 0) reject();
    return millis;
  }

  const asNumber = Number(trimmed);
  if (trimmed !== '' && Number.isFinite(asNumber) && asNumber > 0) {
    return toMillisOrReject(asNumber);
  }

  return reject();
}
