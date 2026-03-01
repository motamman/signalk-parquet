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

/**
 * Parse resolution (in seconds), return milliseconds.
 * Supports: 60, 1m, 5s, 1h
 */
export function parseResolutionToMillis(resolution: string): number {
  const trimmed = resolution.trim();

  // Time expression (1s, 1m, 1h)
  const match = trimmed.match(/^(\d+(?:\.\d+)?)([smh])$/i);
  if (match) {
    const value = parseFloat(match[1]);
    const unit = match[2].toLowerCase();
    switch (unit) {
      case 's':
        return value * 1000;
      case 'm':
        return value * 60 * 1000;
      case 'h':
        return value * 60 * 60 * 1000;
    }
  }

  // Integer/float seconds
  const seconds = parseFloat(trimmed);
  if (!isNaN(seconds)) {
    return seconds * 1000;
  }

  throw new Error(`Invalid resolution: ${resolution}. Use 60 or 1m`);
}
