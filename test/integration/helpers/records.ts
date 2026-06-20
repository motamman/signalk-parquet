/**
 * Shared DataRecord fixture builders for the integration suites.
 *
 * Centralising the record shape here means a change to DataRecord (e.g. a new
 * required column) is made in one place rather than in each suite's local
 * builder. Each suite binds its own context via a thin wrapper.
 */
import type { DataRecord } from '../../../src/types';

/** A scalar-valued record (single `value` column). */
export function makeScalarRecord(
  context: string,
  signalkPath: string,
  value: number,
  isoTime: string,
  sourceLabel = 'test.source'
): DataRecord {
  return {
    received_timestamp: isoTime,
    signalk_timestamp: isoTime,
    context,
    path: signalkPath,
    value,
    source_label: sourceLabel,
  };
}

/** A navigation.position record with flattened latitude/longitude columns. */
export function makePositionRecord(
  context: string,
  latitude: number,
  longitude: number,
  isoTime: string
): DataRecord {
  return {
    received_timestamp: isoTime,
    signalk_timestamp: isoTime,
    context,
    path: 'navigation.position',
    value: { latitude, longitude },
    value_json: { latitude, longitude },
    value_latitude: latitude,
    value_longitude: longitude,
    source_label: 'gps.1',
  };
}
