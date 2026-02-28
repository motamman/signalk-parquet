/**
 * Spatial filtering utilities for History API
 * Supports bbox and radius filters for DuckDB Parquet queries and JavaScript buffer filtering
 */

import { BoundingBox, DataRecord } from '../types';
import {
  isPointInBoundingBox,
  calculateDistance,
  createBoundingBoxFromRadius,
} from './geo-calculator';

/**
 * Spatial filter configuration
 */
export interface SpatialFilter {
  type: 'bbox' | 'radius';
  bbox: BoundingBox;
  // For radius filters, store the center and radius for precise distance checks
  centerLat?: number;
  centerLon?: number;
  radiusMeters?: number;
}

/**
 * Parse bbox query parameter in "west,south,east,north" format
 * @param bboxStr - Comma-separated bbox string
 * @returns BoundingBox or null if invalid
 */
export function parseBboxParam(bboxStr: string): BoundingBox | null {
  if (!bboxStr) return null;

  const parts = bboxStr.split(',').map(s => parseFloat(s.trim()));
  if (parts.length !== 4 || parts.some(isNaN)) {
    return null;
  }

  const [west, south, east, north] = parts;

  // Validate latitude range (-90 to 90)
  if (south < -90 || south > 90 || north < -90 || north > 90) {
    return null;
  }

  // Validate longitude range (-180 to 180)
  if (west < -180 || west > 180 || east < -180 || east > 180) {
    return null;
  }

  // Validate south <= north
  if (south > north) {
    return null;
  }

  return { west, south, east, north };
}

/**
 * Parse radius query parameter in "lat,lon,meters" format
 * @param radiusStr - Comma-separated radius string
 * @returns SpatialFilter with bbox approximation and precise radius info, or null if invalid
 */
export function parseRadiusParam(radiusStr: string): SpatialFilter | null {
  if (!radiusStr) return null;

  const parts = radiusStr.split(',').map(s => parseFloat(s.trim()));
  if (parts.length !== 3 || parts.some(isNaN)) {
    return null;
  }

  const [lat, lon, meters] = parts;

  // Validate latitude range (-90 to 90)
  if (lat < -90 || lat > 90) {
    return null;
  }

  // Validate longitude range (-180 to 180)
  if (lon < -180 || lon > 180) {
    return null;
  }

  // Validate positive radius
  if (meters <= 0) {
    return null;
  }

  // Convert radius to bounding box for initial filtering
  const bbox = createBoundingBoxFromRadius(lat, lon, meters);

  return {
    type: 'radius',
    bbox,
    centerLat: lat,
    centerLon: lon,
    radiusMeters: meters,
  };
}

/**
 * Parse bbox or radius query parameters into a SpatialFilter
 * @param bboxStr - Optional bbox parameter
 * @param radiusStr - Optional radius parameter
 * @returns SpatialFilter or null if no valid spatial params
 */
export function parseSpatialParams(
  bboxStr?: string,
  radiusStr?: string
): SpatialFilter | null {
  // Radius takes precedence over bbox
  if (radiusStr) {
    return parseRadiusParam(radiusStr);
  }

  if (bboxStr) {
    const bbox = parseBboxParam(bboxStr);
    if (bbox) {
      return { type: 'bbox', bbox };
    }
  }

  return null;
}

/**
 * Build DuckDB WHERE clause for spatial filtering on position data
 * Uses value_latitude and value_longitude columns
 * @param filter - Spatial filter configuration
 * @param latColumn - Column name for latitude (default: 'value_latitude')
 * @param lonColumn - Column name for longitude (default: 'value_longitude')
 * @returns SQL WHERE clause fragment (without leading AND)
 */
export function buildSpatialSqlClause(
  filter: SpatialFilter,
  latColumn: string = 'value_latitude',
  lonColumn: string = 'value_longitude'
): string {
  const { bbox } = filter;

  // Latitude check is always simple (no wrap-around)
  const latCondition = `${latColumn} >= ${bbox.south} AND ${latColumn} <= ${bbox.north}`;

  // Longitude check needs to handle 180° meridian crossing
  let lonCondition: string;
  if (bbox.west <= bbox.east) {
    // Normal case: bbox doesn't cross 180° meridian
    lonCondition = `${lonColumn} >= ${bbox.west} AND ${lonColumn} <= ${bbox.east}`;
  } else {
    // Box crosses 180° meridian (e.g., west=170, east=-170)
    // Point is in bbox if lon >= west OR lon <= east
    lonCondition = `(${lonColumn} >= ${bbox.west} OR ${lonColumn} <= ${bbox.east})`;
  }

  // For radius filters, we use the bbox as a coarse filter in SQL
  // The precise distance check happens in post-processing or JS filter
  // For now, just return bbox filter - DuckDB will do the heavy lifting
  if (
    filter.type === 'radius' &&
    filter.centerLat !== undefined &&
    filter.centerLon !== undefined &&
    filter.radiusMeters !== undefined
  ) {
    // Use ST_Distance_Spheroid for precise radius filtering in DuckDB
    // This requires the spatial extension which is already loaded
    const distanceCondition = `ST_Distance_Spheroid(
      ST_Point(${lonColumn}, ${latColumn}),
      ST_Point(${filter.centerLon}, ${filter.centerLat})
    ) <= ${filter.radiusMeters}`;

    // Return bbox filter AND precise distance check
    return `(${latCondition}) AND (${lonCondition}) AND (${distanceCondition})`;
  }

  return `(${latCondition}) AND (${lonCondition})`;
}

/**
 * Filter buffer records spatially using JavaScript
 * @param records - DataRecord array from SQLite buffer
 * @param filter - Spatial filter configuration
 * @returns Filtered records
 */
export function filterBufferRecordsSpatially(
  records: DataRecord[],
  filter: SpatialFilter
): DataRecord[] {
  return records.filter(record => {
    // Extract lat/lon from record - could be in multiple formats
    const lat = extractLatitude(record);
    const lon = extractLongitude(record);

    if (lat === null || lon === null) {
      // Can't filter without position data
      return true; // Include it (non-position record)
    }

    // Check bounding box first (quick rejection)
    if (!isPointInBoundingBox(lat, lon, filter.bbox)) {
      return false;
    }

    // For radius filters, do precise distance check
    if (
      filter.type === 'radius' &&
      filter.centerLat !== undefined &&
      filter.centerLon !== undefined &&
      filter.radiusMeters !== undefined
    ) {
      const distance = calculateDistance(
        lat,
        lon,
        filter.centerLat,
        filter.centerLon
      );
      return distance <= filter.radiusMeters;
    }

    return true;
  });
}

/**
 * Extract latitude from a DataRecord
 * Handles multiple storage formats
 */
function extractLatitude(record: DataRecord): number | null {
  // Check for flattened component columns (value_latitude)
  if (typeof record.value_latitude === 'number') {
    return record.value_latitude;
  }

  // Check for value object with latitude property
  if (record.value && typeof record.value === 'object') {
    if (typeof record.value.latitude === 'number') {
      return record.value.latitude;
    }
  }

  // Check for value_json as object
  if (record.value_json && typeof record.value_json === 'object') {
    const json = record.value_json as { latitude?: number };
    if (typeof json.latitude === 'number') {
      return json.latitude;
    }
  }

  // Check for value_json as string
  if (typeof record.value_json === 'string') {
    try {
      const parsed = JSON.parse(record.value_json);
      if (typeof parsed.latitude === 'number') {
        return parsed.latitude;
      }
    } catch {
      // Not valid JSON
    }
  }

  return null;
}

/**
 * Extract longitude from a DataRecord
 * Handles multiple storage formats
 */
function extractLongitude(record: DataRecord): number | null {
  // Check for flattened component columns (value_longitude)
  if (typeof record.value_longitude === 'number') {
    return record.value_longitude;
  }

  // Check for value object with longitude property
  if (record.value && typeof record.value === 'object') {
    if (typeof record.value.longitude === 'number') {
      return record.value.longitude;
    }
  }

  // Check for value_json as object
  if (record.value_json && typeof record.value_json === 'object') {
    const json = record.value_json as { longitude?: number };
    if (typeof json.longitude === 'number') {
      return json.longitude;
    }
  }

  // Check for value_json as string
  if (typeof record.value_json === 'string') {
    try {
      const parsed = JSON.parse(record.value_json);
      if (typeof parsed.longitude === 'number') {
        return parsed.longitude;
      }
    } catch {
      // Not valid JSON
    }
  }

  return null;
}

/**
 * Check if a SignalK path supports spatial filtering
 * Only paths that contain position data can be spatially filtered
 * @param signalkPath - The SignalK path to check
 * @returns true if path contains position data
 */
export function isPositionPath(signalkPath: string): boolean {
  // Common position-related paths
  const positionPatterns = [
    'navigation.position',
    'navigation.gnss.antennaPosition',
    'navigation.anchor.position',
    'navigation.destination.waypoint.position',
    '.position', // Any path ending in .position
  ];

  const lowerPath = signalkPath.toLowerCase();

  for (const pattern of positionPatterns) {
    if (pattern.startsWith('.')) {
      // Suffix pattern
      if (lowerPath.endsWith(pattern.toLowerCase())) {
        return true;
      }
    } else {
      // Exact or prefix match
      if (
        lowerPath === pattern.toLowerCase() ||
        lowerPath.startsWith(pattern.toLowerCase() + '.')
      ) {
        return true;
      }
    }
  }

  return false;
}
