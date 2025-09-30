/**
 * Geographic calculation utilities for position-based thresholds
 */

import { BoundingBox } from '../types';

/**
 * Calculate distance between two geographic points using Haversine formula
 * @param lat1 - Latitude of point 1 in degrees
 * @param lon1 - Longitude of point 1 in degrees
 * @param lat2 - Latitude of point 2 in degrees
 * @param lon2 - Longitude of point 2 in degrees
 * @returns Distance in meters
 */
export function calculateDistance(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number
): number {
  const R = 6371000; // Earth's radius in meters

  const φ1 = (lat1 * Math.PI) / 180; // Convert latitude to radians
  const φ2 = (lat2 * Math.PI) / 180;
  const Δφ = ((lat2 - lat1) * Math.PI) / 180;
  const Δλ = ((lon2 - lon1) * Math.PI) / 180;

  const a =
    Math.sin(Δφ / 2) * Math.sin(Δφ / 2) +
    Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ / 2) * Math.sin(Δλ / 2);

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return R * c; // Distance in meters
}

/**
 * Check if a point is within a bounding box
 * @param lat - Latitude of point in degrees
 * @param lon - Longitude of point in degrees
 * @param box - Bounding box definition
 * @returns True if point is inside the bounding box
 */
export function isPointInBoundingBox(
  lat: number,
  lon: number,
  box: BoundingBox
): boolean {
  // Handle longitude wrap-around (crossing 180° meridian)
  const isInLatitude = lat >= box.south && lat <= box.north;

  let isInLongitude: boolean;
  if (box.west <= box.east) {
    // Normal case: box doesn't cross 180° meridian
    isInLongitude = lon >= box.west && lon <= box.east;
  } else {
    // Box crosses 180° meridian (e.g., west=170, east=-170)
    isInLongitude = lon >= box.west || lon <= box.east;
  }

  return isInLatitude && isInLongitude;
}

/**
 * Create a bounding box around a center point with a given radius
 * @param centerLat - Center latitude in degrees
 * @param centerLon - Center longitude in degrees
 * @param radiusMeters - Radius in meters
 * @returns Bounding box that encompasses the circle
 */
export function createBoundingBoxFromRadius(
  centerLat: number,
  centerLon: number,
  radiusMeters: number
): BoundingBox {
  // Approximate degrees per meter at this latitude
  // 1 degree latitude ≈ 111,139 meters (constant)
  // 1 degree longitude ≈ 111,139 * cos(latitude) meters (varies by latitude)

  const latRadians = (centerLat * Math.PI) / 180;

  const deltaLat = radiusMeters / 111139; // degrees latitude
  const deltaLon = radiusMeters / (111139 * Math.cos(latRadians)); // degrees longitude

  return {
    north: centerLat + deltaLat,
    south: centerLat - deltaLat,
    east: centerLon + deltaLon,
    west: centerLon - deltaLon,
  };
}

/**
 * Calculate the bearing from point 1 to point 2
 * @param lat1 - Latitude of point 1 in degrees
 * @param lon1 - Longitude of point 1 in degrees
 * @param lat2 - Latitude of point 2 in degrees
 * @param lon2 - Longitude of point 2 in degrees
 * @returns Bearing in degrees (0-360, where 0 is north)
 */
export function calculateBearing(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number
): number {
  const φ1 = (lat1 * Math.PI) / 180;
  const φ2 = (lat2 * Math.PI) / 180;
  const Δλ = ((lon2 - lon1) * Math.PI) / 180;

  const y = Math.sin(Δλ) * Math.cos(φ2);
  const x =
    Math.cos(φ1) * Math.sin(φ2) - Math.sin(φ1) * Math.cos(φ2) * Math.cos(Δλ);

  const θ = Math.atan2(y, x);
  const bearing = ((θ * 180) / Math.PI + 360) % 360; // Convert to degrees and normalize to 0-360

  return bearing;
}

/**
 * Calculate destination point given start point, bearing, and distance
 * @param lat - Starting latitude in degrees
 * @param lon - Starting longitude in degrees
 * @param bearing - Bearing in degrees (0-360)
 * @param distanceMeters - Distance to travel in meters
 * @returns Destination point {latitude, longitude}
 */
export function calculateDestinationPoint(
  lat: number,
  lon: number,
  bearing: number,
  distanceMeters: number
): { latitude: number; longitude: number } {
  const R = 6371000; // Earth's radius in meters
  const δ = distanceMeters / R; // Angular distance in radians
  const θ = (bearing * Math.PI) / 180; // Bearing in radians

  const φ1 = (lat * Math.PI) / 180;
  const λ1 = (lon * Math.PI) / 180;

  const φ2 = Math.asin(
    Math.sin(φ1) * Math.cos(δ) + Math.cos(φ1) * Math.sin(δ) * Math.cos(θ)
  );

  const λ2 =
    λ1 +
    Math.atan2(
      Math.sin(θ) * Math.sin(δ) * Math.cos(φ1),
      Math.cos(δ) - Math.sin(φ1) * Math.sin(φ2)
    );

  return {
    latitude: (φ2 * 180) / Math.PI,
    longitude: (λ2 * 180) / Math.PI,
  };
}

/**
 * Calculate bounding box from home port with anchor point
 * @param homePortLat - Home port latitude in degrees
 * @param homePortLon - Home port longitude in degrees
 * @param boxSizeMeters - Distance from home port to edges in meters
 * @param anchor - Anchor point position (nw, n, ne, w, center, e, sw, s, se)
 * @returns Bounding box
 */
export function calculateBoundingBoxFromHomePort(
  homePortLat: number,
  homePortLon: number,
  boxSizeMeters: number,
  anchor: string
): BoundingBox {
  // Calculate the box center based on anchor point
  let centerLat = homePortLat;
  let centerLon = homePortLon;

  // Special case: if anchor is "center", home port IS the center
  if (anchor === 'center') {
    return createBoundingBoxFromRadius(homePortLat, homePortLon, boxSizeMeters);
  }

  // Determine offsets based on anchor position (exact match)
  let latOffset = 0; // -1 = south, 0 = middle, 1 = north
  let lonOffset = 0; // -1 = west, 0 = middle, 1 = east

  if (anchor === 'nw' || anchor === 'n' || anchor === 'ne') {
    latOffset = 1; // Home port is on north edge, box extends south
  } else if (anchor === 'sw' || anchor === 's' || anchor === 'se') {
    latOffset = -1; // Home port is on south edge, box extends north
  }

  if (anchor === 'nw' || anchor === 'w' || anchor === 'sw') {
    lonOffset = 1; // Home port is on west edge, box extends east
  } else if (anchor === 'ne' || anchor === 'e' || anchor === 'se') {
    lonOffset = -1; // Home port is on east edge, box extends west
  }

  // Calculate center point by moving from home port
  if (latOffset !== 0) {
    // Move north or south
    const point = calculateDestinationPoint(
      homePortLat,
      homePortLon,
      latOffset > 0 ? 180 : 0,
      boxSizeMeters
    );
    centerLat = point.latitude;
  }

  if (lonOffset !== 0) {
    // Move east or west
    const point = calculateDestinationPoint(
      centerLat,
      homePortLon,
      lonOffset > 0 ? 270 : 90,
      boxSizeMeters
    );
    centerLon = point.longitude;
  }

  // Create bounding box around the center point
  return createBoundingBoxFromRadius(centerLat, centerLon, boxSizeMeters);
}
