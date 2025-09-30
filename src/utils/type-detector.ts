import { ServerAPI } from '@signalk/server-api';

/**
 * Data type information for a SignalK path
 */
export interface PathTypeInfo {
  dataType: 'numeric' | 'angular' | 'boolean' | 'string' | 'position' | 'enum' | 'unknown';
  unit?: string;
  enumValues?: string[];
  description?: string;
  rawMetadata?: any;
}

/**
 * Numeric units that indicate a numeric data type
 */
const NUMERIC_UNITS = [
  'm',      // meters
  'm/s',    // meters per second
  'knots',  // nautical miles per hour
  'V',      // volts
  'A',      // amperes
  'Hz',     // hertz
  'K',      // kelvin
  'Pa',     // pascals
  'kg',     // kilograms
  'J',      // joules
  'ratio',  // dimensionless ratio
  'deg',    // degrees (non-angular context like temperature)
];

/**
 * Angular units that require degree/radian conversion
 */
const ANGULAR_UNITS = [
  'rad',    // radians
];

/**
 * Position-specific paths
 */
const POSITION_PATHS = [
  'navigation.position',
  'navigation.gnss.antennaPosition',
];

/**
 * Detect the data type of a SignalK path based on metadata
 * Based on schema-service.ts metadata lookup pattern
 */
export async function detectPathType(
  path: string,
  app?: ServerAPI
): Promise<PathTypeInfo> {
  try {
    // Special case: position paths
    if (POSITION_PATHS.includes(path)) {
      return {
        dataType: 'position',
        description: 'Geographic position (latitude/longitude)',
      };
    }

    // Try to fetch metadata from SignalK API
    const metadataUrl = `http://localhost:3000/signalk/v1/api/vessels/self/${path.replace(/\./g, '/')}/meta`;

    const response = await fetch(metadataUrl);
    if (!response.ok) {
      app?.debug(`Type detection: metadata not found for ${path}`);
      return { dataType: 'unknown' };
    }

    const metadata = await response.json() as any;

    // Check for units to determine numeric vs angular
    if (metadata.units) {
      const unit = metadata.units;

      // Angular data (radians)
      if (ANGULAR_UNITS.includes(unit)) {
        return {
          dataType: 'angular',
          unit,
          description: metadata.description,
          rawMetadata: metadata,
        };
      }

      // Numeric data
      if (NUMERIC_UNITS.includes(unit)) {
        return {
          dataType: 'numeric',
          unit,
          description: metadata.description,
          rawMetadata: metadata,
        };
      }

      app?.debug(`Type detection: unknown unit "${unit}" for ${path}, defaulting to numeric`);
      return {
        dataType: 'numeric',
        unit,
        description: metadata.description,
        rawMetadata: metadata,
      };
    }

    // Check for enum values in metadata
    if (metadata.enum || (Array.isArray(metadata.values) && metadata.values.length > 0)) {
      const enumValues = metadata.enum || metadata.values;
      return {
        dataType: 'enum',
        enumValues,
        description: metadata.description,
        rawMetadata: metadata,
      };
    }

    // Try to infer from description or type field
    if (metadata.type === 'boolean' || metadata.description?.toLowerCase().includes('boolean')) {
      return {
        dataType: 'boolean',
        description: metadata.description,
        rawMetadata: metadata,
      };
    }

    // Default to string if no clear type detected
    app?.debug(`Type detection: no clear type for ${path}, defaulting to string`);
    return {
      dataType: 'string',
      description: metadata.description,
      rawMetadata: metadata,
    };

  } catch (error) {
    app?.error(`Type detection error for ${path}: ${(error as Error).message}`);
    return { dataType: 'unknown' };
  }
}

/**
 * Sample the actual data to help determine type when metadata is unavailable
 * This is a fallback mechanism
 */
export async function detectPathTypeFromSample(
  path: string,
  sampleValue: any,
  app?: ServerAPI
): Promise<PathTypeInfo> {
  // Position object detection
  if (
    typeof sampleValue === 'object' &&
    sampleValue !== null &&
    'latitude' in sampleValue &&
    'longitude' in sampleValue
  ) {
    return {
      dataType: 'position',
      description: 'Geographic position (detected from sample)',
    };
  }

  // Boolean detection
  if (typeof sampleValue === 'boolean') {
    return {
      dataType: 'boolean',
      description: 'Boolean value (detected from sample)',
    };
  }

  // Numeric detection
  if (typeof sampleValue === 'number') {
    // Check if path suggests angular data
    const angularPatterns = [
      /heading/i,
      /course/i,
      /bearing/i,
      /angle/i,
      /direction/i,
    ];

    if (angularPatterns.some(pattern => pattern.test(path))) {
      return {
        dataType: 'angular',
        description: 'Angular value (detected from path pattern)',
      };
    }

    return {
      dataType: 'numeric',
      description: 'Numeric value (detected from sample)',
    };
  }

  // String detection
  if (typeof sampleValue === 'string') {
    return {
      dataType: 'string',
      description: 'String value (detected from sample)',
    };
  }

  app?.debug(`Type detection: unable to detect type from sample for ${path}`);
  return { dataType: 'unknown' };
}