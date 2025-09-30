import { ServerAPI } from '@signalk/server-api';

/**
 * Data type information for a SignalK path
 */
export interface PathTypeInfo {
  dataType:
    | 'numeric'
    | 'angular'
    | 'boolean'
    | 'string'
    | 'position'
    | 'enum'
    | 'unknown';
  unit?: string;
  enumValues?: string[];
  description?: string;
  rawMetadata?: Record<string, unknown>;
}

/**
 * Numeric units that indicate a numeric data type
 */
const NUMERIC_UNITS = [
  'm', // meters
  'm/s', // meters per second
  'knots', // nautical miles per hour
  'V', // volts
  'A', // amperes
  'Hz', // hertz
  'K', // kelvin
  'Pa', // pascals
  'kg', // kilograms
  'J', // joules
  'ratio', // dimensionless ratio
  'deg', // degrees (non-angular context like temperature)
];

/**
 * Angular units that require degree/radian conversion
 */
const ANGULAR_UNITS = [
  'rad', // radians
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
    // Special case: all paths ending with "position" are position types
    if (path.endsWith('position') || POSITION_PATHS.includes(path)) {
      return {
        dataType: 'position',
        description: 'Geographic position (latitude/longitude)',
      };
    }

    // Try to get metadata from SignalK
    let metadata: Record<string, unknown> | undefined;

    if (app) {
      // Use app's getMetadata method if available
      metadata = app.getMetadata(path) as Record<string, unknown> | undefined;
    }

    if (!metadata) {
      app?.debug(`Type detection: metadata not found for ${path}`);
      return { dataType: 'unknown' };
    }

    // Type guard helpers
    const getStringProperty = (
      obj: Record<string, unknown>,
      key: string
    ): string | undefined => {
      const value = obj[key];
      return typeof value === 'string' ? value : undefined;
    };

    const getArrayProperty = (
      obj: Record<string, unknown>,
      key: string
    ): string[] | undefined => {
      const value = obj[key];
      return Array.isArray(value) ? (value as string[]) : undefined;
    };

    // Check for units to determine numeric vs angular
    const unit = getStringProperty(metadata, 'units');
    if (unit) {
      // Angular data (radians)
      if (ANGULAR_UNITS.includes(unit)) {
        return {
          dataType: 'angular',
          unit,
          description: getStringProperty(metadata, 'description'),
          rawMetadata: metadata,
        };
      }

      // Numeric data
      if (NUMERIC_UNITS.includes(unit)) {
        return {
          dataType: 'numeric',
          unit,
          description: getStringProperty(metadata, 'description'),
          rawMetadata: metadata,
        };
      }

      app?.debug(
        `Type detection: unknown unit "${unit}" for ${path}, defaulting to numeric`
      );
      return {
        dataType: 'numeric',
        unit,
        description: getStringProperty(metadata, 'description'),
        rawMetadata: metadata,
      };
    }

    // Check for enum values in metadata
    const enumValues =
      getArrayProperty(metadata, 'enum') ||
      getArrayProperty(metadata, 'values');
    if (enumValues && enumValues.length > 0) {
      return {
        dataType: 'enum',
        enumValues,
        description: getStringProperty(metadata, 'description'),
        rawMetadata: metadata,
      };
    }

    // Try to infer from description or type field
    const type = getStringProperty(metadata, 'type');
    const description = getStringProperty(metadata, 'description');
    if (type === 'boolean' || description?.toLowerCase().includes('boolean')) {
      return {
        dataType: 'boolean',
        description,
        rawMetadata: metadata,
      };
    }

    // Try to sample the actual value to detect boolean
    if (app) {
      try {
        const sampleValue = app.getSelfPath(path as any);
        // Handle both raw values and SignalK value objects
        const actualValue = sampleValue && typeof sampleValue === 'object' && 'value' in sampleValue
          ? sampleValue.value
          : sampleValue;

        if (typeof actualValue === 'boolean') {
          return {
            dataType: 'boolean',
            description: description || 'Boolean value (detected from sample)',
            rawMetadata: metadata,
          };
        }
      } catch (sampleError) {
        // Ignore sampling errors, continue with default detection
      }
    }

    // Default to string if no clear type detected
    app?.debug(
      `Type detection: no clear type for ${path}, defaulting to string`
    );
    return {
      dataType: 'string',
      description,
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
  sampleValue: unknown,
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
