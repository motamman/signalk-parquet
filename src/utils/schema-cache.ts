/**
 * Schema information for an object-valued path. The providers build it from
 * the footers of the files their query reads (parquet-footer.ts); this module
 * keeps the shape and the type-category mapping they share.
 */
export interface PathComponentSchema {
  components: Map<string, ComponentInfo>; // component name -> type info
  timestamp: number;
}

export interface ComponentInfo {
  name: string; // e.g., "latitude", "longitude", "altitude"
  columnName: string; // e.g., "value_latitude", "value_longitude"
  dataType: 'numeric' | 'string' | 'boolean' | 'unknown';
}

/**
 * Infer data type category from DuckDB type string
 */
export function inferDataTypeCategory(
  duckdbType: string
): ComponentInfo['dataType'] {
  const typeUpper = duckdbType.toUpperCase();

  // Numeric types
  if (
    typeUpper.includes('INT') ||
    typeUpper.includes('DOUBLE') ||
    typeUpper.includes('FLOAT') ||
    typeUpper.includes('DECIMAL') ||
    typeUpper.includes('NUMERIC') ||
    typeUpper.includes('REAL') ||
    typeUpper.includes('BIGINT') ||
    typeUpper.includes('SMALLINT') ||
    typeUpper.includes('TINYINT')
  ) {
    return 'numeric';
  }

  // String types
  if (
    typeUpper.includes('VARCHAR') ||
    typeUpper.includes('CHAR') ||
    typeUpper.includes('TEXT') ||
    typeUpper.includes('STRING') ||
    typeUpper.includes('UTF8') ||
    typeUpper.includes('BYTE_ARRAY')
  ) {
    return 'string';
  }

  // Boolean
  if (typeUpper.includes('BOOL')) {
    return 'boolean';
  }

  return 'unknown';
}
