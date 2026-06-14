/**
 * Unit tests for the pure DuckDB-type classifier used when building object
 * path component schemas. The History API decides whether to TRY_CAST a
 * component to DOUBLE or to VARCHAR based on this category, so the mapping
 * from raw DuckDB type strings to numeric/string/boolean/unknown matters.
 */
import { expect } from 'chai';
import { inferDataTypeCategory } from '../../../src/utils/schema-cache';

describe('inferDataTypeCategory', () => {
  const numeric = [
    'INTEGER',
    'INT',
    'BIGINT',
    'SMALLINT',
    'TINYINT',
    'HUGEINT',
    'DOUBLE',
    'FLOAT',
    'REAL',
    'DECIMAL(10,2)',
    'NUMERIC',
    'UINTEGER',
  ];
  numeric.forEach(type => {
    it(`classifies ${type} as numeric`, () => {
      expect(inferDataTypeCategory(type)).to.equal('numeric');
    });
  });

  const string = ['VARCHAR', 'CHAR', 'TEXT', 'STRING', 'UTF8', 'BYTE_ARRAY'];
  string.forEach(type => {
    it(`classifies ${type} as string`, () => {
      expect(inferDataTypeCategory(type)).to.equal('string');
    });
  });

  it('classifies BOOLEAN as boolean', () => {
    expect(inferDataTypeCategory('BOOLEAN')).to.equal('boolean');
  });

  it('classifies the short BOOL alias as boolean', () => {
    expect(inferDataTypeCategory('BOOL')).to.equal('boolean');
  });

  it('is case-insensitive', () => {
    expect(inferDataTypeCategory('double')).to.equal('numeric');
    expect(inferDataTypeCategory('varchar')).to.equal('string');
    expect(inferDataTypeCategory('bool')).to.equal('boolean');
  });

  it('returns unknown for an unrecognized type', () => {
    expect(inferDataTypeCategory('BLOB')).to.equal('unknown');
    expect(inferDataTypeCategory('')).to.equal('unknown');
  });

  it('returns unknown for TIMESTAMP (not treated as numeric or string)', () => {
    // TIMESTAMP/DATE carry no INT/CHAR/BOOL substring, so they fall through.
    expect(inferDataTypeCategory('TIMESTAMP')).to.equal('unknown');
    expect(inferDataTypeCategory('DATE')).to.equal('unknown');
  });

  it('matches on substring, classifying composite numeric types', () => {
    // DuckDB reports list/array types like 'INTEGER[]'; the INT substring
    // still classifies them as numeric.
    expect(inferDataTypeCategory('INTEGER[]')).to.equal('numeric');
  });

  it('prefers numeric when a type contains both numeric and string hints', () => {
    // The numeric branch is checked first, so a contrived 'INTCHAR' lands
    // on numeric. Pins the branch ordering.
    expect(inferDataTypeCategory('INTCHAR')).to.equal('numeric');
  });
});
