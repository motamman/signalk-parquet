/**
 * Unit tests for inline per-path query filters (`path:aggregate|sourceRef`):
 * expression parsing, SQL fragment generation for the parquet and buffer
 * sides, and response echoes.
 */
import { expect } from 'chai';
import {
  FILTER_DELIMITERS,
  PATH_FILTER_DEFS,
  PathFilter,
  buildBufferFilterClause,
  buildParquetFilterClause,
  filterColumns,
  filterEcho,
  filtersFromFields,
  parsePathFilters,
} from '../../../src/utils/path-filters';

const sourceFilter = (value: string): PathFilter => ({
  field: 'sourceRef',
  column: 'source_label',
  value,
});

describe('filter registry', () => {
  it('registers the sourceRef filter on the pipe delimiter', () => {
    expect(PATH_FILTER_DEFS).to.deep.equal([
      { delimiter: '|', field: 'sourceRef', column: 'source_label' },
    ]);
  });

  it('exposes the delimiter set for the path sanitiser', () => {
    expect(FILTER_DELIMITERS).to.equal('|');
  });
});

describe('parsePathFilters', () => {
  it('returns the whole expression when no delimiter is present', () => {
    expect(parsePathFilters('navigation.position:first')).to.deep.equal({
      base: 'navigation.position:first',
      filters: [],
    });
  });

  it('splits a single sourceRef filter off the base expression', () => {
    // Input:  'navigation.position:first|gps-1'
    // Output: base 'navigation.position:first', one source_label filter.
    expect(parsePathFilters('navigation.position:first|gps-1')).to.deep.equal({
      base: 'navigation.position:first',
      filters: [sourceFilter('gps-1')],
    });
  });

  it('parses repeated filters into separate entries', () => {
    expect(parsePathFilters('a.b|one|two').filters).to.deep.equal([
      sourceFilter('one'),
      sourceFilter('two'),
    ]);
  });

  it('drops a trailing delimiter with an empty value', () => {
    expect(parsePathFilters('a.b|')).to.deep.equal({
      base: 'a.b',
      filters: [],
    });
  });

  it('keeps non-delimiter punctuation inside the filter value', () => {
    expect(parsePathFilters('a.b|n2k-on-ve.can0.115').filters).to.deep.equal([
      sourceFilter('n2k-on-ve.can0.115'),
    ]);
  });

  it('yields an empty base when the expression starts with a delimiter', () => {
    expect(parsePathFilters('|gps-1')).to.deep.equal({
      base: '',
      filters: [sourceFilter('gps-1')],
    });
  });
});

describe('filtersFromFields', () => {
  it('reads registered fields from a parsed spec', () => {
    expect(filtersFromFields({ sourceRef: 'gps-1' })).to.deep.equal([
      sourceFilter('gps-1'),
    ]);
  });

  it('ignores empty strings and non-string values', () => {
    expect(filtersFromFields({ sourceRef: '' })).to.deep.equal([]);
    expect(filtersFromFields({ sourceRef: 7 })).to.deep.equal([]);
  });

  it('ignores unregistered fields', () => {
    expect(filtersFromFields({ somethingElse: 'x' })).to.deep.equal([]);
  });
});

describe('filterColumns', () => {
  it('deduplicates columns across filters', () => {
    expect(filterColumns([sourceFilter('a'), sourceFilter('b')])).to.deep.equal(
      ['source_label']
    );
  });

  it('is empty for no filters', () => {
    expect(filterColumns([])).to.deep.equal([]);
  });
});

describe('buildParquetFilterClause', () => {
  it('emits an equality predicate when the column exists', () => {
    const clause = buildParquetFilterClause(
      [sourceFilter('gps-1')],
      new Set(['source_label'])
    );
    expect(clause).to.equal(" AND source_label = 'gps-1'");
  });

  it('emits a never-matching predicate when the column is absent', () => {
    const clause = buildParquetFilterClause([sourceFilter('gps-1')], new Set());
    expect(clause).to.equal(' AND 1=0');
  });

  it('escapes embedded quotes in the value', () => {
    const clause = buildParquetFilterClause(
      [sourceFilter("o'brien")],
      new Set(['source_label'])
    );
    expect(clause).to.equal(" AND source_label = 'o''brien'");
  });

  it('concatenates multiple filters', () => {
    const clause = buildParquetFilterClause(
      [sourceFilter('a'), sourceFilter('b')],
      new Set(['source_label'])
    );
    expect(clause).to.equal(" AND source_label = 'a' AND source_label = 'b'");
  });

  it('is empty for no filters', () => {
    expect(buildParquetFilterClause([], new Set())).to.equal('');
  });
});

describe('buildBufferFilterClause', () => {
  it('returns an empty string for undefined or empty filters', () => {
    expect(buildBufferFilterClause(undefined)).to.equal('');
    expect(buildBufferFilterClause([])).to.equal('');
  });

  it('emits a newline-indented predicate per filter', () => {
    expect(buildBufferFilterClause([sourceFilter('gps-1')])).to.equal(
      "\n    AND source_label = 'gps-1'"
    );
  });

  it('escapes embedded quotes in the value', () => {
    expect(buildBufferFilterClause([sourceFilter("o'brien")])).to.include(
      "= 'o''brien'"
    );
  });
});

describe('filterEcho', () => {
  it('maps filters back to response properties', () => {
    expect(filterEcho([sourceFilter('gps-1')])).to.deep.equal({
      sourceRef: 'gps-1',
    });
  });

  it('is empty for no filters', () => {
    expect(filterEcho([])).to.deep.equal({});
  });
});

describe('unattributed (null-valued) filters', () => {
  const unattributed: PathFilter = {
    field: 'sourceRef',
    column: 'source_label',
    value: null,
  };

  it('selects NULL rows on the parquet side when the column exists', () => {
    expect(
      buildParquetFilterClause([unattributed], new Set(['source_label']))
    ).to.equal(' AND source_label IS NULL');
  });

  it('adds no parquet clause when no file has the column (every row is unattributed)', () => {
    expect(buildParquetFilterClause([unattributed], new Set())).to.equal('');
  });

  it('selects NULL rows on the buffer side', () => {
    expect(buildBufferFilterClause([unattributed])).to.equal(
      '\n    AND source_label IS NULL'
    );
  });

  it('is not echoed in the response', () => {
    expect(filterEcho([unattributed, sourceFilter('gps.1')])).to.deep.equal({
      sourceRef: 'gps.1',
    });
  });
});
