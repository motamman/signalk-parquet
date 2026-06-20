/**
 * Unit tests for inline per-path query filters (`path:aggregate|sourceRef`):
 * expression parsing, SQL fragment generation for the parquet and buffer
 * sides, response echoes, and the parquet schema probing helpers.
 */
import { expect } from 'chai';
import {
  FILTER_DELIMITERS,
  PATH_FILTER_DEFS,
  PathFilter,
  SchemaProbeConnection,
  availableFilterColumns,
  buildBufferFilterClause,
  buildParquetFilterClause,
  filterColumns,
  filterEcho,
  filtersFromFields,
  parsePathFilters,
  parquetHasColumn,
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

/** Stub probe connection returning a fixed row count per call. */
function stubConnection(plan: Array<number | Error>): {
  connection: SchemaProbeConnection;
  queries: string[];
} {
  const queries: string[] = [];
  let call = 0;
  return {
    queries,
    connection: {
      runAndReadAll: async (sql: string) => {
        queries.push(sql);
        const step = plan[Math.min(call++, plan.length - 1)];
        if (step instanceof Error) throw step;
        return { getRowObjects: () => new Array(step).fill({}) };
      },
    },
  };
}

describe('parquetHasColumn', () => {
  it('returns true when a glob exposes the column', async () => {
    const { connection, queries } = stubConnection([1]);
    const result = await parquetHasColumn(
      connection,
      ['/data/*.parquet'],
      'source_label'
    );
    expect(result).to.equal(true);
    expect(queries[0]).to.include("parquet_schema('/data/*.parquet')");
    expect(queries[0]).to.include("name = 'source_label'");
  });

  it('returns false when no glob exposes the column', async () => {
    const { connection } = stubConnection([0]);
    expect(
      await parquetHasColumn(connection, ['/data/*.parquet'], 'col')
    ).to.equal(false);
  });

  it('skips null and undefined paths without querying', async () => {
    const { connection, queries } = stubConnection([1]);
    const result = await parquetHasColumn(
      connection,
      [null, undefined, '/data/*.parquet'],
      'col'
    );
    expect(result).to.equal(true);
    expect(queries).to.have.lengthOf(1);
  });

  it('treats probe errors as absent and tries the next path', async () => {
    const { connection, queries } = stubConnection([
      new Error('no files found'),
      1,
    ]);
    const result = await parquetHasColumn(
      connection,
      ['/missing/*.parquet', '/data/*.parquet'],
      'col'
    );
    expect(result).to.equal(true);
    expect(queries).to.have.lengthOf(2);
  });

  it('returns false when every probe errors', async () => {
    const { connection } = stubConnection([new Error('boom')]);
    expect(
      await parquetHasColumn(connection, ['/a/*.parquet', '/b/*.parquet'], 'c')
    ).to.equal(false);
  });
});

describe('availableFilterColumns', () => {
  it('collects only columns present in the parquet schema', async () => {
    const { connection } = stubConnection([1]);
    const available = await availableFilterColumns(
      connection,
      ['/data/*.parquet'],
      [sourceFilter('gps-1')]
    );
    expect([...available]).to.deep.equal(['source_label']);
  });

  it('returns an empty set when the column is absent', async () => {
    const { connection } = stubConnection([0]);
    const available = await availableFilterColumns(
      connection,
      ['/data/*.parquet'],
      [sourceFilter('gps-1')]
    );
    expect(available.size).to.equal(0);
  });
});
