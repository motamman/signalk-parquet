/**
 * Unit tests for the SQLite-buffer SQL fragment builders. These are pure
 * string builders, so the tests assert on meaningful SQL substrings
 * (whitespace-normalized) rather than full golden strings.
 *
 * The builders read from a staged DuckDB temp table (see buffer-staging.ts)
 * whose name the caller passes in — existence checks and table-name
 * derivation happen at staging time, so the builders always return SQL.
 */
import { expect } from 'chai';
import {
  buildBufferScalarSubquery,
  buildBufferObjectSubquery,
} from '../../../src/utils/buffer-sql-builder';
import type { ComponentInfo } from '../../../src/utils/schema-cache';

const STAGED_TABLE = 'temp.main.stage_buffer_test_path';
const CONTEXT = 'vessels.urn:mrn:imo:mmsi:368204530';
const FROM_ISO = '2024-06-01T00:00:00.000Z';
const TO_ISO = '2024-06-02T00:00:00.000Z';

/** Collapse runs of whitespace so substring assertions ignore formatting. */
function norm(sql: string): string {
  return sql.replace(/\s+/g, ' ').trim();
}

function component(
  name: string,
  dataType: ComponentInfo['dataType']
): ComponentInfo {
  return { name, columnName: `value_${name}`, dataType };
}

function componentMap(...infos: ComponentInfo[]): Map<string, ComponentInfo> {
  return new Map(infos.map(info => [info.name, info]));
}

/** Build a scalar subquery and normalize it. */
function scalarSql(
  path: string,
  options: {
    context?: string;
    fromIso?: string;
    toIso?: string;
  } = {}
): string {
  const sql = buildBufferScalarSubquery(
    STAGED_TABLE,
    options.context ?? CONTEXT,
    path,
    options.fromIso ?? FROM_ISO,
    options.toIso ?? TO_ISO
  );
  expect(sql).to.be.a('string');
  return norm(sql);
}

/** Build an object subquery and normalize it. */
function objectSql(
  path: string,
  components: Map<string, ComponentInfo>,
  options: {
    context?: string;
    bufferTableColumns?: Set<string>;
  } = {}
): string {
  const sql = buildBufferObjectSubquery(
    STAGED_TABLE,
    options.context ?? CONTEXT,
    FROM_ISO,
    TO_ISO,
    components,
    options.bufferTableColumns
  );
  expect(sql).to.be.a('string');
  return norm(sql);
}

describe('buildBufferScalarSubquery', () => {
  it('reads from the staged temp table it is given', () => {
    expect(scalarSql('navigation.speedOverGround')).to.contain(
      `FROM ${STAGED_TABLE}`
    );
  });

  it('wraps dotted (numeric) paths in TRY_CAST', () => {
    const sql = scalarSql('navigation.speedOverGround');

    expect(sql).to.contain('TRY_CAST(value AS DOUBLE) AS value');
  });

  it('selects root-level (string) paths without a cast', () => {
    const sql = scalarSql('name');

    expect(sql).to.contain('value AS value');
    expect(sql).to.not.contain('TRY_CAST');
    expect(sql).to.contain(`FROM ${STAGED_TABLE}`);
  });

  it('emits a NULL placeholder for the value_json column', () => {
    expect(scalarSql('navigation.speedOverGround')).to.contain(
      'NULL::VARCHAR AS value_json'
    );
  });

  it('includes the time-range, export and null filters', () => {
    const sql = scalarSql('navigation.speedOverGround');

    expect(sql).to.contain(`WHERE context = '${CONTEXT}'`);
    expect(sql).to.contain(`received_timestamp >= '${FROM_ISO}'`);
    expect(sql).to.contain(`received_timestamp < '${TO_ISO}'`);
    expect(sql).to.contain('exported = 0');
    expect(sql).to.contain('value IS NOT NULL');
    expect(sql).to.contain('signalk_timestamp');
  });

  it('doubles single quotes in the context', () => {
    const sql = scalarSql('navigation.speedOverGround', {
      context: "vessels.o'brien",
    });

    expect(sql).to.contain("WHERE context = 'vessels.o''brien'");
  });

  it('doubles single quotes in the timestamps (injection attempt stays inert)', () => {
    const sql = scalarSql('navigation.speedOverGround', {
      fromIso: "2024-06-01' OR '1'='1",
    });

    expect(sql).to.contain("received_timestamp >= '2024-06-01'' OR ''1''=''1'");
  });
});

describe('buildBufferObjectSubquery', () => {
  const position = componentMap(
    component('latitude', 'numeric'),
    component('longitude', 'numeric')
  );

  it('selects numeric components via TRY_CAST in map insertion order', () => {
    const sql = objectSql('navigation.position', position);

    expect(sql).to.contain(`FROM ${STAGED_TABLE}`);
    expect(sql).to.contain(
      'TRY_CAST(value_latitude AS DOUBLE) AS value_latitude, ' +
        'TRY_CAST(value_longitude AS DOUBLE) AS value_longitude'
    );
  });

  it('selects string and boolean components via CAST AS VARCHAR', () => {
    const sql = objectSql(
      'navigation.attitude',
      componentMap(
        component('state', 'string'),
        component('engaged', 'boolean')
      )
    );

    expect(sql).to.contain('CAST(value_state AS VARCHAR) AS value_state');
    expect(sql).to.contain('CAST(value_engaged AS VARCHAR) AS value_engaged');
  });

  it('substitutes NULL::DOUBLE for components missing from the buffer table', () => {
    const sql = objectSql(
      'navigation.position',
      componentMap(
        component('latitude', 'numeric'),
        component('longitude', 'numeric'),
        component('altitude', 'numeric')
      ),
      {
        bufferTableColumns: new Set(['value_latitude', 'value_longitude']),
      }
    );

    expect(sql).to.contain('NULL::DOUBLE AS value_altitude');
    expect(sql).to.contain('TRY_CAST(value_latitude AS DOUBLE)');
    expect(sql).to.contain('TRY_CAST(value_longitude AS DOUBLE)');
  });

  it('uses NULL::DOUBLE even for missing string components', () => {
    // The missing-column check runs before the dataType branch, so a string
    // component absent from the buffer table is emitted as NULL::DOUBLE,
    // not NULL VARCHAR. Pinned as current behaviour.
    const sql = objectSql(
      'navigation.gnss',
      componentMap(component('methodQuality', 'string')),
      { bufferTableColumns: new Set<string>() }
    );

    expect(sql).to.contain('NULL::DOUBLE AS value_methodQuality');
    expect(sql).to.not.contain('CAST(value_methodQuality');
  });

  it('selects every component directly when bufferTableColumns is omitted', () => {
    const sql = objectSql(
      'navigation.position',
      componentMap(component('altitude', 'numeric'))
    );

    expect(sql).to.contain('TRY_CAST(value_altitude AS DOUBLE)');
    expect(sql).to.not.contain('NULL::DOUBLE');
  });

  it('filters on value_json instead of value', () => {
    const sql = objectSql('navigation.position', position);

    expect(sql).to.contain('value_json IS NOT NULL');
    expect(sql).to.not.contain('value IS NOT NULL');
  });

  it('includes the time-range and export filters with escaping', () => {
    const sql = objectSql('navigation.position', position, {
      context: "vessels.o'brien",
    });

    expect(sql).to.contain("WHERE context = 'vessels.o''brien'");
    expect(sql).to.contain(`received_timestamp >= '${FROM_ISO}'`);
    expect(sql).to.contain(`received_timestamp < '${TO_ISO}'`);
    expect(sql).to.contain('exported = 0');
    expect(sql).to.contain('signalk_timestamp');
  });
});
