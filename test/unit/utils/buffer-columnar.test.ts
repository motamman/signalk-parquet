/**
 * The columnar page codec that carries buffer rows across a thread boundary.
 *
 * Rows go out as typed arrays and come back as readers, so every value has to
 * survive the trip unchanged — and a null has to stay a null rather than
 * becoming a zero, a NaN or an empty string, because the DuckDB appender on the
 * far side needs to tell them apart.
 */
import { expect } from 'chai';
import {
  columnKindFor,
  encodePage,
  readPage,
} from '../../../src/utils/buffer-columnar';

const schema = [
  { name: 'id', type: 'INTEGER' },
  { name: 'context', type: 'TEXT' },
  { name: 'signalk_timestamp', type: 'TEXT' },
  { name: 'value_latitude', type: 'REAL' },
  { name: 'exported', type: 'INTEGER' },
];

/** Round-trip a page and read it back as rows of plain values. */
function roundTrip(
  rows: Array<Record<string, unknown>>,
  cols = schema
): Array<Record<string, bigint | number | string | null>> {
  const { page, transfer } = encodePage(rows, cols);
  expect(transfer.length).to.be.greaterThan(0);
  const readers = readPage(page);
  const out: Array<Record<string, bigint | number | string | null>> = [];
  for (let i = 0; i < page.rowCount; i++) {
    const row: Record<string, bigint | number | string | null> = {};
    for (const reader of readers) row[reader.name] = reader.at(i);
    out.push(row);
  }
  return out;
}

describe('columnar page codec', () => {
  it('maps declared SQLite types to the channel the appender wants', () => {
    expect(columnKindFor('INTEGER')).to.equal('i64');
    expect(columnKindFor('BIGINT')).to.equal('i64');
    expect(columnKindFor('REAL')).to.equal('f64');
    expect(columnKindFor('DOUBLE')).to.equal('f64');
    expect(columnKindFor('FLOAT')).to.equal('f64');
    expect(columnKindFor('TEXT')).to.equal('str');
    expect(columnKindFor('')).to.equal('str');
  });

  it('carries every value through unchanged', () => {
    const rows = [
      {
        id: 1,
        context: 'vessels.urn:mrn:imo:mmsi:368396230',
        signalk_timestamp: '2026-01-01T00:00:00.000Z',
        value_latitude: 41.4901234,
        exported: 0,
      },
      {
        id: 2,
        context: 'vessels.urn:mrn:signalk:uuid:abc',
        signalk_timestamp: '2026-01-01T00:00:01.500Z',
        value_latitude: -71.3125,
        exported: -1,
      },
    ];
    expect(roundTrip(rows)).to.deep.equal([
      {
        id: 1n,
        context: 'vessels.urn:mrn:imo:mmsi:368396230',
        signalk_timestamp: '2026-01-01T00:00:00.000Z',
        value_latitude: 41.4901234,
        exported: 0n,
      },
      {
        id: 2n,
        context: 'vessels.urn:mrn:signalk:uuid:abc',
        signalk_timestamp: '2026-01-01T00:00:01.500Z',
        value_latitude: -71.3125,
        exported: -1n,
      },
    ]);
  });

  it('keeps a null a null on every channel', () => {
    const [row] = roundTrip([
      {
        id: 7,
        context: null,
        signalk_timestamp: undefined,
        value_latitude: null,
        exported: null,
      },
    ]);
    expect(row.id).to.equal(7n);
    expect(row.context).to.equal(null);
    expect(row.signalk_timestamp).to.equal(null);
    expect(row.value_latitude).to.equal(null);
    expect(row.exported).to.equal(null);
  });

  it('distinguishes an empty string from a null', () => {
    const [row] = roundTrip([
      {
        id: 1,
        context: '',
        signalk_timestamp: null,
        value_latitude: 0,
        exported: 0,
      },
    ]);
    expect(row.context).to.equal('');
    expect(row.signalk_timestamp).to.equal(null);
    expect(row.value_latitude).to.equal(0);
  });

  it('encodes a value that will not convert on its channel as null', () => {
    // SQLite columns are dynamically typed, so stray content is possible; the
    // appender makes the same choice for content it cannot convert.
    const [row] = roundTrip([
      {
        id: 'not a number',
        context: 'c',
        signalk_timestamp: 't',
        value_latitude: 'not a number',
        exported: 0,
      },
    ]);
    expect(row.id).to.equal(null);
    expect(row.value_latitude).to.equal(null);
  });

  it('accepts a bigint from SQLite without going through a double', () => {
    const big = 9007199254740993n; // 2^53 + 1, not representable as a double
    const [row] = roundTrip([
      {
        id: big,
        context: 'c',
        signalk_timestamp: 't',
        value_latitude: 1,
        exported: 0,
      },
    ]);
    expect(row.id).to.equal(big);
  });

  it('carries multi-byte text at the right offsets', () => {
    const rows = [
      {
        id: 1,
        context: 'Ariel',
        signalk_timestamp: 'a',
        value_latitude: 1,
        exported: 0,
      },
      {
        id: 2,
        context: 'Åsgård ⛵',
        signalk_timestamp: 'b',
        value_latitude: 2,
        exported: 0,
      },
      {
        id: 3,
        context: '',
        signalk_timestamp: 'c',
        value_latitude: 3,
        exported: 0,
      },
      {
        id: 4,
        context: '日本語',
        signalk_timestamp: 'd',
        value_latitude: 4,
        exported: 0,
      },
    ];
    const got = roundTrip(rows).map(r => r.context);
    expect(got).to.deep.equal(['Ariel', 'Åsgård ⛵', '', '日本語']);
  });

  it('handles an empty page', () => {
    const { page } = encodePage([], schema);
    expect(page.rowCount).to.equal(0);
    expect(page.columns).to.have.lengthOf(schema.length);
    expect(readPage(page)).to.have.lengthOf(schema.length);
  });

  it('reads columns in the schema order it was given', () => {
    const { page } = encodePage(
      [
        {
          id: 1,
          context: 'c',
          signalk_timestamp: 't',
          value_latitude: 1,
          exported: 0,
        },
      ],
      schema
    );
    expect(readPage(page).map(r => r.name)).to.deep.equal(
      schema.map(c => c.name)
    );
  });
});
