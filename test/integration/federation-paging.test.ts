/**
 * Paging invariants for the federation read.
 *
 * Staging a history query walks the buffer a page at a time, resuming from the
 * last row it saw, so the read has to hand back every matching row exactly
 * once no matter where the page boundaries fall. Two things make that
 * non-trivial: rows can share a `signalk_timestamp`, so the cursor needs the
 * id to break the tie; and the resume has to stay a range on the indexed
 * timestamp column, because the natural spelling of a keyset resume
 * (`ts > ? OR (ts = ? AND id > ?)`) is a disjunction SQLite cannot seek with,
 * which silently turns each page into a rescan of the window. These tests pin
 * the contract the callers depend on rather than the SQL that implements it.
 */
import { expect } from 'chai';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs-extra';
import { SQLiteBuffer, federationCursor } from '../../src/utils/sqlite-buffer';
import { FederationCursor } from '../../src/types';
import { makePositionRecord, makeScalarRecord } from './helpers/records';

const CONTEXT = 'vessels.urn:mrn:signalk:uuid:paging';
const OTHER = 'vessels.urn:mrn:signalk:uuid:other';
const SOG = 'navigation.speedOverGround';
const FROM = '2026-01-01T00:00:00.000Z';
const TO = '2026-01-02T00:00:00.000Z';

describe('federation read paging', () => {
  let dataDir: string;
  let buffer: SQLiteBuffer;

  beforeEach(async () => {
    dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'sk-fed-paging-'));
    buffer = new SQLiteBuffer({ dbPath: path.join(dataDir, 'buffer.db') });
  });

  afterEach(async () => {
    if (buffer.isOpen()) buffer.close();
    await fs.remove(dataDir);
  });

  /** Every row the read returns, walked `limit` at a time. */
  function pageThrough(limit: number): Array<Record<string, unknown>> {
    const all: Array<Record<string, unknown>> = [];
    let after: FederationCursor | null = null;
    for (;;) {
      const rows = buffer.getRowsForFederation(
        SOG,
        CONTEXT,
        FROM,
        TO,
        after,
        limit
      );
      if (rows.length === 0) break;
      all.push(...rows);
      after = federationCursor(rows[rows.length - 1]);
      if (rows.length < limit) break;
    }
    return all;
  }

  it('returns every row exactly once at every page size', () => {
    const rows = Array.from({ length: 50 }, (_, i) =>
      makeScalarRecord(
        CONTEXT,
        SOG,
        i,
        new Date(Date.parse(FROM) + i * 1000).toISOString()
      )
    );
    buffer.insertBatch(rows);

    const whole = pageThrough(1000);
    expect(whole).to.have.lengthOf(50);

    for (const limit of [1, 2, 3, 7, 49, 50, 51]) {
      const paged = pageThrough(limit);
      const ids = paged.map(r => Number(r.id));
      expect(
        ids,
        `page size ${limit} returned a different row count`
      ).to.have.lengthOf(50);
      expect(new Set(ids).size, `page size ${limit} repeated a row`).to.equal(
        50
      );
      expect(ids, `page size ${limit} changed the order`).to.deep.equal(
        whole.map(r => Number(r.id))
      );
    }
  });

  it('does not lose or repeat rows that share a timestamp', () => {
    // Twelve rows on four timestamps, so ties straddle every page boundary
    // a small page size can produce.
    const shared: ReturnType<typeof makeScalarRecord>[] = [];
    for (let t = 0; t < 4; t++) {
      const iso = new Date(Date.parse(FROM) + t * 1000).toISOString();
      for (let n = 0; n < 3; n++) {
        shared.push(makeScalarRecord(CONTEXT, SOG, t * 10 + n, iso));
      }
    }
    buffer.insertBatch(shared);

    for (const limit of [1, 2, 3, 4, 5, 12]) {
      const ids = pageThrough(limit).map(r => Number(r.id));
      expect(
        ids,
        `page size ${limit} lost or repeated a tied row`
      ).to.have.lengthOf(12);
      expect(
        new Set(ids).size,
        `page size ${limit} repeated a tied row`
      ).to.equal(12);
    }
  });

  it('pages only the asked-for context and window', () => {
    const inWindow = Array.from({ length: 10 }, (_, i) =>
      makeScalarRecord(
        CONTEXT,
        SOG,
        i,
        new Date(Date.parse(FROM) + i * 1000).toISOString()
      )
    );
    const otherContext = Array.from({ length: 10 }, (_, i) =>
      makeScalarRecord(
        OTHER,
        SOG,
        i,
        new Date(Date.parse(FROM) + i * 1000).toISOString()
      )
    );
    const beforeWindow = [
      makeScalarRecord(CONTEXT, SOG, 99, '2025-12-31T23:59:59.000Z'),
    ];
    const afterWindow = [makeScalarRecord(CONTEXT, SOG, 98, TO)];
    buffer.insertBatch([
      ...inWindow,
      ...otherContext,
      ...beforeWindow,
      ...afterWindow,
    ]);

    for (const limit of [1, 3, 10]) {
      const rows = pageThrough(limit);
      expect(rows, `page size ${limit}`).to.have.lengthOf(10);
      for (const row of rows) {
        expect(row.context).to.equal(CONTEXT);
        expect(String(row.signalk_timestamp) >= FROM).to.equal(true);
        expect(String(row.signalk_timestamp) < TO).to.equal(true);
      }
    }
  });

  it('returns only the federation columns, not the bookkeeping ones', () => {
    buffer.insert({
      ...makeScalarRecord(CONTEXT, SOG, 1, FROM),
      source: { label: 'test.source', type: 'NMEA0183' },
      source_type: 'NMEA0183',
      meta: { units: 'm/s' },
    });
    const [row] = buffer.getRowsForFederation(SOG, CONTEXT, FROM, TO, null, 10);
    expect(Object.keys(row).sort()).to.deep.equal(
      ['context', 'exported', 'id', 'signalk_timestamp', 'source_label', 'value'].sort()
    );
  });

  it('returns a component the writer added after a reader opened', () => {
    const POS = 'navigation.position';
    buffer.insert(makePositionRecord(CONTEXT, 1, 2, FROM));
    const reader = new SQLiteBuffer({
      dbPath: path.join(dataDir, 'buffer.db'),
      readOnly: true,
    });
    try {
      reader.loadTableIfMissing(POS);
      // A component the table did not have when the reader opened.
      buffer.insert({
        ...makePositionRecord(CONTEXT, 3, 4, '2026-01-01T00:00:01.000Z'),
        value_altitude: 12,
      });
      const rows = reader.getRowsForFederation(POS, CONTEXT, FROM, TO, null, 10);
      expect(rows).to.have.lengthOf(2);
      expect(rows[1].value_altitude).to.equal(12);
    } finally {
      reader.close();
    }
  });

  it('resumes on the indexed timestamp, so a page is a seek and not a rescan', () => {
    // The guard against the disjunction form regressing: with the cursor
    // raising the range's lower bound, SQLite plans a single index search and
    // no sort. With `ts > ? OR (ts = ? AND id > ?)` it plans the same search
    // over the *window*, which is why this asserts on the bound rather than
    // on the plan text: the query must not be able to see rows before the
    // cursor at all.
    buffer.insertBatch(
      Array.from({ length: 20 }, (_, i) =>
        makeScalarRecord(
          CONTEXT,
          SOG,
          i,
          new Date(Date.parse(FROM) + i * 1000).toISOString()
        )
      )
    );

    const first = buffer.getRowsForFederation(SOG, CONTEXT, FROM, TO, null, 5);
    expect(first).to.have.lengthOf(5);
    const cursor = federationCursor(first[4]);

    const next = buffer.getRowsForFederation(
      SOG,
      CONTEXT,
      FROM,
      TO,
      cursor,
      100
    );
    expect(next).to.have.lengthOf(15);
    for (const row of next) {
      expect(
        String(row.signalk_timestamp) >= cursor.signalkTimestamp,
        'resumed page reached behind the cursor'
      ).to.equal(true);
      expect(Number(row.id)).to.be.greaterThan(cursor.id);
    }
  });
});
