/**
 * The contract of `insertOrExtendLatest`.
 *
 * It exists as one buffer operation because the choice between extending a row
 * and inserting one depends on what is stored, and a caller cannot make that
 * choice without holding a row id across a gap in which the row can be
 * exported or deleted. The vessel identity service is its only caller today,
 * and its tests cover the behaviour through that service; these pin the
 * operation itself, because it is the shape the buffer keeps when it moves to
 * a worker and it must not be weakened by whatever the next caller needs.
 */
import { expect } from 'chai';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs-extra';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { DataRecord } from '../../src/types';

const CONTEXT = 'vessels.urn:mrn:signalk:uuid:extend';
const OTHER = 'vessels.urn:mrn:signalk:uuid:other';
const OBJ = 'identity';
const SCALAR = 'navigation.speedOverGround';
const T0 = '2026-01-01T00:00:00.000Z';

describe('buffer insertOrExtendLatest', () => {
  let dataDir: string;
  let buffer: SQLiteBuffer;

  beforeEach(async () => {
    dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'sk-extend-'));
    buffer = new SQLiteBuffer({ dbPath: path.join(dataDir, 'buffer.db') });
  });

  afterEach(async () => {
    if (buffer.isOpen()) buffer.close();
    await fs.remove(dataDir);
  });

  /** An object record whose `value_*` columns mirror its value_json. */
  function objectRecord(
    context: string,
    value: Record<string, unknown>,
    isoTime = T0
  ): DataRecord {
    const record: DataRecord = {
      received_timestamp: isoTime,
      signalk_timestamp: isoTime,
      context,
      path: OBJ,
      value: null,
      value_json: value,
      source_label: 'test.source',
    };
    for (const [key, v] of Object.entries(value)) record[`value_${key}`] = v;
    return record;
  }

  function stored(context = CONTEXT) {
    return buffer
      .getRowsForFederation(
        OBJ,
        context,
        '2025-01-01T00:00:00.000Z',
        '2027-01-01T00:00:00.000Z',
        null,
        100
      )
      .map(r => r as Record<string, unknown>);
  }

  it('extends the latest row when the new value only adds keys', () => {
    expect(
      buffer.insertOrExtendLatest(objectRecord(CONTEXT, { name: 'Ariel' }))
    ).to.deep.equal({ extended: false });
    expect(
      buffer.insertOrExtendLatest(
        objectRecord(CONTEXT, { name: 'Ariel', mmsi: '244813000' })
      )
    ).to.deep.equal({ extended: true });

    const rows = stored();
    expect(rows).to.have.lengthOf(1);
    expect(rows[0].value_name).to.equal('Ariel');
    expect(rows[0].value_mmsi).to.equal('244813000');
    expect(buffer.getStats().totalRecords).to.equal(1);
  });

  it('inserts when a key the stored row carries has changed', () => {
    buffer.insertOrExtendLatest(objectRecord(CONTEXT, { name: 'Ariel' }));
    expect(
      buffer.insertOrExtendLatest(objectRecord(CONTEXT, { name: 'Renamed' }))
    ).to.deep.equal({ extended: false });
    expect(stored()).to.have.lengthOf(2);
  });

  it('inserts when the new value drops a key the stored row carried', () => {
    buffer.insertOrExtendLatest(
      objectRecord(CONTEXT, { name: 'Ariel', mmsi: '244813000' })
    );
    expect(
      buffer.insertOrExtendLatest(objectRecord(CONTEXT, { mmsi: '244813000' }))
    ).to.deep.equal({ extended: false });
    expect(stored()).to.have.lengthOf(2);
  });

  it('never extends a row that has been exported', () => {
    buffer.insertOrExtendLatest(objectRecord(CONTEXT, { name: 'Ariel' }));
    buffer.markDateExported(CONTEXT, OBJ, new Date(T0), 'batch-1');
    expect(stored()).to.have.lengthOf(0);

    expect(
      buffer.insertOrExtendLatest(
        objectRecord(CONTEXT, { name: 'Ariel', mmsi: '244813000' })
      )
    ).to.deep.equal({ extended: false });
    expect(stored()).to.have.lengthOf(1);
    expect(buffer.getStats().totalRecords).to.equal(2);
  });

  it('extends only within one context', () => {
    buffer.insertOrExtendLatest(objectRecord(CONTEXT, { name: 'Ariel' }));
    expect(
      buffer.insertOrExtendLatest(
        objectRecord(OTHER, { name: 'Ariel', mmsi: '244813000' })
      )
    ).to.deep.equal({ extended: false });
    expect(stored(CONTEXT)).to.have.lengthOf(1);
    expect(stored(OTHER)).to.have.lengthOf(1);
  });

  it('extends the newest of several rows, not an older one', () => {
    buffer.insertOrExtendLatest(objectRecord(CONTEXT, { name: 'Ariel' }));
    buffer.insertOrExtendLatest(objectRecord(CONTEXT, { name: 'Renamed' }));
    expect(
      buffer.insertOrExtendLatest(
        objectRecord(CONTEXT, { name: 'Renamed', mmsi: '244813000' })
      )
    ).to.deep.equal({ extended: true });

    const rows = stored();
    expect(rows).to.have.lengthOf(2);
    expect(rows[0].value_name).to.equal('Ariel');
    expect(rows[0].value_mmsi).to.equal(null);
    expect(rows[1].value_name).to.equal('Renamed');
    expect(rows[1].value_mmsi).to.equal('244813000');
  });

  it('carries the new record timestamp and source onto an extended row', () => {
    buffer.insertOrExtendLatest(objectRecord(CONTEXT, { name: 'Ariel' }, T0));
    const later = '2026-01-01T00:00:05.000Z';
    const record = objectRecord(
      CONTEXT,
      { name: 'Ariel', mmsi: '244813000' },
      later
    );
    record.source_label = 'ais.2';
    expect(buffer.insertOrExtendLatest(record)).to.deep.equal({
      extended: true,
    });

    const rows = stored();
    expect(rows).to.have.lengthOf(1);
    expect(rows[0].signalk_timestamp).to.equal(later);
    expect(rows[0].source_label).to.equal('ais.2');
  });

  it('always inserts for a scalar path, which has no value to complete', () => {
    const scalar = (value: number): DataRecord => ({
      received_timestamp: T0,
      signalk_timestamp: T0,
      context: CONTEXT,
      path: SCALAR,
      value,
      source_label: 'test.source',
    });
    expect(buffer.insertOrExtendLatest(scalar(1))).to.deep.equal({
      extended: false,
    });
    expect(buffer.insertOrExtendLatest(scalar(1))).to.deep.equal({
      extended: false,
    });
    expect(
      buffer.getRowsForFederation(
        SCALAR,
        CONTEXT,
        '2025-01-01T00:00:00.000Z',
        '2027-01-01T00:00:00.000Z',
        null,
        100
      )
    ).to.have.lengthOf(2);
  });

  it('refuses to write at all once the buffer is closed', () => {
    buffer.close();
    expect(() =>
      buffer.insertOrExtendLatest(objectRecord(CONTEXT, { name: 'Ariel' }))
    ).to.throw(/closed/);
  });
});
