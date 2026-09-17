/**
 * The one footer reader, exercised on files the plugin's own ParquetWriter
 * produces. If the writer ever stops emitting usable statistics these fail,
 * rather than every caller quietly falling back to reading data through
 * DuckDB and the memory cost returning unnoticed.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as path from 'path';
import { ParquetWriter } from '../../src/parquet-writer';
import {
  componentColumns,
  dataTypeOf,
  footerReaderAvailable,
  hasColumn,
  overlapsWindow,
  readFooter,
  readFooters,
  readRowGroups,
  RowGroup,
} from '../../src/utils/parquet-footer';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makeScalarRecord, makePositionRecord } from './helpers/records';

const CONTEXT = 'vessels.urn:mrn:signalk:uuid:c0d79334-4e25-4149-a2b8-1f7a3210ab42';

describe('parquet footer reader', function () {
  this.timeout(20000);

  let host: FakeSignalK;
  let writer: ParquetWriter;
  let scalarFile: string;
  let objectFile: string;

  beforeEach(async () => {
    host = createFakeSignalK();
    writer = new ParquetWriter({ format: 'parquet', app: host.app });
    scalarFile = path.join(host.dataDir, 'scalar.parquet');
    objectFile = path.join(host.dataDir, 'object.parquet');
    await writer.writeRecords(scalarFile, [
      makeScalarRecord(CONTEXT, 'navigation.speedOverGround', 5.5, '2024-06-01T10:00:00.000Z'),
      makeScalarRecord(CONTEXT, 'navigation.speedOverGround', 6.5, '2024-06-01T10:00:01.000Z'),
      makeScalarRecord(CONTEXT, 'navigation.speedOverGround', 7.5, '2024-06-01T11:30:00.000Z'),
    ]);
    await writer.writeRecords(objectFile, [
      makePositionRecord(CONTEXT, 40.6, -73.98, '2024-06-01T10:00:00.000Z'),
      makePositionRecord(CONTEXT, 40.7, -73.97, '2024-06-01T10:00:01.000Z'),
    ]);
  });

  afterEach(async () => {
    await host.cleanup();
  });

  it('is available with the bundled library', () => {
    expect(footerReaderAvailable()).to.equal(true);
  });

  it('reads the exact context from the statistics', async () => {
    const footer = await readFooter(scalarFile);
    expect(footer.context).to.equal(CONTEXT);
  });

  it('reads the timestamp span', async () => {
    const footer = await readFooter(scalarFile);
    expect(footer.timestampSpans).to.deep.equal([
      ['2024-06-01T10:00:00.000Z', '2024-06-01T11:30:00.000Z'],
    ]);
    expect(overlapsWindow(footer, '2024-06-01T11:00:00Z', '2024-06-02T00:00:00Z')).to.equal(true);
    expect(overlapsWindow(footer, '2024-06-01T11:30:00.001Z', '2024-06-02T00:00:00Z')).to.equal(false);
    expect(overlapsWindow(footer, '2024-06-01T00:00:00Z', '2024-06-01T10:00:00.000Z')).to.equal(false);
  });

  it('does not throw on a numeric column whose statistics are numbers', async () => {
    // The scalar file's `value` is DOUBLE; parquetjs decodes its statistics
    // as numbers, which Buffer.from used to choke on (2026-09-17).
    const footer = await readFooter(scalarFile);
    expect(footer.columns.get('value')?.physicalType).to.equal('DOUBLE');
    expect(footer.context).to.equal(CONTEXT);
  });

  it('types the columns as the DuckDB names used to', async () => {
    const footer = await readFooter(scalarFile);
    expect(dataTypeOf(footer.columns.get('value')!)).to.equal('numeric');
    expect(dataTypeOf(footer.columns.get('context')!)).to.equal('string');
    expect(dataTypeOf(footer.columns.get('signalk_timestamp')!)).to.equal('string');
    expect(dataTypeOf({ name: 'b', physicalType: 'BOOLEAN', logicalType: null })).to.equal('boolean');
    expect(dataTypeOf({ name: 'n', physicalType: 'INT64', logicalType: null })).to.equal('numeric');
    expect(dataTypeOf({ name: 'd', physicalType: 'FIXED_LEN_BYTE_ARRAY', logicalType: 'DECIMAL' })).to.equal('numeric');
    expect(dataTypeOf({ name: 'r', physicalType: 'BYTE_ARRAY', logicalType: null })).to.equal('unknown');
  });

  it('lists the object components of an object path and none for a scalar one', async () => {
    const object = await readFooter(objectFile);
    const components = componentColumns([object]);
    expect([...components.keys()].sort()).to.deep.equal(['latitude', 'longitude']);
    expect(components.get('latitude')).to.deep.equal({
      name: 'latitude',
      columnName: 'value_latitude',
      dataType: 'numeric',
    });
    const scalar = await readFooter(scalarFile);
    expect(componentColumns([scalar]).size).to.equal(0);
    // The union across files: a component any file carries is present.
    expect([...componentColumns([scalar, object]).keys()].sort()).to.deep.equal(['latitude', 'longitude']);
  });

  it('answers column presence across files', async () => {
    const footers = await readFooters([scalarFile, objectFile]);
    expect(hasColumn(footers, 'source_label')).to.equal(true);
    expect(hasColumn(footers, 'value_latitude')).to.equal(true);
    expect(hasColumn([footers.find(f => f.file === scalarFile)!], 'value_latitude')).to.equal(false);
    expect(hasColumn(footers, 'nope')).to.equal(false);
  });

  it('reads many footers and rejects when one cannot be read', async () => {
    const garbage = path.join(host.dataDir, 'garbage.parquet');
    await fs.writeFile(garbage, 'not a parquet file at all');
    const good = await readFooters([scalarFile, objectFile], { concurrency: 2 });
    expect(good.map(f => f.file).sort()).to.deep.equal([objectFile, scalarFile].sort());
    let failed = false;
    try {
      await readFooters([scalarFile, garbage, objectFile], { concurrency: 2 });
    } catch {
      failed = true;
    }
    expect(failed).to.equal(true);
  });

  it('stops launching reads once `until` says so', async () => {
    const seen = await readFooters([scalarFile, objectFile, scalarFile, objectFile], {
      concurrency: 1,
      until: () => true,
    });
    expect(seen).to.have.lengthOf(1);
  });

  describe('row-group statistics', () => {
    const chunk = (name: string, min: unknown, max: unknown) => ({
      meta_data: { path_in_schema: [name], statistics: { min_value: min, max_value: max } },
    });
    const bytes = (s: string) => Buffer.from(s, 'utf8');

    it('trusts a context only when min equals max in every row group', () => {
      const exact: RowGroup[] = [
        { columns: [chunk('context', bytes('vessels.a'), bytes('vessels.a')), chunk('signalk_timestamp', 't1', 't2')] },
        { columns: [chunk('context', 'vessels.a', 'vessels.a'), chunk('signalk_timestamp', 't3', 't4')] },
      ];
      expect(readRowGroups(exact)).to.deep.equal({
        context: 'vessels.a',
        timestampSpans: [['t1', 't2'], ['t3', 't4']],
      });
    });

    it('rejects truncated statistics, disagreeing groups, and missing ones', () => {
      expect(readRowGroups([{ columns: [chunk('context', 'vessels.a', 'vessels.b')] }]).context).to.equal(null);
      expect(
        readRowGroups([
          { columns: [chunk('context', 'vessels.a', 'vessels.a')] },
          { columns: [chunk('context', 'vessels.b', 'vessels.b')] },
        ]).context
      ).to.equal(null);
      expect(
        readRowGroups([
          { columns: [chunk('context', 'vessels.a', 'vessels.a')] },
          { columns: [chunk('value', 1, 2)] },
        ]).context
      ).to.equal(null);
      expect(readRowGroups([]).context).to.equal(null);
      expect(readRowGroups([]).timestampSpans).to.equal(null);
    });

    it('reports no span when any row group lacks timestamp statistics', () => {
      const groups: RowGroup[] = [
        { columns: [chunk('context', 'vessels.a', 'vessels.a'), chunk('signalk_timestamp', 't1', 't2')] },
        { columns: [chunk('context', 'vessels.a', 'vessels.a')] },
      ];
      expect(readRowGroups(groups)).to.deep.equal({ context: 'vessels.a', timestampSpans: null });
    });

    it('treats a numeric statistic on a string column as absent, not as text', () => {
      expect(readRowGroups([{ columns: [chunk('context', 1, 1)] }]).context).to.equal(null);
    });
  });
});
