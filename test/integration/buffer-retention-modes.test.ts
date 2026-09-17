/**
 * Retention modes on the buffer: a row is stamped when it is written and its
 * fate never changes afterwards.
 *
 *   0  tracked, owes a Parquet write
 *   1  written to Parquet
 *  -1  short-term, will never be written
 *
 * The properties that matter, and that are each easy to break silently:
 * a buffer-only row is visible to queries (it exists nowhere else), is never
 * selected for export, and ages out on the retention clock — while a tracked
 * row that has not been exported is never deleted, however old, so an export
 * failure costs a retry rather than the data.
 */
import { expect } from 'chai';
import * as path from 'path';
import {
  SQLiteBuffer,
  EXPORTED_BUFFER_ONLY,
  EXPORTED_DONE,
  EXPORTED_PENDING,
} from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import type { DataRecord } from '../../src/types';

const SELF_ID = 'retentionself';
const SELF = `vessels.${SELF_ID}`;
const TRACKED = 'navigation.speedOverGround';
const SHORT = 'navigation.rateOfTurn';

function record(
  signalkPath: string,
  iso: string,
  value: number,
  exported?: number
): DataRecord {
  return {
    received_timestamp: iso,
    signalk_timestamp: iso,
    context: SELF,
    path: signalkPath,
    value,
    source_label: 'test.source',
    ...(exported !== undefined ? { exported } : {}),
  } as DataRecord;
}

/** Read the stamp on every row of a path, in insert order. */
function stamps(buffer: SQLiteBuffer, signalkPath: string): number[] {
  const db = (buffer as unknown as { db: { prepare(sql: string): { all(): unknown[] } } }).db;
  const table = `buffer_${signalkPath.replace(/\./g, '_')}`;
  return (db.prepare(`SELECT exported FROM ${table} ORDER BY id`).all() as Array<{
    exported: number;
  }>).map(r => r.exported);
}

/** Force every row of a path to look older than the retention window. */
function ageRows(buffer: SQLiteBuffer, signalkPath: string, hours: number): void {
  const db = (buffer as unknown as { db: { prepare(sql: string): { run(...a: unknown[]): unknown } } }).db;
  const table = `buffer_${signalkPath.replace(/\./g, '_')}`;
  db.prepare(
    `UPDATE ${table} SET created_at = datetime('now', '-' || ? || ' hours')`
  ).run(hours);
}

describe('buffer retention modes', function () {
  this.timeout(30000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;

  beforeEach(() => {
    host = createFakeSignalK({ selfId: SELF_ID });
    buffer = new SQLiteBuffer({
      dbPath: path.join(host.dataDir, 'buffer.db'),
      retentionHours: 48,
    });
  });

  afterEach(async () => {
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  it('stamps a row from the mode in force at insert', () => {
    buffer.insert(record(TRACKED, '2024-06-01T10:00:00.000Z', 5));
    buffer.insert(record(SHORT, '2024-06-01T10:00:00.000Z', 1, EXPORTED_BUFFER_ONLY));
    expect(stamps(buffer, TRACKED)).to.deep.equal([EXPORTED_PENDING]);
    expect(stamps(buffer, SHORT)).to.deep.equal([EXPORTED_BUFFER_ONLY]);
  });

  it('shows buffer-only rows to queries, because they exist nowhere else', () => {
    buffer.insert(record(SHORT, '2024-06-01T10:00:00.000Z', 1, EXPORTED_BUFFER_ONLY));
    const rows = buffer.getRowsForFederation(
      SHORT,
      SELF,
      '2024-06-01T00:00:00.000Z',
      '2024-06-02T00:00:00.000Z',
      0,
      100
    );
    expect(rows).to.have.lengthOf(1);

    // The playback reads see them too.
    expect(buffer.hasRowsSince('2024-06-01T00:00:00.000Z', null)).to.equal(true);
    expect(buffer.getNextRowTime('2024-06-01T00:00:00.000Z', null)).to.equal(
      '2024-06-01T10:00:00.000Z'
    );
    expect(
      buffer.getRowsForPlayback(
        '2024-06-01T00:00:00.000Z',
        '2024-06-02T00:00:00.000Z',
        null,
        10
      )
    ).to.have.lengthOf(1);
  });

  it('never offers a buffer-only row for export', () => {
    buffer.insert(record(TRACKED, '2024-06-01T10:00:00.000Z', 5));
    buffer.insert(record(SHORT, '2024-06-01T10:00:00.000Z', 1, EXPORTED_BUFFER_ONLY));
    const forExport = buffer
      .getPathsForDate(new Date('2024-06-01T00:00:00.000Z'))
      .map(p => p.path);
    expect(forExport).to.include(TRACKED);
    expect(forExport).to.not.include(SHORT);
    expect(buffer.getPendingCount()).to.equal(1);
  });

  it('counts the three states apart in the stats', () => {
    buffer.insert(record(TRACKED, '2024-06-01T10:00:00.000Z', 5));
    buffer.insert(record(SHORT, '2024-06-01T10:00:00.000Z', 1, EXPORTED_BUFFER_ONLY));
    buffer.insert(record(SHORT, '2024-06-01T10:00:01.000Z', 2, EXPORTED_BUFFER_ONLY));
    const s = buffer.getStats();
    expect(s.totalRecords).to.equal(3);
    expect(s.pendingRecords).to.equal(1);
    expect(s.bufferOnlyRecords).to.equal(2);
    expect(s.exportedRecords).to.equal(0);
  });

  describe('retention', () => {
    it('ages out buffer-only rows without them ever being exported', () => {
      buffer.insert(record(SHORT, '2024-06-01T10:00:00.000Z', 1, EXPORTED_BUFFER_ONLY));
      ageRows(buffer, SHORT, 72);
      expect(buffer.cleanup()).to.equal(1);
      expect(stamps(buffer, SHORT)).to.have.lengthOf(0);
    });

    it('keeps a tracked row that has not been exported, however old', () => {
      // An export that failed must cost a retry, not the data.
      buffer.insert(record(TRACKED, '2024-06-01T10:00:00.000Z', 5));
      ageRows(buffer, TRACKED, 24 * 30);
      expect(buffer.cleanup()).to.equal(0);
      expect(stamps(buffer, TRACKED)).to.deep.equal([EXPORTED_PENDING]);
    });

    it('ages out a tracked row once it has been exported', () => {
      buffer.insert(record(TRACKED, '2024-06-01T10:00:00.000Z', 5));
      buffer.markDateExported(
        SELF,
        TRACKED,
        new Date('2024-06-01T00:00:00.000Z'),
        'batch-1'
      );
      expect(stamps(buffer, TRACKED)).to.deep.equal([EXPORTED_DONE]);
      ageRows(buffer, TRACKED, 72);
      expect(buffer.cleanup()).to.equal(1);
    });

    it('leaves a settled row alone until it is past the window', () => {
      buffer.insert(record(SHORT, '2024-06-01T10:00:00.000Z', 1, EXPORTED_BUFFER_ONLY));
      ageRows(buffer, SHORT, 24);
      expect(buffer.cleanup()).to.equal(0);
    });
  });

  it('runs retention even on a day with nothing to export', async () => {
    // A config made mostly of short-term paths produces exactly this: nothing
    // to export, and the export is the only caller of cleanup.
    buffer.insert(record(SHORT, '2024-06-01T10:00:00.000Z', 1, EXPORTED_BUFFER_ONLY));
    ageRows(buffer, SHORT, 72);

    const exportService = new ParquetExportService(
      buffer,
      new ParquetWriter({ format: 'parquet', app: host.app }),
      {
        outputDirectory: host.dataDir,
        filenamePrefix: 'signalk_data',
        useHivePartitioning: true,
        dailyExportHour: 4,
      },
      host.app
    );

    const result = await exportService.exportDayToParquet(
      new Date('2024-06-01T00:00:00.000Z')
    );
    expect(result.recordsExported).to.equal(0);
    expect(stamps(buffer, SHORT)).to.have.lengthOf(0);
  });

  it('does not re-stamp a row when the mode changes', () => {
    // Morning as a tracked path, afternoon as short-term: each row keeps the
    // stamp it was written with, and nothing reaches back.
    buffer.insert(record(TRACKED, '2024-06-01T10:00:00.000Z', 5));
    buffer.insert(record(TRACKED, '2024-06-01T14:00:00.000Z', 6, EXPORTED_BUFFER_ONLY));
    expect(stamps(buffer, TRACKED)).to.deep.equal([
      EXPORTED_PENDING,
      EXPORTED_BUFFER_ONLY,
    ]);

    // The morning row still owes its export and gets it; the afternoon one
    // is not offered.
    expect(buffer.getPendingCount()).to.equal(1);
  });

  it('indexes retention and the playback probe rather than scanning', () => {
    buffer.insert(record(TRACKED, '2024-06-01T10:00:00.000Z', 5));
    const db = (buffer as unknown as { db: { prepare(sql: string): { all(): unknown[] } } }).db;
    const plan = (sql: string) =>
      (db.prepare(`EXPLAIN QUERY PLAN ${sql}`).all() as Array<{ detail: string }>)
        .map(r => r.detail)
        .join(' ');

    expect(
      plan(
        `SELECT * FROM buffer_navigation_speedOverGround WHERE exported IN (1, -1) AND created_at < datetime('now','-48 hours')`
      )
    ).to.match(/USING INDEX/);
    expect(
      plan(
        `SELECT MIN(signalk_timestamp) FROM buffer_navigation_speedOverGround WHERE signalk_timestamp >= '2024-06-01T00:00:00Z' AND exported IN (0, -1)`
      )
    ).to.match(/USING (COVERING )?INDEX/);
  });
});
