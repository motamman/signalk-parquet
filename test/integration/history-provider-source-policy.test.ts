/**
 * Per-source history on the v2 provider (signalk-server #2737 and #2817).
 *
 * Two receivers record the same paths, and some rows carry no source at all.
 * A `sourceRef` on a path is a filter and the column reports it as `$source`;
 * `sourcePolicy=all` fans a path out into one column per recording source,
 * named sources first in sorted order, then one unattributed column. Data is
 * federated across exported parquet and the live buffer, so both stores take
 * part in source discovery.
 */
import { expect } from 'chai';
import * as path from 'path';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { HistoryProvider } from '../../src/history-provider';
import { clearSchemaCache } from '../../src/utils/schema-cache';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import type { DataRecord } from '../../src/types';
import type { ValuesRequest } from '@signalk/server-api/dist/history';

const SELF_ID = 'sourcepolicyself';
const STORED_CONTEXT = `vessels.${SELF_ID}`;
const DAY = new Date('2024-06-01T00:00:00.000Z');
const SOG = 'navigation.speedOverGround';
const POSITION = 'navigation.position';

function sogRecord(
  isoTime: string,
  value: number,
  source: string | null
): DataRecord {
  return {
    received_timestamp: isoTime,
    signalk_timestamp: isoTime,
    context: STORED_CONTEXT,
    path: SOG,
    value,
    source_label: source ?? undefined,
  } as DataRecord;
}

function positionRecord(
  isoTime: string,
  latitude: number,
  longitude: number,
  source: string
): DataRecord {
  return {
    received_timestamp: isoTime,
    signalk_timestamp: isoTime,
    context: STORED_CONTEXT,
    path: POSITION,
    value: { latitude, longitude },
    value_json: { latitude, longitude },
    value_latitude: latitude,
    value_longitude: longitude,
    source_label: source,
  };
}

/** A whole-day request at one-minute resolution. */
function request(
  pathSpecs: Array<Record<string, unknown>>,
  extra: Record<string, unknown> = {}
): ValuesRequest {
  return {
    from: '2024-06-01T00:00:00Z',
    to: '2024-06-01T23:59:59Z',
    context: 'vessels.self',
    resolution: 60,
    pathSpecs,
    ...extra,
  } as unknown as ValuesRequest;
}

describe('History API v2 provider: per-source history', function () {
  this.timeout(60000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;
  let provider: HistoryProvider;

  const providerApp = {
    debug: () => {},
    error: () => {},
    getMetadata: () => undefined,
    getSelfPath: () => undefined,
    selfId: SELF_ID,
    selfContext: STORED_CONTEXT,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } as any;

  beforeEach(async () => {
    host = createFakeSignalK({ selfId: SELF_ID });
    buffer = new SQLiteBuffer({ dbPath: path.join(host.dataDir, 'buffer.db') });
    await DuckDBPool.initialize();
    clearSchemaCache();

    const writer = new ParquetWriter({ format: 'parquet', app: host.app });
    const exportService = new ParquetExportService(
      buffer,
      writer,
      {
        outputDirectory: host.dataDir,
        filenamePrefix: 'signalk_data',
        useHivePartitioning: true,
        dailyExportHour: 4,
      },
      host.app
    );

    // 10:00 bucket: both receivers and an unattributed row, exported to
    // parquet. 11:00 bucket: only the backup receiver, exported. 12:00
    // bucket: main receiver and an unattributed row, left in the buffer so
    // discovery has to consult both stores.
    buffer.insert(sogRecord('2024-06-01T10:00:10.000Z', 5, 'gps.main'));
    buffer.insert(sogRecord('2024-06-01T10:00:20.000Z', 10, 'gps.backup'));
    buffer.insert(sogRecord('2024-06-01T10:00:30.000Z', 20, null));
    buffer.insert(sogRecord('2024-06-01T11:00:10.000Z', 12, 'gps.backup'));
    buffer.insert(
      positionRecord('2024-06-01T10:00:10.000Z', 47.5, 8.7, 'gps.main')
    );
    buffer.insert(
      positionRecord('2024-06-01T10:00:20.000Z', 47.6, 8.8, 'gps.backup')
    );
    await exportService.exportDayToParquet(DAY);
    buffer.insert(sogRecord('2024-06-01T12:00:10.000Z', 6, 'gps.main'));
    buffer.insert(sogRecord('2024-06-01T12:00:20.000Z', 22, null));

    provider = new HistoryProvider(SELF_ID, host.dataDir, providerApp, () => {});
    provider.setSqliteBuffer(buffer);
    DuckDBPool.initializeSQLiteBuffer(path.join(host.dataDir, 'buffer.db'));
  });

  afterEach(async () => {
    clearSchemaCache();
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  /** Column values keyed by bucket timestamp, for one column index. */
  function column(
    res: Awaited<ReturnType<HistoryProvider['getValues']>>,
    index: number
  ): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    for (const row of res.data) out[row[0]] = row[index + 1];
    return out;
  }

  it('filters to one source and reports it as $source, not sourceRef', async () => {
    const res = await provider.getValues(
      request([
        { path: SOG, aggregate: 'average', parameter: [], sourceRef: 'gps.main' },
      ])
    );

    expect(res.values).to.deep.equal([
      { path: SOG, method: 'average', $source: 'gps.main' },
    ]);
    expect(res.values[0]).to.not.have.property('sourceRef');
    expect(column(res, 0)).to.deep.equal({
      '2024-06-01T10:00:00Z': 5,
      '2024-06-01T12:00:00Z': 6,
    });
  });

  it('sourcePolicy=all expands a path into one column per source, sorted, then unattributed', async () => {
    const res = await provider.getValues(
      request([{ path: SOG, aggregate: 'average', parameter: [] }], {
        sourcePolicy: 'all',
      })
    );

    expect(res.values).to.deep.equal([
      { path: SOG, method: 'average', $source: 'gps.backup' },
      { path: SOG, method: 'average', $source: 'gps.main' },
      { path: SOG, method: 'average' },
    ]);
    // Rows are the union of every column's buckets; a column with nothing
    // in a bucket reads null there.
    expect(column(res, 0)).to.deep.equal({
      '2024-06-01T10:00:00Z': 10,
      '2024-06-01T11:00:00Z': 12,
      '2024-06-01T12:00:00Z': null,
    });
    expect(column(res, 1)).to.deep.equal({
      '2024-06-01T10:00:00Z': 5,
      '2024-06-01T11:00:00Z': null,
      '2024-06-01T12:00:00Z': 6,
    });
    expect(column(res, 2)).to.deep.equal({
      '2024-06-01T10:00:00Z': 20,
      '2024-06-01T11:00:00Z': null,
      '2024-06-01T12:00:00Z': 22,
    });
  });

  it('leaves a path with an explicit sourceRef as a single filtered column under the policy', async () => {
    const res = await provider.getValues(
      request(
        [
          { path: SOG, aggregate: 'max', parameter: [], sourceRef: 'gps.backup' },
          { path: SOG, aggregate: 'max', parameter: [] },
        ],
        { sourcePolicy: 'all' }
      )
    );

    expect(res.values.map(v => v.$source)).to.deep.equal([
      'gps.backup',
      'gps.backup',
      'gps.main',
      undefined,
    ]);
    expect(column(res, 0)['2024-06-01T11:00:00Z']).to.equal(12);
  });

  it('expands navigation.position per source with [lon, lat] values', async () => {
    const res = await provider.getValues(
      request([{ path: POSITION, aggregate: 'first', parameter: [] }], {
        sourcePolicy: 'all',
      })
    );

    expect(res.values).to.deep.equal([
      { path: POSITION, method: 'first', $source: 'gps.backup' },
      { path: POSITION, method: 'first', $source: 'gps.main' },
    ]);
    expect(column(res, 0)['2024-06-01T10:00:00Z']).to.deep.equal([8.8, 47.6]);
    expect(column(res, 1)['2024-06-01T10:00:00Z']).to.deep.equal([8.7, 47.5]);
  });

  it('keeps a path with no data in range as one empty, unexpanded column', async () => {
    const res = await provider.getValues(
      request(
        [{ path: 'environment.depth.belowKeel', aggregate: 'average', parameter: [] }],
        { sourcePolicy: 'all' }
      )
    );

    expect(res.values).to.deep.equal([
      { path: 'environment.depth.belowKeel', method: 'average' },
    ]);
    expect(res.data).to.deep.equal([]);
  });

  it('rejects a request that would expand past the column cap', async () => {
    // Three sources per spec: 22 specs would be 66 columns.
    const specs = Array.from({ length: 22 }, () => ({
      path: SOG,
      aggregate: 'average',
      parameter: [],
    }));
    let error: Error | undefined;
    try {
      await provider.getValues(request(specs, { sourcePolicy: 'all' }));
    } catch (err) {
      error = err as Error;
    }
    expect(error?.message).to.match(/max 64 columns/);
  });

  it('does not expand without the policy', async () => {
    const res = await provider.getValues(
      request([{ path: SOG, aggregate: 'average', parameter: [] }])
    );

    expect(res.values).to.deep.equal([{ path: SOG, method: 'average' }]);
    // All sources merged: 10:00 averages 5, 10 and 20.
    expect(column(res, 0)['2024-06-01T10:00:00Z']).to.be.closeTo(35 / 3, 1e-9);
  });
});
