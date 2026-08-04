/**
 * Regression test for non-numeric object components in the v2 History API
 * provider. Within one timestamp bucket a component can be NULL on the
 * physically-first row and populated on a later one (e.g. a string state
 * that starts reporting mid-bucket while a numeric component was present
 * all along). FIRST() returned that leading NULL and the value was dropped
 * from the mapped output; ANY_VALUE(... ORDER BY signalk_timestamp) must
 * retain the earliest non-NULL value instead.
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

const SELF_ID = 'objectcomponentself';
const STORED_CONTEXT = `vessels.${SELF_ID}`;
const DAY = new Date('2024-06-01T00:00:00.000Z');
const ENGINE = 'propulsion.main';

/** An object-path record with a numeric and an optional string component. */
function makeEngineRecord(
  isoTime: string,
  temperature: number,
  state: string | null
): DataRecord {
  const value =
    state === null ? { temperature } : { temperature, state };
  const record: DataRecord = {
    received_timestamp: isoTime,
    signalk_timestamp: isoTime,
    context: STORED_CONTEXT,
    path: ENGINE,
    value,
    value_json: value,
    value_temperature: temperature,
    source_label: 'engine.1',
  };
  if (state !== null) {
    record.value_state = state;
  }
  return record;
}

describe('History API v2 provider: non-numeric object components', function () {
  this.timeout(30000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;
  let exportService: ParquetExportService;
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
    exportService = new ParquetExportService(
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

    provider = new HistoryProvider(
      SELF_ID,
      host.dataDir,
      providerApp,
      () => {}
    );
  });

  afterEach(async () => {
    clearSchemaCache();
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  it('retains a late non-NULL string component within a bucket', async () => {
    // Both rows land in the same 60s bucket. The first (earlier) row has no
    // `state` yet while `temperature` is already populated; the second row
    // supplies state='running'. FIRST() on the string column picked the
    // leading NULL and the component vanished from the response object.
    buffer.insert(makeEngineRecord('2024-06-01T10:00:10.000Z', 80, null));
    buffer.insert(makeEngineRecord('2024-06-01T10:00:40.000Z', 82, 'running'));
    await exportService.exportDayToParquet(DAY);

    const res = await provider.getValues({
      from: '2024-06-01T00:00:00Z',
      to: '2024-06-01T23:59:59Z',
      context: 'vessels.self',
      resolution: 60,
      pathSpecs: [{ path: ENGINE, aggregate: 'average', parameter: [] }],
    } as unknown as ValuesRequest);

    expect(res.data).to.have.lengthOf(1);
    const obj = res.data[0][1] as Record<string, unknown>;
    expect(obj.state, 'late non-NULL state must survive bucketing').to.equal(
      'running'
    );
    expect(obj.temperature as number).to.be.closeTo(81, 1e-9);
  });
});
