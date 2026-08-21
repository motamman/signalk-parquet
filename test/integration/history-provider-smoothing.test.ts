/**
 * Provider-level tests for the v2 History API sma/ema aggregate methods.
 *
 * Writes Parquet fixtures through the real export pipeline, then calls
 * HistoryProvider.getValues directly — the path the Signal K core takes when a
 * client hits /signalk/v2/api/history/values. Proves that `path:sma:N` /
 * `path:ema:a` return genuinely smoothed series (not the old AVG fall-through
 * mislabelled as sma), that a smoothed column and a bare column coexist as two
 * distinct, correctly-labelled columns, and that position longitude is smoothed
 * on the circle so the ±180° antimeridian is handled.
 */
import { expect } from 'chai';
import * as path from 'path';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { HistoryProvider } from '../../src/history-provider';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makeScalarRecord, makePositionRecord } from './helpers/records';
import type { ValuesRequest } from '@signalk/server-api/dist/history';

const SELF_ID = 'smoothingself';
const STORED_CONTEXT = `vessels.${SELF_ID}`;
const DAY = new Date('2024-06-01T00:00:00.000Z');
const SOG = 'navigation.speedOverGround';
// One sample per 60s bucket, deliberately varying so the window is observable.
const SAMPLES = [1, 2, 3, 4, 5];

describe('History API v2 provider: sma/ema smoothing', function () {
  this.timeout(30000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;
  let exportService: ParquetExportService;
  let provider: HistoryProvider;

  // Minimal ServerAPI stand-in: getMetadata undefined => non-angular by default.
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

    // Provider left without a SQLite buffer: fixtures live entirely in Parquet.
    provider = new HistoryProvider(SELF_ID, host.dataDir, providerApp, () => {});
  });

  afterEach(async () => {
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  const query = (
    pathSpecs: Array<{ path: string; aggregate: string; parameter: string[] }>
  ): ValuesRequest =>
    ({
      from: '2024-06-01T00:00:00Z',
      to: '2024-06-01T23:59:59Z',
      context: 'vessels.self',
      resolution: 60,
      pathSpecs,
    }) as unknown as ValuesRequest;

  async function writeSogFixture(values: number[]): Promise<void> {
    values.forEach((v, i) => {
      buffer.insert(
        makeScalarRecord(STORED_CONTEXT, SOG, v, `2024-06-01T10:0${i}:00.000Z`)
      );
    });
    await exportService.exportDayToParquet(DAY);
  }

  it('sma returns a trailing moving average, distinct from bare average', async () => {
    await writeSogFixture(SAMPLES);

    const res = await provider.getValues(
      query([
        { path: SOG, aggregate: 'average', parameter: [] },
        { path: SOG, aggregate: 'sma', parameter: ['3'] },
      ])
    );

    // Two distinct, correctly-labelled columns (pathSpecKey keeps them apart).
    expect(res.values.map(v => v.method)).to.deep.equal(['average', 'sma']);
    expect(res.values.map(v => v.path)).to.deep.equal([SOG, SOG]);

    const avg = res.data.map(r => r[1] as number);
    const smaCol = res.data.map(r => r[2] as number);
    expect(avg).to.have.lengthOf(SAMPLES.length);

    // Bare average reproduces the per-bucket samples...
    avg.forEach((v, i) => expect(v).to.be.closeTo(SAMPLES[i], 1e-9));
    // ...and sma:3 is the trailing mean-of-3 of that series.
    [1, 1.5, 2, 3, 4].forEach((exp, i) =>
      expect(smaCol[i]).to.be.closeTo(exp, 1e-9)
    );
    // Crucially, sma is NOT just the bare average (the bug this fixes).
    expect(smaCol).to.not.deep.equal(avg);
  });

  it('ema returns an exponential average distinct from bare average', async () => {
    await writeSogFixture(SAMPLES);

    const res = await provider.getValues(
      query([
        { path: SOG, aggregate: 'average', parameter: [] },
        { path: SOG, aggregate: 'ema', parameter: ['0.5'] },
      ])
    );

    const avg = res.data.map(r => r[1] as number);
    const emaCol = res.data.map(r => r[2] as number);
    // alpha 0.5 over [1,2,3,4,5]: 1, 1.5, 2.25, 3.125, 4.0625
    [1, 1.5, 2.25, 3.125, 4.0625].forEach((exp, i) =>
      expect(emaCol[i]).to.be.closeTo(exp, 1e-9)
    );
    expect(emaCol).to.not.deep.equal(avg);
  });

  it('empty parameter falls back to the spec default (sma -> 5 samples)', async () => {
    await writeSogFixture(SAMPLES);

    const res = await provider.getValues(
      query([{ path: SOG, aggregate: 'sma', parameter: [] }])
    );
    const smaCol = res.data.map(r => r[1] as number);
    // window 5 over [1,2,3,4,5]: [1, 1.5, 2, 2.5, 3]
    [1, 1.5, 2, 2.5, 3].forEach((exp, i) =>
      expect(smaCol[i]).to.be.closeTo(exp, 1e-9)
    );
  });

  it('smooths position longitude on the circle across the ±180° antimeridian', async () => {
    // Three positions straddling the antimeridian, one per 60s bucket.
    const lons = [179, -179, 178];
    lons.forEach((lon, i) => {
      buffer.insert(
        makePositionRecord(
          STORED_CONTEXT,
          10,
          lon,
          `2024-06-01T10:0${i}:00.000Z`
        )
      );
    });
    await exportService.exportDayToParquet(DAY);

    const res = await provider.getValues(
      query([{ path: 'navigation.position', aggregate: 'sma', parameter: ['2'] }])
    );

    // Provider returns position as [longitude, latitude].
    const rows = res.data.map(r => r[1] as [number, number]);
    expect(rows).to.have.lengthOf(3);

    // Bucket 2 = circular mean of 179° and −179°, which must land near ±180°,
    // NOT the linear-average 0° that a naive window would produce.
    expect(Math.abs(rows[1][0])).to.be.greaterThan(170);
    // Latitude is constant 10 and smoothed linearly.
    rows.forEach(([, lat]) => expect(lat).to.be.closeTo(10, 1e-9));
    // All longitudes stay in the canonical (−180, 180] range.
    rows.forEach(([lon]) => {
      expect(lon).to.be.greaterThan(-180 - 1e-9);
      expect(lon).to.be.at.most(180 + 1e-9);
    });
  });
});
