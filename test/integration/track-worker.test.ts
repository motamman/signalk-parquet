/**
 * The Track API through its forked worker (track-worker.ts) answers exactly
 * what the in-process provider answers, against the same store: one day of
 * positions exported to parquet and a later day left in the buffer, which
 * the worker reads through its own read-only connection. The worker is forked
 * as TypeScript under the tsx loader the suite already runs on.
 */
import { expect } from 'chai';
import * as path from 'path';
import { Temporal } from '@js-temporal/polyfill';
import type { Context, Path } from '@signalk/server-api';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { clearFileListCache } from '../../src/utils/context-discovery';
import { TrackProvider, TracksRequest } from '../../src/track-provider';
import {
  TrackWorkerClient,
  TrackWorkerClosedError,
  TrackWorkerGoneError,
  WorkerTrackApi,
} from '../../src/utils/track-worker-client';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makeScalarRecord, makePositionRecord } from './helpers/records';

const SELF_ID = 'trackworkerself';
const STORED_CONTEXT = `vessels.${SELF_ID}` as Context;
const SOG = 'navigation.speedOverGround' as Path;
const DEPTH = 'environment.depth.belowTransducer' as Path;

const WHOLE_DAY: TracksRequest = {
  from: Temporal.Instant.from('2024-06-01T00:00:00Z'),
  to: Temporal.Instant.from('2024-06-02T00:00:00Z'),
};
const BOTH_DAYS: TracksRequest = {
  from: '2024-06-01T00:00:00Z',
  to: '2024-06-03T00:00:00Z',
};

describe('Track API through the forked worker', function () {
  this.timeout(60000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;
  let inProcess: TrackProvider;
  let client: TrackWorkerClient;
  let viaWorker: WorkerTrackApi;

  const providerApp = {
    debug: () => {},
    error: () => {},
    getMetadata: () => undefined,
    getSelfPath: (key: string) => (key === 'name' ? 'Fixture' : undefined),
    selfId: SELF_ID,
    selfContext: STORED_CONTEXT,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } as any;

  const recordLeg = (
    day: string,
    lat: number,
    lon: number,
    times: string[],
    sogBase: number
  ): void => {
    times.forEach((hhmm, i) => {
      const iso = `${day}T${hhmm}:00.000Z`;
      buffer.insert(makePositionRecord(STORED_CONTEXT, lat, lon + i * 0.001, iso));
      buffer.insert(
        makeScalarRecord(STORED_CONTEXT, SOG, sogBase + i, `${day}T${hhmm}:00.200Z`)
      );
    });
  };

  before(async () => {
    host = createFakeSignalK({ selfId: SELF_ID });
    buffer = new SQLiteBuffer({ dbPath: path.join(host.dataDir, 'buffer.db') });
    // This process's pool on DuckDB's default home, as every other suite opens
    // it, not on the temp data directory: an extension this process loads
    // stays loaded after the instance closes, and Windows refuses to delete a
    // loaded DLL, so the directory could not be removed afterwards (EPERM on
    // spatial.duckdb_extension, Windows CI). The worker keeps the data
    // directory as its DuckDB home; it has exited before cleanup runs.
    await DuckDBPool.initialize();
    clearFileListCache();

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
    recordLeg('2024-06-01', 47.5, 9.4, ['10:00', '10:01', '10:02', '10:03', '10:04'], 3);
    recordLeg('2024-06-01', 47.6, 9.6, ['12:00', '12:01', '12:02'], 10);
    await exportService.exportDayToParquet(new Date('2024-06-01T00:00:00.000Z'));
    recordLeg('2024-06-02', 47.7, 9.8, ['08:00', '08:01', '08:02'], 20);

    inProcess = new TrackProvider(SELF_ID, host.dataDir, providerApp, () => {}, buffer);
    client = new TrackWorkerClient({
      dataDir: host.dataDir,
      dbPath: buffer.getDbPath(),
      selfId: SELF_ID,
      workerPath: path.join(__dirname, '..', '..', 'src', 'track-worker.ts'),
      workerExecArgv: ['-r', 'tsx/cjs'],
    });
    await client.start();
    viaWorker = new WorkerTrackApi(inProcess, () => client, providerApp, SELF_ID);
  });

  after(async () => {
    await client?.close();
    clearFileListCache();
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  const queries: Array<[string, TracksRequest]> = [
    ['a whole day with times', { ...WHOLE_DAY, times: true }],
    ['a co-recorded property', { ...WHOLE_DAY, properties: [SOG] }],
    ['a bounding box', { ...WHOLE_DAY, bbox: [9.39, 47.49, 9.41, 47.51] }],
    ['an explicit Temporal resolution', { ...WHOLE_DAY, resolution: Temporal.Duration.from('PT2M') }],
    ['a duration measured back from the end', { duration: 'P1D', to: WHOLE_DAY.to }],
    ['parquet federated with the buffer', { ...BOTH_DAYS, times: true, simplify: true }],
  ];

  for (const [name, query] of queries) {
    it(`answers ${name} exactly as in-process`, async () => {
      expect(client.isAlive()).to.equal(true);
      const expected = await inProcess.getTracks(query);
      expect(expected.features, 'the fixture answers this query').to.not.be.empty;
      expect(await viaWorker.getTracks(query)).to.deep.equal(expected);
    });
  }

  it('lists contexts exactly as in-process, the buffer included', async () => {
    for (const query of [
      WHOLE_DAY,
      { ...WHOLE_DAY, bbox: [1, 1, 2, 2] } as TracksRequest,
      { from: '2024-06-02T00:00:00Z', to: '2024-06-03T00:00:00Z' },
    ]) {
      expect(await viaWorker.getTrackContexts(query)).to.deep.equal(
        await inProcess.getTrackContexts(query)
      );
    }
  });

  it('refuses an invalid request with the provider\'s own error', async () => {
    let message = '';
    try {
      await viaWorker.getTracks({ ...WHOLE_DAY, bbox: [-200, 47, 10, 48] });
    } catch (err) {
      message = (err as Error).message;
    }
    expect(message).to.match(/Invalid bbox/);
  });

  it('sees a buffer path first recorded after the worker opened', async () => {
    // The worker's read-only connection loaded its table map at open; this
    // table did not exist then.
    ['08:00', '08:01', '08:02'].forEach((hhmm, i) => {
      buffer.insert(
        makeScalarRecord(STORED_CONTEXT, DEPTH, 5 + i, `2024-06-02T${hhmm}:00.100Z`)
      );
    });
    const query = { ...BOTH_DAYS, properties: [DEPTH] };
    const res = await viaWorker.getTracks(query);
    expect(res.features[0].properties.appliedProperties).to.deep.equal([DEPTH]);
    expect(res).to.deep.equal(await inProcess.getTracks(query));
  });

  it('fails and kills a worker that does not answer, then answers in-process', async () => {
    const silent = new TrackWorkerClient({
      dataDir: host.dataDir,
      selfId: SELF_ID,
      workerPath: path.join(__dirname, 'helpers', 'silent-track-worker.ts'),
      workerExecArgv: ['-r', 'tsx/cjs'],
      requestTimeoutMs: 500,
    });
    try {
      await silent.start();
      const viaSilent = new WorkerTrackApi(inProcess, () => silent, providerApp, SELF_ID);
      let error: unknown;
      try {
        await viaSilent.getTracks(WHOLE_DAY);
      } catch (err) {
        error = err;
      }
      expect(error).to.be.instanceOf(TrackWorkerGoneError);
      expect((error as Error).message).to.match(/timed out/);
      expect(silent.isAlive()).to.equal(false);
      const query = { ...WHOLE_DAY, properties: [SOG] };
      expect(await viaSilent.getTracks(query)).to.deep.equal(
        await inProcess.getTracks(query)
      );
    } finally {
      await silent.close();
    }
  });

  it('ends a startup that close() interrupts as closed, not failed', async () => {
    // The plugin stopping while its worker starts: nothing failed, and the
    // plugin logs nothing for it. The silent worker answers init with ready,
    // which can reach this side after close(); it must not count.
    const starting = new TrackWorkerClient({
      dataDir: host.dataDir,
      selfId: SELF_ID,
      workerPath: path.join(__dirname, 'helpers', 'silent-track-worker.ts'),
      workerExecArgv: ['-r', 'tsx/cjs'],
    });
    const started = starting.start();
    await starting.close();
    let error: unknown;
    try {
      await started;
    } catch (err) {
      error = err;
    }
    expect(error).to.be.instanceOf(TrackWorkerClosedError);
    expect(starting.isAlive()).to.equal(false);
  });

  it('answers in-process once the worker is closed', async () => {
    await client.close();
    expect(client.isAlive()).to.equal(false);
    const query = { ...WHOLE_DAY, properties: [SOG] };
    expect(await viaWorker.getTracks(query)).to.deep.equal(
      await inProcess.getTracks(query)
    );
  });
});
