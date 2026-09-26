/**
 * Staging through the buffer worker must produce exactly the table staging
 * in-process produces.
 *
 * This is the equivalence that makes the worker a change of *where* the SQLite
 * call runs and nothing else. It is asserted against the temp table's contents
 * rather than against the rows handed to the appender, because the table is what
 * the federated SQL reads: a value that survives the thread boundary but lands
 * in DuckDB as the wrong type, or as 0 where it should be NULL, would change
 * query answers while every row count still matched.
 */
import { expect } from 'chai';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs-extra';
import { DuckDBConnection } from '@duckdb/node-api';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { BufferWorkerClient } from '../../src/utils/buffer-worker-client';
import { stageBufferTable } from '../../src/utils/buffer-staging';
import { makeScalarRecord, makePositionRecord } from './helpers/records';

const CONTEXT = 'vessels.urn:mrn:signalk:uuid:stageworker';
const SOG = 'navigation.speedOverGround';
const POSITION = 'navigation.position';
const BASE = Date.parse('2026-01-01T00:00:00.000Z');
const FROM = '2026-01-01T00:00:00.000Z';
const TO = '2026-01-02T00:00:00.000Z';

const WORKER = path.join(__dirname, '..', '..', 'src', 'buffer-worker.ts');
const EXEC_ARGV = ['-r', 'tsx/cjs'];

describe('staging through the buffer worker', function () {
  this.timeout(60000);

  let dataDir: string;
  let dbPath: string;
  let buffer: SQLiteBuffer;
  let client: BufferWorkerClient | undefined;

  beforeEach(async () => {
    dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'sk-stageworker-'));
    dbPath = path.join(dataDir, 'buffer.db');
    buffer = new SQLiteBuffer({ dbPath });
    await DuckDBPool.initialize();
  });

  afterEach(async () => {
    await client?.close();
    client = undefined;
    await DuckDBPool.shutdown();
    if (buffer.isOpen()) buffer.close();
    await fs.remove(dataDir);
  });

  /** Everything in a staged temp table, ordered so two runs compare directly. */
  async function dump(
    connection: DuckDBConnection,
    table: string
  ): Promise<unknown[][]> {
    const result = await connection.runAndReadAll(
      `SELECT * FROM ${table} ORDER BY signalk_timestamp, id`
    );
    return result.getRows() as unknown[][];
  }

  async function stage(
    signalkPath: string,
    worker?: BufferWorkerClient
  ): Promise<unknown[][]> {
    const connection = await DuckDBPool.getConnection();
    const table = await stageBufferTable(
      connection,
      buffer,
      CONTEXT,
      signalkPath,
      FROM,
      TO,
      undefined,
      worker
    );
    expect(table, 'a window with rows must stage a table').to.be.a('string');
    return dump(connection, table as string);
  }

  async function startClient(): Promise<BufferWorkerClient> {
    client = new BufferWorkerClient({
      dbPath,
      workerPath: WORKER,
      workerExecArgv: EXEC_ARGV,
    });
    await client.start();
    return client;
  }

  it('stages a scalar path identically in-process and through the worker', async () => {
    buffer.insertBatch(
      Array.from({ length: 2500 }, (_, i) =>
        makeScalarRecord(
          CONTEXT,
          SOG,
          i / 7,
          new Date(BASE + i * 400).toISOString()
        )
      )
    );

    const inProcess = await stage(SOG);
    const viaWorker = await stage(SOG, await startClient());

    expect(viaWorker).to.have.lengthOf(2500);
    expect(viaWorker).to.deep.equal(inProcess);
  });

  it('stages an object path with its nulls identically', async () => {
    for (let i = 0; i < 1500; i++) {
      buffer.insert(
        makePositionRecord(
          CONTEXT,
          41.5 + i / 100000,
          -71.3 - i / 100000,
          new Date(BASE + i * 400).toISOString()
        )
      );
    }

    const inProcess = await stage(POSITION);
    const viaWorker = await stage(POSITION, await startClient());

    expect(viaWorker).to.have.lengthOf(1500);
    expect(viaWorker).to.deep.equal(inProcess);
    // The columns a position row leaves empty must be NULL on both paths, not
    // 0 or '': a filter like `meta IS NULL` has to mean the same thing.
    const nullCounts = async (worker?: BufferWorkerClient) => {
      const connection = await DuckDBPool.getConnection();
      const table = await stageBufferTable(
        connection,
        buffer,
        CONTEXT,
        POSITION,
        FROM,
        TO,
        undefined,
        worker
      );
      const r = await connection.runAndReadAll(
        `SELECT COUNT(*) FILTER (WHERE meta IS NULL) AS meta_nulls,
                COUNT(*) FILTER (WHERE export_batch_id IS NULL) AS batch_nulls
         FROM ${table as string}`
      );
      return r.getRows()[0];
    };
    expect(await nullCounts(client)).to.deep.equal(await nullCounts());
  });

  it('returns null for a path with no buffer table, both ways', async () => {
    buffer.insert(makeScalarRecord(CONTEXT, SOG, 1, FROM));
    await startClient();

    const connection = await DuckDBPool.getConnection();
    for (const worker of [undefined, client]) {
      const table = await stageBufferTable(
        connection,
        buffer,
        CONTEXT,
        'no.such.path',
        FROM,
        TO,
        undefined,
        worker
      );
      expect(table).to.equal(null);
    }
  });

  it('stages a path first recorded after the worker opened', async () => {
    buffer.insert(makeScalarRecord(CONTEXT, SOG, 1, FROM));
    const worker = await startClient();
    // The worker's read-only connection loaded its table map with SOG only.
    expect(await worker.getTableSchema(POSITION)).to.equal(null);

    // The writer creates a new path's table while the worker is up.
    for (let i = 0; i < 20; i++) {
      buffer.insert(
        makePositionRecord(
          CONTEXT,
          41.5 + i / 1000,
          -71.3,
          new Date(BASE + i * 1000).toISOString()
        )
      );
    }

    const inProcess = await stage(POSITION);
    const viaWorker = await stage(POSITION, worker);
    expect(viaWorker).to.have.lengthOf(20);
    expect(viaWorker).to.deep.equal(inProcess);
  });

  it('fails the staging rather than answering short when the worker dies', async () => {
    buffer.insertBatch(
      Array.from({ length: 5000 }, (_, i) =>
        makeScalarRecord(
          CONTEXT,
          SOG,
          i,
          new Date(BASE + i * 400).toISOString()
        )
      )
    );
    const dying = await startClient();

    const connection = await DuckDBPool.getConnection();
    // Kill the worker while the scan is in flight: five pages are needed, so a
    // close on the next tick lands mid-scan.
    setTimeout(() => void dying.close(), 5);

    let threw: Error | undefined;
    try {
      await stageBufferTable(
        connection,
        buffer,
        CONTEXT,
        SOG,
        FROM,
        TO,
        undefined,
        dying
      );
    } catch (error) {
      threw = error as Error;
    }
    client = undefined;
    expect(
      threw,
      'a dead worker must fail the query, not truncate it'
    ).to.be.an('error');
  });
});
