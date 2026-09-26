/**
 * The buffer worker's read path, running as a real worker thread.
 *
 * The contract the staging code will depend on: the rows a scan yields are
 * exactly the rows an in-process read of the same window yields, in the same
 * order; the worker does not read ahead of the consumer; a consumer that stops
 * stops the worker; and a scan that cannot finish fails loudly rather than
 * ending early, because a query answered with part of the buffer is wrong in a
 * way nobody notices.
 *
 * The worker is started from TypeScript under the tsx loader the suite already
 * runs on, as the compaction worker test does.
 */
import { expect } from 'chai';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs-extra';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { Worker } from 'node:worker_threads';
import { BufferWorkerClient } from '../../src/utils/buffer-worker-client';
import { SCAN_PAGES_IN_FLIGHT } from '../../src/utils/buffer-worker-protocol';
import { makeScalarRecord, makePositionRecord } from './helpers/records';

const CONTEXT = 'vessels.urn:mrn:signalk:uuid:worker';
const OTHER = 'vessels.urn:mrn:signalk:uuid:other';
const SOG = 'navigation.speedOverGround';
const POSITION = 'navigation.position';
const POSITION_SCALAR = 'environment.depth.belowKeel';
const BASE = Date.parse('2026-01-01T00:00:00.000Z');
const FROM = '2026-01-01T00:00:00.000Z';
const TO = '2026-01-02T00:00:00.000Z';

const WORKER = path.join(__dirname, '..', '..', 'src', 'buffer-worker.ts');
const EXEC_ARGV = ['-r', 'tsx/cjs'];

describe('buffer worker read path', function () {
  this.timeout(30000);

  let dataDir: string;
  let dbPath: string;
  let client: BufferWorkerClient | undefined;

  beforeEach(async () => {
    dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'sk-bufworker-'));
    dbPath = path.join(dataDir, 'buffer.db');
  });

  afterEach(async () => {
    await client?.close();
    client = undefined;
    await fs.remove(dataDir);
  });

  /** Fill the buffer in a separate connection and close it before the worker opens. */
  function seed(fill: (buffer: SQLiteBuffer) => void): void {
    const buffer = new SQLiteBuffer({ dbPath });
    try {
      fill(buffer);
    } finally {
      buffer.close();
    }
  }

  /** The same window read in-process, to compare a scan against. */
  function readDirect(
    signalkPath: string,
    context: string
  ): Array<Record<string, unknown>> {
    const buffer = new SQLiteBuffer({ dbPath });
    try {
      const all: Array<Record<string, unknown>> = [];
      let after = null as
        Parameters<typeof buffer.getRowsForFederation>[4] | null;
      for (;;) {
        const rows = buffer.getRowsForFederation(
          signalkPath,
          context,
          FROM,
          TO,
          after,
          500
        );
        if (rows.length === 0) break;
        all.push(...rows);
        const last = rows[rows.length - 1];
        after = {
          signalkTimestamp: String(last.signalk_timestamp),
          id: Number(last.id),
        };
        if (rows.length < 500) break;
      }
      return all;
    } finally {
      buffer.close();
    }
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

  /** Every row a scan yields, as plain objects keyed by column name. */
  async function scanAll(
    c: BufferWorkerClient,
    signalkPath: string,
    context: string,
    pageRows: number
  ): Promise<{
    rows: Array<Record<string, unknown>>;
    pages: number;
  }> {
    const rows: Array<Record<string, unknown>> = [];
    let pages = 0;
    for await (const page of c.scan({
      signalkPath,
      context,
      fromIso: FROM,
      toIso: TO,
      pageRows,
    })) {
      pages += 1;
      for (let i = 0; i < page.rowCount; i++) {
        const row: Record<string, unknown> = {};
        for (const col of page.columns) row[col.name] = col.at(i);
        rows.push(row);
      }
    }
    return { rows, pages };
  }

  it('opens read-only, so it cannot contend for the write lock', async () => {
    seed(b => b.insert(makeScalarRecord(CONTEXT, SOG, 1, FROM)));
    const readOnly = new SQLiteBuffer({ dbPath, readOnly: true });
    try {
      // Reads work.
      expect(
        readOnly.getRowsForFederation(SOG, CONTEXT, FROM, TO, null, 10)
      ).to.have.lengthOf(1);
      // Writes do not, and say so.
      expect(() =>
        readOnly.insert(makeScalarRecord(CONTEXT, SOG, 2, FROM))
      ).to.throw(/readonly|read-only/i);
    } finally {
      readOnly.close();
    }

    // And a table created after the reader opened is still visible to a reader
    // opened afterwards, because the writer built it and its indexes.
    const writer = new SQLiteBuffer({ dbPath });
    try {
      writer.insert(makeScalarRecord(CONTEXT, POSITION_SCALAR, 3, FROM));
    } finally {
      writer.close();
    }
    const second = new SQLiteBuffer({ dbPath, readOnly: true });
    try {
      expect(second.getTableSchema(POSITION_SCALAR)).to.be.an('array');
    } finally {
      second.close();
    }
  });

  it('starts, answers a ping and reports a path schema', async () => {
    seed(b => b.insert(makeScalarRecord(CONTEXT, SOG, 1, FROM)));
    const c = await startClient();
    await c.ping();

    const schema = await c.getTableSchema(SOG);
    expect(schema).to.be.an('array');
    expect((schema ?? []).map(col => col.name)).to.include.members([
      'id',
      'context',
      'signalk_timestamp',
      'value',
      'exported',
    ]);
    expect(await c.getTableSchema('no.such.path')).to.equal(null);
  });

  it('yields the same rows as an in-process read, at every page size', async () => {
    seed(b =>
      b.insertBatch(
        Array.from({ length: 250 }, (_, i) =>
          makeScalarRecord(
            CONTEXT,
            SOG,
            i,
            new Date(BASE + i * 1000).toISOString()
          )
        )
      )
    );
    const expected = readDirect(SOG, CONTEXT);
    expect(expected).to.have.lengthOf(250);

    const c = await startClient();
    for (const pageRows of [1, 7, 100, 250, 1000]) {
      const { rows } = await scanAll(c, SOG, CONTEXT, pageRows);
      expect(
        rows.map(r => Number(r.id)),
        `page size ${pageRows}`
      ).to.deep.equal(expected.map(r => Number(r.id)));
      expect(rows.map(r => r.value)).to.deep.equal(
        expected.map(r => String(r.value))
      );
    }
  });

  it('carries an object path with its flattened columns and nulls', async () => {
    seed(b => {
      b.insert(makePositionRecord(CONTEXT, 41.5, -71.3, FROM));
      b.insert(
        makePositionRecord(
          CONTEXT,
          41.6,
          -71.4,
          new Date(BASE + 1000).toISOString()
        )
      );
    });
    const c = await startClient();
    const { rows } = await scanAll(c, POSITION, CONTEXT, 10);
    expect(rows).to.have.lengthOf(2);
    expect(rows[0].value_latitude).to.equal(41.5);
    expect(rows[1].value_longitude).to.equal(-71.4);
    // An object table carries value_json and the flattened value_* columns and
    // has no `value` column at all, so the page must not invent one.
    expect(Object.keys(rows[0])).to.include('value_json');
    expect(Object.keys(rows[0])).to.not.include('value');
    expect(JSON.parse(String(rows[0].value_json))).to.deep.equal({
      latitude: 41.5,
      longitude: -71.3,
    });
    // Columns a row has no value for do arrive as null, not as 0 or ''.
    expect(rows[0].export_batch_id).to.equal(null);
    expect(rows[0].meta).to.equal(null);
  });

  it('yields nothing for a path with no table', async () => {
    seed(b => b.insert(makeScalarRecord(CONTEXT, SOG, 1, FROM)));
    const c = await startClient();
    const { rows, pages } = await scanAll(c, 'no.such.path', CONTEXT, 10);
    expect(rows).to.have.lengthOf(0);
    expect(pages).to.equal(0);
  });

  it('scans one context only', async () => {
    seed(b => {
      b.insert(makeScalarRecord(CONTEXT, SOG, 1, FROM));
      b.insert(makeScalarRecord(OTHER, SOG, 2, FROM));
    });
    const c = await startClient();
    const mine = await scanAll(c, SOG, CONTEXT, 10);
    expect(mine.rows).to.have.lengthOf(1);
    expect(mine.rows[0].context).to.equal(CONTEXT);
  });

  it('sends no more than its credit before an ack, then resumes', async () => {
    seed(b =>
      b.insertBatch(
        Array.from({ length: 100 }, (_, i) =>
          makeScalarRecord(
            CONTEXT,
            SOG,
            i,
            new Date(BASE + i * 1000).toISOString()
          )
        )
      )
    );

    // Driven at the protocol level rather than through the client, because
    // what is being tested is that the worker stops on its own: a test that
    // consumes pages through the client would pass with no flow control at all.
    const worker = new Worker(WORKER, {
      workerData: { dbPath },
      execArgv: EXEC_ARGV,
    });
    try {
      const pages: number[] = [];
      let ended = false;
      await new Promise<void>(resolve => {
        worker.once('message', () => resolve());
      });
      worker.on('message', (msg: { type: string; scanId?: number }) => {
        if (msg.type === 'page') pages.push(1);
        if (msg.type === 'end') ended = true;
      });

      worker.postMessage({
        op: 'openScan',
        id: 1,
        signalkPath: SOG,
        context: CONTEXT,
        fromIso: FROM,
        toIso: TO,
        pageRows: 10,
      });

      // Ten pages are available and no ack is sent. The worker must stop at
      // its credit: the page in hand plus SCAN_PAGES_IN_FLIGHT ahead of it.
      await new Promise(r => setTimeout(r, 300));
      expect(pages.length).to.equal(1 + SCAN_PAGES_IN_FLIGHT);
      expect(ended, 'the scan must not have run to the end').to.equal(false);

      // One ack releases exactly one more page.
      const before = pages.length;
      worker.postMessage({ op: 'ackScan', scanId: 1 });
      await new Promise(r => setTimeout(r, 300));
      expect(pages.length).to.equal(before + 1);
      expect(ended).to.equal(false);

      // Acking freely drains the rest and ends the scan.
      for (let i = 0; i < 20; i++) {
        worker.postMessage({ op: 'ackScan', scanId: 1 });
      }
      await new Promise(r => setTimeout(r, 500));
      expect(pages.length).to.equal(10);
      expect(ended, 'the scan must end after the last page').to.equal(true);
    } finally {
      await worker.terminate();
    }
  });

  it('stops the worker when the consumer abandons the scan', async () => {
    seed(b =>
      b.insertBatch(
        Array.from({ length: 100 }, (_, i) =>
          makeScalarRecord(
            CONTEXT,
            SOG,
            i,
            new Date(BASE + i * 1000).toISOString()
          )
        )
      )
    );
    const c = await startClient();
    let seen = 0;
    for await (const page of c.scan({
      signalkPath: SOG,
      context: CONTEXT,
      fromIso: FROM,
      toIso: TO,
      pageRows: 10,
    })) {
      seen += page.rowCount;
      break;
    }
    expect(seen).to.equal(10);

    // The worker is still healthy and will serve another scan.
    await c.ping();
    const { rows } = await scanAll(c, SOG, CONTEXT, 100);
    expect(rows).to.have.lengthOf(100);
  });

  it('rejects in-flight work when the worker goes away', async () => {
    seed(b =>
      b.insertBatch(
        Array.from({ length: 100 }, (_, i) =>
          makeScalarRecord(
            CONTEXT,
            SOG,
            i,
            new Date(BASE + i * 1000).toISOString()
          )
        )
      )
    );
    const c = await startClient();
    const iterator = c.scan({
      signalkPath: SOG,
      context: CONTEXT,
      fromIso: FROM,
      toIso: TO,
      pageRows: 10,
    });
    await iterator.next();

    await c.close();
    client = undefined;

    let threw: Error | undefined;
    try {
      for await (const _page of iterator) {
        // Draining must not quietly stop at the rows already received.
      }
    } catch (error) {
      threw = error as Error;
    }
    expect(threw, 'a dead worker must fail the scan, not end it').to.be.an(
      'error'
    );
  });
});
