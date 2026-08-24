/**
 * Close-state invariants for the SQLite write buffer.
 *
 * The buffer backs the live ingestion path, so a write that lands after
 * close() would hit a database the plugin believes is gone. close() therefore
 * marks the buffer closed *before* handing off to db.close(), and every write
 * entry point checks that flag. These tests pin both halves of that contract:
 * the flag flips, and the batch path honours it the same way the single-record
 * path does.
 */
import { expect } from 'chai';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs-extra';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { makeScalarRecord } from './helpers/records';

const CONTEXT = 'vessels.urn:mrn:signalk:uuid:test';

describe('SQLiteBuffer close-state guards', () => {
  let dataDir: string;
  let buffer: SQLiteBuffer;

  beforeEach(async () => {
    dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'sk-buffer-close-'));
    buffer = new SQLiteBuffer({ dbPath: path.join(dataDir, 'buffer.db') });
  });

  afterEach(async () => {
    if (buffer.isOpen()) {
      buffer.close();
    }
    await fs.remove(dataDir);
  });

  function record(value: number, isoTime = '2026-01-01T00:00:00.000Z') {
    return makeScalarRecord(
      CONTEXT,
      'environment.depth.belowKeel',
      value,
      isoTime
    );
  }

  it('accepts writes while open', () => {
    buffer.insert(record(1));
    buffer.insertBatch([record(2), record(3)]);

    expect(buffer.isOpen()).to.be.true;
  });

  it('reports closed after close()', () => {
    buffer.close();

    expect(buffer.isOpen()).to.be.false;
  });

  it('rejects a single insert after close()', () => {
    buffer.close();

    expect(() => buffer.insert(record(1))).to.throw(/closed/i);
  });

  // insertBatch used to skip the _open check that every other write path has,
  // so a batch could open a transaction against an already-closed database.
  it('rejects a batch insert after close()', () => {
    buffer.close();

    expect(() => buffer.insertBatch([record(1)])).to.throw(/closed/i);
  });

  it('rejects an empty batch after close() rather than silently succeeding', () => {
    buffer.close();

    expect(() => buffer.insertBatch([])).to.throw(/closed/i);
  });
});
