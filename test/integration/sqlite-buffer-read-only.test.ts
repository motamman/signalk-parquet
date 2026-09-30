/**
 * A read-only SQLiteBuffer reads but cannot write, and sees tables a writer
 * created before it opened.
 *
 * Nothing in the plugin opens one today; the buffer worker did, and would again
 * (see devdocs/BUFFER_WORKER_REMOVED.md).
 */
import { expect } from 'chai';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs-extra';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { makeScalarRecord } from './helpers/records';

const CONTEXT = 'vessels.urn:mrn:signalk:uuid:readonly';
const SOG = 'navigation.speedOverGround';
const DEPTH = 'environment.depth.belowKeel';
const FROM = '2026-01-01T00:00:00.000Z';
const TO = '2026-01-02T00:00:00.000Z';

describe('read-only SQLite buffer', () => {
  let dataDir: string;
  let dbPath: string;

  beforeEach(async () => {
    dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'sk-readonly-'));
    dbPath = path.join(dataDir, 'buffer.db');
  });

  afterEach(async () => {
    await fs.remove(dataDir);
  });

  it('opens read-only, so it cannot contend for the write lock', () => {
    const seed = new SQLiteBuffer({ dbPath });
    try {
      seed.insert(makeScalarRecord(CONTEXT, SOG, 1, FROM));
    } finally {
      seed.close();
    }

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
      writer.insert(makeScalarRecord(CONTEXT, DEPTH, 3, FROM));
    } finally {
      writer.close();
    }
    const second = new SQLiteBuffer({ dbPath, readOnly: true });
    try {
      expect(second.getTableSchema(DEPTH)).to.be.an('array');
    } finally {
      second.close();
    }
  });
});
