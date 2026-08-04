/**
 * Regression tests for schema-cache behavior under runtime reconfiguration.
 *
 * The component-schema cache is keyed by data directory as well as
 * context/path: after a plugin reconfigure switches stores, a cache entry
 * discovered in the old directory must not answer for the new one (the
 * History API would then aggregate object components that don't exist
 * there). Also pins that real read failures (corrupt parquet) propagate to
 * the caller instead of returning null — null means "scalar path", and a
 * corrupt store must not be silently misread as one.
 */
import { expect } from 'chai';
import * as path from 'path';
import * as fs from 'fs-extra';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import {
  getPathComponentSchema,
  clearSchemaCache,
} from '../../src/utils/schema-cache';
import { HivePathBuilder } from '../../src/utils/hive-path-builder';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makePositionRecord } from './helpers/records';
import type { Context, Path } from '@signalk/server-api';

const SELF_ID = 'schemacacheself';
const STORED_CONTEXT = `vessels.${SELF_ID}`;
const DAY = new Date('2024-06-01T00:00:00.000Z');
const POSITION = 'navigation.position';

describe('schema cache: data-directory scoping', function () {
  this.timeout(30000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;

  beforeEach(async () => {
    host = createFakeSignalK({ selfId: SELF_ID });
    buffer = new SQLiteBuffer({ dbPath: path.join(host.dataDir, 'buffer.db') });
    await DuckDBPool.initialize();
    clearSchemaCache();

    // Object-path fixture (position has value_latitude/value_longitude
    // columns) written through the real export pipeline.
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
    buffer.insert(
      makePositionRecord(STORED_CONTEXT, 42.1, -70.5, '2024-06-01T10:00:00.000Z')
    );
    buffer.insert(
      makePositionRecord(STORED_CONTEXT, 42.2, -70.6, '2024-06-01T10:01:00.000Z')
    );
    await exportService.exportDayToParquet(DAY);
  });

  afterEach(async () => {
    clearSchemaCache();
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  it("does not serve one directory's schema for another", async () => {
    const schemaA = await getPathComponentSchema(
      host.dataDir,
      STORED_CONTEXT as Context,
      POSITION as Path
    );
    expect(schemaA, 'fixture dir should yield an object schema').to.not.equal(
      null
    );
    expect(Array.from(schemaA!.components.keys())).to.include.members([
      'latitude',
      'longitude',
    ]);

    // A different (empty) data directory queried within the cache TTL must
    // miss the first directory's entry and report no schema.
    const emptyDir = path.join(host.dataDir, 'other-store');
    await fs.ensureDir(emptyDir);
    const schemaB = await getPathComponentSchema(
      emptyDir,
      STORED_CONTEXT as Context,
      POSITION as Path
    );
    expect(schemaB, "empty dir must not reuse dir A's cached schema").to.equal(
      null
    );
  });

  it('keeps colon-bearing dataDir/context tuples on distinct cache keys', async () => {
    // Contexts routinely contain colons (vessel URNs). Under the old
    // ':'-joined cache key, (dir, 'vessels.urn:mrn:imo:mmsi:N') and
    // (dir + ':vessels.urn', 'mrn:imo:mmsi:N') collapsed to the same key,
    // so the second tuple — a directory that doesn't even exist — was
    // answered from the first tuple's cached schema.
    const colonContext = 'vessels.urn:mrn:imo:mmsi:123456789';
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
    buffer.insert(
      makePositionRecord(colonContext, 42.1, -70.5, '2024-06-01T11:00:00.000Z')
    );
    await exportService.exportDayToParquet(DAY);

    const schemaA = await getPathComponentSchema(
      host.dataDir,
      colonContext as Context,
      POSITION as Path
    );
    expect(schemaA, 'colon-context fixture should yield a schema').to.not.equal(
      null
    );

    // Structurally different tuple whose ':'-join is identical to tuple A.
    // The directory doesn't exist, so the only way to get a schema back is
    // a cache-key collision.
    const schemaB = await getPathComponentSchema(
      `${host.dataDir}:vessels.urn`,
      'mrn:imo:mmsi:123456789' as Context,
      POSITION as Path
    );
    expect(
      schemaB,
      "colliding colon-join tuple must not reuse A's cached schema"
    ).to.equal(null);
  });

  it('propagates non-missing-file errors instead of returning null', async () => {
    const hive = new HivePathBuilder();
    const corruptStore = path.join(host.dataDir, 'corrupt-store');
    const dayDir = hive.buildPath(
      corruptStore,
      'raw',
      STORED_CONTEXT,
      POSITION,
      DAY
    );
    await fs.ensureDir(dayDir);
    await fs.writeFile(path.join(dayDir, 'bad.parquet'), 'not parquet at all');

    let thrown: unknown = null;
    try {
      await getPathComponentSchema(
        corruptStore,
        STORED_CONTEXT as Context,
        POSITION as Path
      );
    } catch (err) {
      thrown = err;
    }
    expect(thrown, 'corrupt parquet should reject, not read as scalar').to.not
      .equal(null);
  });
});
