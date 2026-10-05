/**
 * The daily cloud upload asks the bucket only about the directories that
 * hold the day's files, never the whole bucket: listing every key ever
 * uploaded to test one day's files grew the daily peak with the archive.
 */
import { expect } from 'chai';
import * as path from 'path';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import {
  initializeCloudSDK,
  listParquetFilesForDays,
  uploadConsolidatedFilesToS3,
} from '../../src/data-handler';
import type { PluginState } from '../../src/types';
import {
  createFakeSignalK,
  FakeSignalK,
  makeTestConfig,
} from './helpers/fake-signalk';
import { makeScalarRecord } from './helpers/records';

const SELF = 'vessels.uploadself';
const DAY = new Date('2024-06-01T00:00:00.000Z');
const PREFIX = 'archive';

describe('daily cloud upload', function () {
  this.timeout(30000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;

  before(async () => {
    host = createFakeSignalK({ selfId: 'uploadself' });
    buffer = new SQLiteBuffer({ dbPath: path.join(host.dataDir, 'buffer.db') });
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
    for (const p of ['navigation.speedOverGround', 'environment.depth.belowTransducer']) {
      buffer.insert(makeScalarRecord(SELF, p, 1, '2024-06-01T10:00:00.000Z'));
    }
    await exportService.exportDayToParquet(DAY);
  });

  after(async () => {
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  it('lists only the day\'s directories and uploads only what is missing', async () => {
    const config = makeTestConfig(host.dataDir, {
      cloudUpload: { provider: 's3', bucket: 'bucket', keyPrefix: PREFIX },
    });
    await initializeCloudSDK(config, host.app);

    const local = await listParquetFilesForDays(host.dataDir, [DAY]);
    expect(local).to.have.lengthOf(2);
    const keyOf = (f: string) =>
      `${PREFIX}/${path.relative(host.dataDir, f).split(path.sep).join('/')}`;
    const [alreadyUploaded, missing] = local.map(keyOf);

    // The archive: one of the day's files, and many keys of other days that
    // a whole-bucket listing would have read into memory.
    const bucket = new Set<string>([alreadyUploaded]);
    for (let day = 1; day <= 500; day++) {
      bucket.add(`${PREFIX}/tier=raw/context=x/path=y/year=2023/day=${day}/f.parquet`);
    }

    const listedPrefixes: string[] = [];
    const listedKeys: number[] = [];
    const uploaded: string[] = [];
    const client = {
      send: async (cmd: { constructor: { name: string }; input: Record<string, string> }) => {
        if (cmd.constructor.name === 'ListObjectsV2Command') {
          const prefix = cmd.input.Prefix ?? '';
          listedPrefixes.push(prefix);
          const contents = [...bucket]
            .filter(k => k.startsWith(prefix))
            .map(Key => ({ Key }));
          listedKeys.push(contents.length);
          return { Contents: contents, IsTruncated: false };
        }
        if (cmd.constructor.name === 'PutObjectCommand') {
          uploaded.push(cmd.input.Key);
          bucket.add(cmd.input.Key);
          return {};
        }
        throw new Error(`unexpected command ${cmd.constructor.name}`);
      },
    };
    const state = { cloudClient: client } as unknown as PluginState;

    await uploadConsolidatedFilesToS3(config, DAY, state, host.app);

    expect(listedPrefixes).to.have.lengthOf(2);
    for (const prefix of listedPrefixes) {
      expect(prefix, 'a day directory, not the bucket').to.match(
        /^archive\/tier=raw\/context=[^/]+\/path=[^/]+\/year=2024\/day=153\/$/
      );
    }
    expect(Math.max(...listedKeys), 'no other day was read').to.be.at.most(1);
    expect(uploaded).to.deep.equal([missing]);
  });
});
