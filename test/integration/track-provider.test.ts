/**
 * Track API provider against a real store: positions and a co-recorded path
 * are written through the SQLite buffer, one day is exported to raw-tier
 * parquet, a later day is left in the buffer, and the provider is queried the
 * way the server's Track API would.
 *
 * The fixture vessel makes two legs on 2024-06-01 — five fixes a minute
 * apart heading east from 47.5N 9.4E, then after a two-hour stop three fixes
 * heading east from 47.6N 9.6E — and three more fixes on 2024-06-02 that are
 * never exported, so they can only come from the buffer.
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
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makeScalarRecord, makePositionRecord } from './helpers/records';

// No hyphen/colon: sanitizeContext maps ':' -> '-' and the directory name is
// what a context listing round-trips through.
const SELF_ID = 'trackself';
const STORED_CONTEXT = `vessels.${SELF_ID}` as Context;
const DAY = new Date('2024-06-01T00:00:00.000Z');
const SOG = 'navigation.speedOverGround' as Path;
const NOT_RECORDED = 'environment.depth.belowTransducer' as Path;

const LEG1 = {
  lat: 47.5,
  lon: 9.4,
  times: ['10:00', '10:01', '10:02', '10:03', '10:04'],
};
const LEG2 = { lat: 47.6, lon: 9.6, times: ['12:00', '12:01', '12:02'] };
const BUFFER_LEG = { lat: 47.7, lon: 9.8, times: ['08:00', '08:01', '08:02'] };

const WHOLE_DAY: TracksRequest = {
  from: Temporal.Instant.from('2024-06-01T00:00:00Z'),
  to: Temporal.Instant.from('2024-06-02T00:00:00Z'),
};

describe('Track API provider', function () {
  this.timeout(30000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;
  let provider: TrackProvider;

  const providerApp = {
    debug: () => {},
    error: () => {},
    getMetadata: () => undefined,
    getSelfPath: (key: string) => (key === 'name' ? 'Fixture' : undefined),
    selfId: SELF_ID,
    selfContext: STORED_CONTEXT,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } as any;

  beforeEach(async () => {
    host = createFakeSignalK({ selfId: SELF_ID });
    buffer = new SQLiteBuffer({ dbPath: path.join(host.dataDir, 'buffer.db') });
    await DuckDBPool.initialize();
    clearFileListCache();

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

    const record = (
      day: string,
      leg: { lat: number; lon: number; times: string[] },
      sogBase: number
    ) => {
      leg.times.forEach((hhmm, i) => {
        const iso = `${day}T${hhmm}:00.000Z`;
        buffer.insert(
          makePositionRecord(STORED_CONTEXT, leg.lat, leg.lon + i * 0.001, iso)
        );
        // Stamped 200 ms after the fix, as a second sensor on a real feed
        // is: at a sub-second bucket the two never share one, so property
        // alignment has to match on proximity rather than on the exact bucket.
        buffer.insert(
          makeScalarRecord(
            STORED_CONTEXT,
            SOG,
            sogBase + i,
            `${day}T${hhmm}:00.200Z`
          )
        );
      });
    };
    record('2024-06-01', LEG1, 3);
    record('2024-06-01', LEG2, 10);
    await exportService.exportDayToParquet(DAY);
    // Left in the buffer on purpose: only federation can see it.
    record('2024-06-02', BUFFER_LEG, 20);

    provider = new TrackProvider(
      SELF_ID,
      host.dataDir,
      providerApp,
      () => {},
      buffer
    );
  });

  afterEach(async () => {
    clearFileListCache();
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  it('returns one MultiLineString feature per context, split at the gap', async () => {
    const res = await provider.getTracks({ ...WHOLE_DAY, times: true });

    expect(res.type).to.equal('FeatureCollection');
    expect(res.features).to.have.lengthOf(1);
    const feature = res.features[0];
    expect(feature.geometry?.type).to.equal('MultiLineString');
    const coords = feature.geometry!.coordinates;
    expect(coords.map(seg => seg.length)).to.deep.equal([5, 3]);
    expect(coords[0][0]).to.deep.equal([9.4, 47.5]);
    expect(coords[1][2]).to.deep.equal([9.602, 47.6]);

    const props = feature.properties;
    expect(props.context).to.equal(STORED_CONTEXT);
    expect(props.isSelf).to.equal(true);
    expect(props.contextName).to.equal('Fixture');
    expect(props.pointCount).to.equal(8);
    expect(props.from).to.equal('2024-06-01T10:00:00.000Z');
    expect(props.to).to.equal('2024-06-01T12:02:00.000Z');
    expect(props.bbox).to.deep.equal([9.4, 47.5, 9.602, 47.6]);
    expect(props.resolution).to.match(/^PT[\d.]+S$/);

    // coordTimes nest exactly like coordinates
    expect(props.coordTimes!.map(seg => seg.length)).to.deep.equal([5, 3]);
    expect(props.coordTimes![0][0]).to.equal('2024-06-01T10:00:00.000Z');
    expect(props.coordTimes![1][0]).to.equal('2024-06-01T12:00:00.000Z');
  });

  it('defaults to the own vessel when no context is given', async () => {
    const res = await provider.getTracks({ duration: 'P1D', to: WHOLE_DAY.to });
    expect(res.features.map(f => f.properties.context)).to.deep.equal([
      STORED_CONTEXT,
    ]);
  });

  it('returns co-recorded properties aligned with the coordinates', async () => {
    const res = await provider.getTracks({
      ...WHOLE_DAY,
      properties: [SOG, NOT_RECORDED],
    });

    const props = res.features[0].properties;
    expect(props.appliedProperties).to.deep.equal([SOG]);
    expect(props.values).to.have.all.keys(SOG);
    const values = props.values![SOG];
    expect(values.map(seg => seg.length)).to.deep.equal([5, 3]);
    expect(values[0]).to.deep.equal([3, 4, 5, 6, 7]);
    expect(values[1]).to.deep.equal([10, 11, 12]);
  });

  it('selects whole tracks by bounding box and drops contexts with nothing inside it', async () => {
    // Settled on SignalK/signalk-server#2995: the box picks which tracks are
    // returned, it does not clip them. A box around the first leg alone still
    // yields both legs, approach and departure included.
    const around = await provider.getTracks({
      ...WHOLE_DAY,
      bbox: [9.39, 47.49, 9.41, 47.51],
    });
    expect(around.features).to.have.lengthOf(1);
    expect(
      around.features[0].geometry!.coordinates.map(s => s.length)
    ).to.deep.equal([5, 3]);
    expect(around.features[0].properties.pointCount).to.equal(8);

    const elsewhere = await provider.getTracks({
      ...WHOLE_DAY,
      bbox: [1, 1, 2, 2],
    });
    expect(elsewhere.features).to.deep.equal([]);
  });

  it('returns metadata only when geometry is false', async () => {
    const res = await provider.getTracks({
      ...WHOLE_DAY,
      geometry: false,
      times: true,
      properties: [SOG],
    });
    const feature = res.features[0];
    expect(feature.geometry).to.equal(null);
    expect(feature.properties.pointCount).to.equal(8);
    expect(feature.properties.coordTimes).to.equal(undefined);
    expect(feature.properties.values).to.equal(undefined);
  });

  it('simplifies each segment and reports the tolerance', async () => {
    // Both legs are straight lines, so any positive tolerance keeps only
    // their end points.
    const res = await provider.getTracks({
      ...WHOLE_DAY,
      simplify: true,
      epsilon: 5,
    });
    const feature = res.features[0];
    expect(feature.geometry!.coordinates.map(s => s.length)).to.deep.equal([
      2, 2,
    ]);
    expect(feature.properties.pointCount).to.equal(4);
    expect(feature.properties.epsilon).to.equal(5);
  });

  it('honours maxPoints by widening the bucket and reports it', async () => {
    const res = await provider.getTracks({ ...WHOLE_DAY, maxPoints: 4 });
    const props = res.features[0].properties;
    expect(props.pointCount).to.be.at.most(4);
    // A day over four points is six-hour buckets.
    expect(props.resolution).to.equal('PT6H');
  });

  it('honours an explicit resolution as a Temporal.Duration', async () => {
    const res = await provider.getTracks({
      ...WHOLE_DAY,
      resolution: Temporal.Duration.from('PT2M'),
    });
    const props = res.features[0].properties;
    expect(props.resolution).to.equal('PT2M');
    // Five fixes a minute apart fall into three two-minute buckets, three
    // fixes into two.
    expect(props.pointCount).to.equal(5);
  });

  it('federates fixes still in the buffer with exported parquet', async () => {
    const res = await provider.getTracks({
      from: '2024-06-01T00:00:00Z',
      to: '2024-06-03T00:00:00Z',
      times: true,
    });
    const props = res.features[0].properties;
    expect(props.pointCount).to.equal(11);
    expect(props.to).to.equal('2024-06-02T08:02:00.000Z');
    expect(
      res.features[0].geometry!.coordinates.map(s => s.length)
    ).to.deep.equal([5, 3, 3]);
  });

  it('lists contexts with position data in the window', async () => {
    expect(await provider.getTrackContexts(WHOLE_DAY)).to.deep.equal([
      STORED_CONTEXT,
    ]);
    expect(
      await provider.getTrackContexts({ ...WHOLE_DAY, bbox: [1, 1, 2, 2] })
    ).to.deep.equal([]);
    expect(
      await provider.getTrackContexts({
        from: '2024-06-02T00:00:00Z',
        to: '2024-06-03T00:00:00Z',
      }),
      'a context known only from the buffer is still listed'
    ).to.deep.equal([STORED_CONTEXT]);
    expect(
      await provider.getTrackContexts({
        from: '2024-07-01T00:00:00Z',
        to: '2024-07-02T00:00:00Z',
      })
    ).to.deep.equal([]);
  });

  it('rejects a bbox with out-of-range longitudes', async () => {
    let message = '';
    try {
      await provider.getTracks({ ...WHOLE_DAY, bbox: [-200, 47, 10, 48] });
    } catch (err) {
      message = (err as Error).message;
    }
    expect(message).to.match(/Invalid bbox/);
  });

  it('returns no feature for a context with no data', async () => {
    const res = await provider.getTracks({
      ...WHOLE_DAY,
      contexts: ['vessels.urn:mrn:imo:mmsi:123456789' as Context],
    });
    expect(res.features).to.deep.equal([]);
  });
});
