/**
 * Tier-to-tier re-aggregation of angular paths.
 *
 * Angular values cannot be averaged arithmetically — the mean of 10deg and
 * 350deg is 0deg, not 180deg — so each aggregated bucket stores `value_sin_avg`
 * and `value_cos_avg` alongside `value_avg`, and the next tier up recombines
 * those vectors with ATAN2. A source bucket can legitimately lack the sin/cos
 * columns: it may predate them, or the path's metadata may have flapped so the
 * bucket was written down the scalar branch. Re-aggregating such a bucket must
 * derive sin/cos from `value_avg` rather than propagating NULL through SUM()
 * and blanking the whole output bucket.
 *
 * These tests execute the generated re-aggregation SQL against real parquet
 * fixtures, so what is asserted is DuckDB's actual numeric output. They drive
 * the query builder directly rather than going through `aggregateTier()`,
 * because that entry point discovers its inputs with a `glob()` call built via
 * `path.join()` and therefore matches nothing on Windows — a separate,
 * pre-existing platform issue that would make this suite silently vacuous on
 * one of the CI runners.
 */
import { expect } from 'chai';
import * as path from 'path';
import * as fs from 'fs-extra';
import { AggregationService } from '../../src/services/aggregation-service';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';

const CONTEXT = 'vessels.test-self';
const ANGULAR_PATH = 'navigation.headingTrue';

/** 10 degrees and 350 degrees in radians: their circular mean is 0. */
const TEN_DEG = (10 * Math.PI) / 180;
const THREE_FIFTY_DEG = (350 * Math.PI) / 180;

/** Seconds per bucket in the 60s tier, i.e. what a 5s -> 60s roll-up uses. */
const TARGET_INTERVAL_SECONDS = 60;

/** One source row in the aggregated-tier schema. */
interface SourceBucket {
  valueAvg: number;
  /** 'derived' writes NULL sin/cos, modelling a bucket without vectors. */
  vectors: 'derived' | 'stored';
  sampleCount: number;
}

/** DuckDB needs forward slashes in string literals on every platform. */
function toSqlPath(p: string): string {
  return p.split(path.sep).join('/').replace(/'/g, "''");
}

describe('angular re-aggregation between tiers', function () {
  // Native DuckDB init may install the spatial extension on first run.
  this.timeout(30000);

  let host: FakeSignalK;
  let service: AggregationService;

  beforeEach(async () => {
    host = createFakeSignalK({
      selfId: 'test-self',
      metadata: { [`${CONTEXT}.${ANGULAR_PATH}`]: { units: 'rad' } },
    });
    service = new AggregationService(
      {
        outputDirectory: host.dataDir,
        filenamePrefix: 'signalk_data',
        retentionDays: { raw: 0, '5s': 0, '60s': 0, '1h': 0 },
      },
      host.app
    );
    await DuckDBPool.initialize();
  });

  afterEach(async () => {
    await DuckDBPool.shutdown();
    await host?.cleanup();
  });

  /** Write one aggregated-tier parquet file holding the given buckets. */
  async function writeSourceFile(buckets: SourceBucket[]): Promise<string> {
    const file = path.join(host.dataDir, 'source.parquet');
    const rows = buckets.map((b, i) => {
      // All rows share one 60s window so they roll up into a single bucket.
      const ts = `2026-03-04 00:00:${String(i * 5).padStart(2, '0')}`;
      const sin = b.vectors === 'stored' ? `SIN(${b.valueAvg})` : 'NULL';
      const cos = b.vectors === 'stored' ? `COS(${b.valueAvg})` : 'NULL';
      return `(TIMESTAMP '${ts}', '${CONTEXT}', '${ANGULAR_PATH}', ${b.valueAvg}::DOUBLE, NULL::DOUBLE, NULL::DOUBLE, ${b.sampleCount}::BIGINT, ${sin}::DOUBLE, ${cos}::DOUBLE, TIMESTAMP '${ts}', TIMESTAMP '${ts}')`;
    });

    const conn = await DuckDBPool.getConnection();
    try {
      await conn.runAndReadAll(
        `COPY (SELECT * FROM (VALUES ${rows.join(',')}) AS t(
           bucket_time, context, path, value_avg, value_min, value_max,
           sample_count, value_sin_avg, value_cos_avg,
           first_timestamp, last_timestamp
         )) TO '${toSqlPath(file)}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');`
      );
    } finally {
      conn.disconnectSync();
    }
    return file;
  }

  /**
   * Write an aggregated-tier parquet file in the pre-sin/cos (legacy)
   * schema: no `value_sin_avg`/`value_cos_avg` columns at all, as written
   * before the angular vector columns existed.
   */
  async function writeLegacySourceFile(
    buckets: Array<{ valueAvg: number; sampleCount: number }>
  ): Promise<string> {
    const file = path.join(host.dataDir, 'legacy-source.parquet');
    const rows = buckets.map((b, i) => {
      const ts = `2026-03-04 00:00:${String(i * 5).padStart(2, '0')}`;
      return `(TIMESTAMP '${ts}', '${CONTEXT}', '${ANGULAR_PATH}', ${b.valueAvg}::DOUBLE, NULL::DOUBLE, NULL::DOUBLE, ${b.sampleCount}::BIGINT, TIMESTAMP '${ts}', TIMESTAMP '${ts}')`;
    });

    const conn = await DuckDBPool.getConnection();
    try {
      await conn.runAndReadAll(
        `COPY (SELECT * FROM (VALUES ${rows.join(',')}) AS t(
           bucket_time, context, path, value_avg, value_min, value_max,
           sample_count, first_timestamp, last_timestamp
         )) TO '${toSqlPath(file)}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');`
      );
    } finally {
      conn.disconnectSync();
    }
    return file;
  }

  /**
   * Run the angular re-aggregation query the service generates for a
   * non-raw source tier, and return the resulting buckets.
   */
  async function reaggregate(
    buckets: SourceBucket[]
  ): Promise<Array<{ value_avg: number | null; sample_count: number }>> {
    const sourceFile = await writeSourceFile(buckets);
    const outputFile = path.join(host.dataDir, 'out.parquet');

    // buildAggregationQuery is private; the behaviour under test is the SQL it
    // emits, which is exercised for real against DuckDB below.
    const query = (
      service as unknown as {
        buildAggregationQuery: (
          fileListStr: string,
          intervalSeconds: number,
          isSourceRaw: boolean,
          isAngular: boolean,
          outputFile: string
        ) => string;
      }
    ).buildAggregationQuery(
      `'${toSqlPath(sourceFile)}'`,
      TARGET_INTERVAL_SECONDS,
      false,
      true,
      toSqlPath(outputFile)
    );

    const conn = await DuckDBPool.getConnection();
    try {
      await conn.runAndReadAll(query);
      expect(fs.existsSync(outputFile), 'query produced an output file').to.be
        .true;
      const res = await conn.runAndReadAll(
        `SELECT value_avg, sample_count FROM read_parquet('${toSqlPath(outputFile)}') ORDER BY bucket_time`
      );
      return res.getRowObjects().map(r => ({
        value_avg: r.value_avg === null ? null : Number(r.value_avg),
        sample_count: Number(r.sample_count),
      }));
    } finally {
      conn.disconnectSync();
    }
  }

  it('recombines stored sin/cos vectors into a circular mean', async () => {
    const rows = await reaggregate([
      { valueAvg: TEN_DEG, vectors: 'stored', sampleCount: 1 },
      { valueAvg: THREE_FIFTY_DEG, vectors: 'stored', sampleCount: 1 },
    ]);

    expect(rows).to.have.lengthOf(1);
    expect(rows[0].sample_count).to.equal(2);
    // 0 rad, not the ~pi an arithmetic mean of 10deg and 350deg would give.
    expect(rows[0].value_avg).to.be.a('number');
    expect(rows[0].value_avg!).to.be.closeTo(0, 1e-9);
  });

  // The regression: without the COALESCE fallback, SUM(value_sin_avg * …) over
  // a bucket whose vectors are NULL yields NULL, so value_avg came out NULL and
  // the bucket's data was effectively lost on the way up the tiers.
  it('derives vectors from value_avg when the source bucket has none', async () => {
    const rows = await reaggregate([
      { valueAvg: TEN_DEG, vectors: 'derived', sampleCount: 1 },
      { valueAvg: THREE_FIFTY_DEG, vectors: 'derived', sampleCount: 1 },
    ]);

    expect(rows).to.have.lengthOf(1);
    expect(rows[0].value_avg, 'bucket must not be NULL').to.not.be.null;
    expect(rows[0].value_avg!).to.be.closeTo(0, 1e-9);
    expect(rows[0].sample_count).to.equal(2);
  });

  it('handles a bucket mixing stored and missing vectors', async () => {
    const rows = await reaggregate([
      { valueAvg: TEN_DEG, vectors: 'stored', sampleCount: 1 },
      { valueAvg: THREE_FIFTY_DEG, vectors: 'derived', sampleCount: 1 },
    ]);

    expect(rows).to.have.lengthOf(1);
    expect(rows[0].value_avg).to.not.be.null;
    expect(rows[0].value_avg!).to.be.closeTo(0, 1e-9);
  });

  it('weights the circular mean by sample_count', async () => {
    // Three samples at 10deg against one at 350deg pulls the mean toward 10deg.
    const rows = await reaggregate([
      { valueAvg: TEN_DEG, vectors: 'derived', sampleCount: 3 },
      { valueAvg: THREE_FIFTY_DEG, vectors: 'derived', sampleCount: 1 },
    ]);

    expect(rows).to.have.lengthOf(1);
    expect(rows[0].sample_count).to.equal(4);
    const expected = Math.atan2(
      (3 * Math.sin(TEN_DEG) + Math.sin(THREE_FIFTY_DEG)) / 4,
      (3 * Math.cos(TEN_DEG) + Math.cos(THREE_FIFTY_DEG)) / 4
    );
    expect(rows[0].value_avg!).to.be.closeTo(expected, 1e-9);
    expect(rows[0].value_avg!).to.be.greaterThan(0);
  });

  // The regression: when every source file predates the sin/cos columns,
  // the re-aggregation SQL named columns that exist nowhere in the unioned
  // schema, so DuckDB refused to bind the query and the whole group failed.
  // This drives aggregateGroup itself (not just the query builder) because
  // the fix hinges on detecting the absent columns from the parquet schema.
  it('re-aggregates an all-legacy schema lacking both sin/cos columns', async () => {
    const sourceFile = await writeLegacySourceFile([
      { valueAvg: TEN_DEG, sampleCount: 1 },
      { valueAvg: THREE_FIFTY_DEG, sampleCount: 1 },
    ]);

    const result = await (
      service as unknown as {
        aggregateGroup: (
          files: string[],
          context: string,
          signalkPath: string,
          sourceTier: string,
          targetTier: string,
          date: Date
        ) => Promise<{ recordsAggregated: number; outputFile: string | null }>;
      }
    ).aggregateGroup(
      [sourceFile],
      CONTEXT,
      ANGULAR_PATH,
      '5s',
      '60s',
      new Date('2026-03-04T00:00:00Z')
    );

    expect(result.outputFile, 'aggregation produced an output file').to.not.be
      .null;
    expect(result.recordsAggregated).to.equal(1);

    const conn = await DuckDBPool.getConnection();
    try {
      const res = await conn.runAndReadAll(
        `SELECT value_avg, sample_count, value_sin_avg, value_cos_avg FROM read_parquet('${toSqlPath(result.outputFile!)}')`
      );
      const rows = res.getRowObjects();
      expect(rows).to.have.lengthOf(1);
      // Circular mean of 10deg and 350deg derived purely from value_avg.
      expect(Number(rows[0].value_avg)).to.be.closeTo(0, 1e-9);
      expect(Number(rows[0].sample_count)).to.equal(2);
      // The output bucket gains the vector columns for the next tier up.
      expect(rows[0].value_sin_avg).to.not.be.null;
      expect(rows[0].value_cos_avg).to.not.be.null;
    } finally {
      conn.disconnectSync();
    }
  });

  it('carries a lone vectorless bucket through unchanged', async () => {
    const rows = await reaggregate([
      { valueAvg: TEN_DEG, vectors: 'derived', sampleCount: 5 },
    ]);

    expect(rows).to.have.lengthOf(1);
    expect(rows[0].value_avg).to.not.be.null;
    expect(rows[0].value_avg!).to.be.closeTo(TEN_DEG, 1e-9);
    expect(rows[0].sample_count).to.equal(5);
  });
});
