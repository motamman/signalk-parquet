/**
 * Pins that model-generated SQL is guarded before it reaches DuckDB.
 *
 * This path has no opt-in flag: unlike /api/query, the analyzer runs whenever
 * an analysis is requested, and the SQL comes from the model. A statement that
 * opens the live write buffer with DuckDB's bundled SQLite would truncate
 * buffer.db-shm under the writer's mmap and kill the server with SIGBUS (see
 * src/utils/buffer-staging.ts).
 *
 * executeSQLQuery is private and takes no injectable seam, so it is reached
 * through a cast. That is deliberate: the alternative is testing a copy of the
 * logic, which would not catch the guard call being removed.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'os';
import * as path from 'path';
import {
  ClaudeAnalyzer,
  ClaudeAnalyzerConfig,
} from '../../src/claude-analyzer';

type PrivateAnalyzer = {
  executeSQLQuery(sql: string, purpose: string): Promise<unknown[]>;
};

/** Run executeSQLQuery and return the thrown message, or null if it resolved. */
async function rejectionMessage(
  analyzer: ClaudeAnalyzer,
  sql: string
): Promise<string | null> {
  try {
    await (analyzer as unknown as PrivateAnalyzer).executeSQLQuery(sql, 'test');
    return null;
  } catch (err) {
    return (err as Error).message;
  }
}

describe('ClaudeAnalyzer SQL guard', () => {
  let analyzer: ClaudeAnalyzer;
  let dataDir: string;

  beforeEach(async () => {
    // The Anthropic client is constructed but never called on this path; the
    // data directory is only needed by the vessel context manager.
    dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'claude-guard-'));
    analyzer = new ClaudeAnalyzer(
      { apiKey: 'test-key-not-used' } as ClaudeAnalyzerConfig,
      undefined,
      dataDir
    );
  });

  afterEach(async () => {
    await fs.remove(dataDir);
  });

  it('rejects ATTACH of a SQLite database', async () => {
    const message = await rejectionMessage(
      analyzer,
      "ATTACH '/d/buffer.db' AS b (TYPE SQLITE)"
    );
    expect(message).to.match(/^Query rejected for security:/);
    expect(message).to.match(/'ATTACH' is not allowed/);
  });

  it('rejects a sqlite_scan table function inside a SELECT', async () => {
    const message = await rejectionMessage(
      analyzer,
      "SELECT * FROM sqlite_scan('/d/buffer.db', 't')"
    );
    expect(message).to.match(/Table function/);
  });

  it('rejects a quoted-identifier sqlite_scan', async () => {
    const message = await rejectionMessage(
      analyzer,
      `SELECT v FROM "sqlite_scan"('/d/buffer.db','t')`
    );
    expect(message).to.match(/Table function/);
  });

  it('rejects a statement chained after a benign SELECT', async () => {
    const message = await rejectionMessage(
      analyzer,
      'SELECT 1; INSTALL sqlite'
    );
    expect(message).to.match(/'INSTALL' is not allowed/);
  });

  it('does not reject a legitimate read-only query at the guard', async () => {
    // Without a DuckDB pool this still fails, but on the pool rather than the
    // guard — which is what distinguishes a working guard from a no-op one.
    const message = await rejectionMessage(
      analyzer,
      "SELECT signalk_timestamp, value FROM read_parquet('/d/*.parquet')"
    );
    expect(message ?? '').to.not.match(/rejected for security/);
  });
});
