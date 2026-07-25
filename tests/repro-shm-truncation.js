/**
 * Reproduces the buffer.db-shm truncation SIGBUS caused by two SQLite
 * libraries sharing one WAL database in the same process.
 *
 * Mechanism: POSIX advisory locks never conflict within a single process, and
 * each SQLite library instance only coordinates its own connections. When
 * DuckDB's bundled SQLite (sqlite extension) attaches a database that
 * node:sqlite is writing, its "am I the first shm user" probe always
 * succeeds, so it truncates the -shm file to zero and rebuilds it sized to
 * the current WAL. The node:sqlite writer keeps its stale mmap of the upper
 * shm regions; the next write burst that grows the WAL past ~4062 frames
 * touches an unbacked page and dies with SIGBUS (exit code 135).
 *
 * Run manually on Linux (e.g. inside the SignalK test container); the shm
 * behavior is platform-specific and the crash is the expected outcome:
 *
 *   node tests/repro-shm-truncation.js
 *
 * First run needs network access (DuckDB downloads the sqlite extension).
 *
 * Expected output ends like:
 *   phase 3: shm after DuckDB attach: 32768 bytes (was 65536) TRUNCATED
 *   phase 4: writing until the stale region-1 mapping is touched...
 *   Bus error (core dumped)   <- exit code 135
 *
 * If the process survives phase 4 and prints "did not reproduce", the
 * platform or library versions do not exhibit the bug.
 */
const fs = require('fs');
const os = require('os');
const path = require('path');

const REGION_SIZE = 32768; // SQLite WAL-index shm region size
const FILLER = 'x'.repeat(1024);

function shmSize(dbPath) {
  try {
    return fs.statSync(dbPath + '-shm').size;
  } catch {
    return 0;
  }
}

async function main() {
  if (process.platform !== 'linux') {
    console.log(
      `This repro needs Linux POSIX advisory-lock and mmap semantics ` +
        `(current platform: ${process.platform}). Run it inside the SignalK ` +
        `test container.`
    );
    return;
  }

  const { DatabaseSync } = require('node:sqlite');
  const { DuckDBInstance } = require('@duckdb/node-api');

  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'shm-repro-'));
  const dbPath = path.join(dir, 'buffer.db');
  console.log(`database: ${dbPath}`);

  // Phase 1: live writer with a WAL-index spanning two shm regions. With
  // autocheckpoint off, every autocommit insert appends frames; the shm file
  // grows to 64KB exactly when the writer maps region 1 (frame ~4062), which
  // is the mapping the truncation later invalidates.
  const writer = new DatabaseSync(dbPath);
  writer.exec('PRAGMA journal_mode = WAL');
  writer.exec('PRAGMA wal_autocheckpoint = 0');
  writer.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)');
  const insert = writer.prepare('INSERT INTO t (payload) VALUES (?)');

  let rows = 0;
  while (shmSize(dbPath) < 2 * REGION_SIZE) {
    insert.run(FILLER);
    rows += 1;
    if (rows > 200000) {
      throw new Error('WAL never reached region 1; aborting');
    }
  }
  console.log(
    `phase 1: ${rows} rows, shm = ${shmSize(dbPath)} bytes (writer has region 1 mapped)`
  );

  // Phase 2: checkpoint like the export cycle does. The WAL shrinks to zero
  // but the shm keeps its high-water size; the writer keeps its mappings.
  writer.exec('PRAGMA wal_checkpoint(TRUNCATE)');
  const shmBefore = shmSize(dbPath);
  console.log(
    `phase 2: after wal_checkpoint(TRUNCATE), shm = ${shmBefore} bytes`
  );

  // Phase 3: DuckDB's bundled SQLite attaches the live database read-only,
  // wins the dead-man-switch probe (same process, foreign lock table),
  // truncates the shm to zero, and rebuilds only region 0 for the empty WAL.
  const instance = await DuckDBInstance.create(':memory:');
  const conn = await instance.connect();
  await conn.runAndReadAll('INSTALL sqlite;');
  await conn.runAndReadAll('LOAD sqlite;');
  await conn.runAndReadAll(
    `ATTACH '${dbPath}' AS repro (TYPE SQLITE, READ_ONLY);`
  );
  const reader = await conn.runAndReadAll('SELECT count(*) AS n FROM repro.t;');
  console.log(`phase 3: DuckDB sees ${reader.getRowObjects()[0].n} rows`);
  conn.disconnectSync();

  const shmAfter = shmSize(dbPath);
  console.log(
    `phase 3: shm after DuckDB attach: ${shmAfter} bytes (was ${shmBefore})` +
      (shmAfter < shmBefore ? ' TRUNCATED' : '')
  );

  // Phase 4: the export-hour write burst. Frames restart from 1 after the
  // checkpoint, so the writer fills region 0 again and then touches its
  // stale region-1 mapping, which the truncated file no longer backs.
  console.log(
    'phase 4: writing until the stale region-1 mapping is touched...'
  );
  for (let i = 0; i < 6000; i += 1) {
    insert.run(FILLER);
  }

  console.log(
    `did not reproduce: writer survived phase 4 (shm = ${shmSize(dbPath)} bytes)`
  );
  writer.close();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
