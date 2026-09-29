# Buffer worker (removed 2026-09-29)

The buffer worker moved the SQLite reads that stage buffer rows into a history
query off the server's event loop and onto a worker thread. It was built during
1.0.1-beta.1, ran on the test server, and was removed before any npm release
(npm's newest version at removal was 1.0.0). History reads now stage in-process
only: a synchronous `node:sqlite` read per page, yielding to the event loop
between pages (`src/utils/buffer-staging.ts`).

This document is here so it can be put back quickly.

## Reintroducing it

The last commit that contains the worker is **`a04ab5f`** (branch
`sunburned-eyelids`). In order of speed:

1. **Revert the removal commit.** `git revert <removal commit>` restores every
   file and every wiring edit at once. Conflicts, if any, will be in the five
   wiring files listed below.
2. **Apply the saved patch in reverse.**
   `git apply -R devdocs/buffer-worker-removal.patch` — the patch is the
   removal diff of `src/` and `test/`, taken against `a04ab5f`. It applies
   cleanly only while those files have not moved on; after that, use 1 or 3.
3. **By hand.** `git checkout a04ab5f -- <the removed files below>`, then redo
   the wiring edits listed under "Wiring" against the current code.

After 2 or 3, delete `test/integration/sqlite-buffer-read-only.test.ts`: it is
the first test of `buffer-worker-read.test.ts`, which comes back with the rest
(the patch does not cover it because the file was new at removal).

Then: `npm run build`, `npm test`. The worker's own suites are
`test/integration/buffer-worker-read.test.ts` and
`test/integration/buffer-staging-worker.test.ts`, and
`test/unit/utils/buffer-columnar.test.ts`.

## What it did

```
main thread                                    worker thread
-----------                                    -------------
stageBufferTable(..., worker)                  SQLiteBuffer({ readOnly: true })
  └ workerSource(client)                         on 'openScan': schema lookup,
      client.scan({path, context, window})        send reply(scanId), pump()
        ── openScan ─────────────────────────▶   pump(): while credits > 0
        ◀──────────────── page (transferred) ──     getRowsForFederation(cursor)
      appendPage(appender, page)                    encodePage() → ArrayBuffers
        ── ackScan ──────────────────────────▶     credits -= 1
        ◀──────────────── page / end ─────────   on 'ackScan': credits += 1, pump()
```

- **One read-only connection** to `buffer.db`, owned by a `worker_threads`
  Worker (`src/buffer-worker.ts`). It holds a real `SQLiteBuffer` opened with
  `readOnly: true`, so the queries, table map and keyset pagination are the
  same code the in-process read runs. Writes stayed on the main thread's
  connection. Two `node:sqlite` connections share one SQLite library, so their
  POSIX locks are coherent (unlike DuckDB's bundled SQLite; see
  `buffer-staging.ts`).
- **Reads stream.** A scan pushes pages of one path, context and window in
  `(signalk_timestamp, id)` order. The worker sends at most
  `1 + SCAN_PAGES_IN_FLIGHT` (= 2) pages before it needs an ack, so the read of
  one page overlaps the main thread's append of the last, and a slow consumer
  cannot make the worker read the whole window into memory. A consumer that
  `break`s out of the iteration sends `closeScan`.
- **Pages are columnar with transferred buffers** (`buffer-columnar.ts`): one
  typed array per column plus a null mask; text is one utf8 blob plus offsets.
  Receiving a page cost the main thread about 0.2 ms whatever its size.
- **Page size came from a budget, not a constant.** `PAGE_BUDGET_MS = 10`
  divided by what a row costs the main thread: 15 µs in-process (read + append),
  7 µs through the worker (append only). So about 670 rows in-process and about
  1,430 through the worker.
- **Failure rules.** A scan that errors part-way throws into the consumer,
  never ends short. A worker that exits or errors rejects every pending request
  and open scan, and is never retried (`BufferWorkerGoneError`).
- **Fallback.** `index.ts` exposed the worker through
  `liveBufferWorker = () => state.bufferWorker?.isAlive() ? state.bufferWorker : undefined`.
  Each request snapshotted it once, the way it snapshots the buffer. No worker
  (not started yet, failed to start, died) meant the request staged in-process.
- **Lifecycle.** Started without awaiting, right after the in-process
  `SQLiteBuffer` opened in `plugin.start`. It was stored in
  `state.bufferWorker` only once it reported ready. A worker that became ready
  after `stop()` had begun, or after a reconfigure had replaced the buffer it
  was opened against, was closed instead of stored. `stop()` closed and awaited
  the worker before closing the in-process buffer.
- **New paths.** The read-only connection loads the table map once at open, so
  on a `schema`/`openScan` for an unknown path it called
  `SQLiteBuffer.loadTableIfMissing(path)` first.

### Protocol (`src/utils/buffer-worker-protocol.ts`)

Main → worker: `openScan {id, signalkPath, context, fromIso, toIso, pageRows}`,
`ackScan {scanId}`, `closeScan {scanId}`, `schema {id, signalkPath}`,
`ping {id}`, `close {id}`.

Worker → main: `ready`, `fatal {message}`, `reply {id, value}`,
`error {id, message}`, `page {scanId, page, readMs}`, `end {scanId, totalRows}`,
`scanError {scanId, message}`.

`workerData`: `{ dbPath, retentionHours }`.

## Measurements behind it

All on a Raspberry Pi 5 with NVMe, from comments in the removed code and the
SignalK/tracks#118 thread:

- Per row on the main thread: about 8 µs to read from SQLite, about 7 µs to
  append into DuckDB (2026-09-26).
- Sending rows as structured-cloned objects: about 4.3 µs a row on the
  receiving thread (21.7 ms for 5,000 rows). Columnar with transferred buffers:
  about 0.2 ms flat per page. A no-op round trip: about 0.05 ms.
- Read against append for 5,000 rows: about 40 ms against 35 ms, the reason one
  page in flight was enough.
- The comparison posted to SignalK/tracks#118 (worker worse than in-process
  yielding: worst freeze 55 ms against 37 ms for one query, 339–345 ms against
  70–125 ms under load) **predates later changes to the worker**. No
  measurement taken after those changes was found at removal.

## State at removal, and what was still missing

Done: the read half, on by default, used by V1 history
(`HistoryAPI.getNumericValues`, `getSpatialTimestamps`), the V2 provider
(`HistoryProvider.queryPath`, `discoverSources`) and the Track API
(`TrackProvider.queryPositions`, `queryPropertySeries`).

Not done:

1. **Writes** (ingestion, export marking, cleanup, `insertOrExtendLatest`)
   still ran on the main thread's connection.
2. **Other reads** were never routed through it: playback
   (`PlaybackProvider` got no worker), the path-listing buffer probes
   (`bufferPathsInWindow`, `getAvailablePathsArray`), the Track API's
   `bufferHasPositionIn`, the export's reads and `getPaths()`.
3. **The protocol's stated contract was not yet true.** The protocol header
   describes one worker owning the only connection, so that "a read issued
   after a write observes that write" by queue order. With writes still on the
   main thread there were two connections, and that ordering came from SQLite
   WAL, not the queue. It becomes true only once writes move.
4. **No hang detection.** `request()` and `scan()` had no timeout, and
   `isAlive()` detected only an exited worker. A worker stuck inside a long
   SQLite call would have left history requests waiting indefinitely rather
   than falling back. (mairas's note on SignalK/tracks#118 about per-batch ack
   timeouts is the same gap.)
5. A stale comment: `buffer-worker.ts` still said nothing in the plugin talked
   to it.

## Files removed

| File | Lines |
|---|---|
| `src/buffer-worker.ts` | 245 |
| `src/utils/buffer-worker-client.ts` | 320 |
| `src/utils/buffer-worker-protocol.ts` | 153 |
| `src/utils/buffer-columnar.ts` | 199 |
| `test/integration/buffer-worker-read.test.ts` | 402 |
| `test/integration/buffer-staging-worker.test.ts` | 256 |
| `test/unit/utils/buffer-columnar.test.ts` | 208 |

The first test of `buffer-worker-read.test.ts` ("opens read-only, so it cannot
contend for the write lock") tests `SQLiteBuffer`, not the worker, and was kept
as `test/integration/sqlite-buffer-read-only.test.ts`.

## Wiring (line numbers are at `a04ab5f`)

- **`src/index.ts`**
  - import `BufferWorkerClient` (44)
  - `liveBufferWorker` accessor (220–226)
  - worker start after the buffer opens, with the late-arrival guard (435–473)
  - `state.historyApi.setBufferWorkerSource(liveBufferWorker)` on first start
    (960) and on reconfigure (971)
  - `liveBufferWorker` as the last argument to `registerHistoryApiProvider`
    (1003)
  - `trackProvider.setBufferWorkerSource(liveBufferWorker)` (1035)
  - close and await the worker in `stop()`, before the buffer closes
    (1229–1239)
- **`src/types.ts`**: `PluginState.bufferWorker` (745–755).
- **`src/utils/buffer-staging.ts`**: import (31), the `WORKER_US_PER_ROW` /
  `WORKER_BATCH_SIZE` constants (67–84), `workerSource()` (179–202), the
  optional `worker` parameter of `stageBufferTable` and the
  `worker ? workerSource(worker) : inProcessSource(buffer)` choice (254–264),
  and the two-source wording in the header (12–24).
- **`src/HistoryAPI.ts`**: import (67), `getBufferWorker` field (815–819),
  `setBufferWorkerSource` (875–880), a `bufferWorker` parameter on
  `getSpatialTimestamps` (1007–1008), the per-request snapshot in
  `getNumericValues` (1385–1386), and `bufferWorker` passed as the last
  argument to each `stageBufferTable` call (1064, 1498, 1758, 2198).
- **`src/history-provider.ts`**: import (59), `getBufferWorker` field
  (274–278), `setBufferWorkerSource` (292–297), `this.getBufferWorker?.()` as
  the last argument to `stageBufferTable` (585, 694), and the
  `getBufferWorker` parameter of `registerHistoryApiProvider` (1144, 1150).
- **`src/track-provider.ts`**: import (47), `getBufferWorker` field (429–433),
  `setBufferWorkerSource` (445–450), the snapshot in `getTracks` (469–470), and
  a `worker` parameter threaded through `buildFeature`, `queryPositions` and
  `queryPropertySeries` into their `stageBufferTable` calls.

## Kept in place

- `SQLiteBufferConfig.readOnly` and everything it gates in
  `src/utils/sqlite-buffer.ts` (no DDL at open, writes refuse), still tested.
- `SQLiteBuffer.loadTableIfMissing()`. Nothing calls it now; the worker needs it
  back.
- The in-process staging path, unchanged: it was always the fallback.
- The event-loop monitor (`/api/event-loop`), which is how the worker's effect
  was measured and is independent of it.
