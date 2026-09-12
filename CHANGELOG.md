# Changelog

## [0.7.44-beta.5] - 2026-09-12

Five contributions from @msallin (PRs #123–#127).

### Fixed

- **`middle_index` aggregate implemented** (PR #124) — it was documented and offered in the webapp dropdown but never worked: the raw tier, V1 object paths and the v2 provider quietly computed `first`, and the aggregated tiers (5s/60s/1h) rendered invalid SQL so the path came back empty. Every query path now shares one expression from the new `src/utils/aggregate-sql.ts`, `list(value ORDER BY time)[(count(*) + 1) // 2]`: the chronologically middle sample (first of the two middle samples for even counts), independent of file scan order, with every component of an object path taken from the same row so a position's latitude and longitude come from one fix. On aggregated tiers it picks the middle pre-aggregated bucket. `getAggregateFunction('middle_index')` now throws rather than silently substituting another aggregate. Eight integration tests, seven of which failed on the previous code.
- **V1 object paths returned nothing when parquet was the only source** (PR #124) — with the SQLite buffer disabled, the single-source branch selected the raw `value_latitude` / `value_longitude` columns from a subquery that exposes `latitude` / `longitude`; the binder error was swallowed into an empty result for any object path and any aggregate. It now selects by component name, as the multi-source branch already did.
- **Buffer-only fallback ignored the requested aggregate** (PR #124) — when the parquet query fails and the buffer is attached, the fallback averaged numeric paths and took the first string value regardless of method. It now applies the same raw-sample aggregate as the main query's buffer source.
- **File discovery on Windows and under glob-special directory names** (PR #125) — every scan built its pattern with `path.join(dataDir, …)`, and `glob()` reads a backslash as an escape, so on Windows daily aggregation, retention cleanup, compaction, migration, GPX import, cloud compare/sync, schema validation and the two data migrations all matched nothing and silently did no work. A data directory whose name contains glob syntax (`+(1)`, `[1]`, …) broke the same jobs on Linux. New `src/utils/glob-in.ts` passes the directory as glob's `cwd` and keeps patterns relative with forward slashes; all 16 call sites use it. The `/processed/`-style exclusion filters became glob `ignore` patterns (they never excluded anything on Windows). `HivePathBuilder.detectPathStyle` takes the platform separator, so a backslash is a separator on Windows only and stays a legal filename character on POSIX. Cloud object keys built from local paths always use `/`; the daily upload had been producing backslash keys on Windows. Thirteen new tests, including an integration suite that runs the real services under a `+(1)` directory so the failure reproduces on Linux CI.
- **Analysis model selector had no effect** (PR #126) — `/api/analyze` accepted `claudeModel` but never passed it on, so every analysis ran on the configured model. The route now sets the request's model and the selector's first option is "Configured default", which sends nothing. Follow-up questions still use the configured model.

### Changed

- **Analysis moves to current Claude models** (PR #126) — Opus 5 (`claude-opus-5`), Sonnet 5 (`claude-sonnet-5`, the default) and Haiku 4.5 (`claude-haiku-4-5`); the previous list offered Sonnet 4 / Opus 4.1 / Opus 4, and Opus 4.1 has been retired by the API. A saved id from an older generation maps to the current model of the same tier. Opus 5 and Sonnet 5 reject `temperature` and think by default, so `temperature` is only sent where accepted, `max_tokens` is raised to at least 16k where thinking shares the budget, answers are read from the text blocks rather than `content[0]`, and a `refusal` stop reason is raised as an error instead of yielding an empty analysis.
- **Dead code removed** (PR #127), no behaviour change: the unused `initializeS3` / `createS3Client` / `createR2Client` aliases and `S3TestApiResponse` type in `data-handler.ts`, the never-read deprecated `start` history parameter, the ~90-line `ParquetWriter.getTypeFromOtherFiles` whose only caller was commented out, and the `as any` / `as Delta` casts that worked around a `@signalk/server-api` typing bug fixed upstream in SignalK/signalk-server#2043.

### Dependencies

- **Lockfile updated past open advisories** (PR #123): `multer` 2.2.0 → 2.3.0 (high), `body-parser` 2.2.2 → 2.3.0, `qs` 6.15.3 → 6.16.0, `js-yaml` 4.2.0 → 4.3.2, `brace-expansion` 2.1.1 → 2.1.4. No `package.json` change.

---

## [0.7.44-beta.4] - 2026-09-10

### Security (PR #117, @msallin)

- **Untrusted SQL is now read-only** — the sandboxed DuckDB instance added in 0.7.44-beta.2 confines *where* the engine may touch the filesystem, but the plugin's data directory is exactly the allowed directory, so `COPY (...) TO 'navigation_position.parquet'` from the raw `/api/query` endpoint or from model-generated analysis SQL could overwrite recorded data without leaving the sandbox — and the analysis path is not behind `enableRawSql`. New `src/utils/sql-guard.ts` validates every statement at both call sites before a connection is taken: a statement whitelist (SELECT/WITH/FROM/DESCRIBE/SUMMARIZE/EXPLAIN/VALUES/PIVOT/TABLE/SHOW), the file- and state-changing keywords (ATTACH, COPY, EXPORT, SET, PRAGMA, INSTALL, CALL, …) rejected anywhere in a statement so `EXPLAIN ANALYZE COPY …` cannot smuggle one through, table functions that open files or run nested SQL (`read_text`, `read_blob`, `glob`, `query`, `sqlite_*`) rejected by name, and one statement per request. Literals, comments, escape strings, dollar-quoted strings and quoted identifiers are masked in a single lexer pass, because two passes could be desynchronised (`SELECT 1 AS "a--b"; COPY …` executed both statements while a two-pass mask saw one). Replaces the analyzer's old `includes('CREATE')` check, which also rejected any query mentioning `created_at`.
- **Sandbox configuration locked** — `SET lock_configuration=true` is the last step of sandbox setup, so the engine itself refuses `SET memory_limit` / `SET enable_external_access` / `SET allowed_directories` for the life of the instance. Previously `EXPLAIN ANALYZE SET memory_limit='4GB'` lifted the sandbox's 512MB cap to 3.7GiB; now only the keyword guard stood in the way, and the engine should refuse outright.
- **Path substitution hardened** — the raw-query placeholder replacement uses a replacer function, so `$&` in a user-supplied path can no longer splice unvalidated text into the executed SQL; the guard validates the string that actually executes.

### Changed

- **Raw `/api/query` results are capped at 10,000 rows** (PR #117) and read as a bounded prefix rather than materialised in full (a 2M-row result: 3.9 MB / 16 ms instead of 429 MB / 1.2 s). The response carries `truncated: true` when the cap applied. Model-driven analysis SQL is bounded the same way at its existing 500/1000-row limits. Rejections are logged server-side on both paths, and `DuckDBPool.shutdown()` closes both native instances instead of waiting for a finalizer.

### Added

- **Track API provider (SignalK/signalk-server#2995)** — new `src/track-provider.ts` registers signalk-parquet as a provider for the server's upcoming Track API (`GET /signalk/v2/api/tracks`, `/tracks/contexts`), answering from raw-tier `navigation.position` parquet federated with the live SQLite buffer. Implements the contract as settled on that PR: whole-window tracks as a GeoJSON `MultiLineString` per context, split at recording gaps; `bbox` selects tracks rather than clipping them; time-bucket thinning driven by `resolution` / `maxPoints` with the applied spacing reported; Douglas-Peucker `simplify` / `epsilon`; `times` (`coordTimes`); co-recorded `properties` matched to the nearest sample within a tolerance (talkers stamp position and speed a few hundred ms apart, so an exact-timestamp join returns nothing); angular properties use a circular mean folded to `[0, 2π)`. Registration is duck-typed and the typings are copied rather than imported, so nothing new is pulled in.

  **Caveat: inert without server support.** The Track API is not part of a released signalk-server yet. On a release server the plugin logs one debug line and skips registration; history, recording and the webapp are unchanged. The provider only becomes reachable on a server carrying SignalK/signalk-server#2995, where it has been exercised side by side with `@signalk/tracks-plugin`. Until that PR merges, the contract may still move, and the copied typings and behaviour in `track-provider.ts` will need to follow it.

### Dependencies

- **`@dsnp/parquetjs` pinned to `~1.8.9`** — the 1.9.3 release on npm was published without its `dist/` directory (frequency-chain/parquetjs#206), so `require` fails, the writer silently falls back and every parquet export breaks. The previous `^1.8.7` range allowed it on any fresh install; dependabot PR #121, which included that bump, failed 25 integration tests for this reason. Widen the range again once upstream publishes a repaired release.
- CI: `plugin-ci.yml` workflow pin updated (PR #120).

---

## [0.7.44-beta.3] - 2026-08-27

### Fixed

- **Context ids with literal dashes (UUID vessels) returned corrupted by the History API** (#71, PR #115) — the hive directory encoding is many-to-one (`:` and literal `-` both become `-` in directory names), so reconstructing a context from its directory name turned a default (no-MMSI) install's UUID vessel id into a colon-riddled invalid URN (`…uuid:c0d79334:4e25:…`), breaking client-side identity matching against the server's real self context. Contexts are no longer reconstructed from directory names: the contexts listing resolves each matching directory's true context string(s) from the parquet `context` data column (TTL-cached per directory; sequential resolution so a large AIS store can't stampede the DuckDB pool), and the spatial contexts endpoint returns the data column directly, removing a second, independent corruption (it was un-sanitizing an already-correct value). Because the encoding is many-to-one, two distinct contexts (`a:b` / `a-b`) can share one directory — the resolver now recovers **all** colliding contexts, range-filtered so a collider with no data in the requested window isn't reported. Both queries pass `hive_partitioning=false` explicitly: DuckDB auto-detects `key=value` path segments and the sanitized partition value silently shadows the file's identically-named data column. No on-disk change — directories keep their names, no migration, previously-stored (mangled) context strings still resolve to the same partitions.
- **S3-backed history queries silently dropped the final partial day** (#72, PR #116) — `getDayPatterns`, which builds the S3 day-partition brace list, advanced its day cursor while keeping `from`'s time of day, so a query ending mid-day (e.g. `…15T12:00Z → …16T06:00Z`) never read the last day's partition — truncating the tail of cloud query results. The cursor is now normalized to UTC midnight so every calendar day the range touches is covered; reversed ranges are rejected up front; exactly-7-day ranges keep the explicit day list instead of falling to a wildcard (off-by-one against the documented cap); and `buildS3Glob` returns `null` for an empty range instead of a malformed `{}` glob. Same fixes applied to the latent twin `getDaysInRange`.

### Dependencies

- Dev: `@stryker-mutator/core` / `@stryker-mutator/mocha-runner` 9.6.1 → 10.0.0 (bumped together; the mutation config needed no changes).

---

## [0.7.44-beta.2] - 2026-08-26

Rolls up everything merged since 0.7.43. (`0.7.44-beta.1` was an internal deploy only — never published to npm.) Major thanks to @msallin, who contributed the security, correctness, and reliability work in this release (PRs #89, #91, #92, #93, #114).

### Security (PR #89)

- **Boundary validation for untrusted contexts and paths** — new `src/utils/signalk-validation.ts`: `validateContext` / `validateSignalKPath` allow-list validators now guard every place an HTTP-supplied `?context=` or `?paths=` value reaches a DuckDB `read_parquet` glob or a filesystem path (V1 routes, the V2 provider, and path-config POST/PUT). Glob metacharacters, quotes, slashes, and `..` are rejected up front instead of sanitised. `assertWithinDataDir` (lexical + symlink-resolved containment) stops bucket-supplied keys in cloud sync from reaching files outside the data directory.
- **SQL-literal escaping across the query layer** — every interpolated `read_parquet('…')` / `parquet_schema('…')` file path in `HistoryAPI.ts`, `history-provider.ts`, `api-routes.ts`, `context-discovery.ts`, `path-discovery.ts`, and `claude-analyzer.ts` now goes through `escapeSqlString`.
- **Sandboxed DuckDB instance for untrusted SQL** — `/api/query` (the opt-in raw SQL endpoint) and Claude-generated analysis SQL no longer run on the main pool, which holds the S3 credential SECRET. They use a separate sandbox instance with `enable_external_access=false` and `allowed_directories` restricted to the data directory: no network, no httpfs, no cloud credentials, no reads outside the store.
- **SSRF guard on custom S3 endpoints** — a custom `cloudUpload.endpoint` resolving to a private, loopback, or link-local address is now rejected unless the new `allowPrivateEndpoint` config flag is set (needed for self-hosted MinIO/Garage on the boat LAN; off by default so a malicious config can't probe the cloud metadata service or the local network).
- **Information-disclosure cleanups** — the `/tmp/claude-prompt-debug-*.txt` dump of full analysis prompts is gone; cloud connection-test and compare-job errors return generic messages (topology details stay server-side); DuckDB errors reaching LLM tool-results and HTTP responses are genericized; `/api/analyze` no longer logs user prompts to stdout.

### Fixed

- **Angular tier→tier re-aggregation produced NULL buckets** (PR #91) — rolling 5s→60s→1h aggregates of angular paths read `value_sin_avg`/`value_cos_avg` from source files that may predate those columns; `SUM` over the NULLs made the whole bucket NULL. The rollup now falls back per-row with `COALESCE(value_sin_avg, SIN(value_avg))` (and cos), and — follow-up — probes the source schema first so all-legacy file sets, where the columns can't even be named without a binder error, derive from `value_avg` directly.
- **Sub-second history resolutions collapsed into one bucket** — the bucket-timestamp SQL wrapped the (millisecond-correct) `FLOOR(EPOCH_MS(...)/resolution)` arithmetic in `DATE_TRUNC('seconds', …)` with a seconds-only format, so distinct sub-second buckets got identical timestamp strings and merged in GROUP BY. All six query paths now share one `bucketExprSql()` helper that keeps millisecond precision (`%S.%gZ`) for fractional resolutions while preserving the fraction-free format for whole-second ones. With it, the V1 auto-resolution is clamped to ≥1 ms and computed in milliseconds (PR #91) — previously a `from == to` or sub-500 s range truncated to a 0 ms divisor. Regression tests cover both.
- **Timeout-only deltas were silently discarded** (PR #91) — `timeout` was in `metaOnlyKeys`, so an object delta whose only key is `timeout` was treated as metadata and dropped.
- **Spatial-filter query errors blanked every correlated path** (PR #91) — a transient DuckDB failure during `bbox`/`radius` position correlation returned an *empty* timestamp set, which read as "no positions matched" and emptied every correlated path in the response. Errors now return `null` (= no filtering, data served unfiltered) at both correlation sites; end-to-end tests pin all three outcomes (inside box, outside box, query error).
- **Date-only timestamps parsed inconsistently** (PR #91) — `parseDateTime('2025-08-13')` went through `new Date()` as UTC midnight while `'2025-08-13T08:00'` parsed as local time. Date-only inputs now normalise to local midnight, matching the documented bare-timestamp contract.
- **Migration jobs: cancellation overhauled** (PR #91 + follow-ups) — cancel is now per-job (`progress.cancelRequested`) instead of a shared service flag that let concurrent jobs cross-cancel, works during the `scanning` phase, and — via a shared `finishIfCancelled()` at every phase boundary — can no longer be swallowed by the empty-file-list path, the cleanup/aggregation phases, the final file's window, or an error landing after a cancel (the job stays terminal as `cancelled`, not `error`/`completed`).
- **Legacy `retentionDays` migration could be lost on early exit** (PR #91) — the one-time config migration's `savePluginOptions` was fire-and-forget; it is now awaited so the persisted sentinel can't be skipped.
- **Threshold monitor subscription leak** (PR #114) — `updateCommand` built the monitor key ad-hoc (`${commandName}_${watchPath}`) instead of with `buildThresholdMonitorKey()`, so the old streambundle subscription was never found and unsubscribed — every threshold edit leaked a live subscription. Fixed and covered by unit tests that fail on the unfixed code.
- **Silent failures now logged** (PR #114) — three empty `catch {}` blocks in `data-handler.ts` (command handling, per-delta stream handling, buffer flush) now log through `app.error`; a failed AWS SDK import (cloud upload configured but SDK unavailable) is logged instead of silently disabling sync forever.
- **Cloud uploads can no longer hang the export pipeline** (PR #114) — S3/R2 clients now set `connectionTimeout: 10s` and `requestTimeout: 60s` with `throwOnRequestTimeout: true` (without the flag the AWS SDK only *warns* when the ceiling elapses). A dead or stalled uplink now fails the upload (and retries) instead of wedging the daily export.
- **SQLite buffer close hardening** (PR #114) — `_open` flips before `db.close()` so a throwing close can't leave the buffer claiming to be open; `insertBatch()` gained the same `_open` guard as `insert()`. New integration test pins the close-state contract.
- **`stop()` data-loss window narrowed** (PR #114) — subscriptions are unsubscribed *before* the final buffer flush (previously a delta arriving between flush and teardown was lost), with each unsubscribe individually wrapped so one throwing teardown can't skip the flush.

### Changed

- **Dead historical-streaming feature removed** (PR #93) — the never-enabled `HistoricalStreamingService` (~1,800 lines: ws server, commented-out routes, five dead config/state fields) is gone from the codebase and the shipped package.
- **Logging and hygiene** (PR #92) — the Claude analyzer's 13 unconditional `console.log` diagnostics (including full result-set dumps) now route through `app.debug`; job-id generation is centralized in `src/utils/job-id.ts`.
- **eslint 10 / prettier 3.9.6 / TypeScript lib update** — lint stack upgraded (`@eslint/js` added — eslint 10 no longer provides it transitively); the two new recommended rules are satisfied for real: 8 rethrown errors now attach `{ cause }` (`lib: ES2022.Error` added for the typings), and the unreachable manual-traversal block + orphaned `traverseSignalKPaths` helper in `claude-analyzer.ts` (~210 lines, issue #73) are deleted rather than suppressed.

### Dependencies

- Runtime: `@anthropic-ai/sdk`, `@aws-sdk/client-s3`, `@signalk/server-api` 2.31.1, `@dsnp/parquetjs`, `@js-joda/core`, `fs-extra`, `minimatch`, `multer` types, `ws`, and friends (grouped minor/patch updates).
- Dev: `@types/node` 26, `c8` 12, `mocha` 11.8, `@typescript-eslint` 8.67, CI action bumps.

---

## [0.7.43] - 2026-08-21

Stable release — includes the `0.7.43-beta.1` fixes below (daily aggregation moved to a short-lived worker to stop the midnight memory ratchet/OOM; offline startup no longer blocked by the spatial-extension download) plus the following changes since beta.1.

### Added

- **`sma` / `ema` moving averages implemented in the v2 History API provider** — the SignalK spec's `path:sma:5` / `path:ema:0.2` aggregate methods previously fell through the provider's aggregate-function mapping to plain `AVG`, so the provider silently returned unsmoothed bucket averages. A new `src/utils/smoothing.ts` implements the trailing moving window as post-processing over the already-bucketed, time-ordered, federated (parquet + buffer) series: buckets are computed exactly like `average`, then the window is applied per path. Defaults follow the spec: SMA window 5 samples, EMA alpha 0.2, parsed from the PathSpec `parameter` array.
  - **Angular-aware** — paths with `rad` units (heading, COG, wind direction) are smoothed on the circle (sin/cos means recombined with `atan2`) so the 0/2π wrap can't average 359°/1° toward 180°. `navigation.position` smooths latitude linearly and longitude circularly in degrees, so tracks crossing the ±180° antimeridian don't smooth through 0°. Object-path components with `rad` units get the circular variant too; other numeric components smooth linearly.
  - **Non-finite guards** — null/NaN buckets (e.g. `AVG` over values that failed `TRY_CAST`) pass through unsmoothed instead of coercing to 0 or poisoning the recursive EMA; a non-finite window/alpha degrades to identity instead of propagating NaN.
  - Covered by unit tests (`test/unit/utils/smoothing.test.ts`) and integration tests (`test/integration/history-provider-smoothing.test.ts`).

### Fixed

- **Plugin reconfigure silently dropped live data from history reads** — changing the plugin config runs `stop()` → `start()` without a server restart. Every restart re-registered the V1 history express routes, leaving the originally-registered routes bound to a `HistoryAPI` instance whose SQLite buffer `stop()` had closed — and a closed buffer makes federation return *nothing* with *no error*, so all live (unexported) data vanished from history results until a full server restart. The routes are now registered once and the single instance is re-pointed on reconfigure (fresh buffer, S3 config, auto-discovery service, data directory, and retention overrides).
  - The path/context caches and the object-path schema cache now key on the data directory (the schema key is a JSON-encoded tuple so colon-bearing vessel URN contexts or Windows drive paths can't collide), so a changed `outputDirectory` can never serve results discovered in the old store.
  - Requests snapshot the data directory, buffer, S3 config, and retention rules once at request start, so a reconfigure landing mid-request can't mix the old store's parquet data with the new store's buffer.
- **`stop()` left scheduled work running and could leave a truncated aggregation file** — `plugin.stop()` now sets an `isStopping` flag (so export callbacks that fire during the async teardown become no-ops), cancels the pending one-shot daily/startup export timers, and winds down in-flight aggregation workers cooperatively: a shutdown message makes the worker finish its in-flight DuckDB `COPY`, stop at the next group boundary, and report the run as failed — so the retention-cleanup guard skips deletion, same as any other worker failure. Workers still alive after a 15 s grace period are SIGKILLed so a stuck `COPY` can't hang shutdown. Aggregation output is now written to a `*.tmp` name and renamed into place after a validity check, so a process killed mid-`COPY` leaves an invisible temp straggler (removed by the startup sweep) instead of a truncated `.parquet` at the query-visible name.
- **Startup catch-up export could export the current day early** — the boot-time catch-up treated any UTC day before today as eligible, so a restart between UTC midnight and `dailyExportHour` exported the in-progress local day hours ahead of its scheduled run. A day is now eligible only once its scheduled export time (`dailyExportHour` UTC on the following day) has passed; multi-day backlogs still fully catch up. `dailyExportHour` is also validated once at config intake (integer 0–23; anything else logs an error and falls back to the default 4), so the scheduler and the catch-up cutoff can never disagree about which day is eligible.
- **Object-path schema discovery: per-file store walk replaced with one DuckDB query** — `getPathComponentSchema` listed every parquet file under a path (via the cached `DirectoryScanner`) and queried each file's schema one at a time. It now runs a single `DESCRIBE SELECT * FROM read_parquet('year=*/day=*/*.parquet', union_by_name=true)` — the same component union, computed natively, never descending into `quarantine/`/`failed/` siblings, with no retained filename list. Real failures (corruption, permissions) now propagate to the caller instead of being swallowed as "no schema", which had misread failing object paths as scalar. `DirectoryScanner` is also gone from `ParquetWriter`, whose cache-invalidation bookkeeping was obsolete.
- **Angular components in object-path buckets were averaged linearly** — under average-like aggregates (`average`, default, `sma`, `ema`), object-path components with `rad` units now bucket with the circular (vector) mean, and `navigation.position` longitude buckets with a circular mean in degrees, so headings embedded in object paths and antimeridian-crossing tracks aggregate correctly. Non-numeric object components now use `ANY_VALUE(col ORDER BY signalk_timestamp)` instead of `FIRST`, so a bucket whose physically-first row is NULL still returns its earliest non-NULL value deterministically.

---

## [0.7.43-beta.1] - 2026-07-19

### Fixed

- **Nightly memory step / midnight OOM from the daily aggregation** — the daily aggregation (`runDailyAggregation`) globs yesterday's tree and runs tens of thousands of DuckDB `read_parquet` calls (every context/path group × 3 tier transitions × DESCRIBE+COPY+count). DuckDB's allocator frees that memory internally but does **not** return the pages to the OS within the process — verified: `closeSync()` + forced GC reclaims 0 MB, and it is not DuckDB-specific (a node:sqlite read+aggregate loop retains just as much). So running it inside the long-lived SignalK server ratcheted ~¾ GB of non-reclaimable RSS at 00:00 that stuck until the process was restarted; on a busy AIS day this pushed the heap into the ~2 GB ceiling and the server OOM-crashed at midnight (and any instance already carrying a day's accumulation died on the next midnight step). The `read_parquet` volume scales with the store size, which is why large installs stepped at midnight while small ones did not.
  - The daily aggregation now runs in a **short-lived forked worker** (`src/aggregation-worker.ts`) that does the identical work and **exits** — process exit is the only reliable way to return the memory to the OS, regardless of allocator behaviour. The main SignalK process no longer accumulates it (measured: parent RSS flat across a run that would otherwise ratchet).
  - **No behaviour change:** same tiers, same aggregation math, same output files, every vessel still aggregated. Angular paths (heading/COG/wind direction, `units === 'rad'`) still get vector averaging — the angular-path set is computed with the live server's `app.getMetadata` in the parent and passed to the worker (which has no live metadata). New `SQLiteBuffer.getPaths()` exposes the recorded path names for that computation.
  - A worker crash, non-zero exit, or 30-minute timeout is surfaced as a failed aggregation, so the existing retention-cleanup guard (which only deletes rolled-up/uploaded data when aggregation succeeded) is unaffected. Only the aggregation moves to the worker; export (buffer-driven, no ratchet), upload, and retention cleanup stay in-process.
- **Plugin failed to start when first enabled offline** — `INSTALL spatial` runs on the critical startup path, and the first load downloads the extension from DuckDB's repo (it is only cached under `extension_directory` after a successful start with connectivity). If the plugin was installed while online but first *enabled* with no network, that download threw and — because the call was unguarded — rejected `plugin.start()` entirely, taking parquet writing and the history API down with it (not just spatial). Spatial setup is now best-effort: a load failure is logged via `app.error` and startup continues, so everything except spatial queries works offline. A new `DuckDBPool.isSpatialAvailable()` exposes the state. The extension still downloads and caches normally on the next start with connectivity, restoring full spatial support. Instance creation and the memory-limit PRAGMA remain fatal (clean retry), unchanged.

---

## [0.7.42] - 2026-07-18

Stable release — promotes the `0.7.42-beta` line (beta.1, beta.2) to a tagged npm release. No code changes since beta.2; see the beta entries below for the full set of fixes (App Store activation fix, the buffer-staging SIGBUS crash fix, and the PR #96 review fixes).

---

## [0.7.42-beta.2]  - 2026-07-17

### Fixed

- **Plugin failed to activate where `$HOME` is read-only (App Store CI sandbox)** — `DuckDBPool.initialize()` created its instance with no config, so DuckDB defaulted its extension/home directory to `$HOME/.duckdb`. On the Signal K App Store test runner `$HOME` (`/home/runner`) is read-only, so `INSTALL spatial` aborted activation with `IO Error: Failed to create directory "/home/runner/.duckdb": Read-only file system` — which is why recent versions showed red in the App Store while `0.7.15` (pre-spatial) passed. DuckDB is now pointed at `<outputDirectory>/.duckdb` (a writable path under the plugin's own data directory) via `home_directory`/`extension_directory`/`temp_directory`, so activation no longer depends on `$HOME` and the downloaded extensions are cached across restarts. Added `.github/workflows/plugin-ci.yml` (Signal K's reusable plugin-CI workflow) so install/load/activate results populate the App Store "Indicators" tab; armv7 is disabled there because DuckDB ships no 32-bit ARM binding.

---

## [0.7.42-beta.1] - 2026-07-14

### Fixed

- **Nightly SIGBUS crash at midnight (server "Bus error", core dump)** — Federated history queries ATTACHed the live `buffer.db` into DuckDB (`ATTACH ... (TYPE SQLITE, READ_ONLY)`). DuckDB bundles its own SQLite, so two independent SQLite libraries opened the same WAL-mode database inside one process. POSIX advisory locks never conflict within a single process, so DuckDB's copy could take "exclusive" recovery locks while node:sqlite was live, and truncate `buffer.db-shm` (96 KB → 32 KB) under node:sqlite's active mmap. The next large write transaction — the midnight empty sweep, which balloons the WAL past the first 32 KB shm region — then wrote through the stale mapping and died with SIGBUS in `walIndexAppend`, taking the whole SignalK server down at 00:00.
  - DuckDB no longer opens `buffer.db` at all: `DuckDBPool.getConnectionWithBuffer()` and the ATTACH are gone.
  - New `stageBufferTable()` (`src/utils/buffer-staging.ts`) reads the rows a query needs (one path, one context, time-windowed, unexported only) through the buffer's own node:sqlite connection — keyset-paginated in 5,000-row batches so JS heap stays bounded — and copies them into a per-connection DuckDB TEMP table via the appender; queries whose buffer window exceeds the 1M-row cap are logged and rejected rather than answered with incomplete data (landing exactly on the cap succeeds).
  - The federated SQL is unchanged in shape: `read_parquet(...) UNION ALL <buffer subquery>` now reads the staged temp table instead of `buffer.<table>`, so aggregation semantics (time buckets, priority merge, filters) and freshness are identical.
  - `buildBufferScalarSubquery` / `buildBufferObjectSubquery` take the staged table name and always return SQL; the existence check moved to staging (returns null for unknown paths or empty windows, skipping the UNION as before).
- **Query Database "Generate Query" read quarantined files and failed** — for Hive-partitioned paths the generated query and example queries globbed `path=<path>/**/*.parquet`. The `**` recursed into the sibling `quarantine/` (and `failed/`, `processed/`, `repaired/`) directories, so DuckDB tried to read a quarantined 0-byte parquet file and aborted the whole query with `Invalid Input Error: File ... too small to be a Parquet file`. The generators (`generateQueryForPath` and the example-query builder in `public/js/pathBrowser.js`) now emit `year=*/day=*/*.parquet`, which matches only day-partition data files and never descends into sibling directories. Flat (non-Hive) paths still use `*.parquet`.
- **Federated buffer queries windowed by receipt time instead of measurement time** — the buffer half of the federated `read_parquet(...) UNION ALL <buffer>` query filtered its time window on `received_timestamp`, while the parquet half and all time bucketing use `signalk_timestamp`. Buffer samples whose measurement time fell inside the requested window but whose receipt time did not — delayed or replayed NMEA, backfill, clock skew — were dropped from history results (and samples received in-window but measured out-of-window were bucketed outside the range). Live data is unaffected because the two timestamps differ by milliseconds. The window filter now uses `signalk_timestamp` in all three buffer read paths: `getRowsForFederation` (the staging prefilter) and both `buildBufferScalarSubquery` / `buildBufferObjectSubquery` builders. The staging query is keyset-paginated by `id`, so no timestamp index change is needed; `idx_<table>_received` stays for the export path, which is still correctly keyed on receipt time. (Pre-existing since the buffer-federation feature; surfaced during PR #96 review.)
- **Object-path parquet fallback hardened against non-`REAL` numeric columns** — when a parquet read fails and the History API falls back to the buffer, object-path components were typed by testing whether the SQLite column type `includes('REAL')`. Per-path buffer tables currently declare every `value_*` column `REAL`, so this was correct in practice, but it silently coupled the fallback to that declaration. The check now matches all numeric SQLite affinities (`INT`/`REAL`/`FLOA`/`DOUB`/`NUM`/`DEC`), so an integer-typed component can never degrade to a string aggregation if the buffer schema changes.

---

## [0.7.41] - 2026-07-06

Stable release — promotes the 0.7.41-beta line (beta.2, beta.3) to a tagged npm release. No code changes since beta.3; see the beta entries below for the full set of fixes (incremental startup sweep and faster History API path listing, PR #88).

---

## [0.7.41-beta.3] - 2026-06-30

### Fixed

- **Multi-minute startup freeze on large stores** (PR #88) — `quarantineEmptyParquetFiles` ran on every plugin start and globbed the _entire_ data directory (`**/*.parquet`), statting every match. On installs with millions of parquet files this froze `plugin.start` for 6–20 minutes — event loop pegged at 100% CPU, RSS climbing to ~4 GB — before reaching DuckDB initialization. The sweep is now **incremental**: it persists a last-sweep timestamp in `<dataDir>/.last-empty-sweep` and only inspects directories whose mtime is newer than the watermark (a 0-byte stub is a newly _created_ file, which bumps its parent directory's mtime). The first start after upgrading scans once to catch any pre-existing stub, then every later start is O(directories changed since the previous start). The `glob`/micromatch pass is replaced with a plain async `readdir`/`stat` walk, so even the one-time first scan no longer blocks the event loop. The watermark carries a 2 s slack so coarse-resolution filesystems can't round a just-created directory below the cutoff, and the walk falls back to `stat` for symlinked/unknown directory entries.
- **Slow History API path listing on large stores** (PR #88) — the History API path list (`getPaths` and the legacy `/signalk/v1/history/paths` branch) counted _every_ parquet file under each path just to test `> 0`, then discarded the count — a synchronous stat-walk of the whole store on the first request after a restart. It now short-circuits on the first parquet file found per path. Callers that display the count (`GET /api/paths`, the analyzer prompt) are unchanged.

---

## [0.7.40-beta.1] - 2026-05-01

### Breaking

- **`retentionDays` is now actively enforced.** Previous versions defined `retentionDays` but never scheduled cleanup, so any persisted value sat dormant. Cleanup now runs once per day, right after the daily export. **If your persisted config has `retentionDays` set to anything other than `0` and you don't want daily deletion of older data, set it to `0` (keep forever) before upgrading.** Older installs may carry a persisted `retentionDays: 7` that was the legacy default; check via the SignalK admin UI under the SignalK to Parquet plugin config. The new default for fresh installs is `0`.

### Added

- **GPX Track Import** (thanks @msallin, PR #51) — New workflow to load historical GPX tracks (recorded by other vessels, handhelds, or archived logs) directly into the Hive-partitioned parquet store, bypassing the live SignalK subscription path.
  - **Status tab UI** — Drag-and-drop / click-to-pick file zone with keyboard support (`role=button`, Enter/Space, `aria-live` progress). Non-`.gpx` files are ignored with a visible note; folder drops show a clear unsupported message. The Start button is disabled while a job is running or an upload is in flight, and a `beforeunload` warning fires if the user tries to leave mid-upload. Filenames are rendered via `textContent` so user-supplied names cannot inject markup.
  - **Browser uploads** — Multipart upload via `multer` disk storage, streamed to a per-request temp dir under `plugin-config-data/gpx-uploads/<sessionId>/`. Per-file cap is 50 MB (`GPX_UPLOAD_MAX_FILE_BYTES`) with a 500-file ceiling per request (`GPX_UPLOAD_MAX_FILES`). Temp-dir cleanup exits when the job finishes, when the progress entry is TTL-evicted, or after a safety deadline.
  - **Server-directory mode** — Preserved behind an "Advanced" details block for USB-drive bulk imports on the host (Scan + Delete-source checkbox).
  - **Job tracking** — Per-`jobId` cancellation prevents concurrent imports from trampling each other; output filenames carry a ms timestamp + random suffix so two jobs writing into the same partition never collide. Finished jobs stick around for 30 minutes (`IMPORT_JOB_TTL_MS`) for the UI to poll.
  - **API routes** — `POST /api/import/gpx/scan`, `POST /api/import/gpx`, `POST /api/import/gpx/upload`, `GET /api/import/gpx/progress/:jobId`, `POST /api/import/gpx/cancel/:jobId`, `GET /api/import/gpx/jobs`.
  - **Parser** — Dependency-free GPX 1.0 / 1.1 parser (`src/utils/gpx-parser.ts`) that extracts `<trkpt>` lat/lon/time plus optional `<ele>`, `<speed>`, `<course>`. Trackpoints without `<time>` are skipped (no partition key).
  - **Unit handling** — `<speed>` pass-through in m/s, `<ele>` pass-through in metres, `<course>` converted from degrees to radians (matches `navigation.courseOverGroundTrue`).
- **New dependency** — `multer` (with `@types/multer`) for the browser-upload endpoint.

---

## [0.7.30] - 2026-04-22

Stable release — promotes the 0.7.20-beta line (beta.2 through beta.6) to a tagged npm release. No code changes since beta.6; see beta entries below for the full set of features and fixes included.

---

## [0.7.20-beta.6] - 2026-04-21

### Added

- **Position Aggregation Migration** — New `POST /api/migrate/position-aggregation` endpoint re-aggregates position paths into 5s/60s/1h tiers. Scans the raw tier for paths whose schema includes `value_latitude`/`value_longitude`, then runs targeted aggregation against just those paths. Supports `dryRun` for previewing.
  - GPS outlier rejection via `POSITION_MAX_SPEED_MPS` (25 m/s ≈ 48.6 kn) — discards single-point glitches whose implied speed from their temporal neighbor exceeds the cap
  - `AggregationService.aggregateDate()` and `aggregateTier()` now accept an optional `pathFilter` for targeted re-aggregation
  - Aggregator auto-detects position schema (`value_latitude`/`value_longitude`) alongside scalar schema
- **Batched Parquet Writing** — New `ParquetWriter.writeParquetBatched()` streams records to Parquet in pull-based batches instead of loading all rows into memory. Reduces peak memory during large exports from the SQLite buffer.
- **Central Constants Module** — New `src/constants.ts` as the single home for tunable numeric constants. Starts with `POSITION_MAX_SPEED_MPS`; future tunables land here before graduating to plugin config.

### Fixed

- **Output Directory Location** (thanks @tkurki, PR #48) — When `outputDirectory` is unset, it now defaults to `app.getDataDirPath()` (i.e., `plugin-config-data/<pluginId>/`) instead of being created directly under the settings directory.
- **Streambundle Subscription Disposal** — Streambundle subscriptions are now properly disposed during shutdown and in `updateDataSubscriptions`, preventing leaked listeners on regimen changes and plugin restart.

### Changed

- **`api-routes.ts` Refactor** — Extracted `getDataFilesForPath` helper and switched call sites to `PluginState.getDataDirPath()` for consistent data-dir resolution. Removed unused variables.
- **ParquetExportService Formatting** — Minor readability cleanup; no behavior change.

---

## [0.7.20-beta.4] - 2026-04-05

### Fixed

- **Shutdown Data Loss** — Fixed race condition where SQLite buffer was closed before data subscriptions were torn down, causing "SQLite buffer is closed! Data will be lost" errors during plugin restart. Subscriptions are now unsubscribed first, then the buffer is safely closed.

### Added

- **Regimen-Aware Subscription Updates** — Data subscriptions now dynamically update when regimen commands are received via SignalK deltas. Previously, toggling a regimen only took effect after plugin restart.

---

## [0.7.20-beta.3] - 2026-03-23

### Added

- **Saved Areas — SignalK Resources Sync** — Saved search areas now persist to the SignalK server via the Resources API (`zeddisplay-search-areas` resource type)
  - Shared with ZedDisplay: areas saved in either app appear in both
  - Server is source of truth; localStorage used only for unsynced offline saves
  - Automatic push of locally-created areas on next server contact
  - Delete propagates to server so all clients stay in sync
- **High-Resolution Track Option** — Checkbox in Save as Route panel fetches raw-tier position data for maximum track fidelity
  - Fetches full `navigation.position` history at 1-second resolution
  - Status indicator shows point count after fetch
- **Vessel Name Display** — Context dropdown now shows actual vessel names (e.g., "Sea Breeze (338123456)") instead of raw MMSI numbers
  - Batch-fetches `name` path from history for each discovered context
  - Searchable context list with filter-as-you-type
- **Auto-Select First Data Point** — After a query completes, the first data point is automatically selected so detail panel and sparklines are immediately visible

### Changed

- **Route Simplification** — Reduced default turn-detail tolerance for more accurate route geometry; user-adjustable tolerance slider
- **Query Panel Layout** — Reorganized query configuration panel for better flow; date range end auto-suggests start + 1 day
- **String Path Handling** — HistoryAPI now correctly queries string properties (`name`, `mmsi`, `uuid`, `flag`, `port`, `callsignVhf`) using raw tier and `FIRST(value)` aggregation instead of attempting numeric `AVG()` casts

### Fixed

- **String Path Aggregation** — String properties (`name`, `mmsi`, `uuid`, etc.) now return their string value at any tier; previously returned NULL because `AVG()` cast failed. HistoryAPI forces raw tier and uses `FIRST(value)` to preserve the original string

---

## [0.7.20-beta.2] - 2026-03-22

### Added

- **Map Explorer** — New spatial query and visualization tab replicating ZedDisplay Historical Data Explorer
  - Draw bounding box or radius on interactive Leaflet map with draggable/resizable handles
  - Query historical data for any vessel within a geographic area and time range
  - Multi-path overlay (up to 3 paths) with color-coded track, chart, and data table views
  - Playback controls: play/pause, forward/reverse, speed control, scrub slider
  - Export: CSV, GeoJSON, KML
  - Save to SignalK resources: waypoints, tracks, routes
  - Save/load named areas for repeated queries
  - OpenSeaMap overlay toggle
  - Sparkline mini-charts in detail panel
  - Legend with 3-state visibility toggle (visible/active/hidden)
- **Searchable Path Selector** — Replaced 3 dropdown selects with searchable checkbox list
  - Filter paths by substring, select up to 3 with visual chips
  - Remaining checkboxes disabled when 3 selected
  - Removable chip badges for selected paths
- **Display Unit Conversion** — Data table, tooltips, detail panel, and summary stats now show values in user-configured display units
  - Fetches `/signalk/v1/api/{context}/{path}/meta` for each queried path
  - Applies `displayUnits.formula` conversion and `displayFormat` decimal formatting
  - Shows `displayUnits.symbol` as unit label (e.g., kn, °F, ft)
  - Context-aware: uses correct vessel context for meta lookups
- **Vessel Name Display** — Context dropdown shows `MMSI XXXXXXXXX` instead of raw URN strings
  - `contextDisplayName()` extracts MMSI via regex, shows "Self" for own vessel
  - Vessel count label above dropdown (e.g., "115 vessels found")
- **Path Configuration: Scalar Properties** — `name`, `mmsi`, `uuid`, and other root-level string/number/boolean properties now appear in the Add Path tree for both self and other vessels

### Changed

- **Date-Reactive Query Config** — Changing lookback or date range now immediately reloads available paths and vessel contexts (previously required manual interaction)
  - `setLookback()`, `setTimeMode()`, and date input `onchange` all trigger path and context refresh
  - Date range picker simplified to two compact `<input type="date">` in a single connected field
- **Context List Refreshes on Date Change** — When "Look up other vessels" is checked, changing the time range re-fetches the context list with the new date params

---

## [0.7.20-beta.1] - 2026-03-21

### Added

- **Bulk Aggregation Endpoint** — New `POST /api/aggregate/bulk` builds 5s/60s/1h tiers from all raw tier data in a single background job
  - Optional `startDate`/`endDate` to limit scope
  - Progress polling via `GET /api/aggregate/bulk/:jobId`
  - Cancel via `POST /api/aggregate/bulk/cancel/:jobId`
  - Discovers all dates across all contexts automatically
- **Post-Migration Aggregation** — Migration now automatically builds aggregation tiers after moving files to hive structure
  - New `triggerAggregation` param on `POST /api/migrate` (default: `true`)
  - Phase 4 runs after file migration: discovers dates from migrated files and aggregates each
  - Progress shows "Building tiers: 45/120 dates" during aggregation phase
- **Context-Aware Paths Endpoint** — `/history/paths` now accepts `?context=` parameter
  - `?context=vessels.urn:mrn:imo:mmsi:NNNNN` returns paths for that vessel
  - No context param returns self paths only (backward compatible)
  - Works on both `/signalk/v1/history/paths` and `/api/history/paths`
  - SignalK v2 HistoryApi `getPaths()` also supports context

### Fixed

- **Migrated Data Invisible** — Data migrated from flat to hive structure had no aggregated tiers (5s/60s/1h). Auto-tier selection picked empty tiers, returning no data for history queries. Bulk aggregation backfills the missing tiers.

### Changed

- **Migration UI Persistence** — Job ID stored in `localStorage` so migration progress survives page refreshes. Stale job IDs auto-clear when server returns 404.

---

## [0.7.15] - 2026-03-17

### Added

- **Spatial Context Discovery Endpoint** — New `GET /api/history/contexts/spatial` returns vessel contexts with position data inside a bounding box or radius for a given time range
  - `bbox=west,south,east,north` — bounding box filter
  - `radius=lon,lat,meters` — radius filter with precise `ST_Distance_Spheroid` check
  - Single DuckDB query on `navigation__position` files only, with `hive_partitioning=true` for automatic partition pruning
  - Reuses existing `parseSpatialParams()` and `buildSpatialSqlClause()` from spatial-queries.ts

### Changed

- **Context Discovery Rewritten for Hive Partitions** — `getAvailableContextsForTimeRange()` no longer scans 237K parquet files via DuckDB
  - Reads `context=*` directory names under `tier=raw/` and unsanitizes via `HivePathBuilder.unsanitizeContext()`
  - Time-range filtering checks `year=YYYY/day=DDD` subdirectory names with numeric comparison (no SQL)
  - Pure filesystem ops — sub-second even with 2700+ contexts
  - Context list cached with 2-minute TTL
- **Plugin Base Directory Resolution** — Replaced fragile `path.resolve(getDataDirPath(), '..', '..')` with `app.config.configPath` (the property SignalK server uses internally)

### Fixed

- **Context Discovery Returned Empty** — Legacy code scanned `dataDir/vessels/` flat layout which no longer exists; all data is in hive-partitioned `tier=raw/context=*` directories
- **Day-of-Year Off-by-One** — `dateToYearDay()` used `Date.UTC(year, 0, 1)` but `HivePathBuilder.getDayOfYear()` uses `Date.UTC(year, 0, 0)`; now aligned

---

## [0.7.10] - 2026-03-15

### Fixed

- **History Provider: `value_age` binder error** — Parquet files for `navigation.position` include a `value_age` column (GPS fix staleness) that the buffer table lacks. The buffer SQL builder now outputs `NULL::DOUBLE` for any component column missing from the buffer table, preventing DuckDB's "column cannot be referenced before it is defined" error
- **History Provider: `AVG(VARCHAR)` on position data** — Some parquet files store `value_latitude`/`value_longitude` as VARCHAR. With `union_by_name=true`, DuckDB unifies to VARCHAR, breaking `AVG()`. Object path component aggregation now wraps numeric columns with `TRY_CAST(... AS DOUBLE)` before aggregating
- **Object paths forced to raw tier** — Aggregated tiers (5s/60s/1h) collapse object paths into scalar `value_avg`, losing `value_latitude`/`value_longitude`. Object path queries now override to `tier=raw` with correct timestamp column, S3 supplement, and fallback clause
- **Spatial filter on buffer source** — Buffer subquery for position paths was missing the spatial WHERE clause, allowing unfiltered data (e.g. Brooklyn) to appear in results for a distant bbox
- **Spatial correlation with empty results** — When no position timestamps matched the spatial filter, the correlation was skipped entirely and all scalar data returned unfiltered. Now correctly returns empty data
- **Spatial `ST_Point(VARCHAR)` error** — `buildSpatialSqlClause` defaults updated to `TRY_CAST(value_latitude/longitude AS DOUBLE)` and `getSpatialTimestamps` NULL checks similarly wrapped
- **S3 fallback using wrong tier** — When S3 hybrid query failed for object paths, the fallback `localFromClause` still pointed to the original aggregated tier. Now updated alongside the raw tier override
- **Schema cache: excluded `value_age`** — GPS fix age is metadata, not a position component to aggregate. Added to the exclusion list alongside `value_json`, `value_units`, `value_description`

### Changed

- **Fast spatial position queries (100x speedup)** — Spatial queries with `navigation.position` no longer do full raw tier scans. Instead, position is bucketed with `FIRST(lat/lon)` per time bucket and filtered by bbox/radius in JS. Reduces 1-month spatial queries from ~80s to ~850ms
- **Single-scan spatial correlation** — When position is a requested path, its results provide correlation timestamps for other paths, eliminating the separate position scan
- **Buffer federation in spatial queries** — Fast bucket query and `getSpatialTimestamps` now include SQLite buffer data via UNION ALL, so today's unexported position data is included in spatial filtering

### Added

- **`getTableColumns()` on SQLiteBuffer** — Exposes the column set for a given path's buffer table, enabling the buffer SQL builder to detect and handle missing columns dynamically

---

## [0.7.9] - 2026-03-14

### Changed

- **DuckDB Memory Cap** — Set `memory_limit = '512MB'` to prevent OOM when DuckDB's allocator combines with Node.js heap on memory-constrained devices (Raspberry Pi)
- **Historical Streaming Service disabled** — Disabled WebSocket-based historical streaming to prevent unbounded memory growth on long-running instances
- **R2 Startup Sync optimized** — Scoped cloud sync prefix listing to raw-tier directories only
  - Lists only raw-tier prefixes in R2 (~1,400 vs 124K full bucket listing)
  - If raw tier exists for a context/path/year/day, assumes all aggregated tiers are already synced
  - Uploads all tiers for any missing directory
- **Cloud upload lookback extended** — Reduced lookback window from 30 days to 7 days for uploading hive-partitioned Parquet files to cloud storage

---

## [0.7.8-beta.2] - 2026-03-14

### Changed

- **Historical Streaming Service disabled** — Disabled WebSocket-based historical streaming to prevent unbounded memory growth on long-running instances
- **R2 Startup Sync optimized** — Scoped cloud sync prefix listing to raw-tier directories only
  - Lists only raw-tier prefixes in R2 (~1,400 vs 124K full bucket listing)
  - If raw tier exists for a context/path/year/day, assumes all aggregated tiers are already synced
  - Uploads all tiers for any missing directory
- **Cloud upload lookback extended** — Reduced lookback window from 30 days to 7 days for uploading hive-partitioned Parquet files to cloud storage

---

## [0.7.8-beta.1] - 2026-03-13

### Changed

- **SQLite Buffer: better-sqlite3 → node:sqlite** — Replaced `better-sqlite3` native addon with Node.js built-in `node:sqlite` (available since Node 22.5)
  - Eliminates the `prebuild-install` postinstall script that SignalK 2 blocks during plugin installation, which silently prevented the SQLite buffer from loading
  - Zero native dependencies — no binary downloads, no platform-specific builds
  - API-compatible: synchronous `.prepare()/.run()/.get()/.all()/.exec()` interface unchanged
  - Transactions rewritten from `.transaction()` wrapper to manual `BEGIN`/`COMMIT`/`ROLLBACK`
  - Open-state tracking via `_open` flag (node:sqlite's `.open()` is a method, not a property)
  - Minimal type declarations added in `src/types/node-sqlite.d.ts`
- **Node.js minimum version bumped to 22.5.0** (was 18.0.0) — required for `node:sqlite`
- **Removed `better-sqlite3` and `@types/better-sqlite3`** from dependencies

---

## [0.7.7-beta.1] - 2026-03-13

### Added

- **Cloudflare R2 Cloud Provider** — Full support for Cloudflare R2 as an alternative to Amazon S3
  - New `provider` field in cloud config: `'none' | 's3' | 'r2'`
  - R2 uses `accountId` instead of `region`, with automatic endpoint and URL style configuration
  - DuckDB S3 secret extended with conditional `ENDPOINT` and `URL_STYLE 'path'` for R2 compatibility
  - Backward-compatible auto-migration from old `s3Upload` config format to new `cloudUpload`

- **Per-Path SQLite Buffer Tables** — Each SignalK path now gets its own buffer table (`buffer_navigation_position`, `buffer_electrical_batteries_512_voltage`, etc.)
  - Eliminates column pollution from `ALTER TABLE` — each path has exactly the columns it needs
  - New `buffer_tables` metadata table tracks path → table name mapping
  - Automatic one-time migration from legacy `buffer_records` table on startup: discovers all paths and dynamic `value_*` columns, creates per-path tables, migrates data atomically, drops old table
  - New `pathToTableName()` exported function for table name resolution
  - New `getKnownPaths()` returns `Set<string>` of all paths with buffer data

- **SQLite Buffer Federation in DuckDB Queries** — Buffer data is now included directly in DuckDB queries via `ATTACH` and UNION
  - New `src/utils/buffer-sql-builder.ts` with `buildBufferScalarSubquery()` and `buildBufferObjectSubquery()` functions
  - Builder functions return `null` when no table exists for a path, allowing graceful fallback to parquet-only queries
  - Callers pass `knownBufferPaths: Set<string>` for table existence checks without hitting the database

- **Three-Tier Query Federation with Priority Handling** — Queries now resolve data from multiple sources with deduplication
  - Priority 3 (highest): SQLite buffer (real-time data, not yet exported)
  - Priority 2: Raw tier parquet (fills the "tier gap" for today's data when using aggregated tiers)
  - Priority 1: Aggregated tier parquet (5s/60s/1h historical data)
  - Uses `ROW_NUMBER() OVER (PARTITION BY timestamp ORDER BY priority DESC)` to pick highest-priority source per time bucket
  - Fixes the "tier gap" for today: when querying aggregated tiers (60s, 5s, 1h), today's data only exists in raw tier + buffer — query now automatically includes raw tier for today's timestamps

- **Comprehensive Test Suite for SQLite Buffer** — New `tests/test-sqlite-buffer.js` (1608 lines) covering per-path tables, migration, federation, and edge cases

### Changed

- **S3 Terminology → Cloud** — All S3-specific naming generalized to support multiple cloud providers
  - Types: `S3UploadConfig` → `CloudUploadConfig`, `S3TestApiResponse` → `CloudTestApiResponse`
  - State: `s3Client` → `cloudClient`
  - Methods: `initializeS3()` → `initializeCloudSDK()`, `createS3Client()` → `createCloudClient()`
  - API endpoints: `/api/test-s3` → `/api/test-cloud`, `/api/s3/compare` → `/api/cloud/compare`
  - UI: `testS3Connection()` → `testCloudConnection()`, displays provider label (S3 or R2)

- **Export Service Simplified** — Daily export now uses per-path table API
  - `getRecordsForPathAndDate(context, path, date)` returns `DataRecord[]` (no IDs)
  - `markDateExported(context, path, date, batchId)` replaces `markAsExported(ids)`
  - Export status UI enhanced with colored status badges

- **Cloud Upload Days Check** — Reduced cloud upload lookback from 30 days to 7 days

- **README Comprehensive Update** — Fixed stale documentation, added missing features, removed unimplemented sections
  - Fixed resolution parameter incorrectly documented as "milliseconds" (is seconds)
  - Updated PluginConfig, PluginState, PathConfig, CloudUploadConfig code samples to match current types
  - Added aggregated tier schema (bucket_time, value_avg, value_min, value_max, sample_count, sin/cos)
  - Added per-path buffer architecture, three-tier query hierarchy, R2 support
  - Added missing API endpoints: validation, repair, vector-averaging migration, aggregation, cloud sync
  - Expanded project structure with services/, utils/ detail
  - Removed unimplemented `convertUnits` section and `units` response extension
  - Removed stale `timing` field from cloud upload config
  - Moved beta.3 consolidation warning and legacy flat structure to Legacy Notes section

### Removed

- **Legacy Buffer Methods** — `getPendingRecords()`, `getPendingRecordsGrouped()`, `getPendingRecordsGroupedWithIds()`, `markAsExported(ids)` replaced by per-path date-based API
- **Upload Timing Config** — Removed `timing: 'realtime' | 'consolidation'` from cloud upload; uploads now always run as part of the daily export pipeline
- **Legacy Test Scripts** — Removed `aggregate-all-dates.py`, `convert-recovered.py` and related Python helpers, replaced by built-in migration and aggregation services

---

## [0.7.6-beta.8] - 2026-03-07

### Changed

- **Breaking: Radius Parameter Coordinate Order** — Swapped `radius` parameter from `lat,lon,meters` to `lon,lat,meters` to match the SignalK Resources API's GeoJSON convention
  - The `bbox` parameter already used `lon,lat` order; `radius` now aligns
  - API consumers using `radius=lat,lon,meters` must swap to `radius=lon,lat,meters`

### Removed

- **Deprecated Query Parameters** — Removed parameters that are no longer needed after V2 API alignment
  - `useUTC` — bare timestamps now follow ISO 8601 rules (bare = local, `Z` = UTC, offset = explicit)
  - `convertTimesToLocal` — timestamps always returned in server local time with offset
  - `timezone` — no longer converting timestamps to arbitrary timezones
  - `refresh` — auto-refresh removed; clients should poll as needed
  - `tier` — aggregation tier now auto-selected based on resolution and time range
  - `source` — query source (`auto`, `local`, `s3`, `hybrid`) auto-determined
  - `positionPath` — spatial correlation always uses `navigation.position`
- **Timezone conversion helpers** — Removed `getTargetTimezone()` and `convertTimestampToTimezone()` from HistoryAPI
- **`timezone` metadata block** — Removed from `DataResult` response type

### Fixed

- **WAL Bloat After Export** - Added WAL checkpoint after startup and daily export cleanup
  - The heavy export+cleanup batch (hundreds of thousands of record updates and deletes) bloated the SQLite WAL file to ~255 MB
  - WAL now truncates immediately after each export cycle, reclaiming disk space on resource-constrained devices (Pi)
  - Shutdown checkpoint retained as a safety net

---

## [0.7.6-beta.7] - 2026-03-05

### Fixed

- **Shutdown Export Removed** - Removed risky `forceExport()` call during plugin shutdown
  - Async Parquet writes during shutdown are fragile (process may be killed mid-write)
  - Startup export already catches up on all unexported records, making the shutdown export redundant

- **Daily Export Status Not Updating** - `exportDayToParquet` now updates status (Last Process, Last Batch, Last Export time) even when no data is found for the target date
  - Previously the status page would still show the previous export's info after a daily run with 0 records

### Improved

- **Config UI Cleanup** - Removed settings that don't need user configuration
  - Hidden `retentionDays` (only used by manual cleanup API, not normal operation)
  - Hidden `bufferRetentionHours` (hardcoded to 48h to match HistoryAPI assumptions)
  - Updated `exportBatchSize` description to clarify it's per-batch in a loop, not per-cycle
  - Updated `dailyExportHour` description to reference UTC and Status tab

- **Reduced Debug Log Noise** - Silenced per-update threshold monitor logging
  - Threshold evaluation, command state checks, and "no action taken" messages no longer flood logs
  - Action-taken and error messages still logged

---

## [0.7.6-beta.6] - 2026-03-05

### Fixed

- **CRITICAL: Parquet File Overwrite on Batch Export** - Fixed filename collision when exporting multiple batches
  - Filename timestamp was truncated to the minute (`.slice(0, 15)`), causing all batches within the same minute to overwrite each other
  - Only the last batch's data survived; all previous batches' parquet files were silently overwritten
  - Now uses second-level precision (`.slice(0, 17)`) plus a uniqueness suffix for same-second collisions

- **Export Loop for Large Buffers** - `exportPending()` now loops through all pending records in batches
  - Previously exported only one batch (maxBatchSize) and stopped
  - Large backlogs (e.g., 500k+ records) now fully drain on startup or force export

- **Trailing Space in outputDirectory** - Added `.trim()` to config loading to prevent invisible path errors
  - A trailing space caused files to be written to a wrong directory (e.g., `data /` instead of `data/`)

### Improved

- **Buffer Status UI** - Added explainer text and subtitles to SQLite buffer dashboard
  - Description paragraph explaining the buffer-to-parquet pipeline
  - Subtitles under each stat (Total Records, Pending, Exported, DB Size)
  - Last Export timestamp now shows UTC time parenthetically
  - Schedule shows local time with UTC parenthetical
  - Last Process indicator shows whether export was triggered by Daily/Startup/Forced

- **Removed tier dropdown** from migration UI (hardcoded to raw)

---

## [0.7.6-beta.5] - 2026-03-04

### Fixed

- **Query Source Routing Bypassing Local Data** - Fixed `getQuerySource` returning `'s3'` for data older than retention period, completely skipping local Parquet files
  - Queries now always include local data (SQLite buffer + Parquet)
  - S3 only supplements for date ranges before the earliest local Parquet data

- **S3 Hybrid Query Failure** - Fixed S3 UNION queries breaking local results when S3 glob matches no files
  - DuckDB `read_parquet` on empty S3 glob caused the entire UNION (including local) to fail
  - Now gracefully falls back to local-only when S3 portion errors

### Added

- **S3 Supplement Logic** - S3 queries only for dates before earliest local data
  - `findEarliestDate()` scans Hive partition directories to determine local data boundary
  - No S3 calls unless the requested date range extends before local coverage
  - Follows priority: SQLite buffer → local Parquet → S3 (for older data only)

- **Aggregation Test Script** - New `tests/aggregate-all-dates.py`
  - Scans Hive directories for all dates with raw tier data
  - Triggers aggregation API for each date
  - Supports `--year` filter and custom data directory
  - Run: `python3 tests/aggregate-all-dates.py --token TOKEN`

---

## [0.7.5-beta.4] - 2026-03-04

### Changed

- **MAJOR: Simplified Export Pipeline** - Replaced periodic 5-minute exports with daily export mode
  - Data now accumulates in SQLite buffer throughout the day
  - Single daily export at configurable hour (default: 4 AM UTC)
  - Creates consolidated daily Parquet files directly (no separate consolidation step)
  - Eliminates file fragmentation from frequent small exports
  - `exportIntervalMinutes` config option now deprecated

- **Extended SQLite Buffer Retention** - Default changed from 24h to 48h
  - Allows federated queries to span more recent data
  - Better crash recovery window

- **Removed Consolidation System** - No longer needed with daily export
  - Removed `consolidateDaily()` and `mergeFiles()` from parquet-writer.ts
  - Removed `consolidateMissedDays()` and `consolidateYesterday()` from data-handler.ts
  - Daily export creates consolidated files directly

### Fixed

- **Buffer Bucketing in History API** - SQLite buffer data now bucketed before merging with Parquet results
  - Previously: raw per-second buffer records merged directly, flooding results (e.g., 10,000 raw records mixed with 288 bucketed Parquet points for a 24h/5min query)
  - Now: buffer records are bucketed and aggregated using the same resolution as the Parquet query
  - Supports all aggregate methods: average, min, max, first, last
  - Angular paths use vector averaging (`atan2(mean(sin), mean(cos))`)
  - Object paths (e.g., position) average each numeric component

- **CRITICAL: Parquet File Overwrite Bug** - Fixed exports overwriting existing files on restart
  - Previous: Filename used first record's timestamp (could match existing file)
  - Now: Filename uses current time, guaranteeing unique filenames
  - Each export creates: `signalk_data_2026-03-03T1313.parquet`
  - Multiple restarts per day create separate files (no data loss)

- **CRITICAL: Aggregated Tier Queries Returning No Data** - Fixed History API unable to read aggregated tier (5s, 60s, 1h) Parquet files
  - Aggregated tiers use `bucket_time` and `value_avg` columns, but queries were hardcoded to `signalk_timestamp` and `AVG(value)` (raw tier schema)
  - DuckDB's `union_by_name=true` silently returned NULL for missing columns, producing zero rows
  - Now uses tier-aware column names: `bucket_time` for aggregated, `signalk_timestamp` for raw
  - Pre-computed aggregates (`value_avg`, `value_sin_avg`/`value_cos_avg`) used instead of re-aggregating
  - Weighted averaging via `sample_count` for correct multi-bucket rollups
  - Also fixed in spatial filter timestamp correlation queries

- **CRITICAL: Retention Cleanup Deleting Un-aggregated Data** - Removed automatic `cleanupOldData()` from daily aggregation
  - Retention was running after every aggregation, deleting raw parquet files based on age alone
  - No check for whether data had been aggregated or backed up
  - Destroyed freshly migrated 2025 data immediately after migration (files older than retention window)
  - Cleanup endpoint (`POST /api/aggregate/cleanup`) remains available as manual-only

- **Federated Query Cutoff** - Fixed HistoryAPI only looking at last 5 minutes
  - Now correctly uses 48-hour cutoff for SQLite buffer queries
  - Recent data properly included in federated queries

- **S3 Upload Patterns** - Updated to match timestamped filenames
  - Pattern changed from `${prefix}_${dateStr}.parquet` to `${prefix}_${dateStr}*.parquet`
  - Matches both date-only (legacy) and timestamped (new) naming

### Added

- **Vector Averaging for Angular Paths** - Correct aggregation of circular data (headings, bearings, wind angles)
  - Detects angular paths dynamically via `app.getMetadata(path).units === 'rad'`
  - Uses `ATAN2(AVG(SIN(value)), AVG(COS(value)))` instead of arithmetic mean
  - Stores `value_sin_avg`/`value_cos_avg` columns for lossless re-aggregation across tiers
  - `value_min`/`value_max` set to NULL for angular paths (min/max undefined for circular data)
  - Migration endpoint: `POST /api/migrate/vector-averaging` to rebuild existing aggregated files

- **Daily Export Scheduling** - New `dailyExportHour` config option (0-23, default: 4)
  - Configurable hour for daily Parquet export (UTC)
  - Runs once per day, exports previous day's data

- **Diagnostic Test Script** - New `tests/test-data-pipeline.js`
  - Reports SQLite buffer status (records by date, exported vs unexported)
  - Reports Parquet file statistics (counts, sizes, dates)
  - Data integrity checks
  - Run: `node tests/test-data-pipeline.js [--verbose]`

- **Improved UI Status** - Migration tab now shows meaningful status
  - "Export Service: Daily Mode" instead of "Stopped"
  - "Schedule: Daily at 4:00 UTC" instead of "Interval: 5 min"

---

## [0.7.4-beta.3] - 2026-03-02

### Fixed

- **CRITICAL: Duplicate Data in Queries**: Fixed queries returning duplicate records after consolidation
  - After consolidation, source files are moved to `processed/` subdirectory
  - All query endpoints were including these processed files, causing ~36% data inflation
  - Fixed `history-provider.ts`, `HistoryAPI.ts`, and `aggregation-service.ts` to exclude:
    - `/processed/` - consolidated source files
    - `/quarantine/` - corrupt files
    - `/failed/` - failed processing
    - `/repaired/` - repaired files
  - Uses DuckDB `filename` pseudo-column to filter: `WHERE filename NOT LIKE '%/processed/%'`

- **Config Options Not Loading**: Fixed `enableRawSql` and `exportBatchSize` not being read from config
  - Options were defined in plugin schema but not copied to `state.currentConfig`
  - Added missing property assignments in `index.ts`

---

## [0.7.3-beta.2] - 2026-03-01

### Added

- **Plugin Config for Raw SQL**: New `enableRawSql` boolean option in plugin settings
  - Allows enabling raw SQL queries via UI instead of only environment variable
  - Either `SIGNALK_PARQUET_RAW_SQL=true` OR plugin setting enables the feature
  - Updated error message to mention both options

- **Dynamic Query Database Tab**: Tab visibility based on raw SQL setting
  - New `/api/query/enabled` endpoint to check if raw SQL is enabled
  - Query Database tab hidden in webapp when raw SQL is disabled
  - Improves security UX by not showing disabled features

- **Export Batch Size Config**: New `exportBatchSize` setting (default: 50000)
  - Controls max records exported per cycle, independent of `bufferSize`
  - Prevents pending records from backing up during high data inflow
  - Range: 1,000 - 200,000 records per cycle

### Fixed

- **History API Provider Registration**: Fixed v2 History API returning "No history api provider configured"
  - Updated `@signalk/server-api` dependency from 2.10.2 to 2.22.0
  - Version mismatch caused `registerHistoryApiProvider()` to fail silently

- **History API Hive Path Support**: Fixed queries returning 0 results
  - `history-provider.ts` was using legacy flat paths (`/vessels/urn.../navigation/position/`)
  - Updated to use `HivePathBuilder.getGlobPattern()` for correct Hive-partitioned paths
  - Now correctly queries `tier=raw/context=.../path=.../year=.../day=.../*.parquet`

- **Export Service Record Marking Bug**: Fixed records not being marked as exported
  - Bug: After exporting, service fetched DIFFERENT records to get IDs (race condition)
  - Records for other paths would get marked instead of the exported ones
  - Fix: Track record IDs BEFORE exporting via new `getPendingRecordsGroupedWithIds()` method
  - This caused pending records to accumulate indefinitely

### Changed

- Improved debug logging for history provider registration (shows if `registerHistoryApiProvider` exists on app object)

---

## [0.7.0-beta.1] - 2025-03-01

### Breaking Changes

- **Resolution parameter now uses seconds**: The `resolution` query parameter expects seconds instead of milliseconds.
  - Migration: Divide existing values by 1000, or use time expressions (`1m`, `5s`)
  - Old: `?resolution=60000` (1 minute)
  - New: `?resolution=60` or `?resolution=1m`

- **Removed `start` parameter**: The deprecated `start` query parameter has been removed from the History API. Use standard SignalK time patterns instead:
  - `?start=now&duration=1h` → `?duration=1h`
  - `?start=TIME&duration=1h` → `?to=TIME&duration=1h`

### Added

- **ISO 8601 duration support**: Duration parameters now accept ISO 8601 format (`PT1H`, `PT30M`, `P1D`, `PT1H30M`)
- **Integer seconds for duration**: Duration can be specified as plain seconds (`?duration=3600` for 1 hour)
- **Time expressions for resolution**: Resolution accepts time expressions (`?resolution=1m`, `?resolution=5s`, `?resolution=1h`)
- **Official SMA/EMA aggregation methods**: SMA and EMA are now supported as aggregation methods per SignalK spec
  - Syntax: `path:sma:5` or `path:ema:0.2` (returns only the smoothed value)
  - Example: `?paths=navigation.speedOverGround:sma:5`
  - Extension syntax `path:average:sma:5` still supported (returns raw AND smoothed values)

### 🌐 SignalK History API - V1 Extensions vs V2 Spec-Compliant

Separated V1 (with extensions) from V2 (spec-compliant) to support SignalK server's multi-provider system.

#### Changed

- **V2 Routes Now Provider-Handled**: `/signalk/v2/api/history/*` routes are now handled by the registered `HistoryApi` provider instead of direct routes
  - Supports SignalK server PR #2381 multi-provider routing
  - V2 implements the spec-compliant `HistoryApi` interface (see `history-provider.ts`)
  - When server supports multi-provider, V2 works via `app.registerHistoryApiProvider()`

- **V1 Routes Retain Extensions**: `/signalk/v1/history/*` routes keep all signalk-parquet extensions:
  - Shorthand duration (`1h`, `30m`, `2d`) - V2 spec requires ISO 8601 (`PT1H`)
  - Timezone conversion (`convertTimesToLocal`, `timezone`)
  - Spatial filtering (`bbox`, `radius`)
  - Resolution expressions (`resolution=5m`)
  - Auto-refresh mode (`refresh=true`)
  - Moving averages (SMA/EMA)

### 🔒 Security: Raw SQL Disabled by Default

Raw SQL query endpoint now requires explicit opt-in for security.

#### Changed

- **`/api/query` disabled by default**: Returns 403 unless `SIGNALK_PARQUET_RAW_SQL=true` environment variable is set
  - Prevents potential SQL injection or destructive queries even with bearer auth
  - Enable for debugging: `SIGNALK_PARQUET_RAW_SQL=true signalk-server`

---

### 🗄️ SQLite WAL Buffering (Crash-Safe Data Ingestion)

Replace in-memory data buffers with a crash-safe SQLite database using Write-Ahead Logging (WAL) mode.

#### Added

**SQLite Buffer Infrastructure**
- **WAL-Mode SQLite**: Crash-safe data buffering with automatic recovery after power loss or crashes
  - WAL mode provides concurrent read/write access with durability guarantees
  - 64MB cache and 256MB memory-mapped I/O for high performance
  - Automatic transaction handling with batch inserts for efficiency
- **Export Tracking**: Records marked as exported with batch IDs for audit trail
  - Pending records are exported to Parquet on configurable intervals (default: 5 minutes)
  - Exported records retained for configurable period (default: 24 hours)
  - Automatic cleanup of old exported records
- **Buffer Statistics API**: Monitor buffer health and performance
  - Total/pending/exported record counts
  - Oldest pending and newest record timestamps
  - Database and WAL file sizes
- **Path-Based Queries**: Query buffered data for specific paths and time ranges
  - Enables hybrid queries spanning both buffer and Parquet files
  - Supports federated queries with S3 data

**Configuration Options:**
| Setting | Description | Default |
|---------|-------------|---------|
| `useSqliteBuffer` | Enable SQLite WAL buffer instead of in-memory LRU | `false` |
| `exportIntervalMinutes` | How often to export from SQLite to Parquet | `5` |
| `bufferRetentionHours` | How long to keep exported records in SQLite | `24` |

---

### 🏗️ Hive Partitioned Storage & Migration

New storage structure using Hive-style partitioning for better query performance and S3 integration.

#### Added

**Tiered Storage Architecture**
- **Hive Partition Structure**: `tier=raw/context={ctx}/path={path}/year={year}/day={day}/`
  - Aggregation tiers: `raw`, `5s`, `60s`, `1h` for different granularities
  - Context and path partitions enable efficient filtering
  - Year/day partitions enable partition pruning (70-90% data transfer reduction)
- **Automatic Path Sanitization**: Safe encoding of SignalK contexts and paths
  - `vessels.urn:mrn:signalk:uuid:xxx` → `vessels__urn-mrn-signalk-uuid-xxx`
  - `navigation.speedOverGround` → `navigation__speedOverGround`
- **DuckDB Glob Patterns**: Optimized patterns for time-range queries
  - Explicit day patterns for ranges ≤7 days
  - Wildcards for longer ranges with partition pushdown

**Migration Service**
- **Flat-to-Hive Migration**: Convert legacy structure to new Hive partitioning
  - Scans existing files to detect flat vs Hive structure
  - Background migration with progress tracking and cancellation
  - Automatic timestamp extraction from files for proper partitioning
  - Optional deletion of source files after migration
- **Migration API Endpoints**:
  - `POST /api/migrate/scan` - Scan directory for migratable files
  - `POST /api/migrate` - Start migration job
  - `GET /api/migrate/progress/:jobId` - Get job progress
  - `POST /api/migrate/cancel/:jobId` - Cancel running job

**Configuration:**
| Setting | Description | Default |
|---------|-------------|---------|
| `useHivePartitioning` | Use Hive-style partitioning for new files | `false` |

---

### 🔍 Auto-Discovery (Automatic Path Configuration)

Automatically configure SignalK paths for recording when they're queried but not yet configured.

#### Added

**Auto-Discovery Service**
- **On-Demand Configuration**: Paths are automatically added when:
  - A History API query requests data for an unconfigured path
  - The path matches include patterns (if specified)
  - The path doesn't match exclude patterns
  - The path has live data in SignalK (if `requireLiveData` enabled)
- **Pattern-Based Filtering**: Include/exclude paths using glob patterns
  - Example include: `navigation.*`, `environment.wind.*`
  - Example exclude: `propulsion.*`, `*alarm*`
- **Configurable Limits**: Prevent runaway configuration with max paths limit
- **Race Condition Protection**: Serialized operations prevent duplicate configurations
- **Auto-Generated Names**: Human-readable names from path (e.g., `[Auto] Navigation Speed Over Ground`)

**Configuration Options:**
| Setting | Description | Default |
|---------|-------------|---------|
| `autoDiscovery.enabled` | Master switch for auto-discovery | `false` |
| `autoDiscovery.requireLiveData` | Only configure if path has live SignalK data | `true` |
| `autoDiscovery.maxAutoConfiguredPaths` | Maximum number of auto-configured paths | `100` |
| `autoDiscovery.includePatterns` | Glob patterns for paths to include | `[]` |
| `autoDiscovery.excludePatterns` | Glob patterns for paths to exclude | `[]` |

---

### 🌐 S3 Federated Querying with DuckDB

Query historical data directly from S3 using DuckDB's native S3 support, with automatic partition pruning for minimal data transfer.

#### Added

**S3 Query Infrastructure**
- **DuckDB S3 Integration**: Initialize S3 credentials in DuckDB pool via `DuckDBPool.initializeS3()`
  - Installs and loads `httpfs` extension automatically
  - Creates S3 secret with AWS credentials for authenticated access
  - Credentials initialized at plugin startup when S3 is enabled
- **S3 Glob Pattern Builder**: New `buildS3Glob()` method in HivePathBuilder
  - Generates S3 URIs with Hive partition structure: `s3://bucket/tier=raw/context=.../path=.../year=YYYY/day=DDD/*.parquet`
  - Intelligent partition pruning: explicit day directories for ranges ≤7 days, wildcards for longer ranges
  - Reduces S3 data transfer by 70-90% through partition skipping

**Hybrid Local+S3 Queries**
- **Query Source Parameter**: New `?source=` parameter for history API
  - `auto` (default): Automatically determines source based on time range vs. retention cutoff
  - `local`: Force local-only query
  - `s3`: Force S3-only query
  - `hybrid`: Explicit split query across both sources
- **Automatic Source Selection**: Uses `retentionDays` config as the boundary
  - Data within retention period → queries local Hive-partitioned files
  - Data older than retention → queries S3 directly
  - Query spanning boundary → UNION ALL of both sources
- **S3 Config Passthrough**: S3 credentials and bucket info passed through to HistoryAPI

**Spatial Correlation for Non-Position Paths**
- **Position-Based Filtering**: Query non-position paths filtered by vessel location
  - Example: "Get wind data for times when vessel was within 100m of this point"
  - Correlates timestamps between position data and requested paths
- **`positionPath` Parameter**: Specify which position path to use for correlation
  - Default: `navigation.position`
  - Can use alternatives like `navigation.anchor.position`
- **Efficient Implementation**:
  - First queries position data with spatial filter to get valid timestamps
  - Then filters non-position path data to only those timestamps

#### Changed

**Removed Legacy Flat Path Structure**
- **Hive-Only Queries**: HistoryAPI now exclusively uses Hive-partitioned paths (`tier=raw/context=.../path=...`)
- **Schema Cache Updated**: `getPathComponentSchema()` now looks in Hive structure instead of legacy flat paths
- **Removed `selfContextPath`**: No longer needed since flat path queries are removed
- **Breaking Change for Unmigrated Data**: Users with data only in legacy flat structure must run migration first

#### Fixed

- Removed misleading "Showing first 100 items. More files exist." message from S3 comparison UI

### API Changes

**New Query Parameters:**

| Parameter | Description | Default |
|-----------|-------------|---------|
| `source` | Query source: `auto`, `local`, `s3`, `hybrid` | `auto` |
| `positionPath` | Position path for spatial correlation | `navigation.position` |

**Example Queries:**

```bash
# Query S3 directly for old data
/signalk/v1/history/values?paths=navigation.speedOverGround&from=2024-01-01&to=2024-01-07&source=s3

# Wind data when vessel was within 100m of point
/signalk/v1/history/values?paths=environment.wind.speedApparent&duration=24h&radius=40.646,-73.981,100

# Use anchor position for spatial correlation
/signalk/v1/history/values?paths=environment.depth.belowKeel&duration=7d&radius=40.646,-73.981,50&positionPath=navigation.anchor.position
```

### Migration Notes

- **Legacy Flat Path Data**: Data stored in legacy flat structure (`vessels/self/navigation/position/*.parquet`) will no longer be queryable via History API. Run the migration service to convert to Hive structure before upgrading.
- **S3 Credentials**: For S3 querying to work, S3 must be enabled with valid credentials in plugin config.

---

## [0.6.5-beta.1] - 2025-11-02

### 🚀 Major Performance Optimizations & Code Quality

This release delivers **dramatic performance improvements** through systematic optimization.

#### Performance Results
- **67% memory reduction** (1.2GB → 400MB)
- **66% faster queries** (350ms → 120ms)
- **60% CPU reduction** (45% → 18%)
- **5x concurrent capacity** (100+ requests vs OOM at 20)
- **50% faster startup** (cached directory scans)

### Added

#### Performance Infrastructure
- **DuckDB Connection Pooling**: Singleton pool eliminates memory leaks
- **Formula Cache**: Replaces eval() with cached Function constructor (10-100x faster)
- **LRU Cache**: Prevents unbounded buffer growth with configurable size limits
- **Concurrency Limiter**: Controls concurrent queries (max 10) to prevent resource exhaustion
- **Directory Scanner Cache**: 5-min cache reduces 7000+ ops to ~100-200
- **Debug Logger**: Unified logging system (eliminates console.log warnings)
- **Centralized Config**: All cache settings in `src/config/cache-defaults.ts`

### Changed

#### Query Optimizations
- O(n²) nested loops → O(1) Map-based lookups in delta processing
- Schema cache TTL: 2min → 30min
- Timestamp cache keys rounded to minute for better hit rates (60-80% vs ~0%)
- JSON serialization deferred to write time (not per delta)

#### Code Quality
- Removed ~80 lines of duplicate code
- Unified directory filtering with shared constants
- Consolidated parquet file scanning logic
- 0 ESLint warnings (down from 12)
- 100% Prettier formatted

### Fixed

#### Critical Fixes
- **@signalk/server-api moved to dependencies** (was in devDependencies)
  - Fixes: "Cannot find module '@signalk/server-api'" on production installs
- **Icon optimized**: 1.9MB → 14KB (99.3% reduction)
- **Memory leaks**: Fixed unbounded Map/Set growth

### Migration Notes

No breaking changes - all optimizations are backward compatible.

**Files Created (7):**
- `src/utils/duckdb-pool.ts`, `formula-cache.ts`, `lru-cache.ts`
- `src/utils/concurrency-limiter.ts`, `directory-scanner.ts`, `debug-logger.ts`
- `src/config/cache-defaults.ts`

---

## [0.6.0-beta.1] - 2025-10-20

### Added - Unit Conversion & Timezone Support
- **🔄 Automatic Unit Conversion**: Optional integration with `signalk-units-preference` plugin
  - Add `?convertUnits=true` to automatically convert values to user's preferred units
  - Server-side conversion using formulas from units-preference plugin
  - Respects all user unit preferences (knots, km/h, mph, °F, °C, etc.)
  - Zero client-side dependencies - all conversions handled server-side
  - Includes conversion metadata in response (base unit → target unit, symbol, conversions applied)
- **🌍 Timezone Conversion**: Convert UTC timestamps to local or specified timezone
  - Add `?convertTimesToLocal=true` to convert all timestamps to local time
  - Optional `&timezone=America/New_York` parameter for custom IANA timezone
  - Supports all IANA timezone identifiers with automatic DST handling
  - Clean ISO 8601 format with offset (e.g., `2025-10-20T12:34:04-04:00`)
  - Timezone metadata included in response (offset, description)
- **⚙️ Configurable Cache**: User-adjustable unit conversion cache duration
  - New plugin setting: `unitConversionCacheMinutes` (default: 5 minutes, range: 1-60)
  - Balances responsiveness to preference changes vs. performance
  - Automatic cache expiration and reload without server restart
  - Lower values reflect preference changes faster, higher values reduce overhead

### Performance & Integration
- **🔌 Plugin-to-Plugin Communication**: Direct app object function calls (no HTTP auth needed)
  - Units-preference plugin exposes conversion data via `app.getAllUnitsConversions()`
  - Lazy loading with automatic retry handles plugin load order race conditions
  - Efficient caching prevents repeated lookups
  - Debug logging for troubleshooting plugin availability
- **📊 Enhanced Response Metadata**: Rich metadata for client applications
  - `units` object: Shows all conversions applied (path, base→target unit, symbol)
  - `timezone` object: Shows timezone, offset, and conversion description
  - Preserves backward compatibility - metadata only added when features used

### Developer Experience
- **🛠️ Comprehensive Logging**: Detailed debug output for troubleshooting
  - Unit conversion: Plugin detection, cache status, conversion loading
  - Timezone conversion: Target zone, current offset, example conversions
  - Clear error messages with fallback to original values on failures
- **🔄 Graceful Degradation**: Features work independently or fail gracefully
  - Unit conversion: Falls back to SI units if plugin unavailable
  - Timezone conversion: Returns UTC if timezone invalid
  - Both features optional and backward compatible

### Example Usage
```bash
# Convert to preferred units
GET /signalk/v1/history/values?duration=2d&paths=navigation.speedOverGround&convertUnits=true

# Convert timestamps to local time
GET /signalk/v1/history/values?duration=2d&paths=environment.wind.speedApparent&convertTimesToLocal=true

# Specify custom timezone
GET /signalk/v1/history/values?duration=2d&paths=navigation.position&convertTimesToLocal=true&timezone=Pacific/Auckland

# Combine both conversions
GET /signalk/v1/history/values?duration=2d&paths=navigation.speedOverGround,environment.wind.speedApparent&convertUnits=true&convertTimesToLocal=true&timezone=America/New_York
```

### Response Format Changes
**With unit conversion:**
```json
{
  "units": {
    "converted": true,
    "conversions": [
      {
        "path": "navigation.speedOverGround",
        "baseUnit": "m/s",
        "targetUnit": "knots",
        "symbol": "kn"
      }
    ]
  }
}
```

**With timezone conversion:**
```json
{
  "timezone": {
    "converted": true,
    "targetTimezone": "America/New_York",
    "offset": "-04:00",
    "description": "Converted to user-specified timezone: America/New_York (-04:00)"
  }
}
```

## [0.5.6-beta.1] - 2025-10-20

### Added - SignalK History API Compliance
- **🎯 Standard Time Range Parameters**: Full support for all 5 SignalK History API time query patterns
  - Pattern 1: `?duration=1h` - Query back from now
  - Pattern 2: `?from=TIME&duration=1h` - Query forward from start
  - Pattern 3: `?to=TIME&duration=1h` - Query backward to end
  - Pattern 4: `?from=TIME` - From start to now
  - Pattern 5: `?from=TIME&to=TIME` - Specific range
- **⏪ Backward Compatibility**: Legacy `start` parameter still supported with deprecation warnings
- **🎛️ Optional Moving Averages**: EMA/SMA now opt-in via `includeMovingAverages` parameter
  - Default: Returns only requested paths (smaller response size)
  - Opt-in: Add `?includeMovingAverages=true` to include EMA/SMA calculations
  - ~66% reduction in response size without moving averages
- **🔍 Time-Filtered Path Discovery**: `/signalk/v1/history/paths` now accepts time range parameters
  - Returns only paths with actual data in specified time range
  - Useful for dashboards showing only active/recent paths
  - Excludes quarantine and corrupted files automatically
- **🌐 Time-Filtered Context Discovery**: `/signalk/v1/history/contexts` now accepts time range parameters
  - SQL-optimized: Single query across all vessels instead of N queries
  - Returns only contexts (vessels) with data in specified time range
  - 2-minute file list caching for sub-second subsequent queries
  - Handles 2500+ vessels and 28k+ parquet files efficiently (~2-3 seconds)

### Performance Improvements
- **⚡ Context Discovery Optimization**: 4.3x faster (13s → 3s for 28k files across 2500+ vessels)
  - Single SQL query with `DISTINCT filename` instead of per-context queries
  - Filesystem scan cached for 2 minutes (reduces to ~2s with cache hit)
  - Parallel file scanning with excluded directories
- **🚫 Corrupted File Handling**: Automatically excludes quarantine, processed, failed, and corrupted files
  - Prevents "file too small to be a Parquet file" errors
  - Cleaner query results with only valid data files

### Changed
- **📊 Moving Averages**: Changed from automatic to opt-in behavior
  - **Breaking Change**: Clients expecting automatic EMA/SMA must add `includeMovingAverages=true`
  - Improves API compliance with SignalK specification
  - Reduces bandwidth and processing for clients that don't need moving averages
- **🔄 Time Parameter Migration**: `start` parameter deprecated in favor of standard patterns
  - Console warnings shown when using deprecated `start` parameter
  - Full backward compatibility maintained for migration period
  - Will be removed in v2.0

### Fixed
- Fixed HistoryAPI failing to return data when parquet files don't have `value_json` column. The query now only selects `value_json` for paths that actually need it (like navigation.position), preventing "column not found" errors on numeric data paths like wind speed.
- Fixed context discovery errors with corrupted quarantine files
- Fixed path discovery returning stale results by adding time-range filtering
