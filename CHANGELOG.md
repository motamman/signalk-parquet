# Changelog

## [0.7.6-beta.8] - 2026-03-07

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
