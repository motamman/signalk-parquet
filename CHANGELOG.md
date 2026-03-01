# Changelog

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
