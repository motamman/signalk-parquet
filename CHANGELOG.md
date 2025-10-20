# Changelog

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

## [Unreleased]
