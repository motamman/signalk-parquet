# DuckDB Graceful Fallback Plan

> **Status**: Planning
> **Created**: 2026-03-03
> **Issue**: https://github.com/mxtommy/Kip/issues/979
> **Goal**: Allow plugin to run on systems where DuckDB is unavailable (32-bit ARM, etc.)

---

## Problem Statement

DuckDB does not support all platforms. Specifically:
- **32-bit ARM** (VenusOS, older Raspberry Pi OS)
- Some **exotic architectures**

When DuckDB fails to load, the entire plugin crashes:
```
Error loading duckdb native binding: unsupported arch 'arm' for platform 'linux'
```

This prevents users from using ANY functionality of signalk-parquet, even features that don't require DuckDB.

---

## Proposed Solution

Make DuckDB an **optional enhancement** rather than a hard dependency. The plugin should:
1. Attempt to load DuckDB at startup
2. If it fails, log a warning and continue with reduced functionality
3. Disable features that require DuckDB
4. Keep core data collection and export working

---

## Feature Matrix

| Feature | Requires DuckDB | Fallback Behavior |
|---------|-----------------|-------------------|
| Data collection (SignalK → SQLite) | No | ✅ Works normally |
| Daily export (SQLite → Parquet) | No | ✅ Works normally |
| S3 upload | No | ✅ Works normally |
| Path listing (filesystem scan) | No | ✅ Works normally |
| Parquet file queries | **Yes** | ❌ Disabled, returns error |
| History API (Parquet queries) | **Yes** | ⚠️ SQLite-only (recent data) |
| Time-range path discovery | **Yes** | ⚠️ Falls back to filesystem scan |
| Claude SQL analysis | **Yes** | ❌ Disabled, returns error |
| Aggregation service | **Yes** | ❌ Disabled |
| Migration service (timestamp extraction) | **Yes** | ⚠️ Falls back to filename parsing |

---

## Implementation Plan

### Phase 1: Make DuckDBPool Fault-Tolerant

**File**: `src/utils/duckdb-pool.ts`

```typescript
export class DuckDBPool {
  private static instance: DuckDBInstance | null = null;
  private static initialized: boolean = false;
  private static available: boolean = false;
  private static initError: string | null = null;

  /**
   * Initialize the DuckDB instance
   * Returns true if successful, false if DuckDB is unavailable
   */
  static async initialize(): Promise<boolean> {
    if (this.initialized) {
      return this.available;
    }

    try {
      const { DuckDBInstance } = await import('@duckdb/node-api');
      this.instance = await DuckDBInstance.create();

      const setupConn = await this.instance.connect();
      await setupConn.runAndReadAll('INSTALL spatial;');
      await setupConn.runAndReadAll('LOAD spatial;');

      this.available = true;
      this.initialized = true;
      return true;
    } catch (error) {
      this.initError = (error as Error).message;
      this.available = false;
      this.initialized = true;
      console.warn(`[DuckDB] Not available: ${this.initError}`);
      console.warn('[DuckDB] Advanced query features will be disabled');
      return false;
    }
  }

  /**
   * Check if DuckDB is available on this system
   */
  static isAvailable(): boolean {
    return this.available;
  }

  /**
   * Get the initialization error message (if any)
   */
  static getInitError(): string | null {
    return this.initError;
  }

  /**
   * Get a connection - throws if DuckDB not available
   */
  static async getConnection() {
    if (!this.available || !this.instance) {
      throw new Error(
        'DuckDB is not available on this system. ' +
        (this.initError || 'Unknown error during initialization.')
      );
    }
    return await this.instance.connect();
  }
}
```

### Phase 2: Update Plugin Startup

**File**: `src/index.ts`

```typescript
// Initialize DuckDB connection pool (optional - may not be available on all platforms)
const duckdbAvailable = await DuckDBPool.initialize();
if (duckdbAvailable) {
  app.debug('DuckDB connection pool initialized');

  // Initialize S3 credentials if needed
  if (options.s3Upload?.enabled && options.s3Upload?.accessKeyId) {
    await DuckDBPool.initializeS3({...});
  }
} else {
  app.debug('DuckDB not available - advanced query features disabled');
  app.setPluginStatus('DuckDB unavailable - limited functionality');
}

// Store availability in plugin state for other modules to check
state.duckdbAvailable = duckdbAvailable;
```

### Phase 3: Guard DuckDB-Dependent Code

**Pattern for all DuckDB-using code**:

```typescript
// Before
const connection = await DuckDBPool.getConnection();
const result = await connection.runAndReadAll(query);

// After
if (!DuckDBPool.isAvailable()) {
  throw new Error('This feature requires DuckDB which is not available on this system');
}
const connection = await DuckDBPool.getConnection();
const result = await connection.runAndReadAll(query);
```

**Files to update**:
- `src/api-routes.ts` - Query endpoints
- `src/history-provider.ts` - History API
- `src/HistoryAPI.ts` - History API routes
- `src/claude-analyzer.ts` - AI SQL queries
- `src/services/aggregation-service.ts` - Aggregation
- `src/services/migration-service.ts` - Migration timestamp extraction
- `src/utils/context-discovery.ts` - Context discovery
- `src/utils/path-discovery.ts` - Path discovery
- `src/utils/schema-cache.ts` - Schema caching

### Phase 4: Implement Fallbacks

#### 4.1 History API Fallback

When DuckDB unavailable, History API queries should:
1. Check SQLite buffer for recent data (last 48h)
2. Return error for older data with helpful message

```typescript
// In history-provider.ts
if (!DuckDBPool.isAvailable()) {
  // Can only serve data from SQLite buffer
  const bufferData = this.sqliteBuffer?.getRecordsForPath(context, path, fromIso, toIso);
  if (!bufferData || bufferData.length === 0) {
    throw new Error(
      'Historical data queries require DuckDB which is unavailable on this system. ' +
      'Only recent data (last 48h) from SQLite buffer is accessible.'
    );
  }
  return this.formatBufferData(bufferData);
}
```

#### 4.2 Path Discovery Fallback

When DuckDB unavailable, use filesystem-only discovery:

```typescript
// In path-discovery.ts
export async function getAvailablePathsForTimeRange(...): Promise<Path[]> {
  if (!DuckDBPool.isAvailable()) {
    // Fall back to filesystem scan (no time filtering)
    console.warn('[Path Discovery] DuckDB unavailable, returning all paths without time filtering');
    return getAvailablePathsArray(dataDir, app) as Path[];
  }
  // ... existing DuckDB-based implementation
}
```

#### 4.3 Migration Service Fallback

Already has filename-based fallback, just needs graceful handling:

```typescript
// In migration-service.ts
private async extractTimestampFromFile(filePath: string): Promise<Date | null> {
  // Try DuckDB first (if available)
  if (DuckDBPool.isAvailable()) {
    try {
      const connection = await DuckDBPool.getConnection();
      // ... query parquet file
    } catch (error) {
      // Fall through to filename parsing
    }
  }

  // Fallback: parse timestamp from filename
  const filename = path.basename(filePath);
  const match = filename.match(/(\d{4})-?(\d{2})-?(\d{2})T?(\d{2})(\d{2})(\d{2})/);
  if (match) {
    return new Date(`${match[1]}-${match[2]}-${match[3]}T${match[4]}:${match[5]}:${match[6]}Z`);
  }
  return null;
}
```

### Phase 5: UI Feedback

**File**: `public/js/migration.js` (and other UI files)

Show warning banner when DuckDB unavailable:

```javascript
// Add to buffer status display
if (!data.duckdbAvailable) {
  container.innerHTML = `
    <div style="background: #fff3cd; border: 1px solid #ffc107; padding: 10px; margin-bottom: 15px; border-radius: 5px;">
      <strong>⚠️ Limited Functionality</strong><br>
      DuckDB is not available on this system. Data collection and export work normally,
      but advanced query features (History API, Claude analysis) are disabled.
    </div>
  ` + container.innerHTML;
}
```

**API endpoint to expose status**:

```typescript
// In api-routes.ts
router.get('/api/status', (req, res) => {
  res.json({
    success: true,
    duckdbAvailable: DuckDBPool.isAvailable(),
    duckdbError: DuckDBPool.getInitError(),
    sqliteBufferEnabled: !!state.sqliteBuffer,
    // ... other status info
  });
});
```

### Phase 6: Update Types

**File**: `src/types.ts`

```typescript
export interface PluginState {
  // ... existing fields
  duckdbAvailable?: boolean;
}
```

---

## Testing Plan

### Test on Supported Platform
1. Verify all features work normally
2. Verify `DuckDBPool.isAvailable()` returns true

### Test on Unsupported Platform (Simulated)
1. Temporarily modify DuckDBPool to force failure
2. Verify plugin starts without crash
3. Verify data collection works
4. Verify daily export works
5. Verify query endpoints return helpful error messages
6. Verify UI shows warning banner

### Test on Actual 32-bit ARM
1. Install on VenusOS or 32-bit Raspberry Pi
2. Verify plugin starts
3. Verify core functionality works

---

## Rollout Plan

1. **Phase 1-2**: Core changes (DuckDBPool + startup) - Low risk
2. **Phase 3-4**: Guard all DuckDB code + fallbacks - Medium risk
3. **Phase 5**: UI feedback - Low risk
4. **Phase 6**: Types update - Low risk

Recommend implementing in single PR with thorough testing.

---

## Future Considerations

### Alternative Query Engines

If DuckDB unavailability becomes a common issue, consider:

1. **SQLite-based Parquet reading**: Use `better-sqlite3` with a Parquet extension
2. **Pure JS Parquet reader**: Slower but universal compatibility
3. **Server-side query proxy**: Offload queries to a capable server

### Package Structure

Could split into two packages:
- `signalk-parquet-core`: Data collection + export (no DuckDB)
- `signalk-parquet-full`: Full features including DuckDB queries

This would allow lighter installs on constrained systems.

---

## Summary

This plan allows signalk-parquet to run on any platform, with DuckDB features gracefully disabled when the native module cannot load. Users on limited platforms can still:
- Collect SignalK data
- Export to Parquet files
- Upload to S3
- View recent data from SQLite buffer

They lose:
- Historical Parquet queries
- Claude SQL analysis
- Aggregation features

The UI will clearly indicate the reduced functionality.
