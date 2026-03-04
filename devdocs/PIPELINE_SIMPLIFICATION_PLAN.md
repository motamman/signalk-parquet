# Pipeline Simplification Plan

## Executive Summary

Simplify the data pipeline from a 4-stage process with incremental exports and consolidation to a 3-stage process with direct daily exports. This eliminates the consolidation subsystem entirely, reducing complexity, bugs, and I/O overhead.

**Current Pipeline:**
```
Memory Buffer → SQLite (flush every 30s) → Parquet (export every 5 min) → Consolidated Parquet (daily)
```

**Proposed Pipeline:**
```
Memory Buffer → SQLite (flush every 30s, retain 48h) → Daily Parquet Export
```

## Current Architecture Analysis

### Data Flow (Current)

1. **Memory Buffer** (`bufferSize: 1000`, `saveIntervalSeconds: 30`)
   - SignalK deltas collected in memory
   - Flushed to SQLite when buffer full OR interval reached
   - Purpose: Batch writes for throughput at 1000+ deltas/sec

2. **SQLite Buffer** (`bufferRetentionHours: 24`)
   - WAL-mode SQLite database at `{outputDir}/buffer.db`
   - Holds all recent data for fast queries
   - Records marked as exported after Parquet write
   - Cleanup runs periodically to remove old exported records

3. **Incremental Parquet Export** (`exportIntervalMinutes: 5`, `exportBatchSize: 50000`)
   - Every 5 minutes, exports pending records from SQLite to Parquet
   - Creates files like `signalk_data_2026-03-02T1430.parquet`
   - One file per path per export interval
   - **Result: Hundreds of small files per day per path**

4. **Daily Consolidation** (`consolidationLookbackDays: 225`)
   - Runs at startup and daily at midnight UTC
   - Scans for unconsolidated files
   - Merges all files for a date/path into single `_consolidated.parquet`
   - Moves source files to `processed/` folder
   - **Problem: Complex, error-prone, creates I/O storms**

### Problems with Current Architecture

| Problem | Impact | Evidence |
|---------|--------|----------|
| Thousands of small files | Query performance, filesystem overhead | 12,000+ files for single date |
| Consolidation bugs | Data not consolidated, errors abort process | "dest already exists" error |
| Complex error handling | Partial failures leave inconsistent state | Files in limbo between states |
| High I/O | Write files, read files, write consolidated, move originals | 4x file operations |
| Processed folder bloat | Disk usage for files that serve no purpose | GB of processed files |
| Long startup scan | 225 days × 10 sec/day = ~37 min scan | Blocks consolidation |

### Files Involved (Current)

| File | Relevant Functions | Lines |
|------|-------------------|-------|
| `src/data-handler.ts` | `consolidateMissedDays`, `consolidateYesterday` | 700-780 |
| `src/parquet-writer.ts` | `consolidateDaily`, `mergeFiles` | 743-985 |
| `src/services/parquet-export-service.ts` | `exportPendingRecords` | Full file |
| `src/index.ts` | Consolidation scheduling, intervals | 272-330 |
| `src/utils/directory-scanner.ts` | `findFilesByDate` for consolidation | Full file |

## Proposed Architecture

### Data Flow (Proposed)

1. **Memory Buffer** (unchanged)
   - Same batching logic for write throughput
   - No changes needed

2. **SQLite Buffer** (extended retention)
   - Increase default retention to 48 hours
   - Primary source for recent data queries (0-48h)
   - Records NOT marked as exported until daily export
   - Simpler state: just `inserted_at` timestamp

3. **Daily Parquet Export** (new)
   - Runs once daily at configurable time (default: 02:00 UTC)
   - Exports ALL records for previous day (00:00-23:59 UTC)
   - Creates ONE file per path per day: `signalk_data_2026-03-02.parquet`
   - Marks records as exported in SQLite
   - SQLite cleanup removes exported records after retention period

### What Gets Removed

```
DELETED FILES:
- src/data-handler.ts: consolidateMissedDays(), consolidateYesterday(), uploadConsolidatedFilesToS3()
- src/parquet-writer.ts: consolidateDaily(), mergeFiles(), validateParquetFile() (partial)

DELETED CONFIG:
- exportIntervalMinutes (no more incremental exports)
- consolidationLookbackDays (no more consolidation)
- enableRetention (processed folder doesn't exist)
- retentionDays (no processed folder to clean)

DELETED CONCEPTS:
- "processed" folder
- "consolidated" suffix on filenames
- Consolidation intervals and scheduling
- Directory scanning for unconsolidated files
```

### What Gets Modified

```
MODIFIED: src/services/parquet-export-service.ts
- Change from "export pending records every N minutes" to "export full day once daily"
- Export query: WHERE date(received_timestamp) = {yesterday}
- Single call per path, produces single file

MODIFIED: src/index.ts
- Remove consolidation scheduling (lines 272-330)
- Add daily export scheduling at 02:00 UTC
- Simplify config schema (remove obsolete options)

MODIFIED: src/utils/sqlite-buffer.ts
- Simplify export tracking (no incremental batches)
- Change cleanup logic: delete records older than retention AND exported

MODIFIED: src/types.ts
- Remove obsolete config fields
- Add dailyExportHour config
```

### What Gets Added

```
NEW CONFIG:
- dailyExportHour: number (0-23, default 2) - Hour in UTC to run daily export
- sqliteRetentionHours: number (24-168, default 48) - How long to keep in SQLite

NEW FUNCTION: src/services/parquet-export-service.ts
- exportDayToParquet(date: Date): Promise<ExportResult>
  - Exports all records for given date
  - One Parquet file per path
  - Returns count of records/files exported
```

## Detailed Implementation Plan

### Phase 1: Extend SQLite Retention (Low Risk)

**Goal:** Ensure SQLite can reliably hold 48h of data before any other changes.

**Changes:**
1. Update default `bufferRetentionHours` from 24 to 48
2. Test SQLite performance with 48h of data
3. Monitor disk usage

**Files:**
- `src/index.ts`: Change default in schema
- `src/types.ts`: Update comment

**Verification:**
- Run for 48h
- Confirm SQLite queries still fast
- Check disk usage is acceptable

### Phase 2: Create Daily Export Function (Medium Risk)

**Goal:** Build the new daily export without removing old system.

**New Function in `src/services/parquet-export-service.ts`:**

```typescript
async exportDayToParquet(targetDate: Date): Promise<{
  filesCreated: number;
  recordsExported: number;
  errors: string[];
}> {
  const dateStr = targetDate.toISOString().slice(0, 10); // "2026-03-02"

  // Get all paths that have data for this date
  const pathsWithData = await this.sqliteBuffer.getPathsForDate(targetDate);

  let filesCreated = 0;
  let recordsExported = 0;
  const errors: string[] = [];

  for (const pathInfo of pathsWithData) {
    try {
      // Query all records for this path/date
      const records = await this.sqliteBuffer.getRecordsForPathAndDate(
        pathInfo.context,
        pathInfo.path,
        targetDate
      );

      if (records.length === 0) continue;

      // Build output path (Hive or flat structure)
      const outputPath = this.buildOutputPath(pathInfo, dateStr);

      // Write single Parquet file
      await this.parquetWriter.writeRecords(outputPath, records);

      filesCreated++;
      recordsExported += records.length;

      // Mark records as exported
      await this.sqliteBuffer.markDateExported(pathInfo.context, pathInfo.path, targetDate);

    } catch (error) {
      errors.push(`${pathInfo.path}: ${error.message}`);
    }
  }

  return { filesCreated, recordsExported, errors };
}
```

**New Function in `src/utils/sqlite-buffer.ts`:**

```typescript
async getPathsForDate(date: Date): Promise<Array<{context: string, path: string}>> {
  const dateStr = date.toISOString().slice(0, 10);
  return this.db.all(`
    SELECT DISTINCT context, path
    FROM deltas
    WHERE date(received_timestamp) = ?
    AND exported = 0
  `, [dateStr]);
}

async getRecordsForPathAndDate(
  context: string,
  path: string,
  date: Date
): Promise<DataRecord[]> {
  const dateStr = date.toISOString().slice(0, 10);
  return this.db.all(`
    SELECT * FROM deltas
    WHERE context = ? AND path = ? AND date(received_timestamp) = ?
    ORDER BY received_timestamp ASC
  `, [context, path, dateStr]);
}

async markDateExported(context: string, path: string, date: Date): Promise<void> {
  const dateStr = date.toISOString().slice(0, 10);
  await this.db.run(`
    UPDATE deltas SET exported = 1
    WHERE context = ? AND path = ? AND date(received_timestamp) = ?
  `, [context, path, dateStr]);
}
```

**Verification:**
- Add test in `tests/test-daily-export.js`
- Manually trigger export for yesterday
- Verify single Parquet file created per path
- Verify DuckDB can read files

### Phase 3: Schedule Daily Export (Medium Risk)

**Goal:** Add scheduling to trigger daily export automatically.

**Changes to `src/index.ts`:**

```typescript
// Calculate ms until 02:00 UTC tomorrow
const now = new Date();
const nextExport = new Date(now);
nextExport.setUTCDate(nextExport.getUTCDate() + 1);
nextExport.setUTCHours(state.currentConfig.dailyExportHour || 2, 0, 0, 0);
const msUntilExport = nextExport.getTime() - now.getTime();

// Schedule daily export
setTimeout(() => {
  runDailyExport();

  // Then repeat every 24 hours
  state.dailyExportInterval = setInterval(runDailyExport, 24 * 60 * 60 * 1000);
}, msUntilExport);

async function runDailyExport() {
  const yesterday = new Date();
  yesterday.setUTCDate(yesterday.getUTCDate() - 1);

  app.debug(`Starting daily Parquet export for ${yesterday.toISOString().slice(0, 10)}`);

  try {
    const result = await state.parquetExportService.exportDayToParquet(yesterday);
    app.debug(`Daily export complete: ${result.filesCreated} files, ${result.recordsExported} records`);

    if (result.errors.length > 0) {
      app.error(`Daily export had ${result.errors.length} errors`);
      result.errors.forEach(e => app.debug(`  ${e}`));
    }

    // Upload to S3 if enabled
    if (state.currentConfig.s3Upload.enabled) {
      await uploadDayToS3(yesterday);
    }
  } catch (error) {
    app.error(`Daily export failed: ${error.message}`);
  }
}
```

**New Config Options:**
```typescript
dailyExportHour: {
  type: 'number',
  title: 'Daily Export Hour (UTC)',
  description: 'Hour of day (0-23) in UTC to run daily Parquet export. Default 2 (2 AM UTC).',
  default: 2,
  minimum: 0,
  maximum: 23,
},
```

**Verification:**
- Set export hour to current hour + 1 minute
- Watch logs for export trigger
- Verify files created

### Phase 4: Remove Incremental Export (High Risk)

**Goal:** Stop the every-5-minute export cycle.

**Changes:**

1. **Remove from `src/index.ts`:**
   - Delete `exportIntervalMinutes` scheduling
   - Delete `state.exportInterval`

2. **Remove from `src/services/parquet-export-service.ts`:**
   - Delete `exportPendingRecords()` or repurpose
   - Delete batch tracking logic

3. **Remove config:**
   - `exportIntervalMinutes`
   - `exportBatchSize`

**Verification:**
- Run for 24h
- Verify no small Parquet files created
- Verify SQLite holds all data
- Verify daily export still works
- Verify queries still work (hit SQLite for recent, Parquet for old)

### Phase 5: Remove Consolidation System (High Risk)

**Goal:** Delete all consolidation code.

**Delete from `src/data-handler.ts`:**
```typescript
// DELETE: Lines 700-780
export async function consolidateMissedDays(...) { ... }
export async function consolidateYesterday(...) { ... }
```

**Delete from `src/parquet-writer.ts`:**
```typescript
// DELETE: Lines 743-985
async mergeFiles(...) { ... }
async consolidateDaily(...) { ... }
```

**Delete from `src/index.ts`:**
```typescript
// DELETE: Consolidation scheduling (lines 272-330)
// DELETE: consolidateMissedDays import
// DELETE: consolidateYesterday import
```

**Delete config options:**
```typescript
// DELETE from schema:
consolidationLookbackDays
enableRetention
retentionDays
```

**Delete from `src/types.ts`:**
```typescript
// DELETE:
enableRetention?: boolean;
consolidationLookbackDays?: number;
```

**Verification:**
- Build succeeds
- No consolidation errors in logs
- Daily export works
- All queries work

### Phase 6: Cleanup (Low Risk)

**Goal:** Remove dead code and simplify.

1. **Remove `processed/` folder handling:**
   - Delete references in `DirectoryScanner`
   - Delete from `SPECIAL_DIRECTORIES`

2. **Remove `_consolidated` suffix logic:**
   - Files are just `signalk_data_2026-03-02.parquet` now

3. **Simplify `validateParquetFile`:**
   - Keep for write validation, remove consolidation-specific logic

4. **Update documentation:**
   - README
   - Config descriptions
   - This plan → mark as COMPLETED

**Verification:**
- Full test suite passes
- Manual testing of all query types
- 48h burn-in test

## Migration Guide

### For Existing Installations

1. **Before upgrade:**
   - Let current consolidation complete
   - Backup SQLite buffer.db

2. **After upgrade:**
   - Old consolidated files remain queryable
   - Old unconsolidated files remain (but won't be consolidated)
   - New files will be daily exports

3. **Optional cleanup:**
   - Delete `processed/` folders (all data is in consolidated files)
   - Delete old small Parquet files if corresponding consolidated exists

### Breaking Changes

| Change | Impact | Migration |
|--------|--------|-----------|
| `exportIntervalMinutes` removed | Config ignored | Remove from config |
| `consolidationLookbackDays` removed | Config ignored | Remove from config |
| `enableRetention` removed | Config ignored | Remove from config |
| `retentionDays` removed | Config ignored | Remove from config |
| No more `_consolidated` suffix | Query patterns change | Update any external queries |
| No more `processed/` folder | Disk space freed | Delete manually if desired |

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| SQLite corruption loses 48h data | Low | High | WAL mode; optional backup |
| Daily export fails | Medium | Medium | Retry next day; manual trigger |
| Memory spike during large export | Medium | Low | Batch within daily export |
| Query performance regression | Low | Medium | Test thoroughly before deploy |
| External tools depend on old filenames | Medium | Medium | Document breaking changes |

## Success Metrics

| Metric | Before | After | Target |
|--------|--------|-------|--------|
| Files per day per path | 100-300 | 1 | 1 |
| Consolidation errors/week | 2-5 | 0 | 0 |
| Disk I/O (writes/day) | ~10,000 | ~100 | <200 |
| Startup time (consolidation scan) | 30-60 min | 0 | <1 min |
| Code lines (consolidation) | ~500 | 0 | 0 |
| Config options | 15 | 11 | <12 |

## Timeline

| Phase | Duration | Dependencies |
|-------|----------|--------------|
| Phase 1: Extend SQLite | 1 day | None |
| Phase 2: Daily Export Function | 2 days | Phase 1 |
| Phase 3: Schedule Daily Export | 1 day | Phase 2 |
| Phase 4: Remove Incremental | 1 day | Phase 3 tested |
| Phase 5: Remove Consolidation | 1 day | Phase 4 tested |
| Phase 6: Cleanup | 1 day | Phase 5 |
| **Total** | **7 days** | |

## Rollback Plan

If issues arise after deployment:

1. **Restore old code** from git
2. **Keep new Parquet files** (they're valid, just differently named)
3. **Re-enable consolidation** for old unconsolidated files
4. **Investigate** root cause before retry

## Appendix: Query Federation After Change

**Current (3 sources):**
```sql
-- Parquet files (small incremental + consolidated)
SELECT * FROM read_parquet('.../**/*.parquet')
-- SQLite buffer (recent)
SELECT * FROM sqlite_scan('buffer.db', 'deltas')
-- S3 (remote)
SELECT * FROM read_parquet('s3://bucket/...')
```

**After (3 sources, simpler):**
```sql
-- Parquet files (daily only, no consolidation)
SELECT * FROM read_parquet('.../**/*.parquet')
-- SQLite buffer (recent, 48h)
SELECT * FROM sqlite_scan('buffer.db', 'deltas')
-- S3 (remote)
SELECT * FROM read_parquet('s3://bucket/...')
```

No changes needed to query federation - sources are the same, just fewer/larger files.

---

**Document Version:** 1.0
**Created:** 2026-03-02
**Author:** Claude + Maurice
**Status:** DRAFT - Awaiting Approval
