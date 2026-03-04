# DuckDB Node API Upgrade Plan

> **Status**: Review
> **Created**: 2026-03-03
> **Goal**: Audit and optimize @duckdb/node-api usage across codebase

---

## Executive Summary

**Good news**: The upgrade from the old `duckdb` package to `@duckdb/node-api` has **already been completed**. The package.json shows `"@duckdb/node-api": "^1.4.0-r.1"` and all code already uses the new API patterns.

However, there are **optimization opportunities** and **inconsistencies** that should be addressed to improve performance and maintainability.

---

## Current State Analysis

### Package Configuration
```json
{
  "@duckdb/node-api": "^1.4.0-r.1"
}
```

### API Methods Currently Used
| Method | Description | Status |
|--------|-------------|--------|
| `DuckDBInstance.create()` | Create new instance | ✅ Correct |
| `instance.connect()` | Get connection | ✅ Correct |
| `connection.runAndReadAll(query)` | Execute query | ✅ Correct |
| `result.getRowObjects()` | Get rows as objects | ✅ Correct |
| `connection.disconnectSync()` | Cleanup connection | ✅ Correct |

### Files Using DuckDB

| File | Pattern | Issue |
|------|---------|-------|
| `src/utils/duckdb-pool.ts` | Singleton pool | ✅ Good |
| `src/index.ts` | Uses pool | ✅ Good |
| `src/history-provider.ts` | Uses pool | ✅ Good |
| `src/api-routes.ts` | Uses pool | ✅ Good |
| `src/HistoryAPI.ts` | Uses pool | ✅ Good |
| `src/services/aggregation-service.ts` | Uses pool | ✅ Good |
| `src/services/migration-service.ts` | Uses pool | ✅ Good |
| `src/utils/schema-cache.ts` | Uses pool | ✅ Good |
| `src/utils/context-discovery.ts` | Creates ephemeral instance | ⚠️ Inefficient |
| `src/utils/path-discovery.ts` | Creates ephemeral instance | ⚠️ Inefficient |
| `src/claude-analyzer.ts` | Creates ephemeral instance + loads spatial | ⚠️ Very inefficient |
| `src/types.ts` | Custom type definitions | ⚠️ May drift from actual API |

---

## Identified Issues

### Issue 1: Ephemeral DuckDB Instances (Medium Priority)

**Problem**: Three files create their own `DuckDBInstance` for single operations instead of using the shared pool:

```typescript
// context-discovery.ts:74
const duckDB = await DuckDBInstance.create();
const connection = await duckDB.connect();

// path-discovery.ts:220
const duckDB = await DuckDBInstance.create();
const connection = await duckDB.connect();

// claude-analyzer.ts:3368-3373
const instance = await DuckDBInstance.create();
const connection = await instance.connect();
await connection.runAndReadAll('INSTALL spatial;');
await connection.runAndReadAll('LOAD spatial;');
```

**Impact**:
- Creates overhead of new instance + connection per query
- `claude-analyzer.ts` also loads spatial extension on every query (~100ms+ overhead)
- No connection reuse benefits

**Solution**: Migrate to `DuckDBPool.getConnection()` pattern.

### Issue 2: Custom Type Definitions (Low Priority)

**Problem**: `src/types.ts` defines custom DuckDB interfaces (lines 634-647):

```typescript
export interface DuckDBConnection {
  runAndReadAll(query: string): Promise<DuckDBResult>;
  disconnectSync(): void;
}

export interface DuckDBResult {
  getRowObjects(): any[];
}

export interface DuckDBInstance {
  connect(): Promise<DuckDBConnection>;
}
```

**Impact**:
- May not match actual `@duckdb/node-api` type signatures
- Could cause TypeScript errors if API changes
- Duplicates types from the package

**Solution**: Import types from `@duckdb/node-api` directly or remove custom definitions.

### Issue 3: Connection Lifecycle Inconsistency (Low Priority)

**Problem**: Some files call `connection.disconnectSync()` in finally blocks while others rely on garbage collection:

```typescript
// Pattern A: Explicit cleanup (most files)
try {
  const result = await connection.runAndReadAll(query);
  // ...
} finally {
  connection.disconnectSync();
}

// Pattern B: No explicit cleanup (claude-analyzer.ts:3405-3407)
} finally {
  // DuckDB connections close automatically when instance is destroyed
}
```

**Impact**: Minor - both patterns work, but inconsistency can be confusing.

**Solution**: Standardize on explicit `disconnectSync()` in finally blocks.

---

## Improvement Plan

### Phase 1: Migrate Ephemeral Instances to Pool (Medium Effort)

**Goal**: All DuckDB queries use the shared `DuckDBPool`.

#### 1.1 Update context-discovery.ts

**Current** (lines 74-93):
```typescript
const duckDB = await DuckDBInstance.create();
const connection = await duckDB.connect();
try {
  // ... query ...
} finally {
  connection.disconnectSync();
}
```

**Change to**:
```typescript
const connection = await DuckDBPool.getConnection();
try {
  // ... query ...
} finally {
  connection.disconnectSync();
}
```

**Files to modify**: `src/utils/context-discovery.ts`
- Remove: `import { DuckDBInstance } from '@duckdb/node-api';`
- Add: `import { DuckDBPool } from './duckdb-pool';`
- Update: Lines 74-75 to use pool

#### 1.2 Update path-discovery.ts

**Current** (lines 220-234):
```typescript
const duckDB = await DuckDBInstance.create();
const connection = await duckDB.connect();
try {
  // ...
} finally {
  connection.disconnectSync();
}
```

**Change to**:
```typescript
const connection = await DuckDBPool.getConnection();
try {
  // ...
} finally {
  connection.disconnectSync();
}
```

**Files to modify**: `src/utils/path-discovery.ts`
- Remove: `import { DuckDBInstance } from '@duckdb/node-api';`
- Add: `import { DuckDBPool } from './duckdb-pool';`
- Update: Lines 220-221 to use pool

#### 1.3 Update claude-analyzer.ts

**Current** (lines 3368-3406):
```typescript
const instance = await DuckDBInstance.create();
const connection = await instance.connect();
await connection.runAndReadAll('INSTALL spatial;');
await connection.runAndReadAll('LOAD spatial;');
try {
  const result = await connection.runAndReadAll(correctedSQL);
  // ...
} finally {
  // DuckDB connections close automatically when instance is destroyed
}
```

**Change to**:
```typescript
const connection = await DuckDBPool.getConnection();
try {
  const result = await connection.runAndReadAll(correctedSQL);
  // ...
} finally {
  connection.disconnectSync();
}
```

**Files to modify**: `src/claude-analyzer.ts`
- Remove: `import { DuckDBInstance } from '@duckdb/node-api';` (line 14)
- Update: Lines 3368-3373 to use pool (remove extension loading - pool handles this)
- Add: `connection.disconnectSync()` in finally block

**Note**: The pool already loads the spatial extension during initialization, so we remove the per-query extension loading.

### Phase 2: Type Cleanup (Low Effort)

**Goal**: Remove custom DuckDB type definitions.

#### 2.1 Update types.ts

**Remove** (lines 634-647):
```typescript
// DuckDB Related Types
export interface DuckDBConnection {
  runAndReadAll(query: string): Promise<DuckDBResult>;
  disconnectSync(): void;
}

export interface DuckDBResult {
  getRowObjects(): any[];
}

export interface DuckDBInstance {
  connect(): Promise<DuckDBConnection>;
}
```

**Rationale**: These types are now provided by `@duckdb/node-api`.

**Impact check**: Run `npm run build` to verify no type errors after removal.

### Phase 3: Connection Lifecycle Standardization (Low Effort)

**Goal**: All files use explicit `disconnectSync()` in finally blocks.

**Pattern to enforce**:
```typescript
const connection = await DuckDBPool.getConnection();
try {
  const result = await connection.runAndReadAll(query);
  return result.getRowObjects();
} finally {
  connection.disconnectSync();
}
```

---

## API Reference: @duckdb/node-api

For reference, here are the main types from `@duckdb/node-api`:

```typescript
// Instance creation
class DuckDBInstance {
  static create(path?: string, options?: DuckDBOptions): Promise<DuckDBInstance>;
  connect(): Promise<DuckDBConnection>;
}

// Connection methods
interface DuckDBConnection {
  runAndReadAll(sql: string): Promise<DuckDBResult>;
  run(sql: string): Promise<DuckDBPendingResult>;
  prepare(sql: string): Promise<DuckDBPreparedStatement>;
  disconnectSync(): void;
  disconnectAsync(): Promise<void>;
}

// Result methods
interface DuckDBResult {
  getRowObjects(): any[];
  getColumns(): DuckDBColumn[];
  // ... other methods
}
```

---

## Verification Checklist

After each phase:
1. [ ] `npm run lint` - No new lint errors
2. [ ] `npm run build` - TypeScript compiles successfully
3. [ ] Restart plugin - No startup errors
4. [ ] Test query endpoint - `/api/query` returns results
5. [ ] Test history API - Historical data queries work
6. [ ] Test Claude analyzer - AI queries work (if claude integration enabled)

---

## Risk Assessment

| Change | Risk | Mitigation |
|--------|------|------------|
| Pool migration for context-discovery | Low | Pool is already initialized at startup |
| Pool migration for path-discovery | Low | Pool is already initialized at startup |
| Pool migration for claude-analyzer | Low | Spatial extension loaded by pool |
| Remove custom types | Very Low | Types come from package |
| Standardize lifecycle | Very Low | No functional change |

---

## Summary

The codebase has already been upgraded to `@duckdb/node-api`. The remaining work is optimization:

1. **Phase 1**: Migrate 3 files to use `DuckDBPool` instead of ephemeral instances
2. **Phase 2**: Remove redundant custom type definitions
3. **Phase 3**: Standardize connection cleanup pattern

Total estimated effort: **1-2 hours** for all phases.

---

## Appendix: Files Modified Summary

| File | Phase | Changes |
|------|-------|---------|
| `src/utils/context-discovery.ts` | 1.1 | Use pool instead of ephemeral instance |
| `src/utils/path-discovery.ts` | 1.2 | Use pool instead of ephemeral instance |
| `src/claude-analyzer.ts` | 1.3 | Use pool, remove extension loading |
| `src/types.ts` | 2.1 | Remove custom DuckDB interfaces |
