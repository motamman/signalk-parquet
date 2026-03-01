# SignalK History API Comparison Report

## Your Implementation vs Official SignalK Server Specification

**Date:** March 2026
**Official Spec Source:** https://github.com/SignalK/signalk-server
**Documentation:** https://demo.signalk.org/documentation/Developing/REST_APIs/History_API.html

---

## Summary

Your implementation is **largely compliant** with the SignalK History API but includes **significant extensions** and some **intentional deviations**. Below is a detailed breakdown.

---

## 1. API Path Version

| Aspect | Official Spec | Your Implementation | Impact |
|--------|---------------|---------------------|--------|
| Base URL | `/signalk/v2/api/history/*` | `/signalk/v1/history/*` | **Major deviation** |
| Additional routes | None | `/api/history/*` (plugin-style) | Extension |

**Discussion**: The official spec uses `v2`, you use `v1`. This could cause issues with clients expecting v2 semantics. You may want to support both or document this clearly.

---

## 2. Time Range Parameters

| Feature | Official Spec | Your Implementation | Status |
|---------|---------------|---------------------|--------|
| Pattern 1: `duration` only | Yes | Yes | Compliant |
| Pattern 2: `from` + `duration` | Yes | Yes | Compliant |
| Pattern 3: `to` + `duration` | Yes | Yes | Compliant |
| Pattern 4: `from` only | Yes | Yes | Compliant |
| Pattern 5: `from` + `to` | Yes | Yes | Compliant |
| Legacy `start` param | Not in spec | Yes (deprecated) | **Extension** |
| `useUTC` param | Not in spec | Yes | **Extension** |
| `refresh` param | Not in spec | Yes | **Extension** |

**Discussion**: You've added `start` for backward compatibility (with deprecation warning) and `useUTC` for explicit timezone control. Both are reasonable extensions.

---

## 3. Duration Format

| Aspect | Official Spec | Your Implementation |
|--------|---------------|---------------------|
| ISO 8601 Duration | `PT15M`, `P1D`, `PT1H30M` | Supported |
| Integer seconds | `3600` | Supported |
| Custom shorthand | Not specified | `1h`, `30m`, `5s`, `2d` (extension) |

**Your parser** (`src/utils/duration-parser.ts`):
```typescript
// Supports all three formats:
// 1. ISO 8601: PT1H, PT30M, P1D, PT1H30M
// 2. Integer seconds: 3600, 60
// 3. Shorthand: 1h, 30m, 5s, 2d (backward compatible)
```

**Status**: Full compliance with ISO 8601 and integer seconds, plus backward-compatible shorthand extension.

---

## 4. Resolution Parameter

| Aspect | Official Spec | Your Implementation |
|--------|---------------|---------------------|
| Unit | Seconds | Seconds |
| Time expressions | `1s`, `1m` | Supported |
| Default | Server-determined | `(to - from) / 500` seconds |

**Your code** (`src/utils/duration-parser.ts` + `HistoryAPI.ts`):
```typescript
// Supports seconds and time expressions:
// - 60 → 60 seconds
// - 1m → 60 seconds
// - 5s → 5 seconds
// - 1h → 3600 seconds
```

**Status**: Full compliance. Resolution now expects seconds (breaking change from v0.7.0).

---

## 5. Aggregation Methods

| Method | Official Spec | Your Implementation |
|--------|---------------|---------------------|
| `average` | Yes | Yes |
| `min` | Yes | Yes |
| `max` | Yes | Yes |
| `first` | Yes | Yes |
| `last` | Yes | Yes |
| `mid` | Yes | Yes |
| `middle_index` | Yes | Yes (but uses FIRST as fallback) |
| `sma` | Yes (as aggregation method) | Supported |
| `ema` | Yes (as aggregation method) | Supported |

**Official syntax (supported)**: `path:sma:5` or `path:ema:0.2`
- Returns ONLY the smoothed value (per SignalK spec)

**Extension syntax (also supported)**: `path:average:sma:5` (4-part)
- Returns raw value AND smoothed value

**Status**: Full compliance with official spec. Extension syntax preserved for backward compatibility.

---

## 6. Path Specification Syntax

| Aspect | Official Spec | Your Implementation |
|--------|---------------|---------------------|
| Basic | `path` | Yes |
| With method | `path:method` | Yes |
| With parameter | `path:method:param` | Supported |

**Both syntaxes supported**:
```
Official:  navigation.speedOverGround:sma:5     (returns only smoothed value)
Extension: navigation.speedOverGround:average:sma:5  (returns raw AND smoothed)
```

**Status**: Full compliance with official syntax. Extension syntax preserved for backward compatibility.

---

## 7. Response Format

### Standard Fields

| Field | Official Spec | Your Implementation |
|-------|---------------|---------------------|
| `context` | Yes | Yes |
| `range.from/to` | Yes | Yes |
| `values[]` | Yes | Yes |
| `data[][]` | Yes | Yes |

### Your Extensions (not in official spec)

| Field | Purpose | Status |
|-------|---------|--------|
| `units` | Unit conversion metadata | **Extension** |
| `timezone` | Timezone conversion metadata | **Extension** |
| `meta` | Auto-discovery notifications | **Extension** |
| `refresh` | Auto-refresh metadata | **Extension** |

**Your DataResult type** (`HistoryAPI-types.ts` line 14-43):
```typescript
export interface DataResult {
  context: Context;
  range: { from: Timestamp; to: Timestamp; };
  values: ValueList;
  data: Datarow[];
  // Extensions:
  units?: { converted: boolean; conversions: [...] };
  timezone?: { converted: boolean; targetTimezone: string; ... };
  meta?: { autoConfigured: boolean; paths: string[]; ... };
}
```

**Assessment**: These extensions don't break compatibility; clients ignoring them will still work.

---

## 8. Discovery Endpoints (`/contexts`, `/paths`)

| Behavior | Official Spec | Your Implementation |
|----------|---------------|---------------------|
| Time params required | Yes (at least `from` or `duration`) | Optional |
| Without time params | Error | Returns all available |

**Your code** (`HistoryAPI.ts` line 384-416, 426-466):
```typescript
if (hasTimeParams) {
  // Time-range-aware: return only contexts with data
} else {
  // No time range specified: return all (legacy behavior)
}
```

**Deviation**: Official spec requires time parameters; you fall back to returning everything if not provided. This is more permissive.

---

## 9. Your Major Extensions (Not in Official Spec)

### 9.1 Spatial Filtering (`bbox`, `radius`, `positionPath`)

```
?bbox=-74.5,40.2,-73.8,40.9
?radius=40.646,-73.981,100
?positionPath=navigation.anchor.position
```

**Significance**: Major extension for geospatial queries. Allows filtering any path by vessel location.

### 9.2 Unit Conversion (`convertUnits`)

```
?convertUnits=true
```

Integration with `signalk-units-preference` plugin for server-side unit conversion. Returns values in user's preferred units (knots, fahrenheit, etc.).

### 9.3 Timezone Conversion (`convertTimesToLocal`, `timezone`)

```
?convertTimesToLocal=true
?timezone=America/New_York
```

Convert UTC timestamps to local or specified timezone. Supports all IANA timezone identifiers.

### 9.4 Query Source (`source`)

```
?source=auto|local|s3|hybrid
```

For S3 federated querying:
- `auto`: Automatically select based on retention period
- `local`: Only query local parquet files
- `s3`: Only query S3 (for archived data)
- `hybrid`: Query both sources with UNION

### 9.5 Aggregation Tier (`tier`)

```
?tier=raw|5s|60s|1h
```

Pre-aggregated data support for performance optimization. Auto-selects optimal tier based on resolution if not specified.

### 9.6 Auto-Discovery

Automatic path configuration when querying unconfigured paths. Enabled via plugin configuration:

```typescript
autoDiscovery: {
  enabled: true,
  requireLiveData: true,
  maxAutoConfiguredPaths: 100,
  includePatterns: ['navigation.*'],
  excludePatterns: ['propulsion.*']
}
```

### 9.7 SQLite Buffer Federated Queries

Recent data (within export interval) merged from SQLite WAL buffer with historical parquet data. Provides crash-safe near-real-time data access.

### 9.8 Per-Path Smoothing Syntax

```
?paths=navigation.speedOverGround:average:sma:5,environment.wind.speedApparent:max:ema:0.3
```

Apply SMA or EMA smoothing on a per-path basis with configurable parameters.

---

## 10. Type System Comparison

| Aspect | Official Spec | Your Implementation |
|--------|---------------|---------------------|
| DateTime handling | Temporal API | js-joda ZonedDateTime |
| Path type | `@signalk/server-api` Path | Same |
| Context type | `@signalk/server-api` Context | Same |
| AggregateMethod | TypeScript union | Brand type |

**Official types** (from `@signalk/server-api/history`):
```typescript
export type AggregateMethod =
  | 'average'
  | 'min'
  | 'max'
  | 'first'
  | 'last'
  | 'mid'
  | 'middle_index'
  | 'sma'
  | 'ema'
```

**Your types** (`HistoryAPI-types.ts`):
```typescript
export type AggregateMethod = Brand<string, 'aggregatemethod'>;
```

---

## Summary Table

| Feature | Compliance | Notes |
|---------|------------|-------|
| **Endpoints** | Partial | v1 vs v2 path difference |
| **Time patterns** | Full | All 5 patterns supported |
| **Duration format** | Full | ISO 8601 (`PT1H`), seconds (`3600`), shorthand (`1h`) |
| **Resolution units** | Full | Seconds (v0.7.0+), time expressions supported |
| **Aggregation methods** | Full | All supported |
| **SMA/EMA syntax** | Full | Official 3-part and extension 4-part both supported |
| **Response format** | Compatible | Extensions don't break clients |
| **Discovery endpoints** | More permissive | Time params optional |
| **Spatial filtering** | Extension | Not in spec |
| **Unit conversion** | Extension | Not in spec |
| **Timezone conversion** | Extension | Not in spec |
| **S3 federated queries** | Extension | Not in spec |
| **Auto-discovery** | Extension | Not in spec |

---

## Recommendations

### For Full Compliance

1. **Consider v2 support**: Add route aliases for `/signalk/v2/api/history/*` for broader compatibility.

2. ~~**Duration parsing**: Add ISO 8601 duration support (`PT1H`) alongside your shorthand.~~ **DONE (v0.7.0)**

3. ~~**Resolution units**: Document that you expect milliseconds, or add support for seconds with auto-detection.~~ **DONE (v0.7.0)** - Resolution now uses seconds.

4. ~~**SMA/EMA syntax**: Consider supporting both official 3-part (`path:sma:5`) and your 4-part syntax.~~ **DONE (v0.7.0)**

### Documentation

5. **Document extensions**: Your extensions are valuable; make sure they're clearly documented as non-standard but compatible additions.

6. **Client compatibility**: Note that standard SignalK clients may not utilize your extensions but will still function correctly.

---

## References

- [SignalK History API Documentation](https://demo.signalk.org/documentation/Developing/REST_APIs/History_API.html)
- [SignalK History TypeScript Types](https://demo.signalk.org/documentation/_signalk/server-api/history.html)
- [SignalK Server Repository - history.ts](https://github.com/SignalK/signalk-server/blob/master/packages/server-api/src/history.ts)
- [SignalK Server Repository - openApi.json](https://github.com/SignalK/signalk-server/blob/master/src/api/history/openApi.json)
- [REST API Specification 1.7.0](https://signalk.org/specification/1.7.0/doc/rest_api.html)
