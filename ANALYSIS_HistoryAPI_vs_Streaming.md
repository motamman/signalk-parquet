# History API vs Streaming Analysis

## Overview
This document analyzes the data selection, filtering, calculations, and return formats for both the History API and Streaming systems in the SignalK Parquet plugin.

## Data Selection & Filtering

### History API - Data Selection & Filtering

#### 1. Path Selection & Processing (`HistoryAPI.ts:208-211`)
```typescript
const pathExpressions = ((req.query.paths as string) || '')
  .replace(/[^0-9a-z.,:_]/gi, '')  // Character filtering
  .split(',');                      // Multi-path support
const pathSpecs: PathSpec[] = pathExpressions.map(splitPathExpression);
```

**Path Format:** `path:aggregation` (e.g., `navigation.speedOverGround:average`)

#### 2. Path Specification Parsing (`HistoryAPI.ts:399-421`)
```typescript
function splitPathExpression(pathExpression: string): PathSpec {
  const parts = pathExpression.split(':');
  let aggregateMethod = (parts[1] || 'average') as AggregateMethod;
  
  // Auto-select for complex data types
  if (parts[0] === 'navigation.position' && !parts[1]) {
    aggregateMethod = 'first' as AggregateMethod;
  }
  
  return {
    path: parts[0] as Path,
    queryResultName: parts[0].replace(/\./g, '_'),
    aggregateMethod,
    aggregateFunction: (functionForAggregate[aggregateMethod] as string) || 'avg',
  };
}
```

#### 3. Time-Based Filtering (`HistoryAPI.ts:292-307`)
```sql
SELECT
  strftime(DATE_TRUNC('seconds',
    EPOCH_MS(CAST(FLOOR(EPOCH_MS(signalk_timestamp::TIMESTAMP) / ${timeResolutionMillis}) * ${timeResolutionMillis} AS BIGINT))
  ), '%Y-%m-%dT%H:%M:%SZ') as timestamp,
  ${getAggregateExpression(pathSpec.aggregateMethod, pathSpec.path)} as value,
  FIRST(value_json) as value_json
FROM '${filePath}'
WHERE
  signalk_timestamp >= '${fromIso}'
  AND signalk_timestamp < '${toIso}'
  AND (value IS NOT NULL OR value_json IS NOT NULL)
GROUP BY timestamp
ORDER BY timestamp
```

**Key Features:**
- **Time Bucketing:** Groups data into time intervals based on `timeResolutionMillis`
- **Null Filtering:** Excludes records where both `value` and `value_json` are NULL
- **Temporal Range:** Filters between `from` and `to` timestamps

## Aggregation Methods

### Available Aggregation Functions (`HistoryAPI.ts:433-452, 464-474`)
```typescript
const functionForAggregate = {
  average: 'avg',    // AVG() - arithmetic mean
  min: 'min',        // MIN() - minimum value
  max: 'max',        // MAX() - maximum value  
  first: 'first',    // FIRST() - first value in time bucket
  last: 'last',      // LAST() - last value in time bucket
  mid: 'median',     // MEDIAN() - middle value
  middle_index: 'nth_value'  // NTH_VALUE() - middle by index
};
```

### Value Expression Logic (`HistoryAPI.ts:454-462`)
```typescript
function getValueExpression(pathName: string): string {
  // For position data, use value_json since the value is an object
  if (pathName === 'navigation.position') {
    return 'value_json';
  }
  
  // For numeric data, try to cast to DOUBLE, fallback to original
  return 'TRY_CAST(value AS DOUBLE)';
}
```

### Aggregation Expression Building (`HistoryAPI.ts:464-474`)
```typescript
function getAggregateExpression(method: AggregateMethod, pathName: string): string {
  const valueExpr = getValueExpression(pathName);
  
  if (method === 'middle_index') {
    return `FIRST(${valueExpr})`;  // Simplified fallback
  }
  
  return `${getAggregateFunction(method)}(${valueExpr})`;
  // Examples:
  // AVG(TRY_CAST(value AS DOUBLE))
  // FIRST(value_json)
  // MAX(TRY_CAST(value AS DOUBLE))
}
```

## Streaming - Data Selection & Processing

### 1. Stream Configuration (`historical-streaming.ts:335-353`)
```typescript
public createStream(streamConfig: any) {
  const stream = {
    id: streamId,
    name: streamConfig.name,
    path: streamConfig.path,              // Single path only
    rate: streamConfig.rate || 5000,       // Data fetch interval (5s default)
    resolution: streamConfig.resolution || 30000,  // 30s time resolution
    timeRange: streamConfig.timeRange || '1h',     // 1-hour sliding window
    // ...
  };
}
```

**Key Differences from History API:**
- **Single Path:** Only one path per stream (no multi-path queries)
- **Fixed Windows:** Predefined time ranges (`1h`, `30m`, etc.)
- **Continuous:** Ongoing data delivery vs. one-shot queries

### 2. Time Range Parsing (`historical-streaming.ts:316-334`)
```typescript
private parseTimeRange(timeRange: string): number {
  const match = timeRange.match(/^(\d+)([smhd])$/);
  if (!match) {
    return 60 * 60 * 1000; // 1 hour default
  }
  
  const value = parseInt(match[1]);
  const unit = match[2];
  
  switch (unit) {
    case 's': return value * 1000;                    // seconds
    case 'm': return value * 60 * 1000;               // minutes  
    case 'h': return value * 60 * 60 * 1000;          // hours
    case 'd': return value * 24 * 60 * 60 * 1000;     // days
    default: return 60 * 60 * 1000;
  }
}
```

### 3. Streaming Data Retrieval (`historical-streaming.ts:444-499`)
```typescript
private async getHistoricalDataPoint(path: string, resolution: number): Promise<any> {
  // Get recent historical data from the last minute
  const to = ZonedDateTime.now(ZoneOffset.UTC);
  const from = to.minusMinutes(1);  // Fixed 1-minute window
  
  // Create mock request for HistoryAPI
  const mockReq = {
    query: {
      paths: path,                    // Single path
      resolution: resolution.toString()
    }
  };
  
  // Call History API internally
  this.historyAPI.getValues(context, from, to, false, debug, mockReq, mockRes);
}
```

**Processing Logic:**
1. **Fixed 1-minute sliding window** for live data
2. **Single path queries** (no batching)
3. **Latest point extraction:** Takes most recent data point from results
4. **Sample data fallback:** Generates synthetic data if no real data available

## Data Return Formats

### History API Response Format
```typescript
// HistoryAPI-types.ts:14-23
interface DataResult {
  context: Context;                    // "vessels.urn:mrn:..."
  range: {
    from: Timestamp;                   // "2025-08-22T10:00:00.000Z"
    to: Timestamp;                     // "2025-08-22T11:00:00.000Z"  
  };
  values: {                           // Path metadata
    path: Path;                       // "navigation.speedOverGround"
    method: AggregateMethod;          // "average"
  }[];
  data: [Timestamp, ...any[]][];      // Time-series array
}
```

**Example History API Response:**
```typescript
{
  context: "vessels.urn:mrn:imo:mmsi:368396230",
  range: {
    from: "2025-08-22T10:00:00.000Z",
    to: "2025-08-22T11:00:00.000Z"
  },
  values: [
    { path: "navigation.speedOverGround", method: "average" },
    { path: "environment.wind.speedApparent", method: "max" }
  ],
  data: [
    ["2025-08-22T10:00:00.000Z", 5.2, 12.5],    // [timestamp, sog_avg, wind_max]
    ["2025-08-22T10:01:00.000Z", 5.8, 11.2],
    ["2025-08-22T10:02:00.000Z", 6.1, 13.8]
  ]
}
```

### Streaming Response Format
```typescript
// historical-streaming.ts:431-437
const message = JSON.stringify({
  type: 'streamData',
  streamId: streamId,               // "stream_1692712345_abc123"
  timestamp: new Date().toISOString(),  // Current delivery time
  data: {                          // Single data point
    path: path,                    // "navigation.speedOverGround"  
    timestamp: latestPoint[0],     // Data timestamp
    value: latestPoint[1]          // Actual value
  }
});
```

**Example Streaming Message:**
```typescript
{
  type: "streamData",
  streamId: "stream_1692712345_abc123", 
  timestamp: "2025-08-22T15:35:07.000Z",  // Delivery time
  data: {
    path: "navigation.speedOverGround",
    timestamp: "2025-08-22T15:35:06.000Z", // Data time  
    value: 5.8
  }
}
```

## Summary: Key Functional Differences

| **Aspect** | **History API** | **Streaming** |
|------------|-----------------|---------------|
| **Query Model** | Pull-based, one-shot queries | Push-based, continuous delivery |
| **Path Support** | Multi-path: `path1,path2:avg,path3:max` | Single-path per stream |
| **Time Ranges** | User-defined: any `from`/`to` or `start`+`duration` | Fixed windows: `1h`, `30m`, `1d` |
| **Resolution** | User-controlled: any millisecond value | Predefined: typically 30s |
| **Aggregation** | 7 methods: `avg,min,max,first,last,mid,middle_index` | Uses History API's aggregation internally |
| **Data Volume** | Batch results: hundreds/thousands of points | Single points: one value per message |
| **Calculations** | **SQL-based:** `AVG(TRY_CAST(value AS DOUBLE))` | **Pass-through:** uses History API results |
| **Format** | **Tabular:** `[[timestamp, val1, val2], ...]` | **Message:** `{type, streamId, data}` |
| **Refresh** | Optional auto-refresh headers | Continuous WebSocket delivery |

## Data Processing Pipeline Comparison

### History API Pipeline:
1. **Parse** multi-path expressions (`path1:avg,path2:max`)
2. **Generate** time-bucketed SQL with aggregations
3. **Execute** DuckDB queries in parallel for each path  
4. **Merge** results into synchronized time-series matrix
5. **Return** complete dataset as JSON

### Streaming Pipeline:  
1. **Create** individual stream per path
2. **Query** History API with 1-minute sliding window
3. **Extract** latest data point from History API response
4. **Broadcast** single value via WebSocket
5. **Repeat** at configured interval (default 5s)

## Calculation Differences

**History API performs calculations:**
- **Time bucketing:** Groups raw data into time intervals
- **Aggregation:** Applies statistical functions within each bucket
- **Multi-path synchronization:** Aligns different paths to same timestamps

**Streaming delegates calculations:**
- Uses History API internally (inherits all its calculations)  
- Adds **time window management** (sliding 1-minute windows)
- Adds **sample data generation** for fallback scenarios
- **No additional mathematical processing**

The streaming system is essentially a **real-time wrapper** around the History API's batch processing capabilities.

## Critical Technical Issues Identified

### History API Issues:
1. **SQL Injection Vulnerability** - Direct string interpolation in queries
2. **Path Sanitization Issues** - Regex allows path traversal patterns  
3. **Unconstrained Time Resolution** - No bounds checking on resolution values
4. **Date Range Validation Missing** - No limits on query scope
5. **Resource Exhaustion Vulnerabilities** - No query timeouts or memory limits
6. **Error Information Disclosure** - Stack traces exposed to clients

### Streaming Issues:
- **Inherits History API vulnerabilities** through internal calls
- Better **defense in depth** through constrained inputs
- **Lower attack surface** due to fixed parameters

## Variables and Constraints Analysis

### History API Variables (User-Controlled):
- `paths` - Multi-path expressions with aggregation methods
- `from`/`to` or `start`/`duration` - Time range parameters
- `resolution` - Time bucketing resolution (milliseconds)
- `context` - Vessel/entity context
- `useUTC` - Timezone handling flag
- `refresh` - Auto-refresh enable flag

### Streaming Variables (System-Controlled):
- `rate` - Data fetch interval (default 5000ms)
- `resolution` - Time resolution (default 30000ms)
- `timeRange` - Historical window (default '1h')
- Single path per stream (no multi-path)
- Fixed 1-minute sliding window for live data
- Hard-coded contexts and aggregation methods

### Key Constraint Differences:
| **Parameter** | **History API** | **Streaming** |
|---------------|-----------------|---------------|
| Time Range | Unlimited | Fixed patterns (1h, 30m, 1d) |
| Resolution | Any value | Predefined (30s default) |
| Paths | Multiple paths | Single path per stream |
| Rate Limiting | None | 5s intervals |
| Memory Usage | Unbounded | Bounded by fixed windows |
| Query Complexity | Unlimited | Fixed patterns |

## Route Registration Analysis

### History API Routes (index.ts:166-174):
```typescript
registerHistoryApiRoute(
  app as unknown as Router,    // Type casting issue
  app.selfId,                  // Vessel identifier
  state.currentConfig.outputDirectory,  // Data directory
  app.debug,                   // Debug function
  app                          // Full app context
);
```

### Registered Endpoints:
1. `/signalk/v1/history/values` - Main history query endpoint
2. `/signalk/v1/history/contexts` - Available contexts (vessels)
3. `/signalk/v1/history/paths` - Available data paths
4. `/api/history/values` - Plugin-style alias
5. `/api/history/contexts` - Plugin-style alias  
6. `/api/history/paths` - Plugin-style alias (hardcoded response)

### Streaming Routes:
- No direct HTTP endpoints
- WebSocket-based communication through SignalK subscription system
- Internal management through plugin API routes

---

**Analysis Date:** 2025-08-22  
**Files Analyzed:** HistoryAPI.ts, historical-streaming.ts, HistoryAPI-types.ts, index.ts  
**Todo Status:** Completed analysis of data selection, filtering, calculations, and return formats