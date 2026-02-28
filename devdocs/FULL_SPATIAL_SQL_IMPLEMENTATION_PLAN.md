# Spatial SQL Implementation Plan

> **Status**: Planning
> **Created**: 2026-02-19
> **Goal**: Replace JavaScript-based geospatial operations with DuckDB spatial SQL for better performance and richer query capabilities

---

## Current State

### How Position Data is Stored
- Parquet files with separate columns: `value_latitude`, `value_longitude`
- No geometry types or spatial indexes

### How Geospatial Operations Work Today
| Operation | Location | Method |
|-----------|----------|--------|
| Threshold bbox checks | `src/commands.ts:1122-1163` | JavaScript `isPointInBoundingBox()` |
| Distance calculations | `src/utils/geo-calculator.ts` | JavaScript Haversine formula |
| History API bbox param | `src/HistoryAPI.ts:524` | Parsed but **not used** |

### Pain Points
1. Bounding box filtering happens in JS after fetching all data
2. No spatial indexing = full table scans for geo queries
3. Can't do radius queries efficiently on historical data
4. `bbox` parameter in History API is dead code

---

## Goals

1. **Wire up bbox filtering** in History API using spatial SQL
2. **Add radius/distance queries** for historical data
3. **Improve threshold monitoring** with optional SQL-based checks
4. **Enable track analysis** (simplification, length, bounds)

---

## Phase 1: Enable Spatial Queries (No Schema Change)

Create geometry on-the-fly from existing lat/lon columns.

### 1.1 Verify Spatial Extension Loading

Location: `src/utils/duckdb-pool.ts`

```typescript
// Ensure spatial extension is loaded on pool initialization
await connection.run("INSTALL spatial; LOAD spatial;");
```

### 1.2 Add Spatial Query Helper

Create: `src/utils/spatial-queries.ts`

```typescript
import { BoundingBox } from '../types';

/**
 * Build SQL WHERE clause for bounding box filter
 */
export function buildBboxWhereClause(
  bbox: BoundingBox,
  latColumn = 'value_latitude',
  lonColumn = 'value_longitude'
): string {
  // Handle 180° meridian crossing
  if (bbox.west > bbox.east) {
    return `
      ST_Within(
        ST_Point(${lonColumn}, ${latColumn}),
        ST_Union(
          ST_MakeEnvelope(${bbox.west}, ${bbox.south}, 180, ${bbox.north}),
          ST_MakeEnvelope(-180, ${bbox.south}, ${bbox.east}, ${bbox.north})
        )
      )
    `;
  }

  return `
    ST_Within(
      ST_Point(${lonColumn}, ${latColumn}),
      ST_MakeEnvelope(${bbox.west}, ${bbox.south}, ${bbox.east}, ${bbox.north})
    )
  `;
}

/**
 * Build SQL WHERE clause for radius filter (meters)
 */
export function buildRadiusWhereClause(
  centerLat: number,
  centerLon: number,
  radiusMeters: number,
  latColumn = 'value_latitude',
  lonColumn = 'value_longitude'
): string {
  return `
    ST_DWithin_Spheroid(
      ST_Point(${lonColumn}, ${latColumn}),
      ST_Point(${centerLon}, ${centerLat}),
      ${radiusMeters}
    )
  `;
}

/**
 * Parse bbox query parameter: "west,south,east,north"
 */
export function parseBboxParam(bboxStr: string): BoundingBox | null {
  const parts = bboxStr.split(',').map(Number);
  if (parts.length !== 4 || parts.some(isNaN)) {
    return null;
  }
  return {
    west: parts[0],
    south: parts[1],
    east: parts[2],
    north: parts[3],
  };
}
```

### 1.3 Wire Up bbox in HistoryAPI

Location: `src/HistoryAPI.ts`

Modify `getNumericValues()` to accept and apply bbox filter:

```typescript
// In getNumericValues(), add bbox parameter
async getNumericValues(
  context: Context,
  from: ZonedDateTime,
  to: ZonedDateTime,
  timeResolutionMillis: number,
  pathSpecs: PathSpec[],
  includeMovingAverages: boolean,
  bbox: BoundingBox | null,  // NEW
  debug: (k: string) => void
): Promise<DataResult> {
  // ...

  // For position paths, add bbox filter
  if (pathSpec.path === 'navigation.position' && bbox) {
    const bboxClause = buildBboxWhereClause(bbox);
    whereConditions.push(bboxClause);
  }
}
```

### 1.4 Add radius Query Parameter

Add new query param: `?radius=lat,lon,meters`

```typescript
// In HistoryAPI-types.ts
radius?: string;  // "lat,lon,meters" format

// In getRequestParams()
const radius = query.radius ? parseRadiusParam(query.radius) : null;
```

---

## Phase 2: Track Analysis Functions

### 2.1 Track Simplification

For downsampling dense GPS tracks (useful for visualization):

```sql
SELECT
  ST_SimplifyPreserveTopology(
    ST_MakeLine(
      LIST(ST_Point(value_longitude, value_latitude) ORDER BY signalk_timestamp)
    ),
    0.0001  -- tolerance in degrees
  ) as simplified_track
FROM read_parquet('...')
WHERE signalk_timestamp BETWEEN '...' AND '...'
```

### 2.2 Track Statistics Endpoint

New endpoint: `GET /api/history/track-stats`

Returns:
```json
{
  "totalDistance": 12500.5,  // meters
  "bounds": { "north": 40.8, "south": 40.5, "east": -73.9, "west": -74.1 },
  "pointCount": 3420,
  "startTime": "...",
  "endTime": "..."
}
```

SQL:
```sql
SELECT
  ST_Length_Spheroid(ST_MakeLine(LIST(ST_Point(value_longitude, value_latitude) ORDER BY signalk_timestamp))) as total_distance,
  MIN(value_latitude) as south,
  MAX(value_latitude) as north,
  MIN(value_longitude) as west,
  MAX(value_longitude) as east,
  COUNT(*) as point_count,
  MIN(signalk_timestamp) as start_time,
  MAX(signalk_timestamp) as end_time
FROM read_parquet('...')
WHERE signalk_timestamp BETWEEN '...' AND '...'
```

---

## Phase 3: Optimize Threshold Monitoring (Optional)

### Current: JavaScript-based
```typescript
// commands.ts - runs for every position update
const inBox = isPointInBoundingBox(lat, lon, boundingBox);
```

### Alternative: SQL-based batch check
For thresholds that don't need instant response, batch check recent positions:

```sql
SELECT
  signalk_timestamp,
  ST_Within(
    ST_Point(value_longitude, value_latitude),
    ST_MakeEnvelope(...)
  ) as in_zone
FROM read_parquet('...')
WHERE signalk_timestamp > NOW() - INTERVAL '1 minute'
ORDER BY signalk_timestamp DESC
LIMIT 1
```

**Note**: Keep JavaScript approach for real-time thresholds. SQL approach better for periodic checks or historical analysis of when vessel entered/exited zones.

---

## Phase 4: Future Enhancements (Optional)

### 4.1 Store Geometry in Parquet

If query performance becomes critical, add a geometry column:

```typescript
// In parquet-writer.ts
{
  signalk_timestamp: timestamp,
  value_latitude: lat,
  value_longitude: lon,
  position_geom: `POINT(${lon} ${lat})`,  // WKT format
}
```

### 4.2 Spatial Indexes

DuckDB doesn't persist indexes for parquet, but you could:
- Create a DuckDB database file for frequently-queried data
- Add spatial index: `CREATE INDEX idx_position ON table USING RTREE (position_geom)`

### 4.3 Geofence Library

Store named zones and check against them:

```sql
-- zones table
CREATE TABLE geofences (
  name VARCHAR,
  geom GEOMETRY
);

-- Check if position is in any zone
SELECT z.name
FROM geofences z
WHERE ST_Within(ST_Point(?, ?), z.geom)
```

---

## Implementation Checklist

### Phase 1 (Core)
- [ ] Verify spatial extension loads in DuckDB pool
- [ ] Create `src/utils/spatial-queries.ts`
- [ ] Add `parseBboxParam()` function
- [ ] Wire bbox filtering into `getNumericValues()`
- [ ] Add radius query parameter support
- [ ] Update API documentation
- [ ] Add tests for spatial queries

### Phase 2 (Track Analysis)
- [ ] Add track simplification endpoint
- [ ] Add track statistics endpoint
- [ ] Test with large datasets

### Phase 3 (Thresholds)
- [ ] Evaluate if SQL-based threshold checks add value
- [ ] Implement batch zone checking if needed

---

## Testing

### Test Queries to Validate

```sql
-- Test 1: Basic bbox filter
SELECT COUNT(*) FROM read_parquet('navigation/position/*.parquet')
WHERE ST_Within(
  ST_Point(value_longitude, value_latitude),
  ST_MakeEnvelope(-74.1, 40.5, -73.9, 40.8)
);

-- Test 2: Radius query (1km from point)
SELECT * FROM read_parquet('navigation/position/*.parquet')
WHERE ST_DWithin_Spheroid(
  ST_Point(value_longitude, value_latitude),
  ST_Point(-74.0, 40.7),
  1000
)
LIMIT 10;

-- Test 3: Track line creation
SELECT ST_AsGeoJSON(
  ST_MakeLine(LIST(ST_Point(value_longitude, value_latitude) ORDER BY signalk_timestamp))
) FROM read_parquet('navigation/position/*.parquet')
WHERE signalk_timestamp > '2025-01-01';
```

---

## References

- [DuckDB Spatial Extension](https://duckdb.org/docs/extensions/spatial.html)
- [PostGIS ST_Within](https://postgis.net/docs/ST_Within.html) (similar API)
- [GeoJSON Specification](https://geojson.org/)
