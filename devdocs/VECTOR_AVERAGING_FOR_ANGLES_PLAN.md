# Vector Averaging for Angular Data

> **Status**: Planning
> **Created**: 2026-03-03
> **Request**: Teppo Kurki (SignalK Slack)
> **Goal**: Correctly average angular values (heading, COG, wind direction) during aggregation

---

## Problem Statement

When aggregating angular data, simple arithmetic averaging gives incorrect results:

```
AVG(10°, 350°) = 180°  ← WRONG!
Correct answer = 0° (or 360°)
```

This affects all angular SignalK paths:
- `navigation.headingTrue`
- `navigation.headingMagnetic`
- `navigation.courseOverGroundTrue`
- `navigation.courseOverGroundMagnetic`
- `environment.wind.directionTrue`
- `environment.wind.directionMagnetic`
- `environment.wind.angleApparent`
- `environment.wind.angleTrueWater`
- `environment.current.setTrue`
- etc.

---

## Solution: Vector Decomposition

### The Math

1. **Decompose** each angle into unit vector components:
   ```
   x = cos(θ)
   y = sin(θ)
   ```

2. **Average** the x and y components separately:
   ```
   avg_x = AVG(cos(θ₁), cos(θ₂), ...)
   avg_y = AVG(sin(θ₁), sin(θ₂), ...)
   ```

3. **Reconstruct** the angle:
   ```
   avg_θ = atan2(avg_y, avg_x)
   ```

### Example

```
Angles: 10°, 350°

Step 1 - Decompose:
  10°  → (cos(10°), sin(10°))  = (0.985, 0.174)
  350° → (cos(350°), sin(350°)) = (0.985, -0.174)

Step 2 - Average:
  avg_x = (0.985 + 0.985) / 2 = 0.985
  avg_y = (0.174 + -0.174) / 2 = 0

Step 3 - Reconstruct:
  atan2(0, 0.985) = 0°  ✓ CORRECT!
```

### Wind Vector Averaging (Weighted by Speed)

For wind, the direction should be weighted by speed:

```sql
-- Weighted vector average
avg_direction = atan2(
  SUM(speed * sin(direction)) / SUM(speed),
  SUM(speed * cos(direction)) / SUM(speed)
)

avg_speed = AVG(speed)
```

---

## Implementation Plan

### Phase 1: Identify Angular Paths

Create a registry of paths that contain angular data:

**File**: `src/utils/angular-paths.ts`

```typescript
/**
 * SignalK paths that contain angular values (in radians)
 * These require vector averaging, not arithmetic averaging
 */
export const ANGULAR_PATHS: Set<string> = new Set([
  // Navigation angles
  'navigation.headingTrue',
  'navigation.headingMagnetic',
  'navigation.courseOverGroundTrue',
  'navigation.courseOverGroundMagnetic',
  'navigation.courseRhumbline',
  'navigation.courseGreatCircle',

  // Wind angles
  'environment.wind.directionTrue',
  'environment.wind.directionMagnetic',
  'environment.wind.angleApparent',
  'environment.wind.angleTrueGround',
  'environment.wind.angleTrueWater',

  // Current
  'environment.current.setTrue',
  'environment.current.setMagnetic',

  // Attitude
  'navigation.attitude.roll',
  'navigation.attitude.pitch',
  'navigation.attitude.yaw',

  // Rudder
  'steering.rudderAngle',
  'steering.rudderAngleTarget',
]);

/**
 * Paths where angle should be weighted by an associated magnitude
 * Key: angle path, Value: magnitude path
 */
export const WEIGHTED_ANGULAR_PATHS: Map<string, string> = new Map([
  ['environment.wind.directionTrue', 'environment.wind.speedTrue'],
  ['environment.wind.directionMagnetic', 'environment.wind.speedOverGround'],
  ['environment.wind.angleApparent', 'environment.wind.speedApparent'],
  ['environment.current.setTrue', 'environment.current.drift'],
]);

export function isAngularPath(path: string): boolean {
  return ANGULAR_PATHS.has(path);
}

export function getWeightPath(anglePath: string): string | undefined {
  return WEIGHTED_ANGULAR_PATHS.get(anglePath);
}
```

### Phase 2: Add Vector Columns During Export

When exporting angular data to Parquet, also store the x/y components:

**File**: `src/services/parquet-export-service.ts`

```typescript
import { isAngularPath } from '../utils/angular-paths';

function prepareRecord(record: DataRecord): DataRecord {
  const prepared = { ...record };

  // For angular paths, add sin/cos components for vector averaging
  if (isAngularPath(record.path) && typeof record.value === 'number') {
    prepared.value_sin = Math.sin(record.value);
    prepared.value_cos = Math.cos(record.value);
  }

  return prepared;
}
```

**Schema change** - add optional columns:
```typescript
const angularSchema = {
  value_sin: { type: 'DOUBLE', optional: true },
  value_cos: { type: 'DOUBLE', optional: true },
};
```

### Phase 3: Vector Aggregation in DuckDB

**File**: `src/services/aggregation-service.ts`

```typescript
import { isAngularPath, getWeightPath } from '../utils/angular-paths';

private buildAggregationQuery(
  files: string[],
  signalkPath: string,
  intervalSeconds: number,
  isSourceRaw: boolean
): string {
  const fileListStr = files.map(f => `'${f}'`).join(', ');

  if (isAngularPath(signalkPath)) {
    return this.buildAngularAggregationQuery(
      fileListStr,
      signalkPath,
      intervalSeconds,
      isSourceRaw
    );
  }

  // Standard scalar aggregation
  return this.buildScalarAggregationQuery(
    fileListStr,
    intervalSeconds,
    isSourceRaw
  );
}

private buildAngularAggregationQuery(
  fileListStr: string,
  signalkPath: string,
  intervalSeconds: number,
  isSourceRaw: boolean
): string {
  const weightPath = getWeightPath(signalkPath);

  if (isSourceRaw) {
    if (weightPath) {
      // Weighted vector average (for wind direction weighted by speed)
      return `
        COPY (
          SELECT
            time_bucket(INTERVAL '${intervalSeconds} seconds', received_timestamp::TIMESTAMP) as bucket_time,
            context,
            path,
            -- Vector average weighted by magnitude
            ATAN2(
              SUM(CAST(value AS DOUBLE) * SIN(CAST(value AS DOUBLE))) / NULLIF(SUM(CAST(value AS DOUBLE)), 0),
              SUM(CAST(value AS DOUBLE) * COS(CAST(value AS DOUBLE))) / NULLIF(SUM(CAST(value AS DOUBLE)), 0)
            ) as value_avg,
            -- For angles, min/max don't make sense, but we keep structure
            NULL as value_min,
            NULL as value_max,
            COUNT(*) as sample_count,
            -- Store components for further aggregation
            AVG(SIN(CAST(value AS DOUBLE))) as value_sin_avg,
            AVG(COS(CAST(value AS DOUBLE))) as value_cos_avg,
            MIN(received_timestamp) as first_timestamp,
            MAX(received_timestamp) as last_timestamp
          FROM read_parquet([${fileListStr}], union_by_name=true)
          WHERE value IS NOT NULL
          GROUP BY bucket_time, context, path
          ORDER BY bucket_time
        ) TO '...' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
      `;
    } else {
      // Unweighted vector average
      return `
        COPY (
          SELECT
            time_bucket(INTERVAL '${intervalSeconds} seconds', received_timestamp::TIMESTAMP) as bucket_time,
            context,
            path,
            -- Vector average: atan2(avg(sin), avg(cos))
            ATAN2(
              AVG(SIN(CAST(value AS DOUBLE))),
              AVG(COS(CAST(value AS DOUBLE)))
            ) as value_avg,
            NULL as value_min,
            NULL as value_max,
            COUNT(*) as sample_count,
            AVG(SIN(CAST(value AS DOUBLE))) as value_sin_avg,
            AVG(COS(CAST(value AS DOUBLE))) as value_cos_avg,
            MIN(received_timestamp) as first_timestamp,
            MAX(received_timestamp) as last_timestamp
          FROM read_parquet([${fileListStr}], union_by_name=true)
          WHERE value IS NOT NULL
          GROUP BY bucket_time, context, path
          ORDER BY bucket_time
        ) TO '...' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
      `;
    }
  } else {
    // Re-aggregating from already-aggregated data
    // Use stored sin/cos averages weighted by sample count
    return `
      COPY (
        SELECT
          time_bucket(INTERVAL '${intervalSeconds} seconds', bucket_time::TIMESTAMP) as bucket_time,
          context,
          path,
          -- Reconstruct angle from weighted sin/cos averages
          ATAN2(
            SUM(value_sin_avg * sample_count) / SUM(sample_count),
            SUM(value_cos_avg * sample_count) / SUM(sample_count)
          ) as value_avg,
          NULL as value_min,
          NULL as value_max,
          SUM(sample_count)::BIGINT as sample_count,
          SUM(value_sin_avg * sample_count) / SUM(sample_count) as value_sin_avg,
          SUM(value_cos_avg * sample_count) / SUM(sample_count) as value_cos_avg,
          MIN(first_timestamp) as first_timestamp,
          MAX(last_timestamp) as last_timestamp
        FROM read_parquet([${fileListStr}], union_by_name=true)
        GROUP BY time_bucket(INTERVAL '${intervalSeconds} seconds', bucket_time::TIMESTAMP), context, path
        ORDER BY 1
      ) TO '...' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
    `;
  }
}
```

### Phase 4: History API Vector Averaging

**File**: `src/history-provider.ts`

Update the `queryPath` method to use vector averaging for angular paths:

```typescript
private async queryPath(...): Promise<Array<[Timestamp, unknown]>> {
  const isAngular = isAngularPath(pathSpec.path);

  if (isAngular) {
    // Use vector averaging
    const aggFunc = 'ATAN2(AVG(SIN(TRY_CAST(value AS DOUBLE))), AVG(COS(TRY_CAST(value AS DOUBLE))))';

    const query = `
      SELECT
        strftime(...) as timestamp,
        ${aggFunc} as value
      FROM read_parquet('${filePath}', ...)
      WHERE ...
      GROUP BY timestamp
      ORDER BY timestamp
    `;
  } else {
    // Standard averaging
    const aggFunc = this.getAggregateFunction(pathSpec.aggregate);
    // ... existing code
  }
}
```

---

## DuckDB SQL Reference

### Basic Vector Average
```sql
SELECT
  ATAN2(AVG(SIN(angle)), AVG(COS(angle))) as avg_angle
FROM data;
```

### Weighted Vector Average (by speed)
```sql
SELECT
  ATAN2(
    SUM(speed * SIN(direction)) / NULLIF(SUM(speed), 0),
    SUM(speed * COS(direction)) / NULLIF(SUM(speed), 0)
  ) as avg_direction,
  AVG(speed) as avg_speed
FROM data;
```

### Handle NULL and Zero
```sql
-- Use NULLIF to avoid division by zero
-- Use COALESCE to handle NULL results
SELECT
  COALESCE(
    ATAN2(
      AVG(SIN(CAST(value AS DOUBLE))),
      AVG(COS(CAST(value AS DOUBLE)))
    ),
    0
  ) as avg_angle
FROM data
WHERE value IS NOT NULL;
```

---

## Testing

### Test Cases

1. **Angles near 0/360 boundary**
   - Input: [350°, 10°] → Expected: 0°
   - Input: [340°, 20°] → Expected: 0°

2. **Opposite angles**
   - Input: [0°, 180°] → Expected: undefined (should return NULL or handle specially)

3. **Same angles**
   - Input: [45°, 45°, 45°] → Expected: 45°

4. **Wind with varying speed**
   - Input: [(10°, 5kts), (350°, 15kts)] → Expected: ~355° (weighted toward stronger wind)

### Test Script

```javascript
// tests/test-vector-averaging.js
const testCases = [
  { angles: [10, 350], expected: 0 },
  { angles: [0, 90], expected: 45 },
  { angles: [0, 180], expected: null }, // Undefined
  { angles: [270, 90], expected: 0 },
];

// Convert degrees to radians for testing
function degToRad(deg) { return deg * Math.PI / 180; }
function radToDeg(rad) { return rad * 180 / Math.PI; }

function vectorAverage(anglesInDegrees) {
  const radians = anglesInDegrees.map(degToRad);
  const avgSin = radians.reduce((sum, a) => sum + Math.sin(a), 0) / radians.length;
  const avgCos = radians.reduce((sum, a) => sum + Math.cos(a), 0) / radians.length;

  // Check for undefined case (opposite angles cancel out)
  if (Math.abs(avgSin) < 0.0001 && Math.abs(avgCos) < 0.0001) {
    return null;
  }

  let result = Math.atan2(avgSin, avgCos);
  if (result < 0) result += 2 * Math.PI;
  return radToDeg(result);
}
```

---

## Migration Considerations

### Existing Aggregated Data

Existing aggregated Parquet files don't have `value_sin_avg` and `value_cos_avg` columns. Options:

1. **Regenerate** - Re-run aggregation from raw data
2. **Decompose on read** - Calculate sin/cos from `value_avg` when re-aggregating
3. **Ignore** - Old aggregated data keeps incorrect averages

Recommendation: Option 2 for backwards compatibility.

---

## Files to Modify

| File | Change |
|------|--------|
| `src/utils/angular-paths.ts` | New - path registry |
| `src/services/aggregation-service.ts` | Vector aggregation queries |
| `src/services/parquet-export-service.ts` | Add sin/cos columns |
| `src/history-provider.ts` | Vector averaging in queries |
| `src/HistoryAPI.ts` | Vector averaging in queries |
| `tests/test-vector-averaging.js` | New - test cases |

---

## Summary

This plan implements correct vector averaging for angular SignalK data:

1. **Identify** angular paths via registry
2. **Store** sin/cos components alongside raw values
3. **Aggregate** using `ATAN2(AVG(SIN), AVG(COS))`
4. **Weight** wind direction by speed where applicable
5. **Test** edge cases (0/360 boundary, opposite angles)

The result: `AVG(10°, 350°) = 0°` instead of `180°`.
