# Proposed Extensions to the SignalK V2 History API

## Background

The V2 History API (`/signalk/v2/api/history`) currently accepts a defined set of query parameters for its `/values`, `/contexts`, and `/paths` endpoints. The signalk-parquet plugin has implemented several capabilities beyond this set that are available on V1 but cannot be used through V2 because the server does not recognize or pass them through.

This document describes what the V2 API would need to accept in order for history providers to offer these capabilities to clients.

---

## 1. Aggregation Tier Selection

**What it does:** Allows the client to request a specific pre-aggregated resolution of data rather than always querying raw records. The plugin maintains four tiers: raw samples, 5-second averages, 60-second averages, and 1-hour averages. Querying a pre-aggregated tier is dramatically faster and returns far less data for long time ranges.

**What V2 would need:** A `tier` parameter on `/values` accepting values like `raw`, `5s`, `60s`, `1h`, or `auto`. When set to `auto`, the provider selects the most appropriate tier based on the requested resolution and time range.

**Why it matters:** Without this, every V2 query hits raw data. A 30-day query at 1-hour resolution scans millions of raw rows instead of reading a few thousand pre-aggregated ones. 

**Is this essential?** No. It could (and is) automated but this would allow it to be explicit.

---

## 2. Spatial Filtering

**What it does:** Filters results to only include data points recorded while the vessel was within a geographic area. Two modes: bounding box (west, south, east, north coordinates) and radius (center lat/lon plus distance in meters).

**What V2 would need:** Two mutually exclusive parameters on `/values`:
- `bbox` — four comma-separated decimal values representing west, south, east, north bounds
- `radius` — three comma-separated decimal values representing latitude, longitude, and distance in meters

**Why it matters:** Enables queries like "show me engine temperature only while away from home port" or "what was my average speed within 5 miles of port." 

**Is this essential?** Yes. Impossible without.

---

## 3. Position Path for Spatial Correlation

**What it does:** Specifies which SignalK path provides the vessel's position when performing spatial filtering. Defaults to `navigation.position` but can be overridden for vessels with multiple GPS sources or non-standard configurations.

**What V2 would need:** A `positionPath` parameter on `/values`, accepting a SignalK path string.

**Why it matters:** Without this, spatial filtering is locked to a single hardcoded position source, which doesn't work for all vessel setups.

**Is this essential?** Yes. Without only a default could be used.

---

## 4. Timezone Conversion

**What it does:** Converts all timestamps in the response from UTC to a specified local timezone. The response includes metadata indicating the target timezone, UTC offset, and a human-readable description.

**What V2 would need:** Two parameters on `/values`:
- `convertTimesToLocal` — boolean flag to enable conversion
- `timezone` — an IANA timezone identifier (e.g., `America/New_York`, `Europe/Amsterdam`)

**Why it matters:** Client applications displaying data to users in their local time currently have to do this conversion themselves. Providing it server-side simplifies clients and ensures consistency, especially for applications that don't have access to a timezone library.

**Is this essential?** No. Would default to local time.

---

## 5. SMA and EMA as Post-Processing Smoothing (Extended Semantics)

**What V2 currently supports:** The V2 spec already recognizes `sma` and `ema` as aggregate methods in path expressions (e.g., `navigation.speedOverGround:sma`). In the spec, these are treated as aggregation methods — they replace the raw value with the smoothed value.

**What this plugin does differently:** The plugin supports two distinct uses of SMA/EMA:

- **Standard spec usage** (`path:sma:5` or `path:ema:0.3`) — SMA or EMA is the aggregation method. The parameter (5 or 0.3) sets the window period for SMA or the alpha decay factor for EMA. This matches the V2 spec intent.

- **Extended syntax** (`path:average:sma:5` or `path:min:ema:0.3`) — The aggregation method is applied first (average, min, etc.), and then SMA or EMA smoothing is applied as a second pass on top of the aggregated results. This lets a client say "give me the 60-second averages, then smooth them with a 10-period moving average" — which is useful for noisy sensor data where you want both downsampling and noise reduction.

**What V2 would need:** Recognition that the path expression syntax can have four segments (`path:aggregate:smoothing:param`) in addition to the current three (`path:aggregate:param`). The third segment, when it is `sma` or `ema`, indicates post-aggregation smoothing rather than an aggregate parameter. The fourth segment is the smoothing parameter.

**Why it matters:** The two-stage approach (aggregate then smooth) is fundamentally different from using SMA/EMA as the aggregation method. Aggregate-only SMA gives you a rolling average of raw values. Aggregate-then-smooth gives you a smoothed trend line of already-bucketed data. Both are useful for different visualization needs.

**Is this essential?** No but I think very important. EMA and SMA are fundamentally different than other aggregators and are used paired with an aggregation. 

---

## 6. Auto-Refresh Metadata

**What it does:** When enabled, the response includes metadata telling the client when to re-query for updated data: a recommended refresh interval in seconds and an ISO timestamp for the next suggested refresh.

**What V2 would need:** A `refresh` boolean parameter on `/values`. When true, the response includes refresh guidance alongside the data.

**Why it matters:** Clients building live dashboards with historical context need to know how often to poll. Without server guidance, they either poll too aggressively (wasting resources) or too infrequently (showing stale data).

**Is this essential?** No. But why not? Handy.

---

## 7. Data Source Routing

**What it does:** Controls where the provider looks for data. Options are local disk only, S3 archive only, hybrid (local + S3 merged), or auto (provider decides based on data availability and retention boundaries).

**What V2 would need:** A `source` parameter on `/values` accepting `local`, `s3`, `hybrid`, or `auto`.

**Why it matters:** Vessels with limited local storage archive older data to cloud storage. A client querying a multi-year range needs to pull from both local and archived sources. A client querying the last hour should skip the S3 lookup entirely for performance. Letting the client express this intent avoids unnecessary latency or missing data.

**Is this essential?** No. But again, why not?

---

## 8. UTC Interpretation Flag

**What it does:** Controls whether input time parameters (`from`, `to`) are interpreted as UTC. Defaults to true. When false, the provider interprets them in the vessel's local timezone.

**What V2 would need:** A `useUTC` boolean parameter on `/values`.

**Why it matters:** Some client applications work in local time and would need to convert to UTC before querying. This flag lets them pass local times directly.

**Is this essential?** No. But then everything has to be local and set up as the default.

---

## Summary of New Parameters Needed on V2 `/values`

| Parameter | Type | Purpose |
|---|---|---|
| `tier` | string | Aggregation tier selection |
| `bbox` | string (4 decimals) | Spatial bounding box filter |
| `radius` | string (3 decimals) | Spatial radius filter |
| `positionPath` | string | Position source for spatial queries |
| `convertTimesToLocal` | boolean | Enable timezone conversion |
| `timezone` | string | IANA timezone ID |
| `refresh` | boolean | Include refresh metadata |
| `source` | string | Data source routing |
| `useUTC` | boolean | Input time interpretation |

Path expression syntax extension: `path:aggregate:smoothing:param` (4-segment form for post-aggregation SMA/EMA)

## Response Extensions Needed

The V2 `ValuesResponse` currently defines `context`, `range`, `values`, and `data`. To carry the metadata from these features, the response would also need to support optional fields for timezone conversion metadata and refresh guidance.
