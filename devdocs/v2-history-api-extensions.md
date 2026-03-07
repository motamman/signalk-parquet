# Proposed Extensions to the SignalK V2 History API

## Background

The V2 History API (`/signalk/v2/api/history`) currently accepts a defined set of query parameters for its `/values`, `/contexts`, and `/paths` endpoints. The signalk-parquet plugin has implemented several capabilities beyond this set that are available on V1 but cannot be used through V2 because the server does not recognize or pass them through.

This document describes what the V2 API would need to accept in order for history providers to offer these capabilities to clients.

---

## 1. Spatial Filtering

**What it does:** Filters results to only include data points recorded while the vessel was within a geographic area. Two modes: bounding box (west, south, east, north coordinates) and radius (center lat/lon plus distance in meters).

**What V2 would need:** Two mutually exclusive parameters on `/values`:
- `bbox` — four comma-separated decimal values representing west, south, east, north bounds
- `radius` — three comma-separated decimal values representing latitude, longitude, and distance in meters

**Why it matters:** Enables queries like "show me engine temperature only while away from home port" or "what was my average speed within 5 miles of port."

**What V2 would also need:** A `positionPath` parameter on `/values`, accepting a SignalK path string. Defaults to `navigation.position`. This specifies which SignalK path provides the vessel's position when performing the spatial correlation.

Spatial filtering correlates the queried data path with a position path to determine *where the vessel was* when each value was recorded. That correlation is the entire mechanism — without a position path, there is no spatial query.

Hardcoding `navigation.position` works for the simple case, but the moment you think about what spatial filtering actually enables, the need for flexibility becomes clear:

- **Route replay:** A vessel transits a narrow channel approach every week. The owner wants engine load, RPM, and fuel rate *only while in that corridor*, across six months of data. The query uses `bbox` to define the corridor and `positionPath` to say which position source was authoritative during those transits. If the vessel switched from a chartplotter GPS to a standalone unit mid-season, the recorded position path may differ.

- **Track-based analysis:** A racing sailor wants to compare wind angle and boat speed along a specific leg of a course they've sailed dozens of times. The spatial filter defines the leg; the position path ties each sensor reading to the vessel's location at that moment. If race instruments log position under a different path than the cruise setup, the query needs to follow the data.

- **Multi-source vessels:** Commercial and research vessels routinely carry multiple GPS receivers under different paths — a primary navigation unit, a DGPS for survey work, an AIS transponder's position. Which one to correlate against depends on the question being asked.

The default covers most recreational use. But spatial filtering without a configurable position source is spatial filtering that only works until it doesn't. The parameter is trivial to implement and defaults silently — clients that don't need it never see it.

**Is this essential?** Yes. Impossible without.

---

## 2. SMA and EMA as Post-Processing Smoothing (Extended Semantics)

**What V2 currently supports:** The V2 spec already recognizes `sma` and `ema` as aggregate methods in path expressions (e.g., `navigation.speedOverGround:sma`). In the spec, these are treated as aggregation methods — they replace the raw value with the smoothed value.

**What this plugin does differently:** The plugin supports two distinct uses of SMA/EMA:

- **Standard spec usage** (`path:sma:5` or `path:ema:0.3`) — SMA or EMA is the aggregation method. The parameter (5 or 0.3) sets the window period for SMA or the alpha decay factor for EMA. This matches the V2 spec intent.

- **Extended syntax** (`path:average:sma:5` or `path:min:ema:0.3`) — The aggregation method is applied first (average, min, etc.), and then SMA or EMA smoothing is applied as a second pass on top of the aggregated results. This lets a client say "give me the 60-second averages, then smooth them with a 10-period moving average" — which is useful for noisy sensor data where you want both downsampling and noise reduction.

**What V2 would need:** Recognition that the path expression syntax can have four segments (`path:aggregate:smoothing:param`) in addition to the current three (`path:aggregate:param`). The third segment, when it is `sma` or `ema`, indicates post-aggregation smoothing rather than an aggregate parameter. The fourth segment is the smoothing parameter.

**Why it matters:** The two-stage approach (aggregate then smooth) is fundamentally different from using SMA/EMA as the aggregation method. Aggregate-only SMA gives you a rolling average of raw values. Aggregate-then-smooth gives you a smoothed trend line of already-bucketed data. Both are useful for different visualization needs.

**Is this essential?** No but very important. EMA and SMA are fundamentally different than other aggregators and are used paired with an aggregation.

---

## Sections Removed from Previous Draft

The following sections were in the previous version of this proposal and have been removed based on review feedback:

### Aggregation Tier Selection (removed)
Previously proposed a `tier` parameter for clients to select raw/5s/60s/1h aggregation. **Reviewer feedback:** This should be internal and automatic to the implementation. The backend has all the information it needs (time range, resolution) to select the optimal tier. Clients have no knowledge of what aggregations exist, so exposing this creates a leaky abstraction. The provider already does this automatically — no API surface needed.

### Timezone Conversion (removed)
Previously proposed `convertTimesToLocal` and `timezone` parameters for server-side timestamp conversion. **Reviewer feedback:** Everything related to time conversion blows up in complexity. Edge cases like DST fall-back transitions (where e.g. 01:30 occurs twice) make server-side conversion fragile and error-prone. Clients are better positioned to handle this with their own timezone libraries.

### Auto-Refresh Metadata (removed)
Previously proposed a `refresh` flag to include polling guidance in responses. **Reviewer feedback:** Refresh interval is a function of the time range and resolution — information the client already has. The server doesn't know better than the client when to re-query.

### Storage Tier Discovery and Selection (removed)
Previously proposed a `tiers` parameter and a `GET /history/tiers` discovery endpoint for clients to select and inspect storage backends (local, S3, etc.). **Reviewer feedback:** Don't prescribe the values — instead, create a metadata endpoint for storage tiers with an ordered `tiers=local:s3:` list on query endpoints. However, on further consideration this is premature. The provider should handle tier routing automatically. If a discovery endpoint is needed later, it can be added without requiring changes to the query API.

### UTC Interpretation Flag (removed)
Previously proposed a `useUTC` boolean to control whether `from`/`to` are interpreted as UTC or local time. **Reviewer feedback:** The V2 spec already documents these as ISO 8601 timestamps, and ISO 8601 states that timestamps without UTC relation information are assumed to be in local time. Timestamps with a `Z` suffix or `+00:00` offset are UTC. The timezone interpretation is already embedded in the timestamp itself — no additional flag needed, provided the server correctly parses ISO 8601 offsets.

---

## Summary of New Parameters Needed on V2 `/values`

| Parameter | Type | Purpose |
|---|---|---|
| `bbox` | string (4 decimals) | Spatial bounding box filter |
| `radius` | string (3 decimals) | Spatial radius filter |
| `positionPath` | string | Position source for spatial queries |

Path expression syntax extension: `path:aggregate:smoothing:param` (4-segment form for post-aggregation SMA/EMA)

## Response Extensions Needed

The V2 `ValuesResponse` currently defines `context`, `range`, `values`, and `data`. No additional response fields are proposed in this revision.
