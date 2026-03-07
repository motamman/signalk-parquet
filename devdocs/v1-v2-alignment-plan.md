# Plan: Align V1 API with V2 Proposal

## Context

The V2 History API proposal (`devdocs/v2-history-api-extensions.md`) defines a clean, minimal set of extensions: spatial filtering (bbox, radius) and SMA/EMA post-processing smoothing. The V1 API currently has additional parameters and behaviors that are not part of the V2 proposal. The goal is to strip V1 down to only what V2 contemplates, so that when V2 is enacted the transition is seamless.

## What Survives (no changes needed)

1. **Spatial filtering** — `bbox`, `radius` parameters (already implemented, names match proposal)
2. **SMA/EMA smoothing** — 3-segment (`path:sma:5`) and 4-segment (`path:average:sma:5`) path expression syntax (already implemented)

## What Gets Removed from V1

| Parameter | Current Behavior | Reason for Removal |
|---|---|---|
| `useUTC` | Defaults to `true`, treats bare timestamps as UTC | ISO 8601 already defines this — bare = local, Z = UTC |
| `convertTimesToLocal` | Converts response timestamps to a target timezone | Not in V2 proposal — clients handle display conversion |
| `timezone` | IANA timezone ID for response conversion | Paired with `convertTimesToLocal`, removed together |
| `refresh` | Returns auto-refresh metadata in response | Not in V2 proposal — client already has the info to decide |
| `tier` (if client-facing) | Lets client select aggregation tier | Not in V2 proposal — provider selects automatically |
| `source` | Routes query to local/s3/hybrid/auto | Not in V2 proposal — provider handles automatically |
| `positionPath` | Overrides which SignalK path provides vessel position for spatial filtering | Edge case — virtually all vessels use `navigation.position`. Hardcode the default internally. |

## Breaking Change: Time Handling

**This is the big one.**

- **Before:** `useUTC=true` (default) means `2025-03-07T14:00:00` (no Z, no offset) is treated as **UTC**
- **After:** Per ISO 8601, `2025-03-07T14:00:00` (no Z, no offset) is treated as **local time**. Only timestamps with `Z` or an explicit offset (e.g., `+00:00`, `-05:00`) are UTC.

### Files to modify

- `src/HistoryAPI.ts` — `parseDateTime()` (lines ~419-462): Remove `useUTC` parameter. Bare timestamps → local time. Timestamps with Z/offset → parse as-is.
- `src/HistoryAPI.ts` — `getRequestParams()` (lines ~336-398): Remove `useUTC`, `convertTimesToLocal`, `timezone`, `refresh`, `source`, `tier` from parameter extraction.
- `src/HistoryAPI.ts` — Response building: Remove timezone conversion logic (~lines 61-104, 983-985) and refresh metadata logic.
- `src/HistoryAPI-types.ts` — Remove corresponding type definitions for removed parameters.
- `src/utils/` — Remove any utility functions that only served removed features (if any become orphaned).

### Files to verify (no changes expected)

- `src/history-provider.ts` — V2 provider already doesn't have these parameters. Confirm it handles time correctly per ISO 8601.
- `src/utils/spatial-queries.ts` — Spatial filtering stays as-is.
- `src/utils/duration-parser.ts` — Duration/resolution parsing stays as-is.

## Verification

1. Confirm bare timestamps are treated as local time (not UTC)
2. Confirm timestamps with `Z` suffix are treated as UTC
3. Confirm timestamps with explicit offsets (e.g., `-05:00`) are parsed correctly
4. Confirm `bbox`, `radius` still work unchanged
5. Confirm SMA/EMA path expressions (3-segment and 4-segment) still work unchanged
6. Confirm removed parameters (`useUTC`, `convertTimesToLocal`, `timezone`, `refresh`, `tier`, `source`, `positionPath`) are no longer accepted or silently ignored
7. Run existing tests in `tests/` directory
