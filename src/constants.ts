/**
 * Central tunable constants.
 *
 * Single home for hardcoded numeric tunables so they're easy to find and
 * later migrate to plugin config. When adding a new magic number, put it
 * here with a short comment on what it controls.
 */

/**
 * Maximum plausible implied speed between consecutive GPS fixes, in m/s.
 * Used by position aggregation to reject single-point GPS glitches: if the
 * implied speed from a candidate's temporal neighbor exceeds this, the
 * candidate is treated as an outlier.
 *
 * 25 m/s ≈ 48.6 kn — well above any sailing/power vessel's realistic top speed.
 */
export const POSITION_MAX_SPEED_MPS = 25;

/**
 * GPX import: per-file upload size cap, in bytes.
 *
 * The whole file is read into memory and then fanned out into one
 * DataRecord per (point x selected SK path), so the conservative cap
 * is much lower than what multer's wire limit would allow. A 50 MB
 * GPX is roughly half a million trkpts; with four default paths that
 * peaks around 2 GB of working set, which is the upper bound for a
 * Pi-class host. Raise only after the importer streams instead of
 * buffering (parquet-writer.ts:writeParquetBatched is available for
 * this; it just isn't wired into the import path yet).
 */
export const GPX_UPLOAD_MAX_FILE_BYTES = 50 * 1024 * 1024;

/**
 * GPX import: maximum number of files per multipart upload request.
 */
export const GPX_UPLOAD_MAX_FILES = 500;

/**
 * GPX import: how long a finished job's progress entry sticks around for
 * the UI to poll, in milliseconds. Mirrors MIGRATION_JOB_TTL_MS in
 * MigrationService - kept identical so both jobs feel the same.
 */
export const IMPORT_JOB_TTL_MS = 30 * 60 * 1000;
