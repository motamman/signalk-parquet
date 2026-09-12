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
 * The importer streams the file and writes parquet as points arrive (#54),
 * so peak memory no longer scales with file size: it is bounded by the open
 * writers (GPX_IMPORT_MAX_OPEN_WRITERS, one row group each) plus the
 * records waiting for a writer to open (GPX_IMPORT_PENDING_RECORDS_CAP).
 * The cap is now a sanity guard against a runaway upload filling the disk,
 * not a memory limit. Multer also stages the upload on disk, never in RAM.
 */
export const GPX_UPLOAD_MAX_FILE_BYTES = 500 * 1024 * 1024;

/**
 * GPX import: cap on the whole multipart request, in bytes. The per-file
 * cap times GPX_UPLOAD_MAX_FILES would let one request stage hundreds of
 * gigabytes, so the request's Content-Length is checked against this
 * before any file is written to disk.
 */
export const GPX_UPLOAD_MAX_TOTAL_BYTES = 2 * 1024 * 1024 * 1024;

/**
 * GPX import: how many (path, day) parquet writers may be open at once.
 * Each holds one row group (a few thousand rows) in memory. A chronological
 * track touches one or two groups at a time; a file that jumps between
 * days evicts the least recently used writer, and a later point for that
 * day opens a new file in the same partition, which the hive layout allows.
 */
export const GPX_IMPORT_MAX_OPEN_WRITERS = 16;

/**
 * GPX import: records a group collects before its writer opens. The parquet
 * schema is detected from this first batch, so it wants a reasonable sample.
 */
export const GPX_IMPORT_OPEN_AFTER_RECORDS = 500;

/**
 * GPX import: records appended per call once a writer is open.
 */
export const GPX_IMPORT_APPEND_BATCH = 500;

/**
 * GPX import: total records allowed to wait for a writer across all groups.
 * Past this, the largest waiting group is opened early so a file that
 * scatters points over many days cannot accumulate a full day per group.
 */
export const GPX_IMPORT_PENDING_RECORDS_CAP = 20_000;

/**
 * GPX import: points between cancellation checks while streaming a file.
 */
export const GPX_IMPORT_CANCEL_CHECK_POINTS = 1000;

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

/**
 * Retention cleanup: how many files to process between event-loop yields.
 * Walking a large parquet tree shouldn't starve other plugin work (live
 * data ingestion, SignalK websocket heartbeats) on slow disks, so the
 * cleanup loop yields after this many files.
 */
export const CLEANUP_YIELD_INTERVAL = 200;
