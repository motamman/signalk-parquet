/**
 * GPX Import Service
 *
 * Imports historical GPS tracks from GPX files into the Hive-partitioned
 * parquet store. Each <trkpt> with a valid <time> is expanded into SignalK
 * delta-style records for the configured paths (position, SOG, COG,
 * altitude) and written directly as parquet files, bypassing the SQLite
 * buffer (bulk historical load).
 *
 * Files are streamed (#54): the tokenizer emits points as their elements
 * complete and each (path, day) group appends to an open parquet writer,
 * so peak memory is set by the number of open writers, not the file size.
 *
 * Follows the same progress-tracking / cancellable-job pattern as
 * MigrationService.
 *
 * When adding a new SignalK path:
 *   1. Extend the GpxImportPath union below
 *   2. Append an entry to GPX_IMPORT_PATH_OPTIONS (the admin UI builds its
 *      checkboxes from it via GET /api/import/gpx/options, so nothing in
 *      public/ needs editing)
 *   3. Add a case in pointToValue() that maps the <trkpt> to the value
 *      in SignalK units (m/s, radians, etc.)
 *   4. Extend the GpxPoint interface in gpx-parser.ts if a new tag must
 *      be parsed out of the GPX
 */

import * as fs from 'fs-extra';
import * as path from 'path';
import { globIn } from '../utils/glob-in';
import { ServerAPI } from '@signalk/server-api';
import { DataRecord, ParquetAppender, ParquetWriter } from '../types';
import { HivePathBuilder } from '../utils/hive-path-builder';
import { GpxTokenizer, GpxPoint } from '../utils/gpx-parser';
import {
  IMPORT_JOB_TTL_MS,
  GPX_IMPORT_MAX_OPEN_WRITERS,
  GPX_IMPORT_OPEN_AFTER_RECORDS,
  GPX_IMPORT_APPEND_BATCH,
  GPX_IMPORT_PENDING_RECORDS_CAP,
  GPX_IMPORT_CANCEL_CHECK_POINTS,
} from '../constants';
import { AggregationService } from './aggregation-service';

export type GpxImportPath =
  | 'navigation.position'
  | 'navigation.speedOverGround'
  | 'navigation.courseOverGroundTrue'
  | 'navigation.gnss.antennaAltitude';

/** One importable SignalK path as offered to the admin UI (#55). */
export interface GpxImportPathOption {
  path: GpxImportPath;
  /** Whether the UI ticks the box before the user touches it. */
  defaultChecked: boolean;
  /** The GPX element the value comes from, for the UI's hint text. */
  from: string;
  /** SignalK unit the value is stored in. */
  unit: string;
}

/**
 * The supported import paths with their UI metadata. Single source of truth:
 * the route serves it as GET /api/import/gpx/options and the import page
 * renders its checkboxes from the response.
 */
export const GPX_IMPORT_PATH_OPTIONS: readonly GpxImportPathOption[] = [
  {
    path: 'navigation.position',
    defaultChecked: true,
    from: '<trkpt lat/lon>',
    unit: 'deg',
  },
  {
    path: 'navigation.speedOverGround',
    defaultChecked: true,
    from: '<speed>',
    unit: 'm/s',
  },
  {
    path: 'navigation.courseOverGroundTrue',
    defaultChecked: true,
    from: '<course>',
    unit: 'rad',
  },
  {
    path: 'navigation.gnss.antennaAltitude',
    defaultChecked: true,
    from: '<ele>',
    unit: 'm',
  },
];

export const DEFAULT_IMPORT_PATHS: GpxImportPath[] =
  GPX_IMPORT_PATH_OPTIONS.map(option => option.path);

export interface GpxImportConfig {
  sourceDirectory?: string; // Scan recursively for .gpx files
  sourceFiles?: string[]; // Explicit list (absolute paths). Used if set.
  // Original filename per entry of sourceFiles, keyed by the path in that
  // list. Uploads are staged under a de-duplicated name (#68); the name the
  // user gave the file is what progress and the records' source.file show.
  sourceNames?: Record<string, string>;
  targetDirectory: string; // Typically state.getDataDirPath()
  context: string; // SignalK context, e.g. 'vessels.urn:mrn:...'
  paths: GpxImportPath[]; // Which SK paths to emit per point
  filenamePrefix: string; // Output parquet filename prefix
  deleteSourceAfterImport: boolean;
  sourceLabel: string; // Written into record.source_label
}

export interface GpxImportProgress {
  jobId: string;
  status: 'scanning' | 'running' | 'completed' | 'cancelled' | 'error';
  phase: 'scan' | 'parse' | 'write' | 'aggregate';
  processed: number; // files processed
  total: number; // files to process
  percent: number;
  currentFile?: string;
  startTime: Date;
  completedAt?: Date;
  error?: string;
  bytesProcessed: number;
  pointsParsed: number;
  pointsWritten: number; // includes fan-out across paths
  recordsWritten: number; // parquet rows actually emitted
  filesImported: number;
  filesSkipped: number;
  filesCreated: string[];
  errors: string[];
  // Populated only during the post-write aggregation phase
  aggregationDatesTotal?: number;
  aggregationDatesProcessed?: number;
  aggregationCurrentDate?: string;
}

export interface GpxScanResult {
  totalFiles: number;
  totalSize: number;
  files: Array<{ path: string; size: number }>;
}

/** Tunables for the streaming writer pool; defaults come from constants.ts. */
export interface GpxImportServiceOptions {
  /** Parquet writers allowed open at once. */
  maxOpenWriters?: number;
  /** Records a (path, day) group collects before its writer opens. */
  openAfterRecords?: number;
}

const importJobs = new Map<string, GpxImportProgress>();

function scheduleImportJobCleanup(jobId: string) {
  // unref: a pending cleanup must not keep the process alive on its own
  // (the plugin's host decides when to exit, and so does the test runner).
  setTimeout(() => {
    const job = importJobs.get(jobId);
    if (job && job.status !== 'running') {
      importJobs.delete(jobId);
    }
  }, IMPORT_JOB_TTL_MS).unref();
}

export class GpxImportService {
  private readonly app: ServerAPI;
  private readonly parquetWriter: ParquetWriter;
  private readonly hivePathBuilder: HivePathBuilder;
  private readonly aggregationService?: AggregationService;

  private readonly maxOpenWriters: number;
  private readonly openAfterRecords: number;

  // Per-job cancellation. A set (rather than a single flag) so concurrent
  // imports don't trample each other's state — cancelling one job never
  // cancels another.
  private readonly cancelledJobs: Set<string> = new Set();

  constructor(
    app: ServerAPI,
    parquetWriter: ParquetWriter,
    aggregationService?: AggregationService,
    options: GpxImportServiceOptions = {}
  ) {
    this.app = app;
    this.parquetWriter = parquetWriter;
    this.hivePathBuilder = new HivePathBuilder();
    this.aggregationService = aggregationService;
    this.maxOpenWriters = Math.max(
      1,
      options.maxOpenWriters ?? GPX_IMPORT_MAX_OPEN_WRITERS
    );
    this.openAfterRecords = Math.max(
      1,
      options.openAfterRecords ?? GPX_IMPORT_OPEN_AFTER_RECORDS
    );
  }

  /**
   * Scan a directory for .gpx files (non-destructive dry run).
   */
  async scan(sourceDirectory: string): Promise<GpxScanResult> {
    const matches = await globIn(sourceDirectory, '**/*.gpx', {
      nocase: true,
    });

    const files: Array<{ path: string; size: number }> = [];
    let totalSize = 0;

    for (const file of matches) {
      try {
        const stats = await fs.stat(file);
        files.push({ path: file, size: stats.size });
        totalSize += stats.size;
      } catch (error) {
        this.app.debug(`Failed to stat ${file}: ${(error as Error).message}`);
      }
    }

    return { totalFiles: files.length, totalSize, files };
  }

  /**
   * Start an import job. Runs asynchronously; poll progress via getProgress.
   *
   * Validates the requested SK paths against DEFAULT_IMPORT_PATHS even
   * though the route layer already does — defense in depth so a future
   * non-route caller can't slip an unsupported path through to
   * pointToValue (which has no default branch).
   */
  async import(config: GpxImportConfig): Promise<string> {
    const supported = new Set<string>(DEFAULT_IMPORT_PATHS);
    const invalid = config.paths.filter(p => !supported.has(p));
    if (invalid.length > 0) {
      throw new Error(
        `Unsupported paths: ${invalid.join(', ')}. Supported: ${DEFAULT_IMPORT_PATHS.join(', ')}`
      );
    }

    const jobId = `import_${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;

    const progress: GpxImportProgress = {
      jobId,
      status: 'scanning',
      phase: 'scan',
      processed: 0,
      total: 0,
      percent: 0,
      startTime: new Date(),
      bytesProcessed: 0,
      pointsParsed: 0,
      pointsWritten: 0,
      recordsWritten: 0,
      filesImported: 0,
      filesSkipped: 0,
      filesCreated: [],
      errors: [],
    };

    importJobs.set(jobId, progress);

    this.runImport(jobId, config)
      .catch(error => {
        const job = importJobs.get(jobId);
        if (job) {
          job.status = 'error';
          job.error = (error as Error).message;
          job.completedAt = new Date();
        }
      })
      .finally(() => {
        // Drop the cancellation flag once the job is no longer running
        // so the set doesn't grow unboundedly across the plugin's lifetime.
        this.cancelledJobs.delete(jobId);
      });

    return jobId;
  }

  /**
   * Main orchestration: resolve file list, parse each, group records by
   * (path, day), write one parquet per group into the Hive layout.
   */
  private async runImport(
    jobId: string,
    config: GpxImportConfig
  ): Promise<void> {
    const progress = importJobs.get(jobId);
    if (!progress) return;

    try {
      progress.phase = 'scan';
      progress.status = 'scanning';

      // Resolve source files
      const gpxFiles: string[] = [];
      if (config.sourceFiles && config.sourceFiles.length > 0) {
        gpxFiles.push(...config.sourceFiles);
      } else if (config.sourceDirectory) {
        const matches = await globIn(config.sourceDirectory, '**/*.gpx', {
          nocase: true,
        });
        gpxFiles.push(...matches);
      }

      progress.total = gpxFiles.length;

      if (gpxFiles.length === 0) {
        progress.status = 'completed';
        progress.completedAt = new Date();
        scheduleImportJobCleanup(jobId);
        return;
      }

      // Resolve vessels.self to concrete context (stored on disk)
      const resolvedContext =
        config.context === 'vessels.self'
          ? this.app.selfContext
          : config.context;

      // Per-job metadata cache: app.getMetadata(path) is lookup-cheap but
      // the repeated calls are noisy in a busy server. One read per path
      // per job, mirrored from the live handler in data-handler.ts.
      const metadataCache = this.buildMetadataCache(config.paths);

      progress.status = 'running';

      // Distinct (year, dayOfYear) partitions touched by this import,
      // keyed by ISO YYYY-MM-DD. Drives the post-write aggregation phase
      // so we only re-aggregate days we actually wrote raw files into.
      const touchedDays = new Map<string, Date>();

      for (let i = 0; i < gpxFiles.length; i++) {
        if (this.cancelledJobs.has(jobId)) {
          progress.status = 'cancelled';
          progress.completedAt = new Date();
          scheduleImportJobCleanup(jobId);
          return;
        }

        const file = gpxFiles[i];
        const displayName = config.sourceNames?.[file] ?? path.basename(file);
        progress.currentFile = displayName;
        progress.processed = i + 1;
        progress.percent = Math.round(((i + 1) / gpxFiles.length) * 100);
        progress.phase = 'parse'; // reset before each file; importFile flips to 'write' once it starts emitting

        try {
          const stats = await fs.stat(file);
          const imported = await this.importFile(
            jobId,
            file,
            displayName,
            resolvedContext,
            config,
            progress,
            metadataCache,
            touchedDays
          );

          if (imported) {
            progress.filesImported++;
            progress.bytesProcessed += stats.size;

            if (config.deleteSourceAfterImport) {
              await fs.remove(file);
            }
          } else {
            progress.filesSkipped++;
          }
        } catch (error) {
          const errorMsg = `Failed to import ${file}: ${(error as Error).message}`;
          this.app.debug(errorMsg);
          progress.errors.push(errorMsg);
          progress.filesSkipped++;
        }
      }

      // Aggregation phase: rebuild 5s/60s/1h tiers for every (year, day)
      // partition we wrote into. Without this the daily auto-aggregation
      // only covers yesterday, leaving historical imports stranded in raw.
      // aggregateDate() is idempotent (DuckDB COPY ... TO overwrites the
      // single per-day output file), so re-running for already-aggregated
      // dates is safe.
      if (
        this.aggregationService &&
        touchedDays.size > 0 &&
        !this.cancelledJobs.has(jobId)
      ) {
        await this.runAggregationPhase(jobId, touchedDays, progress);
      }

      // Cancellation requested mid-aggregation is reported as cancelled,
      // not completed; partial tier coverage is real and the caller
      // should know.
      progress.status = this.cancelledJobs.has(jobId)
        ? 'cancelled'
        : 'completed';
      progress.completedAt = new Date();
      scheduleImportJobCleanup(jobId);
    } catch (error) {
      progress.status = 'error';
      progress.error = (error as Error).message;
      progress.completedAt = new Date();
      scheduleImportJobCleanup(jobId);
    }
  }

  /**
   * Look up SignalK metadata once per job for each requested path. Mirrors
   * what data-handler.ts does on every live delta — for imports we just
   * cache it once. Failures are silent (metadata is best-effort and the
   * record stays valid without it).
   */
  private buildMetadataCache(
    paths: GpxImportPath[]
  ): Map<string, object | undefined> {
    const cache = new Map<string, object | undefined>();
    for (const p of paths) {
      try {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const meta = (this.app as any).getMetadata?.(p);
        cache.set(p, meta || undefined);
      } catch {
        cache.set(p, undefined);
      }
    }
    return cache;
  }

  /**
   * Stream a single GPX file into the parquet store. Returns true if at
   * least one parquet file was produced.
   *
   * Points are handled as the tokenizer emits them. Each (path, day)
   * partition is a group in a GroupWriterPool: a group collects a first
   * batch (the schema sample), opens a writer, and appends from then on.
   * The pool caps open writers; an evicted group is closed into a finished
   * file and reopens as a new file in the same partition if more points
   * arrive for it. Cancellation closes what is open, so everything parsed
   * so far is kept (a partial import counts).
   *
   * `displayName` is the filename recorded in each record's source.file:
   * the user's original name for an upload, the basename otherwise.
   *
   * `touchedDays` is mutated to record each (year, day) partition this
   * file wrote into; the caller drives a post-import aggregation phase
   * over that set.
   */
  private async importFile(
    jobId: string,
    sourcePath: string,
    displayName: string,
    resolvedContext: string,
    config: GpxImportConfig,
    progress: GpxImportProgress,
    metadataCache: Map<string, object | undefined>,
    touchedDays: Map<string, Date>
  ): Promise<boolean> {
    const pool = new GroupWriterPool({
      maxOpen: this.maxOpenWriters,
      openAfter: this.openAfterRecords,
      appendBatch: GPX_IMPORT_APPEND_BATCH,
      pendingCap: GPX_IMPORT_PENDING_RECORDS_CAP,
      openFile: async (group, firstBatch) => {
        const finalPath = this.buildHiveFilePath(
          config.targetDirectory,
          resolvedContext,
          group.signalkPath,
          group.anchor,
          config.filenamePrefix
        );
        await fs.ensureDir(path.dirname(finalPath));
        const tempPath = finalPath + '.tmp';
        progress.phase = 'write';
        const appender = await this.parquetWriter.openAppender(
          tempPath,
          firstBatch,
          group.signalkPath
        );
        return { appender, finalPath, tempPath };
      },
      onClosed: (group, rows, finalPath) => {
        progress.filesCreated.push(finalPath);
        progress.recordsWritten += rows;
        // Record only after a successful write so a failed group doesn't
        // schedule aggregation for a partition we never produced.
        const dayKey = group.anchor.toISOString().slice(0, 10);
        if (!touchedDays.has(dayKey)) {
          touchedDays.set(dayKey, group.anchor);
        }
      },
    });

    const filesBefore = progress.filesCreated.length;
    const tokenizer = new GpxTokenizer();
    const stream = fs.createReadStream(sourcePath, {
      encoding: 'utf8',
      highWaterMark: 256 * 1024,
    });

    const handlePoint = async (pt: GpxPoint): Promise<void> => {
      progress.pointsParsed++;
      // Only points with a timestamp can be partitioned.
      if (!pt.time) return;
      const ts = pt.time;
      const year = ts.getUTCFullYear();
      const dayOfYear = this.hivePathBuilder.getDayOfYear(ts);

      for (const skPath of config.paths) {
        const value = this.pointToValue(skPath, pt);
        if (value === undefined) continue;
        const record = this.buildRecord(
          skPath,
          resolvedContext,
          ts,
          value,
          config.sourceLabel,
          displayName,
          metadataCache.get(skPath)
        );
        await pool.add(`${skPath}|${year}|${dayOfYear}`, skPath, ts, record);
        progress.pointsWritten++;
      }
    };

    let sinceCancelCheck = 0;
    let cancelled = false;
    try {
      for await (const chunk of stream) {
        for (const event of tokenizer.push(chunk as string)) {
          if (event.type !== 'point') continue;
          await handlePoint(event.point);
          if (++sinceCancelCheck >= GPX_IMPORT_CANCEL_CHECK_POINTS) {
            sinceCancelCheck = 0;
            if (this.cancelledJobs.has(jobId)) {
              cancelled = true;
              break;
            }
          }
        }
        if (cancelled) break;
      }
      if (!cancelled) {
        for (const event of tokenizer.end()) {
          if (event.type === 'point') await handlePoint(event.point);
        }
      }
      // Cancelled or not, what was parsed is written: a partial import counts.
      await pool.finishAll();
    } catch (error) {
      await pool.abortAll();
      throw error;
    } finally {
      stream.destroy();
    }

    return progress.filesCreated.length > filesBefore;
  }

  /**
   * Re-aggregate every (year, day) partition this import wrote into.
   * `aggregateDate()` cascades raw -> 5s -> 60s -> 1h, and is idempotent
   * (DuckDB COPY ... TO overwrites the per-day output file), so running
   * it for already-aggregated dates is safe; it just refolds the new
   * raw rows in alongside the existing ones.
   *
   * Errors per-date are recorded but don't fail the import; partial
   * tier coverage is better than rejecting the whole job.
   */
  private async runAggregationPhase(
    jobId: string,
    touchedDays: Map<string, Date>,
    progress: GpxImportProgress
  ): Promise<void> {
    if (!this.aggregationService) return;

    progress.phase = 'aggregate';
    progress.aggregationDatesTotal = touchedDays.size;
    progress.aggregationDatesProcessed = 0;

    // Stable order: YYYY-MM-DD sorts chronologically as a string.
    const sortedDays = Array.from(touchedDays.entries()).sort(([a], [b]) =>
      a.localeCompare(b)
    );

    let i = 0;
    for (const [dateStr, date] of sortedDays) {
      if (this.cancelledJobs.has(jobId)) return;

      progress.aggregationCurrentDate = dateStr;
      progress.aggregationDatesProcessed = i;

      try {
        await this.aggregationService.aggregateDate(date);
      } catch (error) {
        progress.errors.push(
          `Aggregation failed for ${dateStr}: ${(error as Error).message}`
        );
      }
      i++;
    }

    progress.aggregationDatesProcessed = touchedDays.size;
    progress.aggregationCurrentDate = undefined;
  }

  private buildHiveFilePath(
    basePath: string,
    context: string,
    signalkPath: string,
    anchorTimestamp: Date,
    filenamePrefix: string
  ): string {
    // Imports always land in tier=raw: they're un-aggregated point data
    // (the same shape as live SK deltas write) and should be re-aggregated
    // through the same raw -> 5s -> 60s -> 1h pipeline as live data via
    // AggregationService.aggregateDate().
    const dirPath = this.hivePathBuilder.buildPath(
      basePath,
      'raw',
      context,
      signalkPath,
      anchorTimestamp
    );
    // Wall-clock timestamp so two import jobs writing to the same partition
    // don't collide. Millisecond resolution + random suffix gives safe
    // uniqueness even with concurrent runs.
    const now = new Date();
    const timestampStr = now.toISOString().replace(/[:.]/g, '').slice(0, 18);
    const randomSuffix = Math.random().toString(36).substring(2, 6);
    return path.join(
      dirPath,
      `${filenamePrefix}_${timestampStr}_${randomSuffix}.parquet`
    );
  }

  /**
   * Convert a GPX point into the SignalK value for a given path.
   * Returns undefined if the point lacks the needed field.
   *
   * Units:
   * - navigation.position: object {latitude, longitude} in decimal degrees
   * - navigation.speedOverGround: number in m/s (GPX <speed> is m/s)
   * - navigation.courseOverGroundTrue: number in radians (GPX <course> is degrees → convert)
   * - navigation.gnss.antennaAltitude: number in meters
   *
   * When adding a new GpxImportPath, add a matching case here in the same
   * unit as the SignalK path spec defines.
   */
  private pointToValue(skPath: GpxImportPath, pt: GpxPoint): unknown {
    switch (skPath) {
      case 'navigation.position':
        return { latitude: pt.latitude, longitude: pt.longitude };
      case 'navigation.speedOverGround':
        return pt.speedMs;
      case 'navigation.courseOverGroundTrue':
        return pt.courseDeg !== undefined
          ? (pt.courseDeg * Math.PI) / 180
          : undefined;
      case 'navigation.gnss.antennaAltitude':
        return pt.elevation;
    }
  }

  /**
   * Build a DataRecord matching the shape produced by the live streambundle
   * handler in data-handler.ts: scalar values go in `value`, object values
   * go in `value_json` with their scalar properties flattened into
   * `value_<key>` columns so downstream queries can read them directly.
   * `meta` carries the SK metadata (units, displayUnits, etc.) so
   * downstream consumers see the same units as live-captured rows.
   *
   * Kept inline (rather than shared with data-handler.ts) because the live
   * path also populates source.$source, source.pgn etc. from the delta
   * frame — fields we don't have here. A future refactor could extract
   * the shared object-flattening helper; it isn't big enough to pay yet.
   *
   * `source.type: 'file'` is a plugin-local convention rather than one of
   * SK's canonical source types (NMEA0183 / NMEA2000 / signalk). Anything
   * filtering on canonical types won't match imported rows; if that
   * matters in the future, we could expose it as a config option.
   */
  private buildRecord(
    skPath: GpxImportPath,
    context: string,
    signalkTimestamp: Date,
    value: unknown,
    sourceLabel: string,
    originalFilename: string,
    meta: object | undefined
  ): DataRecord {
    const record: DataRecord = {
      received_timestamp: new Date().toISOString(),
      signalk_timestamp: signalkTimestamp.toISOString(),
      context,
      path: skPath,
      value: null,
      source: { label: sourceLabel, type: 'file', file: originalFilename },
      source_label: sourceLabel,
      source_type: 'file',
      meta,
    };

    if (value !== null && typeof value === 'object') {
      record.value_json = value;
      for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
        if (
          typeof v === 'string' ||
          typeof v === 'number' ||
          typeof v === 'boolean'
        ) {
          (record as Record<string, unknown>)[`value_${k}`] = v;
        }
      }
    } else {
      record.value = value;
    }

    return record;
  }

  getProgress(jobId: string): GpxImportProgress | null {
    return importJobs.get(jobId) || null;
  }

  cancel(jobId: string): boolean {
    const job = importJobs.get(jobId);
    if (job && job.status === 'running') {
      this.cancelledJobs.add(jobId);
      return true;
    }
    return false;
  }

  getJobIds(): string[] {
    return Array.from(importJobs.keys());
  }
}

/** One (path, day) partition being written during a file's import. */
interface WriterGroup {
  key: string;
  signalkPath: GpxImportPath;
  anchor: Date;
  /** Records not yet handed to the appender. */
  pending: DataRecord[];
  appender?: ParquetAppender;
  finalPath?: string;
  tempPath?: string;
  /** Monotonic counter of the last add, for LRU eviction. */
  lastUse: number;
}

interface GroupWriterPoolOptions {
  maxOpen: number;
  openAfter: number;
  appendBatch: number;
  pendingCap: number;
  openFile: (
    group: WriterGroup,
    firstBatch: DataRecord[]
  ) => Promise<{
    appender: ParquetAppender;
    finalPath: string;
    tempPath: string;
  }>;
  onClosed: (group: WriterGroup, rows: number, finalPath: string) => void;
}

/**
 * Routes records to per-partition parquet writers while a file streams in,
 * keeping at most `maxOpen` writers open and at most `pendingCap` records
 * waiting for one.
 *
 * A group opens once it holds `openAfter` records (the schema sample) or
 * when the pending total passes the cap, whichever is first. Opening past
 * the writer limit closes the least recently used group into a finished
 * file; if that partition gets more records later, a new group opens a new
 * file next to it. `finishAll()` closes everything (opening groups that
 * never reached the sample size), `abortAll()` discards open files.
 */
class GroupWriterPool {
  private readonly groups = new Map<string, WriterGroup>();
  private pendingTotal = 0;
  private openCount = 0;
  private tick = 0;

  constructor(private readonly options: GroupWriterPoolOptions) {}

  async add(
    key: string,
    signalkPath: GpxImportPath,
    anchor: Date,
    record: DataRecord
  ): Promise<void> {
    let group = this.groups.get(key);
    if (!group) {
      group = { key, signalkPath, anchor, pending: [], lastUse: 0 };
      this.groups.set(key, group);
    }
    group.pending.push(record);
    group.lastUse = ++this.tick;

    if (group.appender) {
      if (group.pending.length >= this.options.appendBatch) {
        await this.flush(group);
      }
      return;
    }
    this.pendingTotal++;
    if (group.pending.length >= this.options.openAfter) {
      await this.open(group);
    } else if (this.pendingTotal > this.options.pendingCap) {
      await this.open(this.largestWaiting());
    }
  }

  async finishAll(): Promise<void> {
    for (const group of Array.from(this.groups.values())) {
      await this.finish(group);
    }
  }

  async abortAll(): Promise<void> {
    for (const group of Array.from(this.groups.values())) {
      if (group.appender) {
        await group.appender.abort();
      }
    }
    this.groups.clear();
    this.pendingTotal = 0;
    this.openCount = 0;
  }

  private largestWaiting(): WriterGroup {
    let largest: WriterGroup | undefined;
    for (const group of this.groups.values()) {
      if (group.appender) continue;
      if (!largest || group.pending.length > largest.pending.length) {
        largest = group;
      }
    }
    // pendingTotal > 0 guarantees at least one waiting group.
    return largest!;
  }

  private leastRecentlyUsedOpen(): WriterGroup | undefined {
    let lru: WriterGroup | undefined;
    for (const group of this.groups.values()) {
      if (!group.appender) continue;
      if (!lru || group.lastUse < lru.lastUse) {
        lru = group;
      }
    }
    return lru;
  }

  private async open(group: WriterGroup): Promise<void> {
    if (this.openCount >= this.options.maxOpen) {
      const evict = this.leastRecentlyUsedOpen();
      if (evict) await this.finish(evict);
    }
    const firstBatch = group.pending;
    group.pending = [];
    this.pendingTotal -= firstBatch.length;
    const opened = await this.options.openFile(group, firstBatch);
    group.appender = opened.appender;
    group.finalPath = opened.finalPath;
    group.tempPath = opened.tempPath;
    this.openCount++;
  }

  private async flush(group: WriterGroup): Promise<void> {
    const batch = group.pending;
    group.pending = [];
    await group.appender!.append(batch);
  }

  private async finish(group: WriterGroup): Promise<void> {
    // finishAll() walks a snapshot; a group may already have been closed by
    // an eviction that opening an earlier group triggered.
    if (!this.groups.has(group.key)) return;
    if (!group.appender) {
      await this.open(group);
    }
    if (group.pending.length > 0) {
      await this.flush(group);
    }
    const appender = group.appender!;
    const rows = appender.rowCount;
    // close() validates and throws (quarantining the file) on failure; the
    // caller aborts the rest of the pool in that case.
    await appender.close();
    this.openCount--;
    this.groups.delete(group.key);
    await fs.rename(group.tempPath!, group.finalPath!);
    this.options.onClosed(group, rows, group.finalPath!);
  }
}
