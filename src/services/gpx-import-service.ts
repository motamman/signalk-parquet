/**
 * GPX Import Service
 *
 * Imports historical GPS tracks from GPX files into the Hive-partitioned
 * parquet store. Each <trkpt> with a valid <time> is expanded into SignalK
 * delta-style records for the configured paths (position, SOG, COG,
 * altitude) and written directly as parquet files, bypassing the SQLite
 * buffer (bulk historical load).
 *
 * Follows the same progress-tracking / cancellable-job pattern as
 * MigrationService.
 *
 * When adding a new SignalK path:
 *   1. Extend the GpxImportPath union below
 *   2. Append to DEFAULT_IMPORT_PATHS
 *   3. Add a case in pointToValue() that maps the <trkpt> to the value
 *      in SignalK units (m/s, radians, etc.)
 *   4. Extend the GpxPoint interface in gpx-parser.ts if a new tag must
 *      be parsed out of the GPX
 */

import * as fs from 'fs-extra';
import * as path from 'path';
import { glob } from 'glob';
import { ServerAPI } from '@signalk/server-api';
import { DataRecord, ParquetWriter } from '../types';
import { HivePathBuilder } from '../utils/hive-path-builder';
import { parseGpx, GpxPoint } from '../utils/gpx-parser';
import { IMPORT_JOB_TTL_MS } from '../constants';

export type GpxImportPath =
  | 'navigation.position'
  | 'navigation.speedOverGround'
  | 'navigation.courseOverGroundTrue'
  | 'navigation.gnss.antennaAltitude';

export const DEFAULT_IMPORT_PATHS: GpxImportPath[] = [
  'navigation.position',
  'navigation.speedOverGround',
  'navigation.courseOverGroundTrue',
  'navigation.gnss.antennaAltitude',
];

export interface GpxImportConfig {
  sourceDirectory?: string; // Scan recursively for .gpx files
  sourceFiles?: string[]; // Explicit list (absolute paths). Used if set.
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
  phase: 'scan' | 'parse' | 'write';
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
}

export interface GpxScanResult {
  totalFiles: number;
  totalSize: number;
  files: Array<{ path: string; size: number }>;
}

const importJobs = new Map<string, GpxImportProgress>();

function scheduleImportJobCleanup(jobId: string) {
  setTimeout(() => {
    const job = importJobs.get(jobId);
    if (job && job.status !== 'running') {
      importJobs.delete(jobId);
    }
  }, IMPORT_JOB_TTL_MS);
}

export class GpxImportService {
  private readonly app: ServerAPI;
  private readonly parquetWriter: ParquetWriter;
  private readonly hivePathBuilder: HivePathBuilder;

  // Per-job cancellation. A set (rather than a single flag) so concurrent
  // imports don't trample each other's state — cancelling one job never
  // cancels another.
  private readonly cancelledJobs: Set<string> = new Set();

  constructor(app: ServerAPI, parquetWriter: ParquetWriter) {
    this.app = app;
    this.parquetWriter = parquetWriter;
    this.hivePathBuilder = new HivePathBuilder();
  }

  /**
   * Scan a directory for .gpx files (non-destructive dry run).
   */
  async scan(sourceDirectory: string): Promise<GpxScanResult> {
    const pattern = path.join(sourceDirectory, '**', '*.gpx');
    const matches = await glob(pattern, { nocase: true });

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
        const pattern = path.join(config.sourceDirectory, '**', '*.gpx');
        const matches = await glob(pattern, { nocase: true });
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

      for (let i = 0; i < gpxFiles.length; i++) {
        if (this.cancelledJobs.has(jobId)) {
          progress.status = 'cancelled';
          progress.completedAt = new Date();
          scheduleImportJobCleanup(jobId);
          return;
        }

        const file = gpxFiles[i];
        progress.currentFile = path.basename(file);
        progress.processed = i + 1;
        progress.percent = Math.round(((i + 1) / gpxFiles.length) * 100);
        progress.phase = 'parse'; // reset before each file; importFile flips to 'write' once it starts emitting

        try {
          const stats = await fs.stat(file);
          const imported = await this.importFile(
            jobId,
            file,
            resolvedContext,
            config,
            progress,
            metadataCache
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

      progress.status = 'completed';
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
   * Parse a single GPX file and write its points to the parquet store.
   * Returns true if at least one parquet file was produced.
   */
  private async importFile(
    jobId: string,
    sourcePath: string,
    resolvedContext: string,
    config: GpxImportConfig,
    progress: GpxImportProgress,
    metadataCache: Map<string, object | undefined>
  ): Promise<boolean> {
    const xml = await fs.readFile(sourcePath, 'utf8');
    const parsed = parseGpx(xml);
    progress.pointsParsed += parsed.totalPoints;

    if (parsed.totalPoints === 0) {
      return false;
    }

    // Flatten all trkpts across all tracks; keep only points with a timestamp
    const allPoints: GpxPoint[] = [];
    for (const trk of parsed.tracks) {
      for (const pt of trk.points) {
        if (pt.time) {
          allPoints.push(pt);
        }
      }
    }

    if (allPoints.length === 0) {
      return false;
    }

    // Group records by (signalkPath, dayKey) so each group becomes one parquet file.
    // dayKey uses UTC year+dayOfYear which matches the Hive partition granularity.
    type GroupKey = string; // `${signalkPath}|${year}|${dayOfYear}`
    const groups = new Map<
      GroupKey,
      { records: DataRecord[]; signalkPath: GpxImportPath; anchor: Date }
    >();

    for (const pt of allPoints) {
      const ts = pt.time!;
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
          path.basename(sourcePath),
          metadataCache.get(skPath)
        );

        const key: GroupKey = `${skPath}|${year}|${dayOfYear}`;
        let group = groups.get(key);
        if (!group) {
          group = { records: [], signalkPath: skPath, anchor: ts };
          groups.set(key, group);
        }
        group.records.push(record);
        progress.pointsWritten++;
      }
    }

    progress.phase = 'write';

    for (const [, group] of groups) {
      if (this.cancelledJobs.has(jobId)) return true; // partial import counts

      const filePath = this.buildHiveFilePath(
        config.targetDirectory,
        resolvedContext,
        group.signalkPath,
        group.anchor,
        config.filenamePrefix
      );

      await fs.ensureDir(path.dirname(filePath));
      const tempFilePath = filePath + '.tmp';

      try {
        // ParquetWriter.writeRecords validates the file before returning
        // (see validateParquetFile in parquet-writer.ts — checks minimum
        // size and round-trips through the reader) and throws on failure.
        // We don't re-validate here; on success the temp file is present
        // and safe to atomic-rename to the final destination.
        await this.parquetWriter.writeRecords(tempFilePath, group.records);
        await fs.rename(tempFilePath, filePath);
        progress.filesCreated.push(filePath);
        progress.recordsWritten += group.records.length;
      } catch (error) {
        try {
          await fs.remove(tempFilePath);
        } catch {
          // ignore cleanup failures
        }
        throw error;
      }
    }

    return groups.size > 0;
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
