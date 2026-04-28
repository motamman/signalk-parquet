/**
 * Compaction Service
 *
 * Merges per-day parquet files within a (tier, context, path, year) group
 * into a single per-year parquet file, then deletes the source day-files.
 *
 * Why: the live and import write paths emit one parquet per (path, UTC day).
 * Over multiple years a typical vessel ends up with many thousands of small
 * files in tier=raw, each carrying parquet's per-file overhead (schema,
 * page index, footer). DuckDB queries spanning years pay that overhead per
 * file, and the filesystem suffers metadata bloat.
 *
 * Scope: this is purely a layout transformation. Schema is preserved
 * exactly (`SELECT *` with `union_by_name=true`), records are sorted by
 * `signalk_timestamp`, and writes go through a temp-file + atomic-rename
 * pattern so a partial run never corrupts the partition.
 *
 * After compaction the year directory contains one file like
 *
 *   tier=raw/context=.../path=.../year=2024/year_compact_2024_<TS>.parquet
 *
 * and the `day=DDD/` subdirectories under it are removed. DuckDB's
 * hive-partitioning glob still finds the file; `day` becomes NULL for
 * compacted years, which matters only if a downstream consumer filters by
 * `day=…` directly. The History API filters by timestamp range, so
 * partition pruning at the year level still works and per-file min/max
 * stats handle the rest.
 *
 * Mirrors the same job-tracking / cancellable-job pattern as
 * MigrationService, AggregationService and GpxImportService.
 */

import * as fs from 'fs-extra';
import * as path from 'path';
import { glob } from 'glob';
import { ServerAPI } from '@signalk/server-api';
import { DuckDBPool } from '../utils/duckdb-pool';
import { HivePathBuilder, AggregationTier } from '../utils/hive-path-builder';
import { ConcurrencyLimiter } from '../utils/concurrency-limiter';

export interface CompactionConfig {
  baseDirectory: string;
  tier: AggregationTier;

  // Year cutoff: only compact (context, path, year) groups whose year is
  // strictly less than this. The route layer clamps this to <= current
  // calendar year so the in-progress year never gets touched.
  beforeYear: number;

  // Optional substring filter on the SignalK path (e.g. 'navigation.').
  pathFilter?: string;
}

export interface CompactionPlanGroup {
  tier: string;
  context: string;
  path: string;
  year: number;
  yearDir: string;
  // Source files captured at scan time. Reused at compact time to avoid
  // a second glob and to give the plan a stable shape.
  sourcePaths: string[];
  sourceFiles: number;
  sourceBytes: number;
}

export interface CompactionPlan {
  totalGroups: number;
  totalSourceFiles: number;
  totalSourceBytes: number;
  groups: CompactionPlanGroup[];
}

export interface CompactionProgress {
  jobId: string;
  status: 'scanning' | 'running' | 'completed' | 'cancelled' | 'error';
  phase: 'scan' | 'compact';
  processed: number;
  total: number;
  percent: number;
  currentGroup?: string;
  startTime: Date;
  completedAt?: Date;
  error?: string;
  groupsCompacted: number;
  groupsSkipped: number;
  filesRemoved: number;
  bytesBefore: number;
  bytesAfter: number;
  errors: string[];
}

const COMPACTION_OUTPUT_PREFIX = 'year_compact';
const COMPACTION_TEMP_SUFFIX = '.tmp';
// Sources are moved here (atomic renames within yearDir) before the
// new yearly file is published, so queries never see both at once.
// Cleared after the publish rename succeeds; restored on rollback.
const COMPACTION_TRASH_PREFIX = '.compaction-trash-';

// An empty parquet file (magic bytes + footer + schema) lands a few hundred
// bytes; anything visibly smaller than this means the writer produced
// nothing useful and we must not delete the sources.
const MIN_PLAUSIBLE_PARQUET_BYTES = 200;

const COMPACTION_JOB_TTL_MS = 60 * 60 * 1000; // 1 hour
const RENAME_RETRY_DELAY_MS = 200;

// Per-source delete retry: covers Windows transients (antivirus,
// search indexer, file explorer) holding a brief handle on the file.
// Exponential-ish backoff: 100ms, 200ms, 400ms, 800ms, 1600ms.
const SOURCE_DELETE_MAX_ATTEMPTS = 5;
const SOURCE_DELETE_BASE_DELAY_MS = 100;

// Cap parallel filesystem ops during scan so a multi-year tree with
// thousands of (context, path, year) groups doesn't fan out into an
// EMFILE storm or starve other plugin work.
const SCAN_CONCURRENCY = 8;

// Module-level so plugin.stop() can signal cancellation across any live
// CompactionService instance without needing a registry.
const compactionJobs = new Map<string, CompactionProgress>();
const cancelledJobIds: Set<string> = new Set();

/**
 * True if any compaction job is currently scanning or running. Used to
 * serialize jobs at the start endpoint: two concurrent runs could pick
 * up the same (tier, context, path, year) and merge it twice into
 * separate `year_compact_*.parquet` files, making duplicate rows
 * query-visible.
 */
function isAnyCompactionActive(): boolean {
  for (const job of compactionJobs.values()) {
    if (job.status === 'scanning' || job.status === 'running') return true;
  }
  return false;
}

export class CompactionConflictError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CompactionConflictError';
  }
}

/**
 * True if the given parquet file is a compaction output (i.e. the
 * result of a previous run for this year-dir). Used both at scan time
 * to skip already-compacted year-dirs and at compact time as a
 * defense-in-depth check before merging.
 */
function isCompactionOutput(parquetPath: string): boolean {
  return path.basename(parquetPath).startsWith(`${COMPACTION_OUTPUT_PREFIX}_`);
}

function scheduleJobCleanup(jobId: string) {
  setTimeout(() => {
    const job = compactionJobs.get(jobId);
    if (job && job.status !== 'running' && job.status !== 'scanning') {
      compactionJobs.delete(jobId);
      cancelledJobIds.delete(jobId);
    }
  }, COMPACTION_JOB_TTL_MS);
}

/**
 * Mark every running/scanning compaction job for cancellation. Called
 * from plugin.stop() so a SignalK shutdown does not leave a job spinning
 * past the next group boundary. Does not await: a single in-flight
 * DuckDB COPY is uninterruptible, but the per-group loop will exit on
 * the next iteration check.
 */
export function signalShutdownAllCompactionJobs(): number {
  let signalled = 0;
  for (const [jobId, job] of compactionJobs) {
    if (job.status === 'running' || job.status === 'scanning') {
      cancelledJobIds.add(jobId);
      signalled++;
    }
  }
  return signalled;
}

function countActiveCompactionJobs(): number {
  let active = 0;
  for (const job of compactionJobs.values()) {
    if (job.status === 'scanning' || job.status === 'running') active++;
  }
  return active;
}

/**
 * Signal cancellation to every active job and wait for them to reach a
 * terminal state, bounded by `timeoutMs`. Use this from plugin.stop()
 * so DuckDBPool.shutdown() doesn't run while a COPY is still in flight
 * — that race turns a clean stop into a failed compaction at best, or
 * a partial temp file at worst.
 *
 * Returns counts so the caller can log what happened. `remaining > 0`
 * means a job was still running at timeout; the plugin proceeds with
 * shutdown anyway, since blocking forever is worse than leaking a job.
 */
export async function quiesceAllCompactionJobs(
  timeoutMs = 30_000
): Promise<{ signalled: number; quiesced: number; remaining: number }> {
  const signalled = signalShutdownAllCompactionJobs();
  if (signalled === 0) return { signalled: 0, quiesced: 0, remaining: 0 };

  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const remaining = countActiveCompactionJobs();
    if (remaining === 0) {
      return { signalled, quiesced: signalled, remaining: 0 };
    }
    await new Promise(resolve => setTimeout(resolve, 100));
  }

  const remaining = countActiveCompactionJobs();
  return { signalled, quiesced: signalled - remaining, remaining };
}

/**
 * Remove stranded `*.tmp` files left behind by a SignalK crash mid-COPY.
 * Safe to call at every plugin start: only files matching the compaction
 * temp pattern under the data directory are removed.
 */
export async function cleanupStrandedCompactionTempFiles(
  app: ServerAPI,
  baseDirectory: string
): Promise<{ removed: number }> {
  if (!(await fs.pathExists(baseDirectory))) return { removed: 0 };
  const pattern = path.join(
    baseDirectory,
    'tier=*',
    'context=*',
    'path=*',
    'year=*',
    `${COMPACTION_OUTPUT_PREFIX}_*${COMPACTION_TEMP_SUFFIX}`
  );
  const stragglers = await glob(pattern);
  let removed = 0;
  for (const f of stragglers) {
    try {
      await fs.remove(f);
      removed++;
      app.debug(`Removed stranded compaction temp file: ${f}`);
    } catch (err) {
      app.error(
        `Failed to remove stranded compaction temp file ${f}: ${(err as Error).message}`
      );
    }
  }
  if (removed > 0) {
    app.debug(
      `Compaction startup cleanup: removed ${removed} stranded *.tmp file(s)`
    );
  }
  return { removed };
}

/**
 * Recover from stale `.compaction-trash-*` directories left by a crash
 * between move-to-trash and rename-to-published. Two cases:
 *
 *   - Yearly file present in the parent: the publish completed before
 *     the crash; trash holds duplicate sources that the post-publish
 *     cleanup never got to remove. Delete the trash dir.
 *   - Yearly file absent: the crash happened mid-move (or after move
 *     but before publish). Restore the trashed files back to their
 *     mirrored locations under the year-dir, then delete the empty
 *     trash dir.
 *
 * This runs early in plugin start, before any new data writes can land
 * in day-dirs, so a restore can't clobber anything.
 */
export async function recoverStrandedCompactionTrash(
  app: ServerAPI,
  baseDirectory: string
): Promise<{ restored: number; cleaned: number; failed: number }> {
  if (!(await fs.pathExists(baseDirectory))) {
    return { restored: 0, cleaned: 0, failed: 0 };
  }
  const pattern = path.join(
    baseDirectory,
    'tier=*',
    'context=*',
    'path=*',
    'year=*',
    `${COMPACTION_TRASH_PREFIX}*`
  );
  const trashDirs = await glob(pattern);

  let restored = 0;
  let cleaned = 0;
  let failed = 0;

  for (const trashDir of trashDirs) {
    const yearDir = path.dirname(trashDir);
    const compactedSiblings = await glob(
      path.join(yearDir, `${COMPACTION_OUTPUT_PREFIX}_*.parquet`)
    );

    if (compactedSiblings.length > 0) {
      // Publish completed; trash is post-publish residue.
      try {
        await fs.remove(trashDir);
        cleaned++;
        app.debug(`Removed post-publish compaction trash: ${trashDir}`);
      } catch (err) {
        failed++;
        app.error(
          `Failed to remove compaction trash ${trashDir}: ${(err as Error).message}`
        );
      }
      continue;
    }

    // Pre-publish trash: restore by mirroring back to yearDir.
    try {
      const trashedFiles = await glob(path.join(trashDir, '**', '*.parquet'));
      for (const trashed of trashedFiles) {
        const relative = path.relative(trashDir, trashed);
        const original = path.join(yearDir, relative);
        await fs.ensureDir(path.dirname(original));
        await fs.rename(trashed, original);
      }
      await fs.remove(trashDir);
      restored++;
      app.debug(
        `Restored ${trashedFiles.length} pre-publish compaction file(s) from ${trashDir}`
      );
    } catch (err) {
      failed++;
      app.error(
        `Failed to restore compaction trash ${trashDir}: ${(err as Error).message}`
      );
    }
  }

  if (restored + cleaned + failed > 0) {
    app.debug(
      `Compaction trash recovery: restored=${restored}, cleaned=${cleaned}, failed=${failed}`
    );
  }
  return { restored, cleaned, failed };
}

export class CompactionService {
  private readonly app: ServerAPI;
  private readonly hivePathBuilder: HivePathBuilder;

  constructor(app: ServerAPI) {
    this.app = app;
    this.hivePathBuilder = new HivePathBuilder();
  }

  /**
   * Walk the Hive layout for a given tier and return what would be
   * compacted. Groups with only one file are not included (already
   * compact). Non-destructive — safe to call repeatedly.
   */
  async scan(config: CompactionConfig): Promise<CompactionPlan> {
    const groups = await this.findCompactableGroups(config);
    return {
      totalGroups: groups.length,
      totalSourceFiles: groups.reduce((s, g) => s + g.sourceFiles, 0),
      totalSourceBytes: groups.reduce((s, g) => s + g.sourceBytes, 0),
      groups,
    };
  }

  async compact(config: CompactionConfig): Promise<string> {
    // Refuse to start a second job while one is already scanning or
    // running. Two concurrent jobs against overlapping config can both
    // pick up the same year-dir, write separate compaction outputs,
    // and double the rows for that group.
    if (isAnyCompactionActive()) {
      throw new CompactionConflictError(
        'Another compaction job is already running. Wait for it to finish or cancel it first.'
      );
    }

    const jobId = `compact_${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;

    const progress: CompactionProgress = {
      jobId,
      status: 'scanning',
      phase: 'scan',
      processed: 0,
      total: 0,
      percent: 0,
      startTime: new Date(),
      groupsCompacted: 0,
      groupsSkipped: 0,
      filesRemoved: 0,
      bytesBefore: 0,
      bytesAfter: 0,
      errors: [],
    };

    compactionJobs.set(jobId, progress);

    this.app.debug(
      `Compaction job ${jobId} starting: tier=${config.tier}, beforeYear=${config.beforeYear}` +
        (config.pathFilter ? `, pathFilter='${config.pathFilter}'` : '')
    );

    this.run(jobId, config)
      .catch(error => {
        const job = compactionJobs.get(jobId);
        if (job) {
          job.status = 'error';
          job.error = (error as Error).message;
          job.completedAt = new Date();
        }
        this.app.error(
          `Compaction job ${jobId} failed: ${(error as Error).message}`
        );
      })
      .finally(() => {
        cancelledJobIds.delete(jobId);
        const job = compactionJobs.get(jobId);
        if (job && job.status === 'completed') {
          this.app.debug(
            `Compaction job ${jobId} completed: ` +
              `groupsCompacted=${job.groupsCompacted}, ` +
              `groupsSkipped=${job.groupsSkipped}, ` +
              `filesRemoved=${job.filesRemoved}, ` +
              `bytesBefore=${job.bytesBefore}, ` +
              `bytesAfter=${job.bytesAfter}, ` +
              `errors=${job.errors.length}`
          );
        }
      });

    return jobId;
  }

  private async run(jobId: string, config: CompactionConfig): Promise<void> {
    const progress = compactionJobs.get(jobId);
    if (!progress) return;

    try {
      progress.phase = 'scan';
      progress.status = 'scanning';

      const groups = await this.findCompactableGroups(config);
      progress.total = groups.length;

      if (groups.length === 0) {
        progress.status = 'completed';
        progress.completedAt = new Date();
        scheduleJobCleanup(jobId);
        return;
      }

      progress.phase = 'compact';
      progress.status = 'running';

      for (let i = 0; i < groups.length; i++) {
        if (cancelledJobIds.has(jobId)) {
          progress.status = 'cancelled';
          progress.completedAt = new Date();
          this.app.debug(
            `Compaction job ${jobId} cancelled after ${i}/${groups.length} groups`
          );
          scheduleJobCleanup(jobId);
          return;
        }

        const group = groups[i];
        progress.currentGroup = `${group.tier} ${group.context} ${group.path} year=${group.year}`;
        progress.processed = i + 1;
        progress.percent = Math.round(((i + 1) / groups.length) * 100);

        try {
          const result = await this.compactGroup(group, jobId);
          if (result.compacted) {
            progress.groupsCompacted++;
            progress.bytesBefore += group.sourceBytes;
            progress.bytesAfter += result.outputBytes;
            progress.filesRemoved += result.filesRemoved;
            for (const residual of result.residualSources) {
              progress.errors.push(
                `Residual source after compaction: ${residual} (could not be deleted)`
              );
            }
          } else {
            progress.groupsSkipped++;
          }
        } catch (error) {
          const errorMsg = `Failed to compact ${progress.currentGroup}: ${(error as Error).message}`;
          this.app.error(errorMsg);
          progress.errors.push(errorMsg);
        }
      }

      progress.status = 'completed';
      progress.completedAt = new Date();
      scheduleJobCleanup(jobId);
    } catch (error) {
      progress.status = 'error';
      progress.error = (error as Error).message;
      progress.completedAt = new Date();
      scheduleJobCleanup(jobId);
    }
  }

  /**
   * Walk the Hive layout tier=<T>/context=.../path=.../year=... and
   * return one group per year-directory that has more than one parquet
   * file under it.
   *
   * Scale note: result size is O(#contexts × #paths × #years). On a
   * typical SignalK install (one vessel, ~hundreds of paths, single-digit
   * years) this fits comfortably in memory.
   */
  private async findCompactableGroups(
    config: CompactionConfig
  ): Promise<CompactionPlanGroup[]> {
    const tierRoot = path.join(config.baseDirectory, `tier=${config.tier}`);
    if (!(await fs.pathExists(tierRoot))) {
      return [];
    }

    const yearDirs = await glob(
      path.join(tierRoot, 'context=*', 'path=*', 'year=*')
    );

    // Stat each year directory; filter to actual directories matching
    // the cutoff and the optional path substring filter. Capped at
    // SCAN_CONCURRENCY to avoid EMFILE on large trees.
    const limiter = new ConcurrencyLimiter(SCAN_CONCURRENCY);
    const candidates = await limiter.map(yearDirs, async yearDir => {
      const stat = await fs.stat(yearDir).catch(() => null);
      if (!stat || !stat.isDirectory()) return null;
      const parsed = this.parseYearDir(yearDir);
      if (!parsed) return null;
      if (parsed.year >= config.beforeYear) return null;
      if (config.pathFilter && !parsed.path.includes(config.pathFilter)) {
        return null;
      }
      return { yearDir, parsed };
    });

    // For each surviving candidate, list its day-partition parquet
    // files and sum bytes. The glob is intentionally narrow:
    // `day=*/*.parquet` only picks up live day partitions, so siblings
    // like `year_compact_*.parquet`, `repaired/`, `quarantine/` etc.
    // never enter the source list. Year-dirs that already contain a
    // compaction-output file (top-level sibling) are skipped entirely
    // — re-merging them would duplicate rows that already live in the
    // compacted output (the previous run wrote one and then either
    // succeeded entirely or left residual sources behind; either way,
    // no rewrite is safe without manual cleanup).
    const groupResults = await limiter.map(
      candidates.filter(
        (
          c
        ): c is {
          yearDir: string;
          parsed: NonNullable<ReturnType<CompactionService['parseYearDir']>>;
        } => c !== null
      ),
      async ({ yearDir, parsed }) => {
        const existing = await glob(
          path.join(yearDir, `${COMPACTION_OUTPUT_PREFIX}_*.parquet`)
        );
        if (existing.length > 0) return null;
        const parquetFiles = await glob(
          path.join(yearDir, 'day=*', '*.parquet')
        );
        if (parquetFiles.length <= 1) return null;
        const sizes = await limiter.map(parquetFiles, f =>
          fs
            .stat(f)
            .then(s => s.size)
            .catch(() => 0)
        );
        const sourceBytes = sizes.reduce((s, n) => s + n, 0);
        const group: CompactionPlanGroup = {
          tier: config.tier,
          context: parsed.context,
          path: parsed.path,
          year: parsed.year,
          yearDir,
          sourcePaths: parquetFiles,
          sourceFiles: parquetFiles.length,
          sourceBytes,
        };
        return group;
      }
    );

    const groups = groupResults.filter(
      (g): g is CompactionPlanGroup => g !== null
    );

    // Stable order: oldest year first, then alphabetical. Predictable
    // progress + a cancelled job leaves a clean tail of unprocessed groups.
    groups.sort(
      (a, b) =>
        a.year - b.year ||
        a.context.localeCompare(b.context) ||
        a.path.localeCompare(b.path)
    );

    return groups;
  }

  /**
   * Parse the trailing four segments of a Hive year directory into the
   * tier/context/path/year tuple. Returns null on shape mismatch.
   *
   * Example input:  "/data/tier=raw/context=vessels__urn-mrn-…/path=navigation__position/year=2024"
   * Example output: { context: "vessels.urn:mrn:…", path: "navigation.position", year: 2024 }
   */
  private parseYearDir(
    yearDir: string
  ): { context: string; path: string; year: number } | null {
    const parts = yearDir.split(/[\\/]/);
    const yearSeg = parts[parts.length - 1];
    const pathSeg = parts[parts.length - 2];
    const ctxSeg = parts[parts.length - 3];

    if (
      !yearSeg?.startsWith('year=') ||
      !pathSeg?.startsWith('path=') ||
      !ctxSeg?.startsWith('context=')
    ) {
      return null;
    }

    const year = parseInt(yearSeg.slice('year='.length), 10);
    if (!Number.isFinite(year)) return null;

    return {
      context: this.hivePathBuilder.unsanitizeContext(
        ctxSeg.slice('context='.length)
      ),
      path: this.hivePathBuilder.unsanitizePath(pathSeg.slice('path='.length)),
      year,
    };
  }

  /**
   * Merge all parquet files under one (tier, context, path, year) group
   * into a single output file, then remove the sources.
   *
   * Publish sequence (atomic from a reader's perspective):
   *   1. DuckDB COPY into `<output>.tmp`, size-checked.
   *   2. Source files are *moved* (rename within yearDir) into a trash
   *      directory: `<yearDir>/.compaction-trash-<jobId>/`. Mirrors the
   *      original day=DDD/ structure so rollback is just a reverse
   *      rename.
   *   3. The temp file is renamed to its final `year_compact_*.parquet`
   *      name. This is the publish point.
   *   4. The trash directory is recursively removed.
   *
   * Why moves first, then publish: between steps 3 and 4 in a
   * "publish-then-delete" scheme, queries see both the new yearly file
   * and every still-present source — duplicate rows on the wire. The
   * trash-first scheme makes the year-dir's queryable contents flip
   * atomically: before the publish rename a query sees the originals
   * (less any briefly-renamed files mid-step-2), after it the query
   * sees only the yearly file.
   *
   * Failure handling:
   *   - Step 1/2 failure (pre-publish): reverse any moves that did
   *     succeed, delete temp, delete trash dir. Sources end back where
   *     they started; no data lost. The group surfaces an error.
   *   - Step 3 failure: same rollback as above.
   *   - Step 4 failure (post-publish): data is committed; the yearly
   *     file holds everything. Failure to clean trash is logged and
   *     surfaced via `residualSources` (now meaning "files left in
   *     trash that the operator may want to remove"). Startup sweep
   *     handles these on next plugin start.
   */
  private async compactGroup(
    group: CompactionPlanGroup,
    jobId: string
  ): Promise<{
    compacted: boolean;
    outputBytes: number;
    filesRemoved: number;
    residualSources: string[];
  }> {
    const sourceFiles = group.sourcePaths;
    if (sourceFiles.length <= 1) {
      return {
        compacted: false,
        outputBytes: 0,
        filesRemoved: 0,
        residualSources: [],
      };
    }

    // Defense in depth: scan-time filtering is supposed to exclude
    // year-dirs that already hold a compaction output, but a stale
    // plan or hand-edited input could still get here. Refuse rather
    // than re-merge and duplicate rows.
    if (sourceFiles.some(isCompactionOutput)) {
      throw new Error(
        `Refusing to compact ${group.yearDir}: a previous compaction output is present. Remove it manually before re-running.`
      );
    }

    // Filenames in our layout are ASCII (UUID + timestamp + path-derived
    // basename). Refuse to proceed if any filename — source or
    // destination — contains a single quote, since we splice them into
    // a SQL string literal below.
    const stamp = formatCompactionStamp(new Date());
    const randomSuffix = Math.random().toString(36).substring(2, 6);
    const outputFile = path.join(
      group.yearDir,
      `${COMPACTION_OUTPUT_PREFIX}_${group.year}_${stamp}_${randomSuffix}.parquet`
    );
    const tempFile = outputFile + COMPACTION_TEMP_SUFFIX;

    // Forward slashes in SQL paths so DuckDB on Windows is happy. Single
    // quotes in path components are doubled for the SQL string-literal
    // form, so an apostrophe in `outputDirectory` (or any parent dir)
    // doesn't fail compaction outright.
    const toSqlLiteralBody = (p: string): string =>
      p.split(path.sep).join('/').replace(/'/g, "''");
    const fileListSql = sourceFiles
      .map(f => `'${toSqlLiteralBody(f)}'`)
      .join(', ');
    const tempFileSql = toSqlLiteralBody(tempFile);

    // union_by_name=true: a column added partway through the year (e.g.
    // a new value_<key> exploded from value_json) merges cleanly. The
    // result has the union of columns; older rows have NULL where the
    // newer column is absent. Snappy compression matches the existing
    // consolidate-parquet.sh convention.
    const query = `
      COPY (
        SELECT * FROM read_parquet([${fileListSql}], union_by_name=true)
        ORDER BY signalk_timestamp
      ) TO '${tempFileSql}'
        (FORMAT PARQUET, COMPRESSION SNAPPY);
    `;

    const connection = await DuckDBPool.getConnection();
    try {
      await connection.runAndReadAll(query);
    } finally {
      connection.disconnectSync();
    }

    // Guard against a broken-but-present temp file. The size guard is
    // the load-bearing check; we deliberately do not assume DuckDB will
    // throw on an empty COPY since that contract is undocumented.
    const stat = await fs.stat(tempFile);
    if (stat.size < MIN_PLAUSIBLE_PARQUET_BYTES) {
      await fs.remove(tempFile).catch(err => {
        this.app.error(
          `Failed to remove undersized temp file ${tempFile}: ${(err as Error).message}`
        );
      });
      throw new Error(
        `Compaction produced an implausibly small file (${stat.size} bytes); leaving sources untouched.`
      );
    }

    // Step 2: move sources into the trash dir (mirroring day=DDD/
    // structure) and step 3: publish-rename. Wrapped in try/catch so
    // any failure rolls the moves back and leaves the year-dir
    // exactly as we found it.
    const trashDir = path.join(
      group.yearDir,
      `${COMPACTION_TRASH_PREFIX}${jobId}`
    );
    const completedMoves: Array<{ from: string; to: string }> = [];
    try {
      await fs.ensureDir(trashDir);
      for (const f of sourceFiles) {
        if (f === outputFile) continue;
        const relative = path.relative(group.yearDir, f);
        const trashed = path.join(trashDir, relative);
        await fs.ensureDir(path.dirname(trashed));
        await fs.rename(f, trashed);
        completedMoves.push({ from: f, to: trashed });
      }
      await this.renameWithRetry(tempFile, outputFile);
    } catch (err) {
      // Reverse moves so callers see the year-dir untouched.
      for (const m of completedMoves.reverse()) {
        try {
          await fs.ensureDir(path.dirname(m.from));
          await fs.rename(m.to, m.from);
        } catch (restoreErr) {
          this.app.error(
            `Compaction rollback: failed to restore ${m.to} -> ${m.from}: ${(restoreErr as Error).message}`
          );
        }
      }
      await fs.remove(tempFile).catch(() => undefined);
      await fs.remove(trashDir).catch(() => undefined);
      throw err;
    }

    // Step 4: post-publish cleanup. Data is committed; failures here
    // are logged and surfaced as residuals but don't fail the group.
    const movedCount = completedMoves.length;
    const residualSources: string[] = [];
    let removed = movedCount;
    try {
      await this.removeWithRetry(trashDir);
    } catch (err) {
      this.app.error(
        `Failed to remove compaction trash dir ${trashDir} (${SOURCE_DELETE_MAX_ATTEMPTS} attempts): ${(err as Error).message}`
      );
      residualSources.push(trashDir);
      removed = 0;
    }

    await this.removeEmptyDayDirs(group.yearDir);

    return {
      compacted: true,
      outputBytes: stat.size,
      filesRemoved: removed,
      residualSources,
    };
  }

  /**
   * Rename with one retry on EBUSY. On Windows a reader (DuckDB query,
   * antivirus, file explorer preview) can briefly hold the destination
   * path; a single backoff-and-retry covers the common case without
   * making us wait indefinitely.
   */
  private async renameWithRetry(
    tempFile: string,
    outputFile: string
  ): Promise<void> {
    try {
      await fs.rename(tempFile, outputFile);
    } catch (err) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code !== 'EBUSY' && code !== 'EPERM') throw err;
      this.app.debug(
        `rename(${tempFile} -> ${outputFile}) failed with ${code}, retrying once`
      );
      await new Promise(resolve => setTimeout(resolve, RENAME_RETRY_DELAY_MS));
      await fs.rename(tempFile, outputFile);
    }
  }

  /**
   * Remove a file or directory with exponential backoff on transient
   * errors. Windows readers (antivirus, search indexer, explorer
   * preview) can hold a brief handle; one delete attempt isn't enough.
   * After all attempts the last error is rethrown so the caller can
   * record it (e.g. as a residual). Permanent errors (ENOENT, etc.)
   * are not retried.
   */
  private async removeWithRetry(target: string): Promise<void> {
    let lastErr: unknown;
    for (let attempt = 0; attempt < SOURCE_DELETE_MAX_ATTEMPTS; attempt++) {
      try {
        await fs.remove(target);
        return;
      } catch (err) {
        const code = (err as NodeJS.ErrnoException).code;
        if (code === 'ENOENT') return;
        if (code !== 'EBUSY' && code !== 'EPERM') throw err;
        lastErr = err;
        if (attempt < SOURCE_DELETE_MAX_ATTEMPTS - 1) {
          const delay = SOURCE_DELETE_BASE_DELAY_MS * 2 ** attempt;
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }
    throw lastErr;
  }

  private async removeEmptyDayDirs(yearDir: string): Promise<void> {
    const entries = await fs.readdir(yearDir).catch(() => []);
    for (const entry of entries) {
      if (!entry.startsWith('day=')) continue;
      const dayDir = path.join(yearDir, entry);
      const dayEntries = await fs.readdir(dayDir).catch(() => []);
      if (dayEntries.length === 0) {
        await fs.remove(dayDir).catch(err => {
          this.app.debug(
            `Failed to remove empty day dir ${dayDir}: ${(err as Error).message}`
          );
        });
      }
    }
  }

  getProgress(jobId: string): CompactionProgress | null {
    return compactionJobs.get(jobId) || null;
  }

  cancel(jobId: string): boolean {
    const job = compactionJobs.get(jobId);
    if (job && (job.status === 'running' || job.status === 'scanning')) {
      cancelledJobIds.add(jobId);
      return true;
    }
    return false;
  }

  getJobIds(): string[] {
    return Array.from(compactionJobs.keys());
  }
}

/**
 * Format the timestamp suffix for compacted-file basenames as
 * `YYYYMMDDTHHMMSS` (UTC). Explicit format to avoid the silent
 * truncation that comes from slicing toISOString().
 */
function formatCompactionStamp(d: Date): string {
  const pad = (n: number) => n.toString().padStart(2, '0');
  return (
    `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}` +
    `T${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}`
  );
}
