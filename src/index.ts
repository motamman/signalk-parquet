import * as fs from 'fs-extra';
import * as path from 'path';
import { fork, ChildProcess } from 'child_process';
import { Router } from 'express';
import { ParquetWriter, quarantineEmptyParquetFiles } from './parquet-writer';
import { registerHistoryApiRoute } from './HistoryAPI';
import { registerApiRoutes } from './api-routes';
import {
  cleanupStrandedCompactionTempFiles,
  quiesceAllCompactionJobs,
  recoverStrandedCompactionTrash,
} from './services/compaction-service';
import { CACHE_SIZE } from './config/cache-defaults';
import { SignalKPlugin, PluginConfig, PluginState, PathConfig } from './types';
import { Context, SourceRef, Timestamp, Path } from '@signalk/server-api';
import {
  loadWebAppConfig,
  initializeCommandState,
  setCurrentCommands,
  startThresholdMonitoring,
  stopThresholdMonitoring,
} from './commands';
import {
  initializeCloudSDK,
  createCloudClient,
  subscribeToCommandPaths,
  updateDataSubscriptions,
  initializeRegimenStates,
  saveAllBuffers,
  uploadAllConsolidatedFilesToS3,
  uploadConsolidatedFilesToS3,
} from './data-handler';
import { ServerAPI } from '@signalk/server-api';
import { DuckDBPool } from './utils/duckdb-pool';
import { LRUCache } from './utils/lru-cache';
import { resolveCustomS3Endpoint } from './utils/cloud-endpoint';
import {
  registerHistoryApiProvider,
  unregisterHistoryApiProvider,
} from './history-provider';
import { SQLiteBuffer } from './utils/sqlite-buffer';
import { ParquetExportService } from './services/parquet-export-service';
import {
  AggregationService,
  AggregationConfig,
  AggregationResult,
  buildPerTierRetention,
} from './services/aggregation-service';
import {
  PathRetentionRule,
  validatePathRetentionRules,
} from './utils/retention-rules';
import { AutoDiscoveryService } from './services/auto-discovery';

// How long plugin.stop() waits for an aggregation worker to finish its
// in-flight COPY and exit after a cooperative shutdown request, before
// resorting to SIGKILL. Per-group COPYs normally take seconds; a worker
// still alive after this is stuck.
const AGGREGATION_WORKER_SHUTDOWN_GRACE_MS = 15_000;

/**
 * Validate `pathRetentionOverrides` from the persisted config. Bad
 * entries are dropped (with a logged error) rather than crashing
 * plugin start; the SignalK admin UI enforces shape via JSON Schema
 * for UI-driven changes, but a hand-edited config can bypass that.
 */
function parsePathRetentionOverrides(
  raw: unknown,
  app: ServerAPI
): PathRetentionRule[] | undefined {
  if (raw === undefined || raw === null) return undefined;
  const { rules, errors } = validatePathRetentionRules(raw);
  for (const err of errors) {
    app.error(`[Retention] Dropping invalid override: ${err}`);
  }
  return rules.length > 0 ? rules : undefined;
}

/**
 * Run the daily aggregation in a short-lived forked process.
 *
 * The aggregation makes tens of thousands of DuckDB `read_parquet` calls whose
 * memory the allocator never returns to the OS in-process, so doing it in the
 * long-lived server ratchets ~¾ GB that sticks until restart (the midnight
 * OOM). A forked worker does the identical work and EXITS, so the OS reclaims
 * all of it. The worker's results are returned unchanged; a crash/timeout is
 * surfaced as a failed result so the caller skips retention cleanup (as it
 * already does when in-process aggregation fails).
 */
function runAggregationInWorker(
  input: {
    config: AggregationConfig;
    dataDir: string;
    dateISO: string;
    angularPathNames: string[];
  },
  app: ServerAPI,
  activeWorkers?: Set<ChildProcess>
): Promise<AggregationResult[]> {
  return new Promise(resolve => {
    const workerPath = path.join(__dirname, 'aggregation-worker.js');
    const child = fork(workerPath);
    activeWorkers?.add(child);
    let settled = false;
    const TIMEOUT_MS = 30 * 60 * 1000;

    const finishFailure = (reason: string): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      app.error(`[DailyExport] Aggregation worker ${reason}`);
      try {
        child.kill('SIGKILL');
      } catch {
        // already gone
      }
      resolve([
        {
          sourceTier: 'raw',
          targetTier: '5s',
          filesProcessed: 0,
          recordsAggregated: 0,
          filesCreated: 0,
          duration: 0,
          errors: [`aggregation worker ${reason}`],
        },
      ]);
    };

    const timer = setTimeout(() => finishFailure('timed out'), TIMEOUT_MS);

    child.on(
      'message',
      (msg: {
        type?: string;
        level?: string;
        msg?: string;
        message?: string;
        results?: AggregationResult[];
      }) => {
        if (!msg || typeof msg !== 'object') return;
        if (msg.type === 'log') {
          if (msg.level === 'error') app.error(msg.msg || '');
          else app.debug(msg.msg || '');
        } else if (msg.type === 'result') {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          resolve(msg.results || []);
        } else if (msg.type === 'error') {
          finishFailure(`errored: ${msg.message}`);
        }
      }
    );

    child.on('exit', code => {
      activeWorkers?.delete(child);
      if (!settled) finishFailure(`exited early (code ${code})`);
    });
    child.on('error', err => {
      activeWorkers?.delete(child);
      finishFailure(`spawn error: ${err.message}`);
    });

    child.send(input);
  });
}

export default function (app: ServerAPI): SignalKPlugin {
  const plugin: SignalKPlugin = {
    id: 'signalk-parquet',
    name: 'SignalK to Parquet',
    description:
      'Save SignalK marine data directly to Parquet files with regimen-based control',
    schema: {},
    start: () => {},
    stop: () => {},
    registerWithRouter: undefined,
  };

  // Plugin state
  const state: PluginState = {
    unsubscribes: [],
    dataBuffers: new LRUCache<string, import('./types').DataRecord[]>(
      CACHE_SIZE.DATA_BUFFER_MAX
    ),
    activeRegimens: new Set(),
    subscribedPaths: new Set(),
    saveInterval: undefined,
    consolidationInterval: undefined,
    parquetWriter: undefined,
    cloudClient: undefined,
    currentConfig: undefined,
    getDataDirPath: () => {
      if (!state.currentConfig) {
        throw new Error('Current config is not set');
      }
      return state.currentConfig.outputDirectory;
    },
    commandState: {
      registeredCommands: new Map(),
      putHandlers: new Map(),
    },
  };

  let currentPaths: PathConfig[] = [];

  plugin.start = async function (
    options: Partial<PluginConfig>
  ): Promise<void> {
    // Reconfigure runs stop() then start(); re-arm scheduled work.
    state.isStopping = false;
    state.activeAggregationWorkers ??= new Set<ChildProcess>();

    // Get vessel MMSI from SignalK
    // Cast to any for compatibility with different @signalk/server-api versions
    const vesselMMSI =
      (app.getSelfPath('mmsi') as any) ||
      (app.getSelfPath('name') as any) ||
      'unknown_vessel';

    // One-shot legacy retention migration. Pre-0.7.40 the home-port
    // action's savePluginOptions side effect baked retentionDays = 7
    // into saved configs even though cleanup was never scheduled, so
    // the value sat dormant. Now that cleanup actually runs, an
    // upgrader who never deliberately set retention would suddenly
    // start losing data. The configSchemaVersion sentinel lets us
    // distinguish "saved 7 from old default" (no stamp) from "saved 7
    // from explicit choice" (stamp present from prior run): once we
    // stamp, the migration condition fails forever for that install,
    // so re-entering 7 in the UI is honoured on next start.
    const needsLegacyRetentionMigration =
      options?.configSchemaVersion === undefined &&
      options?.retentionDays === 7 &&
      (!Array.isArray(options?.pathRetentionOverrides) ||
        options.pathRetentionOverrides.length === 0);

    if (needsLegacyRetentionMigration) {
      app.error(
        '[Retention] Detected legacy retentionDays=7 from a pre-0.7.40 install. ' +
          'Previous versions baked this default into saved configs as a side effect ' +
          'of the home-port action but never enforced it. Treating as 0 (keep forever) ' +
          'and persisting the change. To opt into 7-day retention, set ' +
          '"Retention Period (days)" to 7 in the plugin admin UI.'
      );
    }

    // Normalise dailyExportHour once at intake and let the single validated
    // value feed every consumer: Date.UTC scheduling rolls an out-of-range 24
    // over to next-day midnight while the SQLite catch-up clamps it to 23, so
    // an unvalidated value from a hand-edited config makes the scheduler and
    // the catch-up disagree about which day is eligible for export.
    const rawDailyExportHour = options?.dailyExportHour;
    const dailyExportHour =
      typeof rawDailyExportHour === 'number' &&
      Number.isInteger(rawDailyExportHour) &&
      rawDailyExportHour >= 0 &&
      rawDailyExportHour <= 23
        ? rawDailyExportHour
        : 4;
    if (
      rawDailyExportHour !== undefined &&
      rawDailyExportHour !== dailyExportHour
    ) {
      app.error(
        `[DailyExport] Invalid dailyExportHour ${JSON.stringify(rawDailyExportHour)}; using default 4 (must be an integer 0-23, UTC)`
      );
    }

    state.currentConfig = {
      bufferSize: options?.bufferSize || 1000,
      saveIntervalSeconds: options?.saveIntervalSeconds || 30,
      outputDirectory: options?.outputDirectory?.trim()
        ? options.outputDirectory.trim()
        : app.getDataDirPath(),
      filenamePrefix: options?.filenamePrefix || 'signalk_data',
      retentionDays: needsLegacyRetentionMigration
        ? 0
        : typeof options?.retentionDays === 'number' &&
            Number.isFinite(options.retentionDays) &&
            options.retentionDays >= 0
          ? options.retentionDays
          : 0,
      pathRetentionOverrides: parsePathRetentionOverrides(
        options?.pathRetentionOverrides,
        app
      ),
      configSchemaVersion: 1,
      fileFormat: options?.fileFormat || 'parquet',
      vesselMMSI: vesselMMSI,
      cloudUpload: (() => {
        // New config format already present
        if (options?.cloudUpload && (options.cloudUpload as any).provider) {
          return options.cloudUpload;
        }
        // Migrate from old s3Upload format
        const oldS3 = (options as any)?.s3Upload;
        if (oldS3) {
          return {
            provider: oldS3.enabled ? ('s3' as const) : ('none' as const),
            bucket: oldS3.bucket,
            region: oldS3.region,
            keyPrefix: oldS3.keyPrefix,
            accessKeyId: oldS3.accessKeyId,
            secretAccessKey: oldS3.secretAccessKey,
            deleteAfterUpload: oldS3.deleteAfterUpload,
          } as import('./types').CloudUploadConfig;
        }
        return { provider: 'none' as const };
      })(),
      homePortLatitude: options?.homePortLatitude || 0,
      homePortLongitude: options?.homePortLongitude || 0,
      setCurrentLocationAction: options?.setCurrentLocationAction || {
        setCurrentLocation: false,
      },
      // SQLite buffer options
      useSqliteBuffer: true, // Always use SQLite buffer
      bufferRetentionHours: options?.bufferRetentionHours || 48,
      useHivePartitioning: true, // Always use Hive partitioning
      // Auto-discovery configuration
      autoDiscovery: options?.autoDiscovery || {
        enabled: true,
        requireLiveData: true,
        maxAutoConfiguredPaths: 100,
        excludePatterns: ['design.*', 'communication.*', 'notifications.*'],
      },
      // Export batch size (how many records to export per cycle)
      exportBatchSize: options?.exportBatchSize || 50000,
      // Enable raw SQL queries via /api/query endpoint
      enableRawSql: options?.enableRawSql || false,
      // Daily export hour (0-23 UTC, default 4 AM), validated above
      dailyExportHour,
    };

    // Persist the migration so the configSchemaVersion sentinel lands
    // on disk; without this, every restart would re-trigger the
    // legacy detection. Also writes the corrected retentionDays = 0.
    if (needsLegacyRetentionMigration) {
      app.savePluginOptions(state.currentConfig, (err?: unknown) => {
        if (err) {
          app.error(
            `[Retention] Failed to persist legacy migration: ${(err as Error).message}`
          );
        }
      });
    }

    // Load webapp configuration including commands
    const webAppConfig = loadWebAppConfig(app);
    currentPaths = webAppConfig.paths;
    setCurrentCommands(webAppConfig.commands);

    // Initialize ParquetWriter
    state.parquetWriter = new ParquetWriter({
      format: state.currentConfig.fileFormat,
      app: app,
    });

    // Initialize SQLite buffer if enabled
    if (state.currentConfig.useSqliteBuffer) {
      // Use absolute path for buffer.db
      const dbPath = path.resolve(
        state.currentConfig.outputDirectory,
        'buffer.db'
      );
      app.debug(`[SQLite] Initializing buffer at: ${dbPath}`);

      try {
        state.sqliteBuffer = new SQLiteBuffer({
          dbPath,
          maxBatchSize: state.currentConfig.exportBatchSize || 50000,
          retentionHours: state.currentConfig.bufferRetentionHours,
        });

        // Verify the buffer is actually open
        if (!state.sqliteBuffer.isOpen()) {
          throw new Error('SQLite buffer created but database is not open');
        }

        app.debug(`[SQLite] Buffer initialized successfully at ${dbPath}`);
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        app.error(`[SQLite] Failed to initialize SQLite buffer: ${msg}`);
        app.error(
          `[SQLite] Falling back to in-memory LRU buffer (data is NOT crash-safe)`
        );
        state.sqliteBuffer = undefined;
        state.sqliteBufferError = msg;
      }

      // Initialize export service (requires working SQLite buffer)
      if (state.sqliteBuffer) {
        state.exportService = new ParquetExportService(
          state.sqliteBuffer as SQLiteBuffer,
          state.parquetWriter,
          {
            outputDirectory: state.currentConfig.outputDirectory,
            filenamePrefix: state.currentConfig.filenamePrefix,
            useHivePartitioning: state.currentConfig.useHivePartitioning!,
            s3Upload: {
              enabled: state.currentConfig.cloudUpload.provider !== 'none',
            },
            dailyExportHour: state.currentConfig.dailyExportHour ?? 4,
          },
          app
        );
        state.exportService.start();
        app.debug('Parquet export service started');
      }
    }

    // Initialize cloud client if enabled (S3 or R2)
    await initializeCloudSDK(state.currentConfig, app);
    state.cloudClient = createCloudClient(state.currentConfig, app);
    if (state.cloudClient) {
      app.debug(
        `${state.currentConfig.cloudUpload.provider.toUpperCase()} client initialized`
      );
    }

    // Ensure output directory exists
    fs.ensureDirSync(state.currentConfig.outputDirectory);

    // Clean up any *.tmp files left behind by a previous SignalK crash
    // mid-COPY. Best-effort; failures are logged but do not block start.
    await cleanupStrandedCompactionTempFiles(
      app,
      state.currentConfig.outputDirectory
    ).catch(err => {
      app.error(`Compaction startup cleanup failed: ${(err as Error).message}`);
    });

    // Recover any stranded `.compaction-trash-*` dirs from a crash
    // between move-to-trash and publish-rename. Runs before any new
    // data subscriptions land so a restore can't clobber fresh files.
    await recoverStrandedCompactionTrash(
      app,
      state.currentConfig.outputDirectory
    ).catch(err => {
      app.error(`Compaction trash recovery failed: ${(err as Error).message}`);
    });

    // Quarantine zero-byte parquet stubs left by a crash between
    // ParquetWriter.openFile() and close(). The read-path filename
    // filter already excludes /quarantine/, so once these are moved
    // they won't break DuckDB queries.
    await quarantineEmptyParquetFiles(
      app,
      state.currentConfig.outputDirectory
    ).catch(err => {
      app.error(
        `Empty parquet startup sweep failed: ${(err as Error).message}`
      );
    });

    // Initialize DuckDB connection pool once. Pass the (writable) plugin data
    // directory so DuckDB puts its extension/home dir there instead of the
    // default `$HOME/.duckdb`, which is read-only in the App Store CI sandbox.
    await DuckDBPool.initialize(state.currentConfig.outputDirectory, msg =>
      app.error(msg)
    );
    app.debug('DuckDB connection pool initialized');

    // Register SQLite buffer path with DuckDB for federated queries
    if (state.sqliteBuffer) {
      DuckDBPool.initializeSQLiteBuffer(state.sqliteBuffer.getDbPath());
      app.debug('DuckDB SQLite buffer federation initialized');
    }

    // Initialize S3 credentials in DuckDB if cloud provider is configured (S3 or R2)
    if (
      state.currentConfig.cloudUpload.provider !== 'none' &&
      state.currentConfig.cloudUpload.accessKeyId &&
      state.currentConfig.cloudUpload.secretAccessKey
    ) {
      const isR2 = state.currentConfig.cloudUpload.provider === 'r2';
      if (isR2 && !state.currentConfig.cloudUpload.accountId) {
        app.error(
          'R2 cloud upload enabled but no account ID configured; skipping DuckDB S3 credential setup'
        );
      } else {
        try {
          let endpoint: string | undefined;
          let useSSL: boolean | undefined;
          let urlStyle: 'path' | 'vhost' | undefined;
          if (isR2) {
            endpoint = `${state.currentConfig.cloudUpload.accountId}.r2.cloudflarestorage.com`;
            urlStyle = 'path';
          } else if (state.currentConfig.cloudUpload.endpoint) {
            // Self-hosted S3-compatible endpoint (e.g. Garage, MinIO)
            const custom = resolveCustomS3Endpoint(
              state.currentConfig.cloudUpload.endpoint,
              state.currentConfig.cloudUpload.forcePathStyle,
              state.currentConfig.cloudUpload.allowPrivateEndpoint
            );
            endpoint = custom.host;
            useSSL = custom.useSSL;
            urlStyle = custom.forcePathStyle ? 'path' : 'vhost';
          }
          await DuckDBPool.initializeS3({
            accessKeyId: state.currentConfig.cloudUpload.accessKeyId,
            secretAccessKey: state.currentConfig.cloudUpload.secretAccessKey,
            region: isR2
              ? 'auto'
              : state.currentConfig.cloudUpload.region || 'us-east-1',
            endpoint,
            useSSL,
            urlStyle,
          });
          app.debug('DuckDB S3 credentials initialized for federated queries');
        } catch (error) {
          app.error(
            `Failed to initialize DuckDB S3 credentials: ${(error as Error).message}`
          );
        }
      }
    }

    // Subscribe to command paths first (these control regimens)
    subscribeToCommandPaths(currentPaths, state, state.currentConfig, app);

    // Check current command values at startup
    initializeRegimenStates(currentPaths, state, app);

    // Initialize command state FIRST so commands are registered
    initializeCommandState(currentPaths, app);

    // Start threshold monitoring AFTER commands are registered
    // Pass config so pluginConfig (with homePort) is available
    startThresholdMonitoring(app, state.currentConfig);

    // Subscribe to data paths based on initial regimen states
    updateDataSubscriptions(currentPaths, state, state.currentConfig, app);

    // Set up periodic save
    state.saveInterval = setInterval(() => {
      saveAllBuffers(state.currentConfig!, state, app);
    }, state.currentConfig.saveIntervalSeconds * 1000);

    // Set up daily export scheduling (new simplified pipeline).
    // dailyExportHour is the 0-23 integer validated at config intake above,
    // so this Date.UTC schedule and the SQLite catch-up cutoff agree.
    const now = new Date();

    // Calculate next daily export time (at configured hour UTC)
    const nextDailyExportUTC = new Date(
      Date.UTC(
        now.getUTCFullYear(),
        now.getUTCMonth(),
        now.getUTCDate(),
        dailyExportHour,
        0,
        0,
        0
      )
    );
    // If we've already passed this hour today, schedule for tomorrow
    if (nextDailyExportUTC.getTime() <= now.getTime()) {
      nextDailyExportUTC.setUTCDate(nextDailyExportUTC.getUTCDate() + 1);
    }
    const msUntilDailyExport = nextDailyExportUTC.getTime() - now.getTime();

    app.debug(
      `[DailyExport] Scheduled for ${dailyExportHour}:00 UTC, next run in ${Math.round(msUntilDailyExport / 60000)} minutes`
    );

    // Initialize aggregation service if Hive partitioning is enabled
    let aggregationService: AggregationService | undefined;
    let aggregationConfig: AggregationConfig | undefined;
    if (state.currentConfig.useHivePartitioning) {
      aggregationConfig = {
        outputDirectory: state.currentConfig.outputDirectory,
        filenamePrefix: state.currentConfig.filenamePrefix,
        retentionDays: buildPerTierRetention(state.currentConfig.retentionDays),
        pathRetentionOverrides: state.currentConfig.pathRetentionOverrides,
      };
      aggregationService = new AggregationService(aggregationConfig, app);
      app.debug('Aggregation service initialized');
    }

    // Daily export function - exports yesterday's data from SQLite to Parquet
    const runDailyExport = async () => {
      if (state.isStopping) return;
      const yesterday = new Date();
      yesterday.setUTCDate(yesterday.getUTCDate() - 1);
      yesterday.setUTCHours(0, 0, 0, 0);

      app.debug(
        `[DailyExport] Running daily export for ${yesterday.toISOString().slice(0, 10)}`
      );

      try {
        // Export yesterday's data to daily Parquet files
        if (state.exportService) {
          const result =
            await state.exportService.exportDayToParquet(yesterday);
          app.debug(
            `[DailyExport] Exported ${result.recordsExported} records to ${result.filesCreated.length} files`
          );

          // Run aggregation after daily export if Hive partitioning is enabled.
          // Track whether aggregation and upload succeeded — cleanup must NOT
          // run if either failed, because the whole point of post-pipeline
          // cleanup is that files about to be deleted have already been
          // rolled up and (when configured) uploaded.
          let aggregationOk = false;
          let uploadOk = false;
          if (aggregationService) {
            try {
              // Angular paths (units === 'rad': heading, COG, wind direction)
              // must keep vector averaging in the aggregated tiers. The worker
              // has no live metadata, so compute the set here (from the live
              // server) and pass it in; getPaths() is the small set of recorded
              // path names.
              let angularPathNames: string[] = [];
              const appWithMetadata = app as unknown as {
                getMetadata?: (x: string) => { units?: string } | undefined;
              };
              if (!state.sqliteBuffer) {
                app.debug(
                  '[DailyExport] No SQLite buffer — cannot enumerate recorded paths, angular path set is empty'
                );
              } else if (typeof appWithMetadata.getMetadata !== 'function') {
                app.error(
                  '[DailyExport] app.getMetadata unavailable — angular paths cannot be detected; aggregation will linear-average all paths'
                );
              } else {
                const recordedPaths = state.sqliteBuffer.getPaths();
                angularPathNames = recordedPaths.filter(p => {
                  try {
                    return (
                      appWithMetadata.getMetadata!(`vessels.self.${p}`)
                        ?.units === 'rad'
                    );
                  } catch {
                    return false;
                  }
                });
                if (angularPathNames.length === 0 && recordedPaths.length > 0) {
                  // A legitimate state for vessels recording no rad-unit
                  // paths — debug, not error (metadata-unavailable above
                  // stays an error).
                  app.debug(
                    `[DailyExport] No angular paths detected among ${recordedPaths.length} recorded paths — any heading/bearing paths will be linear-averaged in aggregated tiers`
                  );
                } else {
                  app.debug(
                    `[DailyExport] Angular paths for aggregation: ${JSON.stringify(angularPathNames)}`
                  );
                }
              }
              const aggResults = await runAggregationInWorker(
                {
                  config: aggregationConfig!,
                  dataDir: state.currentConfig!.outputDirectory,
                  dateISO: yesterday.toISOString(),
                  angularPathNames,
                },
                app,
                state.activeAggregationWorkers
              );
              const aggHadErrors = aggResults.some(r => r.errors.length > 0);
              aggregationOk = !aggHadErrors;
              app.debug(
                `[DailyExport] Aggregation complete: ${JSON.stringify(aggResults.map(r => ({ tier: r.targetTier, files: r.filesCreated, errors: r.errors.length })))}`
              );
            } catch (aggErr) {
              app.error(
                `[DailyExport] Aggregation failed: ${(aggErr as Error).message}`
              );
            }

            const cloudEnabled =
              state.currentConfig?.cloudUpload.provider !== 'none';
            if (cloudEnabled && aggregationOk) {
              try {
                await uploadConsolidatedFilesToS3(
                  state.currentConfig!,
                  yesterday,
                  state,
                  app
                );
                uploadOk = true;
              } catch (upErr) {
                app.error(
                  `[DailyExport] Cloud upload failed: ${(upErr as Error).message}`
                );
              }
            } else if (!cloudEnabled) {
              uploadOk = true; // No upload required → vacuously OK.
            }

            // Apply retention rules. Skipped if aggregation or (when
            // applicable) upload didn't complete cleanly: short-retention
            // raw files we'd be about to delete may not have been rolled
            // up or synced yet, so deleting them now would lose data.
            const cfg = state.currentConfig!;
            // A pure skipAggregation override (days = 0) doesn't expire
            // anything, so it shouldn't trigger a daily walk of the
            // store. Only count overrides that can actually delete.
            const willDeleteAnything =
              cfg.retentionDays > 0 ||
              !!cfg.pathRetentionOverrides?.some(rule => rule.days > 0);
            if (willDeleteAnything && aggregationOk && uploadOk) {
              try {
                const cleanup = await aggregationService.cleanupOldData();
                const summary = `[DailyExport] Retention cleanup: ${cleanup.deletedFiles} files removed, ${cleanup.failedFiles} failed, ${(cleanup.freedBytes / 1024 / 1024).toFixed(2)} MB freed`;
                if (cleanup.failedFiles > 0) {
                  app.error(summary);
                } else {
                  app.debug(summary);
                }
              } catch (cleanErr) {
                app.error(
                  `[DailyExport] Retention cleanup failed: ${(cleanErr as Error).message}`
                );
              }
            } else if (willDeleteAnything) {
              app.error(
                `[DailyExport] Retention cleanup SKIPPED: aggregation or upload did not complete cleanly. ` +
                  `Short-retention paths will accumulate until the next successful run.`
              );
            }
          }
        }
      } catch (error) {
        app.error(`[DailyExport] Failed: ${(error as Error).message}`);
      }
    };

    // Schedule first daily export
    state.dailyExportTimeout = setTimeout(() => {
      state.dailyExportTimeout = undefined;
      if (state.isStopping) return;
      runDailyExport();

      // Then run daily export every 24 hours
      state.consolidationInterval = setInterval(
        runDailyExport,
        24 * 60 * 60 * 1000
      );
    }, msUntilDailyExport);

    // Run startup export for ALL unexported records (catches up after downtime)
    state.startupExportTimeout = setTimeout(async () => {
      state.startupExportTimeout = undefined;
      if (state.isStopping) return;
      if (state.exportService) {
        try {
          const result = await state.exportService.exportAllUnexported();
          if (result.recordsExported > 0) {
            app.debug(
              `[StartupExport] Exported ${result.recordsExported} records to ${result.filesCreated.length} files`
            );

            // Re-aggregate each date that had late exports
            if (aggregationService) {
              // Extract unique dates from exported file paths
              // Paths contain .../year=YYYY/day=DDD/... — parse dates from them
              const exportedDates = new Set<string>();
              for (const filePath of result.filesCreated) {
                const yearMatch = filePath.match(/year=(\d{4})/);
                const dayMatch = filePath.match(/day=(\d{3})/);
                if (yearMatch && dayMatch) {
                  const year = parseInt(yearMatch[1], 10);
                  const dayOfYear = parseInt(dayMatch[1], 10);
                  const date = new Date(Date.UTC(year, 0, dayOfYear));
                  exportedDates.add(date.toISOString().slice(0, 10));
                }
              }

              for (const dateStr of exportedDates) {
                try {
                  const date = new Date(dateStr + 'T00:00:00.000Z');
                  const aggResults =
                    await aggregationService.aggregateDate(date);
                  app.debug(
                    `[StartupExport] Aggregation for ${dateStr}: ${JSON.stringify(aggResults.map(r => ({ tier: r.targetTier, files: r.filesCreated })))}`
                  );
                } catch (aggErr) {
                  app.error(
                    `[StartupExport] Aggregation failed for ${dateStr}: ${(aggErr as Error).message}`
                  );
                }
              }
            }
          }
        } catch (error) {
          app.error(`[StartupExport] Failed: ${(error as Error).message}`);
        }
      }

      // Upload recent hive files to cloud (3-day lookback)
      if (state.currentConfig?.cloudUpload.provider !== 'none') {
        try {
          app.debug('[StartupSync] Starting cloud sync...');
          await uploadAllConsolidatedFilesToS3(
            state.currentConfig!,
            state,
            app
          );
          app.debug('[StartupSync] Cloud upload complete');
        } catch (s3Err) {
          app.error(
            `[StartupSync] Cloud upload failed: ${(s3Err as Error).message}`
          );
        }
      }
    }, 10000); // Wait 10 seconds after startup

    // Always initialize auto-discovery service - it checks enabled state at runtime
    app.debug(
      `[AutoDiscovery] Config: ${JSON.stringify(state.currentConfig?.autoDiscovery)}`
    );
    state.autoDiscoveryService = new AutoDiscoveryService(
      app,
      state.currentConfig,
      state,
      currentPaths
    );

    // Recover counter from existing auto-discovered paths
    const existingAutoDiscovered = currentPaths.filter(
      p => p.autoDiscovered
    ).length;
    state.autoDiscoveryService.setInitialCount(existingAutoDiscovered);
    app.debug(
      `[AutoDiscovery] Service initialized with ${existingAutoDiscovered} existing auto-discovered paths`
    );

    // Register History API routes directly with the main app
    try {
      // Build S3 query config if cloud provider is configured (S3 or R2)
      const isR2 = state.currentConfig.cloudUpload.provider === 'r2';
      const s3QueryConfig =
        state.currentConfig.cloudUpload.provider !== 'none'
          ? {
              enabled: true,
              bucket: state.currentConfig.cloudUpload.bucket || '',
              keyPrefix: state.currentConfig.cloudUpload.keyPrefix || '',
              region: isR2
                ? 'auto'
                : state.currentConfig.cloudUpload.region || 'us-east-1',
            }
          : undefined;

      if (!state.historyApi) {
        // First start: register the V1 express routes and keep the instance.
        state.historyApi = registerHistoryApiRoute(
          app as unknown as Router,
          app.selfId,
          state.currentConfig.outputDirectory,
          app.debug,
          app,
          state.sqliteBuffer, // Pass SQLite buffer for federated queries
          state.autoDiscoveryService, // Pass auto-discovery service
          s3QueryConfig, // S3 config for federated queries
          state.currentConfig.pathRetentionOverrides // skipAggregation read-path fallback
        );
      } else {
        // Reconfigure (stop→start without a full process restart): the V1 express
        // routes registered on the first start are still live and bound to this
        // same HistoryAPI instance, but stop() closed the previous SQLite buffer.
        // Express has no clean route-removal, so instead of registering a
        // duplicate route bound to a now-closed buffer, re-point the existing
        // instance at the fresh buffer/config. A closed buffer makes federation
        // return nothing with NO error, silently dropping all live (unexported)
        // data from history reads until a full restart — this keeps it live.
        state.historyApi.setSqliteBuffer(state.sqliteBuffer);
        state.historyApi.setS3Config(s3QueryConfig);
        state.historyApi.setAutoDiscoveryService(state.autoDiscoveryService);
        state.historyApi.setDataDir(state.currentConfig.outputDirectory);
        state.historyApi.setPathRetentionOverrides(
          state.currentConfig.pathRetentionOverrides
        );
      }
      app.debug(
        `[AutoDiscovery] History API registered with autoDiscoveryService: ${!!state.autoDiscoveryService}`
      );
      if (s3QueryConfig) {
        app.debug(
          `[S3Query] S3 federated queries enabled for bucket: ${s3QueryConfig.bucket}`
        );
      }
    } catch (error) {
      app.error(
        `Failed to register History API routes with main server: ${error}`
      );
    }

    // Register as the official SignalK History API provider
    // This allows other plugins to discover and use our history implementation
    try {
      registerHistoryApiProvider(
        app,
        app.selfId,
        state.currentConfig.outputDirectory,
        app.debug,
        state.sqliteBuffer
      );
    } catch (error) {
      app.error(`Failed to register as History API provider: ${error}`);
    }

    // Handle "Set Current Location" action
    handleSetCurrentLocationAction(state.currentConfig).catch(err => {
      app.error(`Error handling set current location action: ${err}`);
    });

    // Publish home port position to SignalK if configured
    if (
      state.currentConfig.homePortLatitude &&
      state.currentConfig.homePortLongitude &&
      state.currentConfig.homePortLatitude !== 0 &&
      state.currentConfig.homePortLongitude !== 0
    ) {
      publishHomePortToSignalK(
        state.currentConfig.homePortLatitude,
        state.currentConfig.homePortLongitude
      );
    }
  };

  plugin.stop = async function (): Promise<void> {
    // Flag first so any timer/interval callback that fires during this
    // async teardown becomes a no-op instead of starting a new export.
    state.isStopping = true;

    // Signal any running compaction jobs to cancel and wait for them
    // to land at a group boundary. A single in-flight DuckDB COPY is
    // uninterruptible, so a multi-GB year group still finishes before
    // the loop exits — but we need to wait for that, otherwise
    // DuckDBPool.shutdown() below races the COPY and leaves a partial
    // temp file behind. Bounded so a stuck job can't hang the stop
    // path indefinitely.
    const quiesce = await quiesceAllCompactionJobs();
    if (quiesce.signalled > 0) {
      app.debug(
        `Compaction quiesce: signalled=${quiesce.signalled}, ` +
          `quiesced=${quiesce.quiesced}, remaining=${quiesce.remaining}`
      );
      if (quiesce.remaining > 0) {
        app.error(
          `${quiesce.remaining} compaction job(s) still running at shutdown timeout; proceeding anyway`
        );
      }
    }

    // Unregister as History API provider
    unregisterHistoryApiProvider(app);

    // Stop threshold monitoring system
    stopThresholdMonitoring();

    // Clear intervals and pending one-shot export timers
    if (state.saveInterval) {
      clearInterval(state.saveInterval);
    }
    if (state.consolidationInterval) {
      clearInterval(state.consolidationInterval);
    }
    if (state.dailyExportTimeout) {
      clearTimeout(state.dailyExportTimeout);
      state.dailyExportTimeout = undefined;
    }
    if (state.startupExportTimeout) {
      clearTimeout(state.startupExportTimeout);
      state.startupExportTimeout = undefined;
    }

    // Wind down any in-flight aggregation workers cooperatively: a shutdown
    // message makes the worker cancel at the next group boundary, so the
    // COPY currently writing finishes and no final Parquet file is left
    // half-written. The worker then reports the run as an error, which makes
    // the daily-export caller skip retention cleanup, same as any other
    // worker failure. Workers still alive after the grace period are
    // SIGKILLed so a stuck COPY can't hang shutdown.
    if (state.activeAggregationWorkers?.size) {
      const workers = Array.from(state.activeAggregationWorkers);
      for (const worker of workers) {
        try {
          worker.send({ type: 'shutdown' });
        } catch {
          // IPC channel already closed — the exit wait below still applies
        }
      }
      await Promise.all(
        workers.map(
          worker =>
            new Promise<void>(resolve => {
              if (worker.exitCode !== null || worker.signalCode !== null) {
                resolve();
                return;
              }
              const killTimer = setTimeout(() => {
                app.error(
                  '[DailyExport] Aggregation worker did not exit within shutdown grace period; killing'
                );
                try {
                  worker.kill('SIGKILL');
                } catch {
                  // already gone
                }
                // Stop waiting here so the bound on plugin.stop() holds even
                // if the worker can't be reaped (e.g. uninterruptible I/O).
                resolve();
              }, AGGREGATION_WORKER_SHUTDOWN_GRACE_MS);
              worker.once('exit', () => {
                clearTimeout(killTimer);
                resolve();
              });
            })
        )
      );
      state.activeAggregationWorkers.clear();
    }

    // Save any remaining buffered data
    if (state.currentConfig) {
      saveAllBuffers(state.currentConfig, state, app);
    }

    // Unsubscribe from all paths FIRST to stop data flow before closing buffer
    state.unsubscribes.forEach(unsubscribe => {
      if (typeof unsubscribe === 'function') {
        unsubscribe();
      }
    });
    state.unsubscribes = [];

    // Clean up stream subscriptions (new streambundle approach)
    if (state.streamSubscriptions) {
      state.streamSubscriptions.forEach(sub => {
        if (typeof sub === 'function') {
          sub();
        } else if (sub && typeof sub === 'object') {
          const candidate = sub as {
            unsubscribe?: () => void;
            dispose?: () => void;
            end?: () => void;
          };
          if (typeof candidate.unsubscribe === 'function') {
            candidate.unsubscribe();
          } else if (typeof candidate.dispose === 'function') {
            candidate.dispose();
          } else if (typeof candidate.end === 'function') {
            candidate.end();
          }
        }
      });
      state.streamSubscriptions = [];
    }

    // Stop export service (pending records will be exported on next startup)
    if (state.exportService) {
      try {
        state.exportService.stop();
        app.debug('Parquet export service stopped');
      } catch (error) {
        app.error(`Error stopping export service: ${error}`);
      }
    }

    // Close SQLite buffer (safe now that subscriptions are torn down)
    if (state.sqliteBuffer) {
      try {
        state.sqliteBuffer.close();
        app.debug('SQLite buffer closed');
      } catch (error) {
        app.error(`Error closing SQLite buffer: ${error}`);
      }
    }

    // Clear data structures
    state.dataBuffers.clear();
    state.activeRegimens.clear();
    state.subscribedPaths.clear();

    // Shutdown DuckDB connection pool
    await DuckDBPool.shutdown();
    app.debug('DuckDB connection pool shut down');
  };

  plugin.schema = {
    type: 'object',
    title: 'SignalK to Parquet Data Store',
    description:
      "The archiving commands, paths and other processes are managed in the companion 'SignalK to Parquet Data Store' in the Webapp section.\n\nThese settings here underpin the system.",
    properties: {
      bufferSize: {
        type: 'number',
        title: 'Memory Buffer Size',
        description:
          'Number of SignalK data records to hold in memory before writing to the SQLite buffer. Higher values use more RAM but reduce disk writes. Recommended: 1000-5000 for most systems.',
        default: 1000,
        minimum: 10,
        maximum: 10000,
      },
      saveIntervalSeconds: {
        type: 'number',
        title: 'Buffer Flush Interval (seconds)',
        description:
          'Maximum time between flushing the memory buffer to SQLite. Data is written when either this interval passes OR the buffer size is reached, whichever comes first.',
        default: 30,
        minimum: 5,
        maximum: 300,
      },
      outputDirectory: {
        type: 'string',
        title: 'Data Storage Directory',
        description:
          'Relative path from ~/.signalk (e.g., "data" becomes ~/.signalk/data). Leave empty for default (~/.signalk/signalk-parquet-data). Absolute paths also supported.',
        default: '',
      },
      filenamePrefix: {
        type: 'string',
        title: 'Filename Prefix',
        description:
          'Prefix added to all generated Parquet files. Useful if running multiple instances or for organizing data. Example: "boat_name" produces "boat_name_2024-01-15T1200.parquet"',
        default: 'signalk_data',
      },
      retentionDays: {
        type: 'integer',
        title: 'Retention Period (days)',
        description:
          'Days to keep raw-tier Parquet files. Aggregated tiers scale automatically (5s: 2x, 60s: 4x, 1h: 12x). 0 = keep forever (default). Cleanup runs once per day, right after the daily export.',
        default: 0,
        minimum: 0,
        maximum: 36500,
      },
      pathRetentionOverrides: {
        type: 'array',
        title: 'Per-Path Retention Overrides',
        description:
          'Override retention for specific SignalK paths. Each rule has a glob pattern (* matches any chars, including dots), a number of days (0 = keep forever), and an optional skipAggregation flag (true = path stays raw-only, never rolled up). The most specific pattern wins; ties broken in declaration order. Example: pattern "environment.wind.*", days 1, skipAggregation true => wind paths kept for 24h in tier=raw and never aggregated.',
        items: {
          type: 'object',
          required: ['pattern', 'days'],
          properties: {
            pattern: {
              type: 'string',
              title: 'Path pattern',
              description: 'Glob over the SignalK path. * matches any chars.',
            },
            days: {
              type: 'integer',
              title: 'Days',
              description: '0 = keep forever.',
              minimum: 0,
              maximum: 36500,
            },
            skipAggregation: {
              type: 'boolean',
              title: 'Skip aggregation',
              description:
                'When true, paths matching this rule are not rolled up into 5s/60s/1h tiers.',
              default: false,
            },
          },
        },
        default: [],
      },
      exportBatchSize: {
        type: 'number',
        title: 'Export Batch Size',
        description:
          'Number of records loaded from SQLite into memory per batch when exporting to Parquet. The export loops through all pending records in chunks of this size. Higher values use more memory but export faster.',
        default: 50000,
        minimum: 1000,
        maximum: 200000,
      },
      // bufferRetentionHours: {
      //   type: 'number',
      //   title: 'SQLite Buffer Retention (hours)',
      //   description:
      //     'How long to keep already-exported records in SQLite as a backup after they have been written to Parquet. Longer retention allows re-export if a Parquet file is lost, but uses more disk space.',
      //   default: 48,
      //   minimum: 48,
      //   maximum: 168,
      // },
      dailyExportHour: {
        type: 'number',
        title: 'Daily Export Hour (UTC)',
        description:
          'Hour of day in UTC (0-23) when daily Parquet export runs. This is shown in local time on the Status tab. Default is 4 (4:00 UTC).',
        default: 4,
        minimum: 0,
        maximum: 23,
      },
      autoDiscovery: {
        type: 'object',
        title: 'Auto-Discovery',
        description:
          'Automatically configure paths for recording when historical data is requested but not available',
        properties: {
          enabled: {
            type: 'boolean',
            title: 'Enable auto-discovery',
            description:
              'When enabled, paths requested via history API that are not being recorded will be automatically configured',
            default: true,
          },
          maxAutoConfiguredPaths: {
            type: 'number',
            title: 'Max auto-configured paths',
            description:
              'Maximum number of paths that can be auto-configured to prevent runaway configuration',
            default: 100,
            minimum: 1,
            maximum: 1000,
          },
          requireLiveData: {
            type: 'boolean',
            title: 'Require live data',
            description:
              'Only auto-configure paths that have live data in SignalK',
            default: true,
          },
          excludePatterns: {
            type: 'array',
            title: 'Exclude patterns',
            description:
              'Glob patterns for paths that should never be auto-configured',
            items: {
              type: 'string',
            },
            default: ['design.*', 'communication.*', 'notifications.*'],
          },
        },
      },
      cloudUpload: {
        type: 'object',
        title: 'Cloud Storage Configuration',
        description:
          'Upload Parquet files to Amazon S3 or Cloudflare R2 for backup/archive',
        properties: {
          provider: {
            type: 'string',
            title: 'Cloud Provider',
            description: 'Select cloud storage provider (S3 or R2)',
            enum: ['none', 's3', 'r2'],
            enumNames: ['Disabled', 'Amazon S3', 'Cloudflare R2'],
            default: 'none',
          },
          bucket: {
            type: 'string',
            title: 'Bucket Name',
            description: 'Name of the S3 or R2 bucket to upload to',
            default: '',
          },
          keyPrefix: {
            type: 'string',
            title: 'Key Prefix',
            description:
              'Optional prefix for object keys (e.g., "marine-data/")',
            default: '',
          },
          accessKeyId: {
            type: 'string',
            title: 'Access Key ID',
            description: 'AWS Access Key ID or R2 API token access key',
            default: '',
          },
          secretAccessKey: {
            type: 'string',
            title: 'Secret Access Key',
            description: 'AWS Secret Access Key or R2 API token secret key',
            default: '',
          },
          deleteAfterUpload: {
            type: 'boolean',
            title: 'Delete Local Files After Upload',
            description:
              'Delete local files after successful upload to cloud storage',
            default: false,
          },
        },
        dependencies: {
          provider: {
            oneOf: [
              {
                properties: {
                  provider: { enum: ['none'] },
                },
              },
              {
                properties: {
                  provider: { enum: ['s3'] },
                  region: {
                    type: 'string',
                    title: 'AWS Region',
                    description: 'AWS region where the S3 bucket is located.',
                    default: 'us-east-1',
                  },
                  endpoint: {
                    type: 'string',
                    title: 'Custom Endpoint URL (optional)',
                    description:
                      'Override the S3 endpoint for self-hosted S3-compatible storage (e.g. Garage, MinIO). Include the protocol and port, e.g. "https://garage.example.com:3900". Leave blank for AWS S3.',
                    default: '',
                  },
                  forcePathStyle: {
                    type: 'boolean',
                    title: 'Use Path-Style Addressing',
                    description:
                      'Use path-style bucket addressing (https://endpoint/bucket) instead of virtual-hosted-style (https://bucket.endpoint). Often required by self-hosted S3-compatible services like Garage or MinIO. If left unset, defaults to enabled when a custom endpoint is configured.',
                  },
                  allowPrivateEndpoint: {
                    type: 'boolean',
                    title: 'Allow Private/Local Endpoints',
                    description:
                      'Permit custom S3 endpoints on private, loopback, or link-local addresses (e.g. 192.168.x.x, localhost). Required for self-hosted services on the local network, but disabled by default to prevent SSRF.',
                    default: false,
                  },
                },
                description:
                  'NOTE: Querying S3 buckets incurs AWS data transfer costs that can rise quickly with large or frequent queries. Typically, R2 has no such fees. Self-hosted services like Garage typically have no such fees either.',
              },
              {
                properties: {
                  provider: { enum: ['r2'] },
                  accountId: {
                    type: 'string',
                    title: 'Cloudflare Account ID',
                    description:
                      'Your Cloudflare account ID (found in the R2 dashboard URL)',
                    default: '',
                  },
                },
              },
            ],
          },
        },
      },
      enableRawSql: {
        type: 'boolean',
        title: 'Enable Raw SQL Queries',
        description:
          'Enable the /api/query endpoint for raw SQL queries. Use with caution.',
        default: false,
      },
      homePortLatitude: {
        type: 'number',
        title: 'Home Port Latitude (Optional)',
        description:
          'Home port latitude for vessel context. If not set (0), will use vessel position from navigation.position',
        default: 0,
      },
      homePortLongitude: {
        type: 'number',
        title: 'Home Port Longitude (Optional)',
        description:
          'Home port longitude for vessel context. If not set (0), will use vessel position from navigation.position',
        default: 0,
      },
      setCurrentLocationAction: {
        type: 'object',
        title: 'Home Port Location Actions',
        description: 'Actions for setting the home port location',
        properties: {
          setCurrentLocation: {
            type: 'boolean',
            title: 'Set Current Location as Home Port',
            description:
              "Check this box and save to use the vessel's current position as the home port coordinates",
            default: false,
          },
        },
      },
    },
  };

  // Webapp static files and API routes
  plugin.registerWithRouter = function (router: Router): void {
    registerApiRoutes(router, state, app);
  };

  // Handle "Set Current Location" action
  async function handleSetCurrentLocationAction(
    config: PluginConfig
  ): Promise<void> {
    app.debug(
      `handleSetCurrentLocationAction called with setCurrentLocation: ${config.setCurrentLocationAction?.setCurrentLocation}`
    );

    if (config.setCurrentLocationAction?.setCurrentLocation) {
      // Get current position from SignalK
      const currentPosition = getCurrentVesselPosition();
      app.debug(
        `Current position: ${currentPosition ? `${currentPosition.latitude}, ${currentPosition.longitude}` : 'null'}`
      );

      if (currentPosition) {
        // Update the configuration with current position
        const updatedConfig = {
          ...config,
          homePortLatitude: currentPosition.latitude,
          homePortLongitude: currentPosition.longitude,
          setCurrentLocationAction: {
            setCurrentLocation: false, // Reset the checkbox
          },
        };

        // Save the updated configuration
        app.savePluginOptions(updatedConfig, (err?: unknown) => {
          if (err) {
            app.error(`Failed to save current location as home port: ${err}`);
          } else {
            app.debug(
              `Set home port location to: ${currentPosition!.latitude}, ${currentPosition!.longitude}`
            );

            // Update current config
            state.currentConfig = updatedConfig;

            // Publish home port position to SignalK
            publishHomePortToSignalK(
              currentPosition!.latitude,
              currentPosition!.longitude
            );
          }
        });
      } else {
        app.error(
          'No current vessel position available. Ensure navigation.position is being published to SignalK.'
        );
      }
    }
  }

  // Get current vessel position from SignalK
  function getCurrentVesselPosition(): {
    latitude: number;
    longitude: number;
    timestamp: Date;
  } | null {
    try {
      // Cast to any for compatibility with different @signalk/server-api versions
      const position = app.getSelfPath('navigation.position') as any;
      if (
        position &&
        position.value &&
        position.value.latitude &&
        position.value.longitude
      ) {
        return {
          latitude: position.value.latitude,
          longitude: position.value.longitude,
          timestamp: new Date(position.timestamp || Date.now()),
        };
      }
    } catch (error) {
      app.debug(`Error getting vessel position: ${error}`);
    }
    return null;
  }

  // Publish home port position to SignalK
  function publishHomePortToSignalK(latitude: number, longitude: number): void {
    const homePortPosition = {
      latitude: latitude,
      longitude: longitude,
    };

    const delta = {
      context: app.selfContext as Context,
      updates: [
        {
          $source: 'signalk-parquet.homePort' as SourceRef,
          timestamp: new Date().toISOString() as Timestamp,
          values: [
            {
              path: 'navigation.homePort.position' as Path,
              value: homePortPosition,
            },
          ],
        },
      ],
    };

    app.handleMessage(plugin.id, delta);
    app.debug(
      `Published home port position to SignalK: ${latitude}, ${longitude}`
    );
  }

  return plugin;
}

// Re-export utility functions for backward compatibility
export { toContextFilePath, toParquetFilePath } from './utils/path-helpers';
