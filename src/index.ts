import * as fs from 'fs-extra';
import * as path from 'path';
import { Router } from 'express';
import { ParquetWriter } from './parquet-writer';
import { registerHistoryApiRoute } from './HistoryAPI';
import { registerApiRoutes } from './api-routes';
import { CACHE_SIZE } from './config/cache-defaults';
// import { HistoricalStreamingService } from './historical-streaming';
import { SignalKPlugin, PluginConfig, PluginState, PathConfig } from './types';
import { Context, SourceRef, Timestamp, Path } from '@signalk/server-api';
import {
  loadWebAppConfig,
  initializeCommandState,
  getCurrentCommands,
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
import {
  registerHistoryApiProvider,
  unregisterHistoryApiProvider,
} from './history-provider';
import { SQLiteBuffer } from './utils/sqlite-buffer';
import { ParquetExportService } from './services/parquet-export-service';
import { AggregationService } from './services/aggregation-service';
import { AutoDiscoveryService } from './services/auto-discovery';

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
    commandState: {
      registeredCommands: new Map(),
      putHandlers: new Map(),
    },
  };

  let currentPaths: PathConfig[] = [];

  plugin.start = async function (
    options: Partial<PluginConfig>
  ): Promise<void> {
    // Get vessel MMSI from SignalK
    // Cast to any for compatibility with different @signalk/server-api versions
    const vesselMMSI =
      (app.getSelfPath('mmsi') as any) ||
      (app.getSelfPath('name') as any) ||
      'unknown_vessel';

    // Use SignalK's main config directory (~/.signalk/)
    const signalkDataDir = (app as any).config.configPath;
    const defaultOutputDir = path.join(signalkDataDir, 'signalk-parquet-data');

    state.currentConfig = {
      bufferSize: options?.bufferSize || 1000,
      saveIntervalSeconds: options?.saveIntervalSeconds || 30,
      outputDirectory: options?.outputDirectory?.trim()
        ? path.isAbsolute(options.outputDirectory.trim())
          ? options.outputDirectory.trim()
          : path.join(signalkDataDir, options.outputDirectory.trim())
        : defaultOutputDir,
      filenamePrefix: options?.filenamePrefix || 'signalk_data',
      retentionDays: options?.retentionDays || 7,
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
      // enableStreaming: options?.enableStreaming ?? false,
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
      // Daily export hour (0-23 UTC, default 2 AM)
      dailyExportHour: options?.dailyExportHour ?? 4,
    };

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

    // Initialize DuckDB connection pool once
    await DuckDBPool.initialize();
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
      try {
        const isR2 = state.currentConfig.cloudUpload.provider === 'r2';
        await DuckDBPool.initializeS3({
          accessKeyId: state.currentConfig.cloudUpload.accessKeyId,
          secretAccessKey: state.currentConfig.cloudUpload.secretAccessKey,
          region: isR2 ? 'auto' : (state.currentConfig.cloudUpload.region || 'us-east-1'),
          endpoint: isR2 ? `${state.currentConfig.cloudUpload.accountId}.r2.cloudflarestorage.com` : undefined,
        });
        app.debug('DuckDB S3 credentials initialized for federated queries');
      } catch (error) {
        app.error(`Failed to initialize DuckDB S3 credentials: ${error}`);
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

    // Set up daily export scheduling (new simplified pipeline)
    const dailyExportHour = state.currentConfig.dailyExportHour ?? 4;
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
    if (state.currentConfig.useHivePartitioning) {
      aggregationService = new AggregationService(
        {
          outputDirectory: state.currentConfig.outputDirectory,
          filenamePrefix: state.currentConfig.filenamePrefix,
          retentionDays: {
            raw: state.currentConfig.retentionDays,
            '5s': state.currentConfig.retentionDays * 2,
            '60s': state.currentConfig.retentionDays * 4,
            '1h': state.currentConfig.retentionDays * 12,
          },
        },
        app
      );
      app.debug('Aggregation service initialized');
    }

    // Daily export function - exports yesterday's data from SQLite to Parquet
    const runDailyExport = async () => {
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

          // Run aggregation after daily export if Hive partitioning is enabled
          if (aggregationService) {
            try {
              const aggResults = await aggregationService.runDailyAggregation();
              app.debug(
                `[DailyExport] Aggregation complete: ${JSON.stringify(aggResults.map(r => ({ tier: r.targetTier, files: r.filesCreated })))}`
              );

              // Upload files to cloud after aggregation
              if (state.currentConfig?.cloudUpload.provider !== 'none') {
                await uploadConsolidatedFilesToS3(
                  state.currentConfig!,
                  yesterday,
                  state,
                  app
                );
              }
            } catch (aggErr) {
              app.error(
                `[DailyExport] Aggregation failed: ${(aggErr as Error).message}`
              );
            }
          }
        }
      } catch (error) {
        app.error(`[DailyExport] Failed: ${(error as Error).message}`);
      }
    };

    // Schedule first daily export
    setTimeout(() => {
      runDailyExport();

      // Then run daily export every 24 hours
      state.consolidationInterval = setInterval(
        runDailyExport,
        24 * 60 * 60 * 1000
      );
    }, msUntilDailyExport);

    // Run startup export for ALL unexported records (catches up after downtime)
    setTimeout(async () => {
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
              region: isR2 ? 'auto' : (state.currentConfig.cloudUpload.region || 'us-east-1'),
            }
          : undefined;

      registerHistoryApiRoute(
        app as unknown as Router,
        app.selfId,
        state.currentConfig.outputDirectory,
        app.debug,
        app,
        state.sqliteBuffer, // Pass SQLite buffer for federated queries
        state.autoDiscoveryService, // Pass auto-discovery service
        s3QueryConfig, // S3 config for federated queries
        state.currentConfig.retentionDays // Retention days for local/S3 cutoff
      );
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

    // Historical streaming service disabled — all routes are commented out
    // and the timeout accumulation bug causes unbounded memory growth.
    // See devdocs/SQLITE_NODE_SQLITE_MIGRATION.md for context.
    // try {
    //   state.historicalStreamingService = new HistoricalStreamingService(
    //     app,
    //     state.currentConfig.outputDirectory
    //   );
    // } catch (error) {
    //   app.error(`Failed to initialize historical streaming service: ${error}`);
    // }

    // Initialize runtime streaming service if enabled in configuration
    // if (state.currentConfig.enableStreaming) {
    //   try {
    //     const result = await initializeStreamingService(state, app);
    //     if (!result.success) {
    //       app.error(`Failed to initialize runtime streaming service: ${result.error}`);
    //     }
    //   } catch (error) {
    //     app.error(`Error initializing runtime streaming service: ${error}`);
    //   }
    // }

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
    // Unregister as History API provider
    unregisterHistoryApiProvider(app);

    // Stop threshold monitoring system
    stopThresholdMonitoring();

    // Clear intervals
    if (state.saveInterval) {
      clearInterval(state.saveInterval);
    }
    if (state.consolidationInterval) {
      clearInterval(state.consolidationInterval);
    }

    // Save any remaining buffered data
    if (state.currentConfig) {
      saveAllBuffers(state.currentConfig, state, app);
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

    // Close SQLite buffer
    if (state.sqliteBuffer) {
      try {
        state.sqliteBuffer.close();
        app.debug('SQLite buffer closed');
      } catch (error) {
        app.error(`Error closing SQLite buffer: ${error}`);
      }
    }

    // Unsubscribe from all paths
    state.unsubscribes.forEach(unsubscribe => {
      if (typeof unsubscribe === 'function') {
        unsubscribe();
      }
    });
    state.unsubscribes = [];

    // Clean up stream subscriptions (new streambundle approach)
    if (state.streamSubscriptions) {
      state.streamSubscriptions.forEach(stream => {
        if (stream && typeof stream.end === 'function') {
          stream.end();
        }
      });
      state.streamSubscriptions = [];
    }

    // Shutdown runtime streaming service
    if (state.streamingService) {
      try {
        shutdownStreamingService(state, app);
      } catch (error) {
        app.error(`Error shutting down runtime streaming service: ${error}`);
      }
    }

    // Shutdown historical streaming service
    if (state.historicalStreamingService) {
      try {
        state.historicalStreamingService.shutdown();
        state.historicalStreamingService = undefined;
      } catch (error) {
        app.error(`Error shutting down historical streaming service: ${error}`);
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
      // retentionDays: {
      //   type: 'number',
      //   title: 'Retention Period (days)',
      //   description:
      //     'Number of days to keep raw Parquet files on disk. Higher tiers are retained longer automatically (5s: 2x, 60s: 4x, 1h: 12x).',
      //   default: 7,
      //   minimum: 1,
      //   maximum: 365,
      // },
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
                    description: 'AWS region where the S3 bucket is located',
                    default: 'us-east-1',
                  },
                },
                description:
                  'NOTE: Querying S3 buckets incurs AWS data transfer costs that can rise quickly with large or frequent queries. Typically, R2 has no such fees.',
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
      // enableStreaming: {
      //   type: 'boolean',
      //   title: 'Enable WebSocket Streaming',
      //   description: 'Enable real-time streaming of historical data via WebSocket connections',
      //   default: false,
      // },
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

// Streaming service lifecycle functions for runtime control
export async function initializeStreamingService(
  state: PluginState,
  app: ServerAPI
): Promise<{ success: boolean; error?: string }> {
  try {
    if (state.streamingService) {
      return { success: true, error: 'Streaming service is already running' };
    }

    if (!state.currentConfig?.enableStreaming) {
      return {
        success: false,
        error:
          'Streaming is disabled in plugin configuration. Enable it in settings first.',
      };
    }

    // Historical streaming disabled — see comment at init above
    // state.streamingService = state.historicalStreamingService;
    // state.streamingEnabled = true;

    // Restore any previous subscriptions if available
    // The historical streaming service will automatically handle incoming subscriptions

    return { success: true };
  } catch (error) {
    app.error(`Failed to initialize streaming service: ${error}`);
    return { success: false, error: (error as Error).message };
  }
}

export function shutdownStreamingService(
  state: PluginState,
  app: ServerAPI
): { success: boolean; error?: string } {
  try {
    if (!state.streamingService) {
      return { success: true, error: 'Streaming service is not running' };
    }

    // Store active subscriptions for potential restoration
    if (state.streamingService.getActiveSubscriptions) {
      const activeSubscriptions =
        state.streamingService.getActiveSubscriptions();
      if (activeSubscriptions.length > 0) {
        state.restoredSubscriptions = new Map();
        activeSubscriptions.forEach((sub: any, index: number) => {
          state.restoredSubscriptions!.set(`sub_${index}`, sub);
        });
      }
    }

    // Shutdown the streaming service
    state.streamingService.shutdown();
    state.streamingService = undefined;
    state.streamingEnabled = false;

    return { success: true };
  } catch (error) {
    app.error(`Error shutting down streaming service: ${error}`);
    return { success: false, error: (error as Error).message };
  }
}

// Re-export utility functions for backward compatibility
export { toContextFilePath, toParquetFilePath } from './utils/path-helpers';
