import * as fs from 'fs-extra';
import * as path from 'path';
import { Router } from 'express';
import { ParquetWriter } from './parquet-writer';
import { registerHistoryApiRoute } from './HistoryAPI';
import { registerApiRoutes } from './api-routes';
import { CACHE_SIZE } from './config/cache-defaults';
import { HistoricalStreamingService } from './historical-streaming';
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
  initializeS3,
  createS3Client,
  subscribeToCommandPaths,
  updateDataSubscriptions,
  initializeRegimenStates,
  saveAllBuffers,
  consolidateMissedDays,
  consolidateYesterday,
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
    s3Client: undefined,
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

    // Use SignalK's main data directory (go up from plugin-config-data/<plugin-name>)
    const signalkDataDir = path.resolve(app.getDataDirPath(), '..', '..');
    const defaultOutputDir = path.join(signalkDataDir, 'signalk-parquet');

    state.currentConfig = {
      bufferSize: options?.bufferSize || 1000,
      saveIntervalSeconds: options?.saveIntervalSeconds || 30,
      outputDirectory: options?.outputDirectory
        ? (path.isAbsolute(options.outputDirectory)
            ? options.outputDirectory
            : path.join(signalkDataDir, options.outputDirectory))
        : defaultOutputDir,
      filenamePrefix: options?.filenamePrefix || 'signalk_data',
      retentionDays: options?.retentionDays || 7,
      fileFormat: options?.fileFormat || 'parquet',
      vesselMMSI: vesselMMSI,
      s3Upload: options?.s3Upload || { enabled: false },
      homePortLatitude: options?.homePortLatitude || 0,
      homePortLongitude: options?.homePortLongitude || 0,
      setCurrentLocationAction: options?.setCurrentLocationAction || {
        setCurrentLocation: false,
      },
      // enableStreaming: options?.enableStreaming ?? false,
      // SQLite buffer options
      useSqliteBuffer: true, // Always use SQLite buffer
      exportIntervalMinutes: options?.exportIntervalMinutes || 5,
      bufferRetentionHours: options?.bufferRetentionHours || 24,
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
        app.error(
          `[SQLite] CRITICAL: Failed to initialize SQLite buffer at ${dbPath}: ${error}`
        );
        app.error(
          `[SQLite] Data recording will NOT work. Fix the configuration and restart.`
        );
        throw error; // Fail loudly - don't continue with broken state
      }

      // Initialize export service
      state.exportService = new ParquetExportService(
        state.sqliteBuffer as SQLiteBuffer,
        state.parquetWriter,
        {
          exportIntervalMinutes: state.currentConfig.exportIntervalMinutes!,
          outputDirectory: state.currentConfig.outputDirectory,
          filenamePrefix: state.currentConfig.filenamePrefix,
          useHivePartitioning: state.currentConfig.useHivePartitioning!,
          s3Upload: state.currentConfig.s3Upload,
        },
        app
      );
      state.exportService.start();
      app.debug('Parquet export service started');
    }

    // Initialize S3 client if enabled
    await initializeS3(state.currentConfig, app);
    state.s3Client = createS3Client(state.currentConfig, app);

    // Ensure output directory exists
    fs.ensureDirSync(state.currentConfig.outputDirectory);

    // Initialize DuckDB connection pool once
    await DuckDBPool.initialize();
    app.debug('DuckDB connection pool initialized');

    // Initialize S3 credentials in DuckDB if S3 upload is enabled
    if (
      state.currentConfig.s3Upload.enabled &&
      state.currentConfig.s3Upload.accessKeyId &&
      state.currentConfig.s3Upload.secretAccessKey
    ) {
      try {
        await DuckDBPool.initializeS3({
          accessKeyId: state.currentConfig.s3Upload.accessKeyId,
          secretAccessKey: state.currentConfig.s3Upload.secretAccessKey,
          region: state.currentConfig.s3Upload.region || 'us-east-1',
        });
        app.debug('DuckDB S3 credentials initialized for federated queries');
      } catch (error) {
        app.error(`Failed to initialize DuckDB S3 credentials: ${error}`);
        // Don't fail startup, S3 queries will just not work
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

    // Set up daily consolidation (run at midnight UTC)
    const now = new Date();
    const nextMidnightUTC = new Date(
      Date.UTC(
        now.getUTCFullYear(),
        now.getUTCMonth(),
        now.getUTCDate() + 1,
        0,
        0,
        0,
        0
      )
    );
    const msUntilMidnightUTC = nextMidnightUTC.getTime() - now.getTime();

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

    setTimeout(() => {
      consolidateYesterday(state.currentConfig!, state, app);

      // Run aggregation after consolidation if Hive partitioning is enabled
      if (aggregationService) {
        const yesterday = new Date();
        yesterday.setUTCDate(yesterday.getUTCDate() - 1);

        aggregationService
          .runDailyAggregation()
          .then(async results => {
            app.debug(
              `Daily aggregation complete: ${JSON.stringify(results.map(r => ({ tier: r.targetTier, files: r.filesCreated })))}`
            );
            // Upload aggregated files to S3 AFTER aggregation completes
            if (state.currentConfig?.s3Upload.enabled && state.currentConfig?.s3Upload.timing === 'consolidation') {
              await uploadConsolidatedFilesToS3(state.currentConfig, yesterday, state, app);
            }
          })
          .catch(err => {
            app.error(`Daily aggregation failed: ${err.message}`);
          });
      }

      // Then run daily consolidation every 24 hours
      state.consolidationInterval = setInterval(
        () => {
          consolidateYesterday(state.currentConfig!, state, app);

          // Run aggregation after consolidation
          if (aggregationService) {
            const yesterday = new Date();
            yesterday.setUTCDate(yesterday.getUTCDate() - 1);

            aggregationService
              .runDailyAggregation()
              .then(async results => {
                app.debug(
                  `Daily aggregation complete: ${JSON.stringify(results.map(r => ({ tier: r.targetTier, files: r.filesCreated })))}`
                );
                // Upload aggregated files to S3 AFTER aggregation completes
                if (state.currentConfig?.s3Upload.enabled && state.currentConfig?.s3Upload.timing === 'consolidation') {
                  await uploadConsolidatedFilesToS3(state.currentConfig, yesterday, state, app);
                }
              })
              .catch(err => {
                app.error(`Daily aggregation failed: ${err.message}`);
              });
          }
        },
        24 * 60 * 60 * 1000
      );
    }, msUntilMidnightUTC);

    // Run startup consolidation for missed previous days
    setTimeout(() => {
      consolidateMissedDays(state.currentConfig!, state, app);
    }, 5000); // Wait 5 seconds after startup to avoid conflicts

    // Upload all existing consolidated files to S3 (for catching up after BigInt fix)
    if (state.currentConfig.s3Upload.enabled) {
      setTimeout(() => {
        uploadAllConsolidatedFilesToS3(state.currentConfig!, state, app);
      }, 10000); // Wait 10 seconds after startup to avoid conflicts
    }

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
      // Build S3 query config if S3 is enabled
      const s3QueryConfig = state.currentConfig.s3Upload.enabled
        ? {
            enabled: true,
            bucket: state.currentConfig.s3Upload.bucket || '',
            keyPrefix: state.currentConfig.s3Upload.keyPrefix || '',
            region: state.currentConfig.s3Upload.region || 'us-east-1',
          }
        : undefined;

      registerHistoryApiRoute(
        app as unknown as Router,
        app.selfId,
        state.currentConfig.outputDirectory,
        app.debug,
        app,
        state.sqliteBuffer, // Pass SQLite buffer for federated queries
        state.currentConfig.exportIntervalMinutes || 5, // Export interval for buffer cutoff
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
        app.debug
      );
    } catch (error) {
      app.error(`Failed to register as History API provider: ${error}`);
    }

    // Initialize historical streaming service (for history API endpoints)
    try {
      state.historicalStreamingService = new HistoricalStreamingService(
        app,
        state.currentConfig.outputDirectory
      );
    } catch (error) {
      app.error(`Failed to initialize historical streaming service: ${error}`);
    }

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

    // Stop export service and flush SQLite buffer
    if (state.exportService) {
      try {
        state.exportService.stop();
        // Force final export of pending records
        await state.exportService.forceExport();
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
        title: 'Buffer Size',
        description: 'Number of records to buffer before writing to file',
        default: 1000,
        minimum: 10,
        maximum: 10000,
      },
      saveIntervalSeconds: {
        type: 'number',
        title: 'Save Interval (seconds)',
        description: 'How often to save buffered data to files',
        default: 30,
        minimum: 5,
        maximum: 300,
      },
      outputDirectory: {
        type: 'string',
        title: 'Output Directory',
        description:
          'Directory to save data files (defaults to application_data/{vessel}/signalk-parquet)',
        default: '',
      },
      filenamePrefix: {
        type: 'string',
        title: 'Filename Prefix',
        description: 'Prefix for generated filenames',
        default: 'signalk_data',
      },
      fileFormat: {
        type: 'string',
        title: 'File Format',
        description: 'Format for saved data files',
        enum: ['json', 'csv', 'parquet'],
        default: 'parquet',
      },
      retentionDays: {
        type: 'number',
        title: 'Retention Days',
        description: 'Days to keep processed files',
        default: 7,
        minimum: 1,
        maximum: 365,
      },
      exportIntervalMinutes: {
        type: 'number',
        title: 'Export Interval (minutes)',
        description:
          'How often to export data from SQLite buffer to Parquet files.',
        default: 5,
        minimum: 1,
        maximum: 60,
      },
      exportBatchSize: {
        type: 'number',
        title: 'Export Batch Size',
        description:
          'Maximum number of records to export per cycle. Increase if pending records are backing up.',
        default: 50000,
        minimum: 1000,
        maximum: 200000,
      },
      bufferRetentionHours: {
        type: 'number',
        title: 'Buffer Retention (hours)',
        description:
          'How long to keep exported records in SQLite buffer before cleanup.',
        default: 24,
        minimum: 1,
        maximum: 168,
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
      s3Upload: {
        type: 'object',
        title: 'S3 Upload Configuration',
        description: 'Optional S3 backup/archive functionality',
        properties: {
          enabled: {
            type: 'boolean',
            title: 'Enable S3 Upload',
            description: 'Enable uploading files to Amazon S3',
            default: false,
          },
          timing: {
            type: 'string',
            title: 'Upload Timing',
            description: 'When to upload files to S3',
            enum: ['realtime', 'consolidation'],
            enumNames: [
              'Real-time (after each file save)',
              'At consolidation (daily)',
            ],
            default: 'consolidation',
          },
          bucket: {
            type: 'string',
            title: 'S3 Bucket Name',
            description: 'Name of the S3 bucket to upload to',
            default: '',
          },
          region: {
            type: 'string',
            title: 'AWS Region',
            description: 'AWS region where the S3 bucket is located',
            default: 'us-east-1',
          },
          keyPrefix: {
            type: 'string',
            title: 'S3 Key Prefix',
            description:
              'Optional prefix for S3 object keys (e.g., "marine-data/")',
            default: '',
          },
          accessKeyId: {
            type: 'string',
            title: 'AWS Access Key ID',
            description:
              'AWS Access Key ID (leave empty to use IAM role or environment variables)',
            default: '',
          },
          secretAccessKey: {
            type: 'string',
            title: 'AWS Secret Access Key',
            description:
              'AWS Secret Access Key (leave empty to use IAM role or environment variables)',
            default: '',
          },
          deleteAfterUpload: {
            type: 'boolean',
            title: 'Delete Local Files After Upload',
            description: 'Delete local files after successful upload to S3',
            default: false,
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

    // Reuse the existing historical streaming service instead of creating a new one
    state.streamingService = state.historicalStreamingService;
    state.streamingEnabled = true;

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
