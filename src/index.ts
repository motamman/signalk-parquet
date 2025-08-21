import * as fs from 'fs-extra';
import * as path from 'path';
import { Router } from 'express';
import { ParquetWriter } from './parquet-writer';
import { registerHistoryApiRoute } from './HistoryAPI';
import { registerApiRoutes } from './api-routes';
import { StreamingService } from './streaming-service';
import {
  SignalKPlugin,
  PluginConfig,
  PluginState,
  PathConfig,
  StreamingSubscriptionConfig,
} from './types';
import {
  loadWebAppConfig,
  initializeCommandState,
  getCurrentCommands,
  setCurrentCommands,
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
} from './data-handler';
import { ServerAPI } from '@signalk/server-api';


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
    dataBuffers: new Map(),
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
    app.debug('Starting...');

    // Get vessel MMSI from SignalK
    const vesselMMSI =
      app.getSelfPath('mmsi') || app.getSelfPath('name') || 'unknown_vessel';

    // Use SignalK's application data directory
    const defaultOutputDir = path.join(app.getDataDirPath(), 'signalk-parquet');

    state.currentConfig = {
      bufferSize: options?.bufferSize || 1000,
      saveIntervalSeconds: options?.saveIntervalSeconds || 30,
      outputDirectory: options?.outputDirectory || defaultOutputDir,
      filenamePrefix: options?.filenamePrefix || 'signalk_data',
      retentionDays: options?.retentionDays || 7,
      fileFormat: options?.fileFormat || 'parquet',
      vesselMMSI: vesselMMSI,
      s3Upload: options?.s3Upload || { enabled: false },
      enableStreaming: options?.enableStreaming ?? true,
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

    // Initialize S3 client if enabled
    await initializeS3(state.currentConfig, app);
    state.s3Client = createS3Client(state.currentConfig, app);

    // Ensure output directory exists
    fs.ensureDirSync(state.currentConfig.outputDirectory);

    // Subscribe to command paths first (these control regimens)
    subscribeToCommandPaths(currentPaths, state, state.currentConfig, app);

    // Check current command values at startup
    initializeRegimenStates(currentPaths, state, app);

    // Initialize command state
    initializeCommandState(currentPaths, app);

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

    app.debug(
      `Next consolidation at ${nextMidnightUTC.toISOString()} (in ${Math.round(msUntilMidnightUTC / 1000 / 60)} minutes)`
    );

    setTimeout(() => {
      consolidateYesterday(state.currentConfig!, state, app);

      // Then run daily consolidation every 24 hours
      state.consolidationInterval = setInterval(
        () => {
          consolidateYesterday(state.currentConfig!, state, app);
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

    // Register History API routes directly with the main app
    app.debug('Registering History API routes with main server...');
    try {
      registerHistoryApiRoute(
        app as unknown as Router,
        app.selfId,
        state.currentConfig.outputDirectory,
        app.debug,
        app
      );
      app.debug('History API routes registered with main server successfully');
    } catch (error) {
      app.error(`Failed to register History API routes with main server: ${error}`);
    }

    // Initialize streaming service if enabled (with delay to ensure clean startup)
    if (state.currentConfig.enableStreaming) {
      setTimeout(async () => {
        try {
          app.debug('Initializing streaming service...');
          
          // We'll create the HistoryAPI instance here for streaming
          const { HistoryAPI } = require('./HistoryAPI');
          const historyAPI = new HistoryAPI(
            app.selfId,
            state.currentConfig!.outputDirectory
          );
          
          // Access the HTTP server from the SignalK app
          // SignalK exposes the HTTP server via the router's parent app
          app.debug('Attempting to find HTTP server...');
          app.debug(`app.router exists: ${!!(app as any).router}`);
          app.debug(`app.router.parent exists: ${!!(app as any).router?.parent}`);
          app.debug(`app.router.parent.server exists: ${!!(app as any).router?.parent?.server}`);
          
          const httpServer = (app as any).router?.parent?.server || 
                           (app as any).httpServer || 
                           (app as any).server;
          
          if (httpServer) {
            app.debug(`HTTP server found: ${httpServer.constructor.name}`);
            app.debug(`HTTP server listening: ${httpServer.listening}`);
            app.debug(`HTTP server address: ${JSON.stringify(httpServer.address())}`);
            
            // Create streaming service with error isolation
            try {
              state.streamingService = new StreamingService(httpServer, {
                historyAPI: historyAPI,
                selfId: app.selfId,
                debug: true  // Always enable debug for troubleshooting
              });
              
              // Only log success if streaming service was actually created
              if (state.streamingService) {
                app.debug('Streaming service initialized successfully');
                
                // New streaming service doesn't need auto-restore
                app.debug('New streaming service ready for subscriptions');
              } else {
                app.error('Streaming service creation returned null/undefined');
              }
            } catch (streamingError) {
              app.error(`Error creating streaming service: ${streamingError}`);
              app.error(`Streaming service error stack: ${(streamingError as Error).stack}`);
            }
          } else {
            app.error('HTTP server not available, streaming service not initialized');
            app.debug(`Available app properties: ${Object.keys(app).filter(k => !k.startsWith('_')).join(', ')}`);
          }
        } catch (error) {
          app.error(`Failed to initialize streaming service: ${error}`);
          app.debug(`Streaming service error stack: ${(error as Error).stack}`);
          // Don't let streaming failure crash the plugin
          state.streamingService = undefined;
        }
      }, 1000); // Increased delay to 1000ms for better stability
    } else {
      app.debug('Streaming service disabled in configuration');
    }

    app.debug('Started');
  };

  plugin.stop = function (): void {
    app.debug('Stopping...');

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

    // Unsubscribe from all paths
    state.unsubscribes.forEach(unsubscribe => {
      if (typeof unsubscribe === 'function') {
        unsubscribe();
      }
    });
    state.unsubscribes = [];

    // No more restored subscriptions to clean up

    // Shutdown streaming service
    if (state.streamingService) {
      try {
        state.streamingService.shutdown();
        app.debug('Streaming service shut down successfully');
      } catch (error) {
        app.error(`Error shutting down streaming service: ${error}`);
      }
      state.streamingService = undefined;
    }

    // Clean up stream subscriptions (new streambundle approach)
    if (state.streamSubscriptions) {
      state.streamSubscriptions.forEach(stream => {
        if (stream && typeof stream.end === 'function') {
          stream.end();
        }
      });
      state.streamSubscriptions = [];
    }

    // Clear data structures
    state.dataBuffers.clear();
    state.activeRegimens.clear();
    state.subscribedPaths.clear();
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
      enableStreaming: {
        type: 'boolean',
        title: 'Enable Streaming Service',
        description: 'Enable real-time data streaming via WebSockets for live data monitoring (stream configurations are managed in the webapp)',
        default: true,
      },
    },
  };

  // Webapp static files and API routes
  plugin.registerWithRouter = function (router: Router): void {
    registerApiRoutes(router, state, app);
  };

  return plugin;
}

// Re-export utility functions for backward compatibility
export { toContextFilePath, toParquetFilePath } from './utils/path-helpers';
