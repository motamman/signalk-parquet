import * as fs from 'fs-extra';
import * as path from 'path';
import { Router } from 'express';
import { ParquetWriter } from './parquet-writer';
import { registerHistoryApiRoute } from './HistoryAPI';
import { registerApiRoutes } from './api-routes';
import { HistoricalStreamingService } from './historical-streaming';
import {
  SignalKPlugin,
  PluginConfig,
  PluginState,
  PathConfig,
} from './types';
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
  SUPPORTED_CLAUDE_MODELS,
  DEFAULT_CLAUDE_MODEL,
  CLAUDE_MODEL_DESCRIPTIONS,
  getValidClaudeModel,
} from './claude-models';
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

    // Get vessel MMSI from SignalK
    const vesselMMSI =
      app.getSelfPath('mmsi') || app.getSelfPath('name') || 'unknown_vessel';

    // Use SignalK's application data directory
    const defaultOutputDir = path.join(app.getDataDirPath(), 'signalk-parquet');

    // Validate and migrate Claude model if needed
    let claudeConfig = options?.claudeIntegration || { enabled: false };
    if (claudeConfig.model) {
      const validatedModel = getValidClaudeModel(claudeConfig.model);
      if (validatedModel !== claudeConfig.model) {
        app.debug(`⚠️ Saved Claude model "${claudeConfig.model}" is no longer supported. Migrating to ${validatedModel}`);
        claudeConfig = { ...claudeConfig, model: validatedModel };

        // Save migrated config
        app.savePluginOptions(
          { ...options, claudeIntegration: claudeConfig },
          (err?: unknown) => {
            if (err) {
              app.error(`Failed to save migrated Claude model: ${err}`);
            } else {
              app.debug(`✅ Successfully migrated Claude model to ${validatedModel}`);
            }
          }
        );
      }
    }

    state.currentConfig = {
      bufferSize: options?.bufferSize || 1000,
      saveIntervalSeconds: options?.saveIntervalSeconds || 30,
      outputDirectory: options?.outputDirectory || defaultOutputDir,
      filenamePrefix: options?.filenamePrefix || 'signalk_data',
      retentionDays: options?.retentionDays || 7,
      fileFormat: options?.fileFormat || 'parquet',
      vesselMMSI: vesselMMSI,
      s3Upload: options?.s3Upload || { enabled: false },
      claudeIntegration: claudeConfig,
      homePortLatitude: options?.homePortLatitude || 0,
      homePortLongitude: options?.homePortLongitude || 0,
      setCurrentLocationAction: options?.setCurrentLocationAction || {
        setCurrentLocation: false,
      },
      // enableStreaming: options?.enableStreaming ?? false,
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

    // Start threshold monitoring system
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
    try {
      registerHistoryApiRoute(
        app as unknown as Router,
        app.selfId,
        state.currentConfig.outputDirectory,
        app.debug,
        app
      );
    } catch (error) {
      app.error(`Failed to register History API routes with main server: ${error}`);
    }

    // Initialize historical streaming service (for history API endpoints)
    try {
      state.historicalStreamingService = new HistoricalStreamingService(app, state.currentConfig.outputDirectory);
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
    if (state.currentConfig.homePortLatitude && state.currentConfig.homePortLongitude &&
        state.currentConfig.homePortLatitude !== 0 && state.currentConfig.homePortLongitude !== 0) {
      publishHomePortToSignalK(state.currentConfig.homePortLatitude, state.currentConfig.homePortLongitude);
    }

  };

  plugin.stop = function (): void {

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
      claudeIntegration: {
        type: 'object',
        title: 'Claude AI Analysis',
        description: 'Configure Claude AI for intelligent data analysis and insights',
        properties: {
          enabled: {
            type: 'boolean',
            title: 'Enable Claude AI Analysis',
            description: 'Enable AI-powered analysis of your maritime data',
            default: false,
          },
          apiKey: {
            type: 'string',
            title: 'Claude API Key',
            description: 'Your Anthropic Claude API key (get one at console.anthropic.com)',
            default: '',
          },
          model: {
            type: 'string',
            title: 'Default Claude Model (Optional)',
            description: 'Default Claude model for analysis. Can be overridden in the web interface.',
            enum: [...SUPPORTED_CLAUDE_MODELS],
            enumNames: SUPPORTED_CLAUDE_MODELS.map(m => CLAUDE_MODEL_DESCRIPTIONS[m]),
            default: DEFAULT_CLAUDE_MODEL,
          },
          maxTokens: {
            type: 'number',
            title: 'Max Tokens',
            description: 'Maximum tokens for Claude responses (higher = more detailed analysis)',
            default: 4000,
            minimum: 1000,
            maximum: 8192,
          },
          temperature: {
            type: 'number',
            title: 'Temperature',
            description: 'Analysis creativity (0.0 = focused, 1.0 = creative)',
            default: 0.3,
            minimum: 0.0,
            maximum: 1.0,
          },
          autoAnalysis: {
            type: 'object',
            title: 'Automated Analysis',
            description: 'Configure automated analysis features',
            properties: {
              daily: {
                type: 'boolean',
                title: 'Daily Summary Reports',
                description: 'Generate daily analysis reports automatically',
                default: false,
              },
              anomaly: {
                type: 'boolean',
                title: 'Anomaly Detection',
                description: 'Automatically detect unusual patterns in real-time',
                default: false,
              },
              threshold: {
                type: 'number',
                title: 'Anomaly Threshold',
                description: 'Statistical threshold for anomaly detection (higher = fewer alerts)',
                default: 2.5,
                minimum: 1.0,
                maximum: 5.0,
              },
            },
          },
          cacheEnabled: {
            type: 'boolean',
            title: 'Enable Result Caching',
            description: 'Cache analysis results to reduce API calls and costs',
            default: true,
          },
        },
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
            publishHomePortToSignalK(currentPosition!.latitude, currentPosition!.longitude);
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
  function getCurrentVesselPosition(): { latitude: number; longitude: number; timestamp: Date } | null {
    try {
      const position = app.getSelfPath('navigation.position');
      if (position && position.value && position.value.latitude && position.value.longitude) {
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
    app.debug(`Published home port position to SignalK: ${latitude}, ${longitude}`);
  }

  return plugin;
}

// Streaming service lifecycle functions for runtime control
export async function initializeStreamingService(state: PluginState, app: ServerAPI): Promise<{ success: boolean; error?: string }> {
  try {
    if (state.streamingService) {
      return { success: true, error: 'Streaming service is already running' };
    }

    if (!state.currentConfig?.enableStreaming) {
      return { success: false, error: 'Streaming is disabled in plugin configuration. Enable it in settings first.' };
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

export function shutdownStreamingService(state: PluginState, app: ServerAPI): { success: boolean; error?: string } {
  try {
    if (!state.streamingService) {
      return { success: true, error: 'Streaming service is not running' };
    }

    // Store active subscriptions for potential restoration
    if (state.streamingService.getActiveSubscriptions) {
      const activeSubscriptions = state.streamingService.getActiveSubscriptions();
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