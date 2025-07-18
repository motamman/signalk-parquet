import * as fs from 'fs-extra';
import * as path from 'path';
import express from 'express';
import { Router } from 'express';
import { ParquetWriter } from './parquet-writer';
import {
  SignalKPlugin,
  PluginConfig,
  PathConfig,
  DataRecord,
  PluginState,
  TypedRequest,
  TypedResponse,
  PathsApiResponse,
  FilesApiResponse,
  QueryApiResponse,
  SampleApiResponse,
  ConfigApiResponse,
  HealthApiResponse,
  S3TestApiResponse,
  QueryRequest,
  PathConfigRequest,
  PathInfo,
  DuckDBInstance,
  WebAppPathConfig,
  CommandConfig,
  CommandRegistrationState,
  CommandExecutionRequest,
  CommandRegistrationRequest,
  CommandApiResponse,
  CommandExecutionResponse,
  CommandPutHandler,
  CommandExecutionResult,
  CommandHistoryEntry,
} from './types';
import { glob } from 'glob';
import {
  Context,
  Delta,
  hasValues,
  Path,
  PathValue,
  ServerAPI,
  Update,
  Timestamp,
} from '@signalk/server-api';

// AWS S3 for file upload
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let S3Client: any, PutObjectCommand: any, ListObjectsV2Command: any;
import('@aws-sdk/client-s3')
  .then(s3 => {
    S3Client = s3.S3Client;
    PutObjectCommand = s3.PutObjectCommand;
    ListObjectsV2Command = s3.ListObjectsV2Command;
  })
  .catch(() => {
    // eslint-disable-next-line no-console
    console.warn('AWS S3 SDK not available for file uploads');
  });

// DuckDB for webapp queries
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let DuckDBInstance: any;
import('@duckdb/node-api')
  .then(duckdb => (DuckDBInstance = duckdb.DuckDBInstance))
  .catch(() => {
    // eslint-disable-next-line no-console
    console.warn('DuckDB not available for webapp queries');
  });

// Global variables for path and command management
let currentPaths: PathConfig[] = [];
let currentCommands: CommandConfig[] = [];
let commandHistory: CommandHistoryEntry[] = [];
let appInstance: ServerAPI;

// Command state management
const commandState: CommandRegistrationState = {
  registeredCommands: new Map<string, CommandConfig>(),
  putHandlers: new Map<string, CommandPutHandler>(),
};

// Enhanced function to load webapp configuration
function loadWebAppConfig(): WebAppPathConfig {
  const webAppConfigPath = path.join(
    appInstance.getDataDirPath(),
    'signalk-parquet',
    'webapp-config.json'
  );
  try {
    if (fs.existsSync(webAppConfigPath)) {
      const configData = fs.readFileSync(webAppConfigPath, 'utf8');
      const webAppConfig: WebAppPathConfig = JSON.parse(configData);
      return {
        paths: webAppConfig.paths || [],
        commands: webAppConfig.commands || [],
      };
    }
  } catch (error) {
    appInstance.debug(`Failed to load webapp configuration: ${error}`);
  }
  return { paths: [], commands: [] };
}

// Enhanced function to save webapp configuration
function saveWebAppConfig(
  paths: PathConfig[],
  commands: CommandConfig[]
): void {
  const webAppConfigPath = path.join(
    appInstance.getDataDirPath(),
    'signalk-parquet',
    'webapp-config.json'
  );
  try {
    const configDir = path.dirname(webAppConfigPath);
    if (!fs.existsSync(configDir)) {
      fs.mkdirSync(configDir, { recursive: true });
    }

    const webAppConfig: WebAppPathConfig = { paths, commands };
    fs.writeFileSync(webAppConfigPath, JSON.stringify(webAppConfig, null, 2));
    appInstance.debug(
      `Saved webapp configuration: ${paths.length} paths, ${commands.length} commands`
    );
  } catch (error) {
    appInstance.error(`Failed to save webapp configuration: ${error}`);
  }
}

// Legacy function for backward compatibility
function loadWebAppPaths(): PathConfig[] {
  return loadWebAppConfig().paths;
}

// Legacy function for backward compatibility
function saveWebAppPaths(paths: PathConfig[]): void {
  saveWebAppConfig(paths, currentCommands);
}

export = function (app: ServerAPI): SignalKPlugin {
  // Store app instance for global access
  appInstance = app;
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
    dataBuffers: new Map<string, DataRecord[]>(),
    activeRegimens: new Set<string>(),
    subscribedPaths: new Set<string>(),
    saveInterval: undefined,
    consolidationInterval: undefined,
    parquetWriter: undefined,
    s3Client: undefined,
    currentConfig: undefined,
    commandState: commandState,
  };

  plugin.start = function (options: Partial<PluginConfig>): void {
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
    };

    // Load webapp configuration including commands
    const webAppConfig = loadWebAppConfig();
    currentPaths = webAppConfig.paths;
    currentCommands = webAppConfig.commands;

    // Initialize ParquetWriter
    state.parquetWriter = new ParquetWriter({
      format: state.currentConfig.fileFormat,
      app: app,
    });

    // Initialize S3 client if enabled
    if (state.currentConfig.s3Upload.enabled && S3Client) {
      try {
        const s3Config: {
          region: string;
          credentials?: { accessKeyId: string; secretAccessKey: string };
        } = {
          region: state.currentConfig.s3Upload.region || 'us-east-1',
        };

        // Add credentials if provided
        if (
          state.currentConfig.s3Upload.accessKeyId &&
          state.currentConfig.s3Upload.secretAccessKey
        ) {
          s3Config.credentials = {
            accessKeyId: state.currentConfig.s3Upload.accessKeyId,
            secretAccessKey: state.currentConfig.s3Upload.secretAccessKey,
          };
        }

        state.s3Client = new S3Client(s3Config);
        app.debug(
          `S3 client initialized for bucket: ${state.currentConfig.s3Upload.bucket}`
        );
      } catch (error) {
        app.debug(`Error initializing S3 client: ${error}`);
        state.s3Client = undefined;
      }
    }

    // Ensure output directory exists
    fs.ensureDirSync(state.currentConfig.outputDirectory);

    // Subscribe to command paths first (these control regimens)
    subscribeToCommandPaths(state.currentConfig);

    // Check current command values at startup
    initializeRegimenStates(state.currentConfig);

    // Initialize command state
    initializeCommandState();

    // Subscribe to data paths based on initial regimen states
    updateDataSubscriptions(state.currentConfig);

    // Set up periodic save
    state.saveInterval = setInterval(() => {
      saveAllBuffers(state.currentConfig!);
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
      consolidateYesterday(state.currentConfig!);

      // Then run daily consolidation every 24 hours
      state.consolidationInterval = setInterval(
        () => {
          consolidateYesterday(state.currentConfig!);
        },
        24 * 60 * 60 * 1000
      );
    }, msUntilMidnightUTC);

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
    saveAllBuffers();

    // Unsubscribe from all paths
    state.unsubscribes.forEach(unsubscribe => {
      if (typeof unsubscribe === 'function') {
        unsubscribe();
      }
    });
    state.unsubscribes = [];

    // Clear data structures
    state.dataBuffers.clear();
    state.activeRegimens.clear();
    state.subscribedPaths.clear();
  };

  // Command Management Functions

  // Initialize command state on plugin start
  function initializeCommandState(): void {
    // Clear existing command state
    commandState.registeredCommands.clear();
    commandState.putHandlers.clear();

    // Re-register commands from configuration
    currentCommands.forEach((commandConfig: CommandConfig) => {
      const result = registerCommand(
        commandConfig.command,
        commandConfig.description
      );
      if (result.state === 'COMPLETED') {
        app.debug(`✅ Restored command: ${commandConfig.command}`);
      } else {
        app.error(
          `❌ Failed to restore command: ${commandConfig.command} - ${result.message}`
        );
      }
    });

    // Reset all commands to false on startup
    currentCommands.forEach((commandConfig: CommandConfig) => {
      initializeCommandValue(commandConfig.command, false);
    });

    app.debug(
      `🎮 Command state initialized with ${currentCommands.length} commands`
    );
  }

  // Command registration with full type safety
  function registerCommand(
    commandName: string,
    description?: string
  ): CommandExecutionResult {
    try {
      // Validate command name
      if (!isValidCommandName(commandName)) {
        return {
          state: 'FAILED',
          statusCode: 400,
          message:
            'Invalid command name. Use alphanumeric characters and underscores only.',
          timestamp: new Date().toISOString(),
        };
      }

      // Check if command already exists
      if (commandState.registeredCommands.has(commandName)) {
        return {
          state: 'FAILED',
          statusCode: 409,
          message: `Command '${commandName}' already registered`,
          timestamp: new Date().toISOString(),
        };
      }

      const commandPath = `commands.${commandName}`;
      const fullPath = `vessels.self.${commandPath}`;

      // Create command configuration
      const commandConfig: CommandConfig = {
        command: commandName,
        path: fullPath,
        registered: new Date().toISOString(),
        description: description || `Command: ${commandName}`,
        active: false,
        lastExecuted: undefined,
      };

      // Create PUT handler with proper typing
      const putHandler: CommandPutHandler = (
        context: string,
        path: string,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        value: any,
        _callback?: (result: CommandExecutionResult) => void
      ): CommandExecutionResult => {
        app.debug(
          `Handling PUT for commands.${commandName} with value: ${JSON.stringify(value)}`
        );
        return executeCommand(commandName, Boolean(value));
      };

      // Register PUT handler with SignalK
      app.registerPutHandler(
        'vessels.self',
        commandPath,
        putHandler as unknown as () => void, //FIXME server api registerPutHandler is incorrectly typed https://github.com/SignalK/signalk-server/pull/2043
        'zennora-parquet-commands'
      );

      // Store command and handler
      commandState.registeredCommands.set(commandName, commandConfig);
      commandState.putHandlers.set(commandName, putHandler);

      // Initialize command value to false
      initializeCommandValue(commandName, false);

      // Update current commands and save
      currentCommands = Array.from(commandState.registeredCommands.values());
      saveWebAppConfig(currentPaths, currentCommands);

      // Log command registration
      addCommandHistoryEntry(commandName, 'REGISTER', undefined, true);

      app.debug(`✅ Registered command: ${commandName} at ${fullPath}`);

      return {
        state: 'COMPLETED',
        statusCode: 200,
        message: `Command '${commandName}' registered successfully`,
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      const errorMessage = `Failed to register command '${commandName}': ${error}`;
      app.error(errorMessage);

      return {
        state: 'FAILED',
        statusCode: 500,
        message: errorMessage,
        timestamp: new Date().toISOString(),
      };
    }
  }

  // Command unregistration with type safety
  function unregisterCommand(commandName: string): CommandExecutionResult {
    try {
      const commandConfig = commandState.registeredCommands.get(commandName);
      if (!commandConfig) {
        return {
          state: 'FAILED',
          statusCode: 404,
          message: `Command '${commandName}' not found`,
          timestamp: new Date().toISOString(),
        };
      }

      // Remove PUT handler (SignalK API doesn't have unregister, but we can track it)
      commandState.putHandlers.delete(commandName);
      commandState.registeredCommands.delete(commandName);

      // Update current commands and save
      currentCommands = Array.from(commandState.registeredCommands.values());
      saveWebAppConfig(currentPaths, currentCommands);

      // Log command unregistration
      addCommandHistoryEntry(commandName, 'UNREGISTER', undefined, true);

      app.debug(`🗑️ Unregistered command: ${commandName}`);

      return {
        state: 'COMPLETED',
        statusCode: 200,
        message: `Command '${commandName}' unregistered successfully`,
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      const errorMessage = `Failed to unregister command '${commandName}': ${error}`;
      app.error(errorMessage);

      return {
        state: 'FAILED',
        statusCode: 500,
        message: errorMessage,
        timestamp: new Date().toISOString(),
      };
    }
  }

  // Command execution with full type safety
  function executeCommand(
    commandName: string,
    value: boolean
  ): CommandExecutionResult {
    try {
      const commandConfig = commandState.registeredCommands.get(commandName);
      if (!commandConfig) {
        return {
          state: 'FAILED',
          statusCode: 404,
          message: `Command '${commandName}' not found`,
          timestamp: new Date().toISOString(),
        };
      }

      // Execute the command by sending delta
      const timestamp = new Date().toISOString();
      const delta: Delta = {
        context: 'vessels.self' as Context,
        updates: [
          {
            source: {
              label: 'signalk-parquet-commands',
              type: 'plugin',
            },
            timestamp: timestamp as Timestamp,
            values: [
              {
                path: `commands.${commandName}` as Path,
                value: value,
              },
            ],
          },
        ],
      };

      // Send delta message
      //FIXME see if delta can be Delta from the beginning
      app.handleMessage('signalk-parquet', delta as Delta);

      // Update command state
      commandConfig.active = value;
      commandConfig.lastExecuted = timestamp;
      commandState.registeredCommands.set(commandName, commandConfig);

      // Update current commands and save
      currentCommands = Array.from(commandState.registeredCommands.values());
      saveWebAppConfig(currentPaths, currentCommands);

      // Log command execution
      addCommandHistoryEntry(commandName, 'EXECUTE', value, true);

      app.debug(`🎮 Executed command: ${commandName} = ${value}`);

      return {
        state: 'COMPLETED',
        statusCode: 200,
        message: `Command '${commandName}' executed: ${value}`,
        timestamp: timestamp,
      };
    } catch (error) {
      const errorMessage = `Failed to execute command '${commandName}': ${error}`;
      app.error(errorMessage);

      addCommandHistoryEntry(
        commandName,
        'EXECUTE',
        value,
        false,
        errorMessage
      );

      return {
        state: 'FAILED',
        statusCode: 500,
        message: errorMessage,
        timestamp: new Date().toISOString(),
      };
    }
  }

  // Helper functions with type safety
  function isValidCommandName(commandName: string): boolean {
    const validPattern = /^[a-zA-Z0-9_]+$/;
    return (
      validPattern.test(commandName) &&
      commandName.length > 0 &&
      commandName.length <= 50
    );
  }

  function initializeCommandValue(commandName: string, value: boolean): void {
    const timestamp = new Date().toISOString() as Timestamp;
    const delta: Delta = {
      context: 'vessels.self' as Context,
      updates: [
        {
          source: {
            label: 'signalk-parquet-commands',
            type: 'plugin',
          },
          timestamp,
          values: [
            {
              path: `commands.${commandName}` as Path,
              value,
            },
          ],
        },
      ],
    };

    //FIXME
    app.handleMessage('signalk-parquet', delta as Delta);
  }

  function addCommandHistoryEntry(
    command: string,
    action: 'EXECUTE' | 'STOP' | 'REGISTER' | 'UNREGISTER',
    value?: boolean,
    success: boolean = true,
    error?: string
  ): void {
    const entry: CommandHistoryEntry = {
      command,
      action,
      value,
      timestamp: new Date().toISOString(),
      success,
      error,
    };

    commandHistory.push(entry);

    // Keep only last 100 entries
    if (commandHistory.length > 100) {
      commandHistory = commandHistory.slice(-100);
    }
  }

  // Subscribe to command paths that control regimens using proper subscription manager
  function subscribeToCommandPaths(config: PluginConfig): void {
    const commandPaths = currentPaths.filter(
      (pathConfig: PathConfig) =>
        pathConfig &&
        pathConfig.path &&
        pathConfig.path.startsWith('commands.') &&
        pathConfig.enabled
    );

    if (commandPaths.length === 0) return;

    const commandSubscription = {
      context: 'vessels.self' as Context,
      subscribe: commandPaths.map((pathConfig: PathConfig) => ({
        path: pathConfig.path,
        period: 1000, // Check commands every second
        policy: 'fixed' as const,
      })),
    };

    app.debug(
      `Subscribing to ${commandPaths.length} command paths via subscription manager`
    );

    app.subscriptionmanager.subscribe(
      commandSubscription,
      state.unsubscribes,
      (subscriptionError: unknown) => {
        app.debug(`Command subscription error: ${subscriptionError}`);
      },
      (delta: Delta) => {
        // Process each update in the delta
        if (delta.updates) {
          delta.updates.forEach((update: Update) => {
            if (hasValues(update)) {
              update.values.forEach((valueUpdate: PathValue) => {
                const pathConfig = commandPaths.find(
                  p => p.path === valueUpdate.path
                );
                if (pathConfig) {
                  handleCommandMessage(valueUpdate, pathConfig, config, update);
                }
              });
            }
          });
        }
      }
    );

    commandPaths.forEach(pathConfig => {
      state.subscribedPaths.add(pathConfig.path);
    });
  }

  // Handle command messages (regimen control) - now receives complete delta structure
  function handleCommandMessage(
    valueUpdate: PathValue,
    pathConfig: PathConfig,
    config: PluginConfig,
    update: Update
  ): void {
    try {
      app.debug(
        `📦 Received command update for ${pathConfig.path}: ${JSON.stringify(valueUpdate, null, 2)}`
      );

      // Check source filter if specified for commands too
      if (pathConfig.source && pathConfig.source.trim() !== '') {
        const messageSource =
          update.$source || (update.source ? update.source.label : null);
        if (messageSource !== pathConfig.source.trim()) {
          app.debug(
            `🚫 Command from source "${messageSource}" filtered out (expecting "${pathConfig.source.trim()}")`
          );
          return;
        }
      }

      if (valueUpdate.value !== undefined) {
        const commandName = extractCommandName(pathConfig.path);
        const isActive = Boolean(valueUpdate.value);

        app.debug(
          `Command ${commandName}: ${isActive ? 'ACTIVE' : 'INACTIVE'}`
        );

        if (isActive) {
          state.activeRegimens.add(commandName);
        } else {
          state.activeRegimens.delete(commandName);
        }

        // Debug active regimens state
        app.debug(
          `🎯 Active regimens: [${Array.from(state.activeRegimens).join(', ')}]`
        );

        // Update data subscriptions based on new regimen state
        updateDataSubscriptions(config);

        // Buffer this command change with complete metadata
        const bufferKey = `${pathConfig.context || 'vessels.self'}:${pathConfig.path}`;
        bufferData(
          bufferKey,
          {
            received_timestamp: new Date().toISOString(),
            signalk_timestamp: update.timestamp || new Date().toISOString(),
            context: 'vessels.self',
            path: valueUpdate.path,
            value: valueUpdate.value,
            source: update.source ? JSON.stringify(update.source) : undefined,
            source_label:
              update.$source ||
              (update.source ? update.source.label : undefined),
            source_type: update.source ? update.source.type : undefined,
            source_pgn: update.source ? update.source.pgn : undefined,
            source_src: update.source ? update.source.src : undefined,
            //FIXME
            // meta: valueUpdate.meta
            //   ? JSON.stringify(valueUpdate.meta)
            //   : undefined,
          },
          config
        );
      }
    } catch (error) {
      app.debug(`Error handling command message: ${error}`);
    }
  }

  // Helper function to handle wildcard contexts
  function handleWildcardContext(pathConfig: PathConfig): PathConfig {
    const context = pathConfig.context || 'vessels.self';

    if (context === 'vessels.*') {
      // For vessels.*, we create a subscription that will receive deltas from any vessel
      // The actual filtering by MMSI will happen in the delta handler
      return {
        ...pathConfig,
        context: 'vessels.*' as Context, // Keep the wildcard for the subscription
      };
    }

    // Not a wildcard, return as-is
    return pathConfig;
  }

  // Helper function to check if a vessel should be excluded based on MMSI
  function shouldExcludeVessel(
    vesselContext: string,
    pathConfig: PathConfig
  ): boolean {
    if (!pathConfig.excludeMMSI || pathConfig.excludeMMSI.length === 0) {
      return false; // No exclusions specified
    }

    try {
      // For vessels.self, use getSelfPath
      if (vesselContext === 'vessels.self') {
        const mmsiData = app.getSelfPath('mmsi');
        if (mmsiData && mmsiData.value) {
          const mmsi = String(mmsiData.value);
          return pathConfig.excludeMMSI.includes(mmsi);
        }
      } else {
        // For other vessels, we would need to get their MMSI from the delta or other means
        // For now, we'll skip MMSI filtering for other vessels
        app.debug(
          `MMSI filtering not implemented for vessel context: ${vesselContext}`
        );
      }
    } catch (error) {
      app.debug(`Error checking MMSI for vessel ${vesselContext}: ${error}`);
    }

    return false; // Don't exclude if we can't determine MMSI
  }

  // Update data path subscriptions based on active regimens
  function updateDataSubscriptions(config: PluginConfig): void {
    // First, unsubscribe from all existing subscriptions
    state.unsubscribes.forEach(unsubscribe => {
      if (typeof unsubscribe === 'function') {
        unsubscribe();
      }
    });
    state.unsubscribes = [];
    state.subscribedPaths.clear();

    app.debug('Cleared all existing subscriptions');

    // Re-subscribe to command paths
    subscribeToCommandPaths(config);

    // Now subscribe to data paths using currentPaths
    const dataPaths = currentPaths.filter(
      (pathConfig: PathConfig) =>
        pathConfig &&
        pathConfig.path &&
        !pathConfig.path.startsWith('commands.')
    );

    const shouldSubscribePaths = dataPaths.filter((pathConfig: PathConfig) =>
      shouldSubscribeToPath(pathConfig)
    );

    // Handle wildcard contexts (like vessels.*)
    const processedPaths: PathConfig[] = shouldSubscribePaths.map(pathConfig =>
      handleWildcardContext(pathConfig)
    );

    if (processedPaths.length === 0) {
      app.debug('No data paths need subscription currently');
      return;
    }

    // Group paths by context for separate subscriptions
    const contextGroups = new Map<Context, PathConfig[]>();
    processedPaths.forEach((pathConfig: PathConfig) => {
      const context = (pathConfig.context || 'vessels.self') as Context;
      if (!contextGroups.has(context)) {
        contextGroups.set(context, []);
      }
      contextGroups.get(context)!.push(pathConfig);
    });

    // Create subscriptions for each context group
    contextGroups.forEach((pathConfigs, context) => {
      const dataSubscription = {
        context: context,
        subscribe: pathConfigs.map(pathConfig => ({
          path: pathConfig.path as Path,
          period: 1000, // Get updates every second max
          policy: 'fixed' as const,
        })),
      };

      app.debug(
        `Subscribing to ${pathConfigs.length} data paths for context ${context}`
      );

      app.subscriptionmanager.subscribe(
        dataSubscription,
        state.unsubscribes,
        (subscriptionError: unknown) => {
          app.debug(
            `Data subscription error for ${context}: ${subscriptionError}`
          );
        },
        (delta: Delta) => {
          // Process each update in the delta
          if (delta.updates) {
            delta.updates.forEach((update: Update) => {
              if (hasValues(update)) {
                update.values.forEach((valueUpdate: PathValue) => {
                  const pathConfig = pathConfigs.find(
                    (p: PathConfig) => p.path === valueUpdate.path
                  );
                  if (pathConfig) {
                    handleDataMessage(
                      valueUpdate,
                      pathConfig,
                      config,
                      update,
                      delta
                    );
                  }
                });
              }
            });
          }
        }
      );

      pathConfigs.forEach(pathConfig => {
        state.subscribedPaths.add(pathConfig.path);
      });
    });
  }

  // Determine if we should subscribe to a path based on regimens
  function shouldSubscribeToPath(pathConfig: PathConfig): boolean {
    // Always subscribe if explicitly enabled
    if (pathConfig.enabled) {
      app.debug(`✅ Path ${pathConfig.path} enabled (always on)`);
      return true;
    }

    // Check if any required regimens are active
    if (pathConfig.regimen) {
      const requiredRegimens = pathConfig.regimen.split(',').map(r => r.trim());
      const hasActiveRegimen = requiredRegimens.some(regimen =>
        state.activeRegimens.has(regimen)
      );
      app.debug(
        `🔍 Path ${pathConfig.path} requires regimens [${requiredRegimens.join(', ')}], active: [${Array.from(state.activeRegimens).join(', ')}] → ${hasActiveRegimen ? 'SUBSCRIBE' : 'SKIP'}`
      );
      return hasActiveRegimen;
    }

    app.debug(
      `❌ Path ${pathConfig.path} has no regimen control and not enabled`
    );
    return false;
  }

  // Handle data messages from SignalK - now receives complete delta structure
  function handleDataMessage(
    valueUpdate: PathValue,
    pathConfig: PathConfig,
    config: PluginConfig,
    update: Update,
    delta: Delta
  ): void {
    try {
      // Check if we should still process this path
      if (!shouldSubscribeToPath(pathConfig)) {
        return;
      }

      // Check if this vessel should be excluded based on MMSI
      const vesselContext = delta.context || 'vessels.self';
      if (shouldExcludeVessel(vesselContext, pathConfig)) {
        app.debug(
          `Excluding data from vessel ${vesselContext} due to MMSI filter`
        );
        return;
      }

      // Check source filter if specified
      if (pathConfig.source && pathConfig.source.trim() !== '') {
        const messageSource =
          update.$source || (update.source ? update.source.label : null);
        if (messageSource !== pathConfig.source.trim()) {
          // Source doesn't match filter, skip this message
          return;
        }
      }

      const record: DataRecord = {
        received_timestamp: new Date().toISOString(),
        signalk_timestamp: update.timestamp || new Date().toISOString(),
        context: delta.context || pathConfig.context || 'vessels.self', // Use actual context from delta message
        path: valueUpdate.path,
        value: null,
        value_json: undefined,
        source: update.source ? JSON.stringify(update.source) : undefined,
        source_label:
          update.$source || (update.source ? update.source.label : undefined),
        source_type: update.source ? update.source.type : undefined,
        source_pgn: update.source ? update.source.pgn : undefined,
        source_src: update.source ? update.source.src : undefined,
        //FIXME should meta be removed from the stored values? normal value updates don't have them
        // meta: valueUpdate.meta ? JSON.stringify(valueUpdate.meta) : undefined,
      };

      // Handle different value types (matching Python logic)
      if (typeof valueUpdate.value === 'object' && valueUpdate.value !== null) {
        record.value_json = JSON.stringify(valueUpdate.value);

        // Flatten object properties for easier querying
        for (const [key, val] of Object.entries(valueUpdate.value)) {
          if (
            typeof val === 'string' ||
            typeof val === 'number' ||
            typeof val === 'boolean'
          ) {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            (record as any)[`value_${key}`] = val;
          }
        }
      } else {
        record.value = valueUpdate.value;
      }

      // Use actual context + path as buffer key to separate data from different vessels
      const actualContext =
        delta.context || pathConfig.context || 'vessels.self';

      const bufferKey = `${actualContext}:${pathConfig.path}`;
      bufferData(bufferKey, record, config);
    } catch (error) {
      app.debug(`Error handling data message: ${error}`);
    }
  }

  // Buffer data and trigger save if buffer is full
  function bufferData(
    signalkPath: string,
    record: DataRecord,
    config: PluginConfig
  ): void {
    if (!state.dataBuffers.has(signalkPath)) {
      state.dataBuffers.set(signalkPath, []);
      app.debug(`🆕 Created new buffer for path: ${signalkPath}`);
    }

    const buffer = state.dataBuffers.get(signalkPath)!;
    buffer.push(record);

    // Debug every 100 records to show buffer growth
    if (buffer.length % 100 === 0) {
      app.debug(
        `📊 Buffer for ${signalkPath}: ${buffer.length}/${config.bufferSize} records`
      );
    }

    if (buffer.length >= config.bufferSize) {
      app.debug(
        `🚀 Buffer full for ${signalkPath} (${buffer.length} records) - triggering save`
      );
      // Extract the actual SignalK path from the buffer key (context:path format)
      // Find the separator between context and path - look for the last colon followed by a valid SignalK path
      const pathMatch = signalkPath.match(/^.*:([a-zA-Z][a-zA-Z0-9._]*)$/);
      const actualPath = pathMatch ? pathMatch[1] : signalkPath;
      saveBufferToParquet(actualPath, buffer, config);
      state.dataBuffers.set(signalkPath, []); // Clear buffer
      app.debug(`🧹 Buffer cleared for ${signalkPath}`);
    }
  }

  // Save all buffers (called periodically and on shutdown)
  function saveAllBuffers(config?: PluginConfig): void {
    const totalBuffers = state.dataBuffers.size;
    let buffersWithData = 0;
    let totalRecords = 0;

    state.dataBuffers.forEach((buffer, signalkPath) => {
      if (buffer.length > 0) {
        buffersWithData++;
        totalRecords += buffer.length;
        app.debug(
          `⏰ Periodic save for ${signalkPath}: ${buffer.length} records`
        );
        // Extract the actual SignalK path from the buffer key (context:path format)
        // Find the separator between context and path - look for the last colon followed by a valid SignalK path
        const pathMatch = signalkPath.match(/^.*:([a-zA-Z][a-zA-Z0-9._]*)$/);
        const actualPath = pathMatch ? pathMatch[1] : signalkPath;
        saveBufferToParquet(actualPath, buffer, config || state.currentConfig!);
        state.dataBuffers.set(signalkPath, []); // Clear buffer
      }
    });

    if (buffersWithData > 0) {
      app.debug(
        `💾 Periodic save completed: ${buffersWithData}/${totalBuffers} paths, ${totalRecords} total records`
      );
    }
  }

  // Save buffer to Parquet file
  async function saveBufferToParquet(
    signalkPath: string,
    buffer: DataRecord[],
    config: PluginConfig
  ): Promise<void> {
    try {
      // Get context from first record in buffer (all records in buffer have same path/context)
      const context = buffer.length > 0 ? buffer[0].context : 'vessels.self';

      // Create proper directory structure
      let contextPath: string;
      if (context === 'vessels.self') {
        // Clean the self context for filesystem usage (replace dots with slashes, colons with underscores)
        contextPath = app.selfContext.replace(/\./g, '/').replace(/:/g, '_');
      } else if (context.startsWith('vessels.')) {
        // Extract vessel identifier and clean it for filesystem
        const vesselId = context.replace('vessels.', '').replace(/:/g, '_');
        contextPath = `vessels/${vesselId}`;
      } else if (context.startsWith('meteo.')) {
        // Extract meteo station identifier and clean it for filesystem
        const meteoId = context.replace('meteo.', '').replace(/:/g, '_');
        contextPath = `meteo/${meteoId}`;
      } else {
        // Fallback: clean the entire context
        contextPath = context.replace(/:/g, '_').replace(/\./g, '/');
      }

      const dirPath = path.join(
        config.outputDirectory,
        contextPath,
        signalkPath.replace(/\./g, '/')
      );
      await fs.ensureDir(dirPath);

      // Generate filename with timestamp
      const timestamp = new Date()
        .toISOString()
        .replace(/[:.]/g, '')
        .slice(0, 15);
      const fileExt =
        config.fileFormat === 'csv'
          ? 'csv'
          : config.fileFormat === 'parquet'
            ? 'parquet'
            : 'json';
      const filename = `${config.filenamePrefix}_${timestamp}.${fileExt}`;
      const filepath = path.join(dirPath, filename);

      // Use ParquetWriter to save in the configured format
      const savedPath = await state.parquetWriter!.writeRecords(
        filepath,
        buffer
      );

      app.debug(
        `💾 Saved ${buffer.length} records to ${path.basename(savedPath)} for path: ${signalkPath}`
      );

      // Upload to S3 if enabled and timing is real-time
      if (config.s3Upload.enabled && config.s3Upload.timing === 'realtime') {
        await uploadToS3(savedPath, config);
      }
    } catch (error) {
      app.debug(`❌ Error saving buffer for ${signalkPath}: ${error}`);
    }
  }

  // Daily consolidation function
  async function consolidateYesterday(config: PluginConfig): Promise<void> {
    try {
      const yesterday = new Date();
      yesterday.setUTCDate(yesterday.getUTCDate() - 1);

      const consolidatedCount = await state.parquetWriter!.consolidateDaily(
        config.outputDirectory,
        yesterday,
        config.filenamePrefix
      );

      if (consolidatedCount > 0) {
        app.debug(
          `Consolidated ${consolidatedCount} topic directories for ${yesterday.toISOString().split('T')[0]}`
        );

        // Upload consolidated files to S3 if enabled and timing is consolidation
        if (
          config.s3Upload.enabled &&
          config.s3Upload.timing === 'consolidation'
        ) {
          await uploadConsolidatedFilesToS3(config, yesterday);
        }
      }
    } catch (error) {
      app.debug(`Error during daily consolidation: ${error}`);
    }
  }

  // Upload consolidated files to S3
  async function uploadConsolidatedFilesToS3(
    config: PluginConfig,
    date: Date
  ): Promise<void> {
    try {
      const dateStr = date.toISOString().split('T')[0];
      const consolidatedPattern = `**/*_${dateStr}_consolidated.parquet`;

      // Find all consolidated files for the date
      const consolidatedFiles = await glob(consolidatedPattern, {
        cwd: config.outputDirectory,
        absolute: true,
        nodir: true,
      });

      app.debug(
        `Found ${consolidatedFiles.length} consolidated files to upload for ${dateStr}`
      );

      // Upload each consolidated file
      for (const filePath of consolidatedFiles) {
        await uploadToS3(filePath, config);
      }
    } catch (error) {
      app.debug(`Error uploading consolidated files to S3: ${error}`);
    }
  }

  // Initialize regimen states from current API values at startup
  function initializeRegimenStates(_config: PluginConfig): void {
    const commandPaths = currentPaths.filter(
      (pathConfig: PathConfig) =>
        pathConfig &&
        pathConfig.path &&
        pathConfig.path.startsWith('commands.') &&
        pathConfig.enabled
    );

    app.debug(
      `🔍 Checking current command values for ${commandPaths.length} command paths at startup`
    );

    commandPaths.forEach((pathConfig: PathConfig) => {
      try {
        // Get current value from SignalK API
        const currentData = app.getSelfPath(pathConfig.path);

        if (currentData !== undefined && currentData !== null) {
          app.debug(
            `📋 Found current value for ${pathConfig.path}: ${JSON.stringify(currentData)}`
          );

          // Check if there's source information
          const shouldProcess = true;

          // If source filter is specified, check it
          if (pathConfig.source && pathConfig.source.trim() !== '') {
            // For startup, we need to check the API source info
            // This is a simplified check - in real deltas we get more source info
            app.debug(
              `🔍 Source filter specified for ${pathConfig.path}: "${pathConfig.source.trim()}"`
            );

            // For now, we'll process the value if it exists and log a warning
            // In practice, you might want to check the source here too
            app.debug(
              `⚠️  Startup value processed without source verification for ${pathConfig.path}`
            );
          }

          if (shouldProcess && currentData.value !== undefined) {
            const commandName = extractCommandName(pathConfig.path);
            const isActive = Boolean(currentData.value);

            app.debug(
              `🚀 Startup: Command ${commandName}: ${isActive ? 'ACTIVE' : 'INACTIVE'}`
            );

            if (isActive) {
              state.activeRegimens.add(commandName);
            } else {
              state.activeRegimens.delete(commandName);
            }
          }
        } else {
          app.debug(`📭 No current value found for ${pathConfig.path}`);
        }
      } catch (error) {
        app.debug(
          `❌ Error checking startup value for ${pathConfig.path}: ${error}`
        );
      }
    });

    app.debug(
      `🎯 Startup regimens initialized: [${Array.from(state.activeRegimens).join(', ')}]`
    );
  }

  // Helper functions
  function extractCommandName(signalkPath: string): string {
    // Extract command name from "commands.captureWeather"
    const parts = signalkPath.split('.');
    return parts[parts.length - 1];
  }

  // Convert BigInt values to regular numbers for JSON serialization
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  function mapForJSON(rawData: any[]): any[] {
    return rawData.map(row => {
      const convertedRow: typeof row = {};
      for (const [key, value] of Object.entries(row)) {
        convertedRow[key] = typeof value === 'bigint' ? Number(value) : value;
      }
      return convertedRow;
    });
  }

  // S3 upload function
  async function uploadToS3(
    filePath: string,
    config: PluginConfig
  ): Promise<boolean> {
    if (!config.s3Upload.enabled || !state.s3Client || !PutObjectCommand) {
      return false;
    }

    try {
      // Read the file
      const fileContent = await fs.readFile(filePath);

      // Generate S3 key
      const relativePath = path.relative(config.outputDirectory, filePath);
      let s3Key = relativePath;
      if (config.s3Upload.keyPrefix) {
        const prefix = config.s3Upload.keyPrefix.endsWith('/')
          ? config.s3Upload.keyPrefix
          : `${config.s3Upload.keyPrefix}/`;
        s3Key = `${prefix}${relativePath}`;
      }

      // Upload to S3
      const command = new PutObjectCommand({
        Bucket: config.s3Upload.bucket,
        Key: s3Key,
        Body: fileContent,
        ContentType: filePath.endsWith('.parquet')
          ? 'application/octet-stream'
          : 'application/json',
      });

      await state.s3Client.send(command);
      app.debug(`✅ Uploaded to S3: s3://${config.s3Upload.bucket}/${s3Key}`);

      // Delete local file if configured
      if (config.s3Upload.deleteAfterUpload) {
        await fs.unlink(filePath);
        app.debug(`🗑️ Deleted local file: ${filePath}`);
      }

      return true;
    } catch (error) {
      app.debug(`❌ Error uploading ${filePath} to S3: ${error}`);
      return false;
    }
  }

  plugin.schema = {
    type: 'object',
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
    },
  };

  // Webapp static files and API routes
  plugin.registerWithRouter = function (router: Router): void {
    // Serve static files from public directory
    const publicPath = path.join(__dirname, '../public');
    if (fs.existsSync(publicPath)) {
      router.use(express.static(publicPath));
      app.debug(`Static files served from: ${publicPath}`);
    }

    // Get the current configuration for data directory
    const getDataDir = (): string => {
      // Use the user-configured output directory, fallback to SignalK default
      return state.currentConfig?.outputDirectory || app.getDataDirPath();
    };

    // Helper function to get available paths from directory structure
    function getAvailablePaths(dataDir: string): PathInfo[] {
      const paths: PathInfo[] = [];
      // Clean the self context for filesystem usage (replace dots with slashes, colons with underscores)
      const selfContextPath = app.selfContext
        .replace(/\./g, '/')
        .replace(/:/g, '_');
      const vesselsDir = path.join(dataDir, selfContextPath);

      app.debug(`🔍 Looking for paths in vessel directory: ${vesselsDir}`);
      app.debug(
        `📡 Using vessel context: ${app.selfContext} → ${selfContextPath}`
      );

      if (!fs.existsSync(vesselsDir)) {
        app.debug(`❌ Vessel directory does not exist: ${vesselsDir}`);
        return paths;
      }

      function walkPaths(currentPath: string, relativePath: string = ''): void {
        try {
          app.debug(
            `🚶 Walking path: ${currentPath} (relative: ${relativePath})`
          );
          const items = fs.readdirSync(currentPath);
          items.forEach((item: string) => {
            const fullPath = path.join(currentPath, item);
            const stat = fs.statSync(fullPath);

            if (
              stat.isDirectory() &&
              item !== 'processed' &&
              item !== 'failed'
            ) {
              const newRelativePath = relativePath
                ? `${relativePath}.${item}`
                : item;

              // Check if this directory has parquet files
              const hasParquetFiles = fs
                .readdirSync(fullPath)
                .some((file: string) => file.endsWith('.parquet'));

              if (hasParquetFiles) {
                const fileCount = fs
                  .readdirSync(fullPath)
                  .filter((file: string) => file.endsWith('.parquet')).length;
                app.debug(
                  `✅ Found SignalK path with data: ${newRelativePath} (${fileCount} files)`
                );
                paths.push({
                  path: newRelativePath,
                  directory: fullPath,
                  fileCount: fileCount,
                });
              } else {
                app.debug(
                  `📁 Directory ${newRelativePath} has no parquet files`
                );
              }

              walkPaths(fullPath, newRelativePath);
            }
          });
        } catch (error) {
          app.debug(
            `❌ Error reading directory ${currentPath}: ${(error as Error).message}`
          );
        }
      }

      if (fs.existsSync(vesselsDir)) {
        walkPaths(vesselsDir);
      }

      app.debug(
        `📊 Path discovery complete: found ${paths.length} paths with data`
      );
      return paths;
    }

    // Get available SignalK paths
    router.get(
      '/api/paths',
      (_: TypedRequest, res: TypedResponse<PathsApiResponse>) => {
        try {
          const dataDir = getDataDir();
          const paths = getAvailablePaths(dataDir);

          return res.json({
            success: true,
            dataDirectory: dataDir,
            paths: paths,
          });
        } catch (error) {
          return res.status(500).json({
            success: false,
            error: (error as Error).message,
          });
        }
      }
    );

    // Get files for a specific path
    router.get(
      '/api/files/:path(*)',
      (req: TypedRequest, res: TypedResponse<FilesApiResponse>) => {
        try {
          const dataDir = getDataDir();
          const signalkPath = req.params.path;
          const selfContextPath = app.selfContext
            .replace(/\./g, '/')
            .replace(/:/g, '_');
          const pathDir = path.join(
            dataDir,
            selfContextPath,
            signalkPath.replace(/\./g, '/')
          );

          if (!fs.existsSync(pathDir)) {
            return res.status(404).json({
              success: false,
              error: `Path not found: ${signalkPath}`,
            });
          }

          const files = fs
            .readdirSync(pathDir)
            .filter((file: string) => file.endsWith('.parquet'))
            .map((file: string) => {
              const filePath = path.join(pathDir, file);
              const stat = fs.statSync(filePath);
              return {
                name: file,
                path: filePath,
                size: stat.size,
                modified: stat.mtime.toISOString(),
              };
            })
            .sort(
              (a, b) =>
                new Date(b.modified).getTime() - new Date(a.modified).getTime()
            );

          return res.json({
            success: true,
            path: signalkPath,
            directory: pathDir,
            files: files,
          });
        } catch (error) {
          return res.status(500).json({
            success: false,
            error: (error as Error).message,
          });
        }
      }
    );

    // Get sample data from a specific file
    router.get(
      '/api/sample/:path(*)',
      async (req: TypedRequest, res: TypedResponse<SampleApiResponse>) => {
        try {
          if (!DuckDBInstance) {
            return res.status(503).json({
              success: false,
              error: 'DuckDB not available',
            });
          }

          const dataDir = getDataDir();
          const signalkPath = req.params.path;
          const limit = parseInt(req.query.limit as string) || 10;

          const selfContextPath = app.selfContext
            .replace(/\./g, '/')
            .replace(/:/g, '_');
          const pathDir = path.join(
            dataDir,
            selfContextPath,
            signalkPath.replace(/\./g, '/')
          );

          if (!fs.existsSync(pathDir)) {
            return res.status(404).json({
              success: false,
              error: `Path not found: ${signalkPath}`,
            });
          }

          // Get the most recent parquet file
          const files = fs
            .readdirSync(pathDir)
            .filter((file: string) => file.endsWith('.parquet'))
            .map((file: string) => {
              const filePath = path.join(pathDir, file);
              const stat = fs.statSync(filePath);
              return { name: file, path: filePath, modified: stat.mtime };
            })
            .sort((a, b) => b.modified.getTime() - a.modified.getTime());

          if (files.length === 0) {
            return res.status(404).json({
              success: false,
              error: `No parquet files found for path: ${signalkPath}`,
            });
          }

          const sampleFile = files[0];
          const query = `SELECT * FROM '${sampleFile.path}' LIMIT ${limit}`;

          const instance = await DuckDBInstance.create();
          const connection = await instance.connect();
          try {
            const reader = await connection.runAndReadAll(query);
            const rawData = reader.getRowObjects();

            const data = mapForJSON(rawData);

            // Get column info
            const columns = data.length > 0 ? Object.keys(data[0]) : [];

            return res.json({
              success: true,
              path: signalkPath,
              file: sampleFile.name,
              columns: columns,
              rowCount: data.length,
              data: data,
            });
          } catch (err) {
            return res.status(400).json({
              success: false,
              error: (err as Error).message,
            });
          } finally {
            connection.disconnectSync();
          }
        } catch (error) {
          return res.status(500).json({
            success: false,
            error: (error as Error).message,
          });
        }
      }
    );

    // Query parquet data
    router.post(
      '/api/query',
      async (
        req: TypedRequest<QueryRequest>,
        res: TypedResponse<QueryApiResponse>
      ) => {
        try {
          if (!DuckDBInstance) {
            return res.status(503).json({
              success: false,
              error: 'DuckDB not available',
            });
          }

          const { query } = req.body;

          if (!query) {
            return res.status(400).json({
              success: false,
              error: 'Query is required',
            });
          }

          const dataDir = getDataDir();

          // Replace placeholder paths in query with actual file paths
          let processedQuery = query;

          // Find all quoted paths in the query that might be SignalK paths
          const pathMatches = query.match(/'([^']+)'/g);
          if (pathMatches) {
            pathMatches.forEach(match => {
              const quotedPath = match.slice(1, -1); // Remove quotes

              // If it looks like a SignalK path, convert to file path
              const selfContextPath = app.selfContext
                .replace(/\./g, '/')
                .replace(/:/g, '_');
              if (
                quotedPath.includes(`/${selfContextPath}/`) ||
                quotedPath.includes('.parquet')
              ) {
                // It's already a file path, use as is
                return;
              } else if (
                quotedPath.includes('.') &&
                !quotedPath.includes('/')
              ) {
                // It's a SignalK path, convert to file path
                const filePath = path.join(
                  dataDir,
                  selfContextPath,
                  quotedPath.replace(/\./g, '/'),
                  '*.parquet'
                );
                processedQuery = processedQuery.replace(match, `'${filePath}'`);
              }
            });
          }

          app.debug(`Executing query: ${processedQuery}`);

          const instance = await DuckDBInstance.create();
          const connection = await instance.connect();
          try {
            const reader = await connection.runAndReadAll(processedQuery);
            const rawData = reader.getRowObjects();

            const data = mapForJSON(rawData);

            return res.json({
              success: true,
              query: processedQuery,
              rowCount: data.length,
              data: data,
            });
          } catch (err) {
            app.error(`Query error: ${err}`);
            return res.status(400).json({
              success: false,
              error: (err as Error).message,
            });
          } finally {
            connection.disconnectSync();
          }
        } catch (error) {
          return res.status(500).json({
            success: false,
            error: (error as Error).message,
          });
        }
      }
    );

    // Test S3 connection
    router.post(
      '/api/test-s3',
      async (_: TypedRequest, res: TypedResponse<S3TestApiResponse>) => {
        try {
          if (!state.currentConfig) {
            return res.status(500).json({
              success: false,
              error: 'Plugin not started or configuration not available',
            });
          }

          if (!state.currentConfig.s3Upload.enabled) {
            return res.status(400).json({
              success: false,
              error: 'S3 upload is not enabled in configuration',
            });
          }

          if (!S3Client || !state.s3Client) {
            return res.status(503).json({
              success: false,
              error: 'S3 client not available or not initialized',
            });
          }

          // Test S3 connection by listing bucket
          const listCommand = new ListObjectsV2Command({
            Bucket: state.currentConfig.s3Upload.bucket,
            MaxKeys: 1,
          });

          await state.s3Client.send(listCommand);

          return res.json({
            success: true,
            message: 'S3 connection successful',
            bucket: state.currentConfig.s3Upload.bucket,
            region: state.currentConfig.s3Upload.region || 'us-east-1',
            keyPrefix: state.currentConfig.s3Upload.keyPrefix || 'none',
          });
        } catch (error) {
          app.debug(`S3 test connection error: ${error}`);
          return res.status(500).json({
            success: false,
            error: (error as Error).message || 'S3 connection failed',
          });
        }
      }
    );

    // Web App Path Configuration API Routes (manages separate config file)

    // Get current path configurations
    router.get(
      '/api/config/paths',
      (_: TypedRequest, res: TypedResponse<ConfigApiResponse>) => {
        try {
          const paths = loadWebAppPaths();
          return res.json({
            success: true,
            paths: paths,
          });
        } catch (error) {
          return res.status(500).json({
            success: false,
            error: (error as Error).message,
          });
        }
      }
    );

    // Add new path configuration
    router.post(
      '/api/config/paths',
      (req: TypedRequest<PathConfigRequest>, res: TypedResponse): void => {
        try {
          const newPath = req.body;

          // Validate required fields
          if (!newPath.path) {
            res.status(400).json({
              success: false,
              error: 'Path is required',
            });
            return;
          }

          // Add to current paths
          currentPaths.push(newPath);

          // Save to web app configuration
          saveWebAppPaths(currentPaths);

          // Update subscriptions
          updateDataSubscriptions(state.currentConfig!);

          res.json({
            success: true,
            message: 'Path configuration added successfully',
            path: newPath,
          });
        } catch (error) {
          res.status(500).json({
            success: false,
            error: (error as Error).message,
          });
        }
      }
    );

    // Update existing path configuration
    router.put(
      '/api/config/paths/:index',
      (req: TypedRequest<PathConfigRequest>, res: TypedResponse): void => {
        try {
          const index = parseInt(req.params.index);
          const updatedPath = req.body;

          if (index < 0 || index >= currentPaths.length) {
            res.status(404).json({
              success: false,
              error: 'Path configuration not found',
            });
            return;
          }

          // Validate required fields
          if (!updatedPath.path) {
            res.status(400).json({
              success: false,
              error: 'Path is required',
            });
            return;
          }

          // Update the path configuration
          currentPaths[index] = updatedPath;

          // Save to web app configuration
          saveWebAppPaths(currentPaths);

          // Update subscriptions
          updateDataSubscriptions(state.currentConfig!);

          res.json({
            success: true,
            message: 'Path configuration updated successfully',
            path: updatedPath,
          });
        } catch (error) {
          res.status(500).json({
            success: false,
            error: (error as Error).message,
          });
        }
      }
    );

    // Remove path configuration
    router.delete(
      '/api/config/paths/:index',
      (req: TypedRequest, res: TypedResponse): void => {
        try {
          const index = parseInt(req.params.index);

          if (index < 0 || index >= currentPaths.length) {
            res.status(404).json({
              success: false,
              error: 'Path configuration not found',
            });
            return;
          }

          // Get the path being removed for response
          const removedPath = currentPaths[index];

          // Remove from current paths
          currentPaths.splice(index, 1);

          // Save to web app configuration
          saveWebAppPaths(currentPaths);

          // Update subscriptions
          updateDataSubscriptions(state.currentConfig!);

          app.debug(
            `Removed path configuration: ${removedPath.name} (${removedPath.path})`
          );

          res.json({
            success: true,
            message: 'Path configuration removed successfully',
            removedPath: removedPath,
          });
        } catch (error) {
          res.status(500).json({
            success: false,
            error: (error as Error).message,
          });
        }
      }
    );

    // Command Management API endpoints

    // Get all registered commands
    router.get(
      '/api/commands',
      (_: TypedRequest, res: TypedResponse<CommandApiResponse>) => {
        try {
          const commands = Array.from(commandState.registeredCommands.values());
          return res.json({
            success: true,
            commands: commands,
            count: commands.length,
          });
        } catch (error) {
          app.error(`Error retrieving commands: ${error}`);
          return res.status(500).json({
            success: false,
            error: 'Failed to retrieve commands',
          });
        }
      }
    );

    // Register a new command
    router.post(
      '/api/commands',
      (
        req: TypedRequest<CommandRegistrationRequest>,
        res: TypedResponse<CommandApiResponse>
      ) => {
        try {
          const { command, description } = req.body;

          if (!command || !isValidCommandName(command)) {
            return res.status(400).json({
              success: false,
              error:
                'Invalid command name. Must be alphanumeric with underscores, 1-50 characters.',
            });
          }

          const result = registerCommand(command, description);

          if (result.state === 'COMPLETED') {
            // Add to command history
            addCommandHistoryEntry(command, 'REGISTER', undefined, true);

            // Update webapp config
            const webAppConfig = loadWebAppConfig();
            saveWebAppConfig(webAppConfig.paths, currentCommands);

            const commandConfig = commandState.registeredCommands.get(command);
            return res.json({
              success: true,
              message: `Command '${command}' registered successfully`,
              command: commandConfig,
            });
          } else {
            return res.status(400).json({
              success: false,
              error: result.message || 'Failed to register command',
            });
          }
        } catch (error) {
          app.error(`Error registering command: ${error}`);
          return res.status(500).json({
            success: false,
            error: 'Internal server error',
          });
        }
      }
    );

    // Execute a command
    router.put(
      '/api/commands/:command/execute',
      (
        req: TypedRequest<CommandExecutionRequest>,
        res: TypedResponse<CommandExecutionResponse>
      ) => {
        try {
          const { command } = req.params;
          const { value } = req.body;

          if (typeof value !== 'boolean') {
            return res.status(400).json({
              success: false,
              error: 'Command value must be a boolean',
            });
          }

          const result = executeCommand(command, value);

          if (result.state === 'COMPLETED') {
            // Add to command history
            addCommandHistoryEntry(command, 'EXECUTE', value, true);

            return res.json({
              success: true,
              command: command,
              value: value,
              executed: true,
              timestamp: result.timestamp,
            });
          } else {
            // Add to command history for failed execution
            addCommandHistoryEntry(
              command,
              'EXECUTE',
              value,
              false,
              result.message
            );

            return res.status(400).json({
              success: false,
              error: result.message || 'Failed to execute command',
            });
          }
        } catch (error) {
          app.error(`Error executing command: ${error}`);
          return res.status(500).json({
            success: false,
            error: 'Internal server error',
          });
        }
      }
    );

    // Unregister a command
    router.delete(
      '/api/commands/:command',
      (req: TypedRequest, res: TypedResponse<CommandApiResponse>) => {
        try {
          const { command } = req.params;

          const result = unregisterCommand(command);

          if (result.state === 'COMPLETED') {
            // Add to command history
            addCommandHistoryEntry(command, 'UNREGISTER', undefined, true);

            // Update webapp config
            const webAppConfig = loadWebAppConfig();
            saveWebAppConfig(webAppConfig.paths, currentCommands);

            return res.json({
              success: true,
              message: `Command '${command}' unregistered successfully`,
            });
          } else {
            return res.status(404).json({
              success: false,
              error: result.message || 'Command not found',
            });
          }
        } catch (error) {
          app.error(`Error unregistering command: ${error}`);
          return res.status(500).json({
            success: false,
            error: 'Internal server error',
          });
        }
      }
    );

    // Get command history
    router.get(
      '/api/commands/history',
      (_: TypedRequest, res: TypedResponse<CommandApiResponse>) => {
        try {
          // Return the last 50 history entries
          const recentHistory = commandHistory.slice(-50);
          return res.json({
            success: true,
            data: recentHistory,
          });
        } catch (error) {
          app.error(`Error retrieving command history: ${error}`);
          return res.status(500).json({
            success: false,
            error: 'Failed to retrieve command history',
          });
        }
      }
    );

    // Get command status
    router.get(
      '/api/commands/:command/status',
      (req: TypedRequest, res: TypedResponse<CommandApiResponse>) => {
        try {
          const { command } = req.params;
          const commandConfig = commandState.registeredCommands.get(command);

          if (!commandConfig) {
            return res.status(404).json({
              success: false,
              error: 'Command not found',
            });
          }

          // Get current value from SignalK
          const currentValue = app.getSelfPath(`commands.${command}`);

          return res.json({
            success: true,
            command: {
              ...commandConfig,
              active: currentValue === true,
            },
          });
        } catch (error) {
          app.error(`Error retrieving command status: ${error}`);
          return res.status(500).json({
            success: false,
            error: 'Failed to retrieve command status',
          });
        }
      }
    );

    // Health check
    router.get(
      '/api/health',
      (_: TypedRequest, res: TypedResponse<HealthApiResponse>) => {
        return res.json({
          success: true,
          status: 'healthy',
          timestamp: new Date().toISOString(),
          duckdb: DuckDBInstance ? 'available' : 'not available',
        });
      }
    );

    app.debug('Webapp API routes registered');
  };

  return plugin;
};
