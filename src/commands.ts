import {
  CommandConfig,
  CommandRegistrationState,
  CommandExecutionResult,
  CommandPutHandler,
  CommandHistoryEntry,
  PathConfig,
  WebAppPathConfig,
  PluginConfig,
  ThresholdConfig,
} from './types';
import {
  Context,
  Delta,
  Path,
  ServerAPI,
  SourceRef,
  Timestamp,
} from '@signalk/server-api';
import * as fs from 'fs-extra';
import * as path from 'path';
import { degreesToRadians, radiansToDegrees } from './utils/angle-converter';
import { calculateDistance, isPointInBoundingBox } from './utils/geo-calculator';

// Global variables for command management
let currentCommands: CommandConfig[] = [];
let commandHistory: CommandHistoryEntry[] = [];
let appInstance: ServerAPI;
let pluginConfig: PluginConfig | null = null;

// Threshold processing lock system for first-in rule
const thresholdProcessingLocks = new Map<string, boolean>();
const THRESHOLD_PROCESSING_TIMEOUT = 5000; // 5 seconds

// Command state management
const commandState: CommandRegistrationState = {
  registeredCommands: new Map<string, CommandConfig>(),
  putHandlers: new Map<string, CommandPutHandler>(),
};

// Threshold monitoring state
const thresholdState = new Map<string, {
  lastValue: any;
  lastTriggered: number;
  unsubscribe?: () => void;
}>();

// Configuration management functions
export function loadWebAppConfig(app?: ServerAPI): WebAppPathConfig {
  const appToUse = app || appInstance;
  if (!appToUse) {
    throw new Error('App instance not provided and not initialized');
  }
  
  const webAppConfigPath = path.join(
    appToUse.getDataDirPath(),
    'signalk-parquet',
    'webapp-config.json'
  );

  try {
    if (fs.existsSync(webAppConfigPath)) {
      const configData = fs.readFileSync(webAppConfigPath, 'utf8');
      const rawConfig = JSON.parse(configData);

      // Migrate old config format to new format with backward compatibility
      const migratedPaths = (rawConfig.paths || []).map(
        (path: Partial<PathConfig>) => ({
          path: path.path,
          name: path.name,
          enabled: path.enabled,
          regimen: path.regimen,
          source: path.source || undefined,
          context: path.context,
          excludeMMSI: path.excludeMMSI,
        })
      );

      const migratedCommands = rawConfig.commands || [];

      const migratedConfig = {
        paths: migratedPaths,
        commands: migratedCommands,
      };

      appToUse.debug(
        `Loaded and migrated ${migratedPaths.length} paths and ${migratedCommands.length} commands from existing config`
      );

      // Save the migrated config back to preserve the new format
      try {
        fs.writeFileSync(
          webAppConfigPath,
          JSON.stringify(migratedConfig, null, 2)
        );
        appToUse.debug(
          'Saved migrated configuration with source field compatibility'
        );
      } catch (saveError) {
        appToUse.debug(
          `Warning: Could not save migrated config: ${saveError}`
        );
      }

      return migratedConfig;
    }
  } catch (error) {
    appToUse.error(`Failed to load webapp configuration: ${error}`);

    // BACKUP the broken file instead of destroying it
    try {
      const backupPath = webAppConfigPath + '.backup.' + Date.now();
      if (fs.existsSync(webAppConfigPath)) {
        fs.copyFileSync(webAppConfigPath, backupPath);
        appToUse.debug(`Backed up broken config to: ${backupPath}`);
      }
    } catch (backupError) {
      appToUse.debug(`Could not backup broken config: ${backupError}`);
    }
  }

  // Only create defaults if NO config file exists
  if (!fs.existsSync(webAppConfigPath)) {
    const defaultConfig = getDefaultWebAppConfig();
    appToUse.debug(
      'No existing configuration found, using default installation'
    );

    // Save the default configuration for future use
    try {
      const configDir = path.dirname(webAppConfigPath);
      if (!fs.existsSync(configDir)) {
        fs.mkdirSync(configDir, { recursive: true });
      }
      fs.writeFileSync(
        webAppConfigPath,
        JSON.stringify(defaultConfig, null, 2)
      );
      appToUse.debug('Saved default installation configuration');
    } catch (error) {
      appToUse.debug(`Failed to save default configuration: ${error}`);
    }

    return defaultConfig;
  }

  // If file exists but couldn't be parsed, return empty config to avoid data loss
  appInstance.debug(
    'Config file exists but could not be parsed, returning empty config to avoid data loss'
  );
  return {
    paths: [],
    commands: [],
  };
}

function getDefaultWebAppConfig(): WebAppPathConfig {
  const defaultCommands: CommandConfig[] = [
    {
      command: 'captureMoored',
      path: 'commands.captureMoored',
      registered: 'COMPLETED',
      description: 'Capture data when moored (position and wind)',
      active: false,
    },
  ];

  const defaultPaths: PathConfig[] = [
    {
      path: 'commands.captureMoored' as Path,
      name: 'Command: captureMoored',
      enabled: true,
      regimen: 'commands',
      source: undefined,
      context: 'vessels.self' as Context,
    },
    {
      path: 'navigation.position' as Path,
      name: 'Navigation Position',
      enabled: true,
      regimen: 'captureMoored',
      source: undefined,
      context: 'vessels.self' as Context,
    },
    {
      path: 'environment.wind.speedApparent' as Path,
      name: 'Apparent Wind Speed',
      enabled: true,
      regimen: 'captureMoored',
      source: undefined,
      context: 'vessels.self' as Context,
    },
  ];

  return {
    commands: defaultCommands,
    paths: defaultPaths,
  };
}

export function saveWebAppConfig(
  paths: PathConfig[],
  commands: CommandConfig[],
  app?: ServerAPI
): void {
  const appToUse = app || appInstance;
  if (!appToUse) {
    throw new Error('App instance not provided and not initialized');
  }
  
  const webAppConfigPath = path.join(
    appToUse.getDataDirPath(),
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
    appToUse.debug(
      `Saved webapp configuration: ${paths.length} paths, ${commands.length} commands`
    );
  } catch (error) {
    appToUse.error(`Failed to save webapp configuration: ${error}`);
  }
}

// Initialize command state on plugin start
export function initializeCommandState(
  currentPaths: PathConfig[],
  app: ServerAPI
): void {
  appInstance = app;
  
  // Clear existing command state
  commandState.registeredCommands.clear();
  commandState.putHandlers.clear();

  // Re-register commands from configuration
  currentCommands.forEach((commandConfig: CommandConfig) => {
    const result = registerCommand(
      commandConfig.command,
      commandConfig.description,
      commandConfig.keywords,
      commandConfig.defaultState,
      commandConfig.thresholds
    );
    if (result.state === 'COMPLETED') {
    } else {
      app.error(
        `❌ Failed to restore command: ${commandConfig.command} - ${result.message}`
      );
    }
  });

  // Ensure all commands have path configurations (for backwards compatibility)
  let addedMissingPaths = false;
  currentCommands.forEach((commandConfig: CommandConfig) => {
    const commandPath = `commands.${commandConfig.command}`;
    const autoPath = `commands.${commandConfig.command}.auto`;

    // Add main command path if missing
    const existingCommandPath = currentPaths.find(p => p.path === commandPath);
    if (!existingCommandPath) {
      const commandPathConfig: PathConfig = {
        path: commandPath as Path,
        name: `Command: ${commandConfig.command}`,
        enabled: true,
        regimen: undefined,
        source: undefined,
        context: 'vessels.self' as Context,
        excludeMMSI: undefined,
      };
      currentPaths.push(commandPathConfig);
      addedMissingPaths = true;
    }

    // Add .auto path if command has thresholds and path is missing
    if (commandConfig.thresholds && commandConfig.thresholds.length > 0) {
      const existingAutoPath = currentPaths.find(p => p.path === autoPath);
      if (!existingAutoPath) {
        const autoPathConfig: PathConfig = {
          path: autoPath as Path,
          name: `Command Auto: ${commandConfig.command}`,
          enabled: true,
          regimen: 'commands',
          source: undefined,
          context: 'vessels.self' as Context,
          excludeMMSI: undefined,
        };
        currentPaths.push(autoPathConfig);
        addedMissingPaths = true;
      }
    }
  });

  // Save the updated configuration if we added missing paths
  if (addedMissingPaths) {
    saveWebAppConfig(currentPaths, currentCommands, app);
  }

  // Reset all commands to false on startup
  currentCommands.forEach((commandConfig: CommandConfig) => {
    initializeCommandValue(commandConfig.command, false);
  });

}

// Command registration with full type safety
export function registerCommand(
  commandName: string,
  description?: string,
  keywords?: string[],
  defaultState?: boolean,
  thresholds?: ThresholdConfig[]
): CommandExecutionResult {
  try {
    // Validate command name
    if (!isValidCommandName(commandName)) {
      return {
        success: false,
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
        success: false,
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
      keywords: keywords || [],
      active: false,
      lastExecuted: undefined,
      defaultState: defaultState,
      thresholds: thresholds,
    };

    // Create PUT handler with proper typing
    const putHandler: CommandPutHandler = (
      context: string,
      path: string,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      value: any,
      _callback?: (result: CommandExecutionResult) => void
    ): CommandExecutionResult => {
      appInstance.debug(
        `Handling PUT for commands.${commandName} with value: ${JSON.stringify(value)}`
      );
      return executeCommand(commandName, Boolean(value));
    };

    // Register PUT handler with SignalK
    appInstance.registerPutHandler(
      'vessels.self',
      commandPath,
      putHandler as any, //FIXME server api registerPutHandler is incorrectly typed https://github.com/SignalK/signalk-server/pull/2043
      'zennora-parquet-commands'
    );

    // Store command and handler
    commandState.registeredCommands.set(commandName, commandConfig);
    commandState.putHandlers.set(commandName, putHandler);

    // Initialize command value to defaultState or false
    initializeCommandValue(commandName, defaultState || false);

    // Initialize .auto path for threshold automation control
    const autoPath = `commands.${commandName}.auto`;
    const autoEnabled = !!(commandConfig.thresholds && commandConfig.thresholds.length > 0);
    initializeCommandValue(`${commandName}.auto`, autoEnabled);

    // Register PUT handler for .auto control
    const autoPutHandler = (
      _context: string,
      _path: string,
      value: any,
      _callback?: (result: CommandExecutionResult) => void
    ): CommandExecutionResult => {
      appInstance.debug(
        `Handling PUT for ${autoPath} with value: ${JSON.stringify(value)}`
      );

      // Update the .auto value
      const autoValue = Boolean(value);
      const timestamp = new Date().toISOString();

      appInstance.handleMessage('zennora-parquet-commands', {
        context: 'vessels.self' as Context,
        updates: [
          {
            timestamp: timestamp as Timestamp,
            values: [
              {
                path: autoPath as Path,
                value: autoValue,
              },
            ],
          },
        ],
      });

      return {
        success: true,
        state: 'COMPLETED',
        message: `Automation ${autoValue ? 'enabled' : 'disabled'} for ${commandName}`,
        timestamp
      };
    };

    // Register .auto PUT handler with SignalK
    appInstance.registerPutHandler(
      'vessels.self',
      autoPath,
      autoPutHandler as any,
      'zennora-parquet-commands'
    );

    // Start threshold monitoring for this command if thresholds are defined
    if (commandConfig.thresholds && commandConfig.thresholds.length > 0) {
      commandConfig.thresholds.forEach(threshold => {
        if (threshold.enabled) {
          setupThresholdMonitoring(commandConfig, threshold);
        }
      });
    }

    // Update current commands
    currentCommands = Array.from(commandState.registeredCommands.values());

    // Log command registration
    addCommandHistoryEntry(commandName, 'REGISTER', undefined, true);

    appInstance.debug(`✅ Registered command: ${commandName} at ${fullPath}`);

    return {
      success: true,
      state: 'COMPLETED',
      statusCode: 200,
      message: `Command '${commandName}' registered successfully with automatic path configuration`,
      timestamp: new Date().toISOString(),
    };
  } catch (error) {
    const errorMessage = `Failed to register command '${commandName}': ${error}`;
    appInstance.error(errorMessage);

    return {
      success: false,
      state: 'FAILED',
      statusCode: 500,
      message: errorMessage,
      timestamp: new Date().toISOString(),
    };
  }
}

// Command update (description and keywords only)
export function updateCommand(
  commandName: string,
  description?: string,
  keywords?: string[],
  defaultState?: boolean,
  thresholds?: any[]
): CommandExecutionResult {
  try {
    // Check if command exists
    if (!commandState.registeredCommands.has(commandName)) {
      return {
        success: false,
        state: 'FAILED',
        statusCode: 404,
        message: `Command '${commandName}' not found`,
        timestamp: new Date().toISOString(),
      };
    }

    // Get existing command config
    const existingCommand = commandState.registeredCommands.get(commandName)!;
    
    // Update only the fields that were provided
    const updatedCommand: CommandConfig = {
      ...existingCommand,
      description: description !== undefined ? description : existingCommand.description,
      keywords: keywords !== undefined ? keywords : existingCommand.keywords,
      defaultState: defaultState !== undefined ? defaultState : existingCommand.defaultState,
      thresholds: thresholds !== undefined ? thresholds : existingCommand.thresholds,
    };

    // Update the command in the registry
    commandState.registeredCommands.set(commandName, updatedCommand);

    // Update current commands array
    currentCommands = Array.from(commandState.registeredCommands.values());

    // If thresholds were updated, restart threshold monitoring for this command
    if (thresholds !== undefined) {
      // Stop existing threshold monitoring for this command
      const existingThresholds = existingCommand.thresholds || [];
      existingThresholds.forEach(threshold => {
        const monitorKey = `${commandName}_${threshold.watchPath}`;
        const state = thresholdState.get(monitorKey);
        if (state?.unsubscribe) {
          state.unsubscribe();
          thresholdState.delete(monitorKey);
        }
      });

      // Start new threshold monitoring
      if (updatedCommand.thresholds && updatedCommand.thresholds.length > 0) {
        updatedCommand.thresholds.forEach(threshold => {
          if (threshold.enabled) {
            setupThresholdMonitoring(updatedCommand, threshold);
          }
        });
      }
    }

    // Log the update
    addCommandHistoryEntry(commandName, 'UPDATE', undefined, true);
    
    appInstance.debug(`✅ Updated command: ${commandName}`);
    
    return {
      success: true,
      state: 'COMPLETED',
      statusCode: 200,
      message: `Command '${commandName}' updated successfully`,
      timestamp: new Date().toISOString(),
    };
    
  } catch (error) {
    appInstance.error(`Failed to update command ${commandName}: ${(error as Error).message}`);
    return {
      success: false,
      state: 'FAILED',
      statusCode: 500,
      message: `Failed to update command: ${(error as Error).message}`,
      timestamp: new Date().toISOString(),
    };
  }
}

// Command unregistration with type safety
export function unregisterCommand(commandName: string): CommandExecutionResult {
  try {
    const commandConfig = commandState.registeredCommands.get(commandName);
    if (!commandConfig) {
      return {
        success: false,
        state: 'FAILED',
        statusCode: 404,
        message: `Command '${commandName}' not found`,
        timestamp: new Date().toISOString(),
      };
    }

    // Remove PUT handler (SignalK API doesn't have unregister, but we can track it)
    commandState.putHandlers.delete(commandName);
    commandState.registeredCommands.delete(commandName);

    // Update current commands
    currentCommands = Array.from(commandState.registeredCommands.values());

    // Log command unregistration
    addCommandHistoryEntry(commandName, 'UNREGISTER', undefined, true);

    appInstance.debug(`🗑️ Unregistered command: ${commandName}`);

    return {
      success: true,
      state: 'COMPLETED',
      statusCode: 200,
      message: `Command '${commandName}' unregistered successfully with path cleanup`,
      timestamp: new Date().toISOString(),
    };
  } catch (error) {
    const errorMessage = `Failed to unregister command '${commandName}': ${error}`;
    appInstance.error(errorMessage);

    return {
      success: false,
      state: 'FAILED',
      statusCode: 500,
      message: errorMessage,
      timestamp: new Date().toISOString(),
    };
  }
}

// Command execution with full type safety
export function executeCommand(
  commandName: string,
  value: boolean
): CommandExecutionResult {
  try {
    const commandConfig = commandState.registeredCommands.get(commandName);
    if (!commandConfig) {
      return {
        success: false,
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
          $source: 'signalk-parquet-commands' as SourceRef,
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
    appInstance.handleMessage('signalk-parquet-commands', delta as Delta);

    // Update command state
    commandConfig.active = value;
    commandConfig.lastExecuted = timestamp;
    commandState.registeredCommands.set(commandName, commandConfig);

    // Update current commands
    currentCommands = Array.from(commandState.registeredCommands.values());

    // Log command execution
    addCommandHistoryEntry(commandName, 'EXECUTE', value, true);

    appInstance.debug(`🎮 Executed command: ${commandName} = ${value}`);

    return {
      success: true,
      state: 'COMPLETED',
      statusCode: 200,
      message: `Command '${commandName}' executed: ${value}`,
      timestamp: timestamp,
    };
  } catch (error) {
    const errorMessage = `Failed to execute command '${commandName}': ${error}`;
    appInstance.error(errorMessage);

    addCommandHistoryEntry(
      commandName,
      'EXECUTE',
      value,
      false,
      errorMessage
    );

    return {
      success: false,
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
        $source: 'signalk-parquet-commands' as SourceRef,
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
  appInstance.handleMessage('signalk-parquet-commands', delta as Delta);
}

function addCommandHistoryEntry(
  command: string,
  action: 'EXECUTE' | 'STOP' | 'REGISTER' | 'UNREGISTER' | 'UPDATE',
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

// Helper function to extract command name from SignalK path
export function extractCommandName(signalkPath: string): string {
  // Extract command name from "commands.captureWeather"
  const parts = signalkPath.split('.');
  return parts[parts.length - 1];
}

// Getters for external access
export function getCurrentCommands(): CommandConfig[] {
  return currentCommands;
}

export function getCommandHistory(): CommandHistoryEntry[] {
  return commandHistory;
}

export function getCommandState(): CommandRegistrationState {
  return commandState;
}

export function setCurrentCommands(commands: CommandConfig[]): void {
  currentCommands = commands;
}

// Threshold monitoring system
export function startThresholdMonitoring(app: ServerAPI, config?: PluginConfig): void {
  appInstance = app;
  if (config) {
    pluginConfig = config;
  }
  app.debug('🔄 Starting threshold monitoring system');

  // Process all commands
  currentCommands.forEach(command => {
    if (command.thresholds && command.thresholds.length > 0) {
      // Set up monitoring for all enabled thresholds
      command.thresholds.forEach(threshold => {
        if (threshold.enabled) {
          setupThresholdMonitoring(command, threshold);
        }
      });
    } else {
      // Apply default state for commands without thresholds or manual override
      applyDefaultState(command);
    }
  });
}

function applyDefaultState(command: CommandConfig): void {
  // Skip if manual override is active
  if (command.manualOverride) {
    return;
  }

  // Apply default state if specified and command is not already active
  if (command.defaultState !== undefined && command.active !== command.defaultState) {
    appInstance?.debug(`🔧 Applying default state for ${command.command}: ${command.defaultState ? 'ON' : 'OFF'}`);

    const result = executeCommand(command.command, command.defaultState);
    if (result.success) {
      appInstance?.debug(`✅ Default state applied: ${command.command} = ${command.defaultState}`);
    } else {
      appInstance?.error(`❌ Failed to apply default state: ${command.command} - ${result.message}`);
    }
  }
}

export function stopThresholdMonitoring(): void {
  appInstance?.debug('⏹️ Stopping threshold monitoring system');

  // Unsubscribe from all threshold monitors
  thresholdState.forEach(state => {
    if (state.unsubscribe) {
      state.unsubscribe();
    }
  });
  thresholdState.clear();
}

function setupThresholdMonitoring(command: CommandConfig, threshold: ThresholdConfig): void {
  if (threshold?.enabled === false || !threshold.watchPath) {
    return;
  }
  const monitorKey = `${command.command}_${threshold.watchPath}`;

  appInstance?.debug(`🎯 Setting up threshold monitoring for ${command.command} watching ${threshold.watchPath}`);

  // Clean up existing monitoring for this command
  const existingState = thresholdState.get(monitorKey);
  if (existingState?.unsubscribe) {
    existingState.unsubscribe();
  }

  // Subscribe to the watch path
  const unsubscribe = appInstance?.streambundle?.getSelfStream(threshold.watchPath as Path)?.onValue((value: any) => {
    try {
      processThresholdValue(command, threshold, value, monitorKey);
    } catch (error) {
      appInstance?.error(`❌ Error processing threshold for ${command.command}: ${(error as Error).message}`);
    }
  });

  // Store the monitoring state
  thresholdState.set(monitorKey, {
    lastValue: undefined,
    lastTriggered: 0,
    unsubscribe
  });

  // Evaluate immediately with current value if available
  try {
    const currentValue = appInstance?.getSelfPath(threshold.watchPath as Path);
    if (currentValue !== undefined) {
      processThresholdValue(command, threshold, currentValue, monitorKey);
    }
  } catch (error) {
    appInstance?.debug(`ℹ️ Unable to read current value for ${threshold.watchPath}: ${(error as Error).message}`);
  }
}

function processThresholdValue(command: CommandConfig, threshold: ThresholdConfig, value: any, monitorKey: string): void {
  const state = thresholdState.get(monitorKey);
  if (!state) return;

  const commandName = command.command;

  // Check if threshold processing is already running for this command (first-in rule)
  if (thresholdProcessingLocks.get(commandName)) {
    appInstance?.debug(`🔒 Threshold processing already running for ${commandName}, skipping`);
    return;
  }

  // Acquire lock
  thresholdProcessingLocks.set(commandName, true);

  // Set timeout to prevent deadlocks
  const timeoutId = setTimeout(() => {
    if (thresholdProcessingLocks.get(commandName)) {
      appInstance?.error(`⚠️ Threshold processing timeout for ${commandName}, releasing lock`);
      thresholdProcessingLocks.set(commandName, false);
    }
  }, THRESHOLD_PROCESSING_TIMEOUT);

  try {
    const normalizedValue = normalizeThresholdValue(value, threshold);
    appInstance?.debug(`📈 Threshold monitor ${commandName}:${threshold.watchPath} received value: ${JSON.stringify(normalizedValue)}`);

    // Check if automation is enabled for this command
    const autoEnabledRaw = appInstance?.getSelfPath(`commands.${commandName}.auto` as Path);
    const autoEnabled = (autoEnabledRaw && typeof autoEnabledRaw === 'object' && 'value' in autoEnabledRaw)
      ? autoEnabledRaw.value : autoEnabledRaw;

    if (!autoEnabled) {
      appInstance?.debug(`🚫 Threshold automation disabled for ${commandName} (auto=${autoEnabled})`);
      return;
    }

    // Skip processing if manual override is active (backward compatibility)
    if (command.manualOverride) {
      // Check if manual override has expired
      if (command.manualOverrideUntil) {
        const expiry = new Date(command.manualOverrideUntil);
        if (new Date() > expiry) {
          // Override expired, clear it
          command.manualOverride = false;
          command.manualOverrideUntil = undefined;
          appInstance?.debug(`⏰ Manual override expired for ${commandName}`);
        } else {
          // Override still active, skip threshold processing
          return;
        }
      } else {
        // Permanent manual override, skip threshold processing
        return;
      }
    }

    const now = Date.now();

    // Get homePort from config for position-based thresholds
    const homePort = pluginConfig && pluginConfig.homePortLatitude && pluginConfig.homePortLongitude
      ? { latitude: pluginConfig.homePortLatitude, longitude: pluginConfig.homePortLongitude }
      : undefined;

    const shouldActivate = evaluateThreshold(threshold, normalizedValue, homePort);
    appInstance?.debug(`📉 Threshold evaluation for ${commandName}:${threshold.watchPath} operator=${threshold.operator} threshold=${threshold.value} result=${shouldActivate}`);

    // Apply hysteresis for numeric values
    if (threshold.hysteresis && typeof normalizedValue === 'number' && typeof state.lastValue === 'number') {
      const timeSinceLastTrigger = now - state.lastTriggered;
      if (timeSinceLastTrigger < (threshold.hysteresis * 1000)) {
        // Within hysteresis period, skip
        return;
      }
    }

    // Determine if command state should change
    const currentCommandValue = appInstance?.getSelfPath(`commands.${commandName}` as Path);
    // Handle SignalK delta structure - extract .value if it's an object
    const actualValue = (currentCommandValue && typeof currentCommandValue === 'object' && 'value' in currentCommandValue)
      ? currentCommandValue.value
      : currentCommandValue;
    const currentlyActive = Boolean(actualValue);
    const shouldBeActive = threshold.activateOnMatch ? shouldActivate : !shouldActivate;

    appInstance?.debug(`🔍 Current state check for ${commandName}: SignalK=${actualValue} (from ${JSON.stringify(currentCommandValue)}), shouldBe=${shouldBeActive}, threshold=${threshold.operator} ${threshold.value}`);

    if (shouldBeActive !== currentlyActive) {
      appInstance?.debug(`🎯 Threshold triggered for ${commandName}: ${threshold.watchPath} = ${normalizedValue}, switching to ${shouldBeActive ? 'ON' : 'OFF'}`);

      // Execute the command
      const result = executeCommand(commandName, shouldBeActive);
      if (result.success) {
        state.lastTriggered = now;
        appInstance?.debug(`✅ Threshold-triggered command executed: ${commandName} = ${shouldBeActive}`);
      } else {
        appInstance?.error(`❌ Threshold-triggered command failed: ${commandName} - ${result.message}`);
      }
    }

    command.active = shouldBeActive;
    commandState.registeredCommands.set(commandName, command);
    currentCommands = Array.from(commandState.registeredCommands.values());

    // Update last value
    state.lastValue = normalizedValue;

  } catch (error) {
    appInstance?.error(`❌ Error processing threshold for ${commandName}: ${(error as Error).message}`);
  } finally {
    // Always release lock and clear timeout
    clearTimeout(timeoutId);
    thresholdProcessingLocks.set(commandName, false);
  }
}

function evaluateThreshold(
  threshold: ThresholdConfig,
  currentValue: any,
  homePort?: { latitude: number; longitude: number }
): boolean {
  switch (threshold.operator) {
    // Numeric operators
    case 'gt':
      return typeof currentValue === 'number' && typeof threshold.value === 'number' && currentValue > threshold.value;

    case 'lt':
      return typeof currentValue === 'number' && typeof threshold.value === 'number' && currentValue < threshold.value;

    case 'eq':
      return currentValue === threshold.value;

    case 'ne':
      return currentValue !== threshold.value;

    case 'range':
      if (typeof currentValue !== 'number' || typeof threshold.valueMin !== 'number' || typeof threshold.valueMax !== 'number') {
        return false;
      }
      return currentValue >= threshold.valueMin && currentValue <= threshold.valueMax;

    // String operators
    case 'contains':
      return typeof currentValue === 'string' && typeof threshold.value === 'string' && currentValue.includes(threshold.value);

    case 'startsWith':
      return typeof currentValue === 'string' && typeof threshold.value === 'string' && currentValue.startsWith(threshold.value);

    case 'endsWith':
      return typeof currentValue === 'string' && typeof threshold.value === 'string' && currentValue.endsWith(threshold.value);

    case 'stringEquals':
      return typeof currentValue === 'string' && typeof threshold.value === 'string' && currentValue === threshold.value;

    // Boolean operators
    case 'true':
      return currentValue === true || currentValue === 'true' || currentValue === 1;

    case 'false':
      return currentValue === false || currentValue === 'false' || currentValue === 0;

    // Position operators
    case 'withinRadius':
    case 'outsideRadius':
      if (!currentValue || typeof currentValue.latitude !== 'number' || typeof currentValue.longitude !== 'number') {
        appInstance?.error(`❌ Threshold ${threshold.watchPath}: Current value is not a valid position`);
        return false;
      }

      const targetLat = threshold.useHomePort && homePort ? homePort.latitude : threshold.latitude;
      const targetLon = threshold.useHomePort && homePort ? homePort.longitude : threshold.longitude;

      if (typeof targetLat !== 'number' || typeof targetLon !== 'number') {
        appInstance?.error(`❌ Threshold ${threshold.watchPath}: Target position not configured`);
        return false;
      }

      if (typeof threshold.radius !== 'number') {
        appInstance?.error(`❌ Threshold ${threshold.watchPath}: Radius not configured`);
        return false;
      }

      const distance = calculateDistance(
        currentValue.latitude,
        currentValue.longitude,
        targetLat,
        targetLon
      );

      return threshold.operator === 'withinRadius'
        ? distance <= threshold.radius
        : distance > threshold.radius;

    case 'inBoundingBox':
    case 'outsideBoundingBox':
      if (!currentValue || typeof currentValue.latitude !== 'number' || typeof currentValue.longitude !== 'number') {
        appInstance?.error(`❌ Threshold ${threshold.watchPath}: Current value is not a valid position`);
        return false;
      }

      if (!threshold.boundingBox) {
        appInstance?.error(`❌ Threshold ${threshold.watchPath}: Bounding box not configured`);
        return false;
      }

      const inBox = isPointInBoundingBox(
        currentValue.latitude,
        currentValue.longitude,
        threshold.boundingBox
      );

      return threshold.operator === 'inBoundingBox' ? inBox : !inBox;

    default:
      appInstance?.error(`❌ Unknown threshold operator: ${threshold.operator}`);
      return false;
  }
}

function normalizeThresholdValue(value: any, threshold: ThresholdConfig): any {
  if (value === null || value === undefined) {
    return value;
  }

  if (typeof value === 'object') {
    if ('value' in value) {
      return value.value;
    }
    if ('values' in value && value.values && typeof value.values === 'object') {
      if ('value' in value.values) {
        return value.values.value;
      }
    }
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed === '') {
      return value;
    }
    const lower = trimmed.toLowerCase();
    if (lower === 'true' || lower === 'false') {
      return lower === 'true';
    }
    const num = Number(trimmed);
    if (!Number.isNaN(num)) {
      return num;
    }
  }

  if (typeof value === 'number') {
    return value;
  }

  if (typeof value === 'boolean') {
    return value;
  }

  // If threshold expects numeric and value is object with number
  if (typeof threshold.value === 'number') {
    const extracted = Number(value);
    if (!Number.isNaN(extracted)) {
      return extracted;
    }
  }

  return value;
}

export function updateCommandThreshold(commandName: string, threshold: ThresholdConfig): CommandExecutionResult {
  const command = commandState.registeredCommands.get(commandName);
  if (!command) {
    return { success: false, state: 'FAILED', message: `Command ${commandName} not found`, timestamp: new Date().toISOString() };
  }

  // Update the thresholds configuration (replace single threshold with array)
  command.thresholds = [threshold];

  // Restart monitoring for this command
  if (threshold.enabled) {
    setupThresholdMonitoring(command, threshold);
  } else {
    // Stop monitoring if disabled
    const monitorKey = `${commandName}_${threshold.watchPath}`;
    const state = thresholdState.get(monitorKey);
    if (state?.unsubscribe) {
      state.unsubscribe();
      thresholdState.delete(monitorKey);
    }
  }

  // Save configuration
  const config = loadWebAppConfig();
  saveWebAppConfig(config.paths, config.commands, appInstance);

  appInstance?.debug(`🎯 Updated threshold configuration for ${commandName}`);
  return { success: true, state: 'COMPLETED', message: `Threshold updated for ${commandName}`, timestamp: new Date().toISOString() };
}

export function setManualOverride(commandName: string, override: boolean, expiryMinutes?: number): CommandExecutionResult {
  const command = commandState.registeredCommands.get(commandName);
  if (!command) {
    return { success: false, state: 'FAILED', message: `Command ${commandName} not found`, timestamp: new Date().toISOString() };
  }

  command.manualOverride = override;

  if (override && expiryMinutes) {
    const expiry = new Date(Date.now() + expiryMinutes * 60 * 1000);
    command.manualOverrideUntil = expiry.toISOString();
    appInstance?.debug(`🔒 Manual override set for ${commandName} until ${expiry.toISOString()}`);
  } else if (override) {
    command.manualOverrideUntil = undefined;
    appInstance?.debug(`🔒 Permanent manual override set for ${commandName}`);
  } else {
    command.manualOverrideUntil = undefined;
    appInstance?.debug(`🔓 Manual override cleared for ${commandName}`);
  }

  // Save configuration
  const config = loadWebAppConfig();
  saveWebAppConfig(config.paths, config.commands, appInstance);

  return { success: true, state: 'COMPLETED', message: `Manual override ${override ? 'enabled' : 'disabled'} for ${commandName}`, timestamp: new Date().toISOString() };
}
