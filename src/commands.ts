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

// Global variables for command management
let currentCommands: CommandConfig[] = [];
let commandHistory: CommandHistoryEntry[] = [];
let appInstance: ServerAPI;

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
    appInstance.handleMessage('signalk-parquet', delta as Delta);

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
  appInstance.handleMessage('signalk-parquet', delta as Delta);
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
export function startThresholdMonitoring(app: ServerAPI): void {
  appInstance = app;
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
  if (!threshold?.enabled || !threshold.watchPath) {
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
  const unsubscribe = appInstance?.streambundle?.getSelfBus(threshold.watchPath as Path)?.onValue((value: any) => {
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
}

function processThresholdValue(command: CommandConfig, threshold: ThresholdConfig, value: any, monitorKey: string): void {
  const state = thresholdState.get(monitorKey);
  if (!state) return;

  // Skip processing if manual override is active
  if (command.manualOverride) {
    // Check if manual override has expired
    if (command.manualOverrideUntil) {
      const expiry = new Date(command.manualOverrideUntil);
      if (new Date() > expiry) {
        // Override expired, clear it
        command.manualOverride = false;
        command.manualOverrideUntil = undefined;
        appInstance?.debug(`⏰ Manual override expired for ${command.command}`);
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
  const shouldActivate = evaluateThreshold(threshold, value);

  // Apply hysteresis for numeric values
  if (threshold.hysteresis && typeof value === 'number' && typeof state.lastValue === 'number') {
    const timeSinceLastTrigger = now - state.lastTriggered;
    if (timeSinceLastTrigger < (threshold.hysteresis * 1000)) {
      // Within hysteresis period, skip
      return;
    }
  }

  // Determine if command state should change
  const currentlyActive = command.active || false;
  const shouldBeActive = threshold.activateOnMatch ? shouldActivate : !shouldActivate;

  if (shouldBeActive !== currentlyActive) {
    appInstance?.debug(`🎯 Threshold triggered for ${command.command}: ${threshold.watchPath} = ${value}, switching to ${shouldBeActive ? 'ON' : 'OFF'}`);

    // Execute the command
    const result = executeCommand(command.command, shouldBeActive);
    if (result.success) {
      state.lastTriggered = now;
      appInstance?.debug(`✅ Threshold-triggered command executed: ${command.command} = ${shouldBeActive}`);
    } else {
      appInstance?.error(`❌ Threshold-triggered command failed: ${command.command} - ${result.message}`);
    }
  }

  // Update last value
  state.lastValue = value;
}

function evaluateThreshold(threshold: ThresholdConfig, currentValue: any): boolean {
  switch (threshold.operator) {
    case 'gt':
      return typeof currentValue === 'number' && typeof threshold.value === 'number' && currentValue > threshold.value;

    case 'lt':
      return typeof currentValue === 'number' && typeof threshold.value === 'number' && currentValue < threshold.value;

    case 'eq':
      return currentValue === threshold.value;

    case 'ne':
      return currentValue !== threshold.value;

    case 'true':
      return currentValue === true || currentValue === 'true' || currentValue === 1;

    case 'false':
      return currentValue === false || currentValue === 'false' || currentValue === 0;

    default:
      appInstance?.error(`❌ Unknown threshold operator: ${threshold.operator}`);
      return false;
  }
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