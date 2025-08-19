import {
  CommandConfig,
  CommandRegistrationState,
  CommandExecutionResult,
  CommandPutHandler,
  CommandHistoryEntry,
  PathConfig,
  WebAppPathConfig,
  PluginConfig,
  StreamingSubscriptionConfig,
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
      const streamingSubscriptions = rawConfig.streamingSubscriptions || [];

      const migratedConfig = {
        paths: migratedPaths,
        commands: migratedCommands,
        streamingSubscriptions: streamingSubscriptions,
      };

      appToUse.debug(
        `Loaded and migrated ${migratedPaths.length} paths, ${migratedCommands.length} commands, and ${streamingSubscriptions.length} streaming subscriptions from existing config`
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
    streamingSubscriptions: [],
  };
}

export function saveWebAppConfig(
  paths: PathConfig[],
  commands: CommandConfig[],
  app?: ServerAPI,
  streamingSubscriptions?: StreamingSubscriptionConfig[]
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

    // Load existing config to preserve streaming subscriptions if not provided
    let existingStreamingSubscriptions: StreamingSubscriptionConfig[] = [];
    if (!streamingSubscriptions && fs.existsSync(webAppConfigPath)) {
      try {
        const existingConfig = JSON.parse(fs.readFileSync(webAppConfigPath, 'utf8'));
        existingStreamingSubscriptions = existingConfig.streamingSubscriptions || [];
      } catch (error) {
        // If we can't read existing config, continue with empty array
      }
    }

    const webAppConfig: WebAppPathConfig = { 
      paths, 
      commands, 
      streamingSubscriptions: streamingSubscriptions || existingStreamingSubscriptions 
    };
    fs.writeFileSync(webAppConfigPath, JSON.stringify(webAppConfig, null, 2));
    appToUse.debug(
      `Saved webapp configuration: ${paths.length} paths, ${commands.length} commands, ${webAppConfig.streamingSubscriptions?.length || 0} streaming subscriptions`
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
      app.debug(
        `✅ Added missing path configuration for existing command: ${commandConfig.command}`
      );
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

  app.debug(
    `🎮 Command state initialized with ${currentCommands.length} commands`
  );
}

// Command registration with full type safety
export function registerCommand(
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
      appInstance.debug(
        `Handling PUT for commands.${commandName} with value: ${JSON.stringify(value)}`
      );
      return executeCommand(commandName, Boolean(value));
    };

    // Register PUT handler with SignalK
    appInstance.registerPutHandler(
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

    // Update current commands
    currentCommands = Array.from(commandState.registeredCommands.values());

    // Log command registration
    addCommandHistoryEntry(commandName, 'REGISTER', undefined, true);

    appInstance.debug(`✅ Registered command: ${commandName} at ${fullPath}`);

    return {
      state: 'COMPLETED',
      statusCode: 200,
      message: `Command '${commandName}' registered successfully with automatic path configuration`,
      timestamp: new Date().toISOString(),
    };
  } catch (error) {
    const errorMessage = `Failed to register command '${commandName}': ${error}`;
    appInstance.error(errorMessage);

    return {
      state: 'FAILED',
      statusCode: 500,
      message: errorMessage,
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
      state: 'COMPLETED',
      statusCode: 200,
      message: `Command '${commandName}' unregistered successfully with path cleanup`,
      timestamp: new Date().toISOString(),
    };
  } catch (error) {
    const errorMessage = `Failed to unregister command '${commandName}': ${error}`;
    appInstance.error(errorMessage);

    return {
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