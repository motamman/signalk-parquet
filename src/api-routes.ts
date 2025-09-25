import * as fs from 'fs-extra';
import * as path from 'path';
import express, { Router } from 'express';
import { getAvailablePaths } from './utils/path-discovery';
import { DuckDBInstance } from '@duckdb/node-api';
import {
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
  CommandApiResponse,
  CommandRegistrationRequest,
  CommandExecutionRequest,
  CommandExecutionResponse,
  PluginState,
  PluginConfig,
  PathConfig,
  AnalysisApiResponse,
  ClaudeConnectionTestResponse,
  ValidationApiResponse,
} from './types';
import {
  loadWebAppConfig,
  saveWebAppConfig,
  registerCommand,
  updateCommand,
  unregisterCommand,
  executeCommand,
  getCurrentCommands,
  getCommandHistory,
  getCommandState,
} from './commands';
import { updateDataSubscriptions } from './data-handler';
import { toContextFilePath, toParquetFilePath } from './utils/path-helpers';
import { ServerAPI, Context } from '@signalk/server-api';
import { ClaudeAnalyzer, AnalysisRequest, FollowUpRequest } from './claude-analyzer';
import { AnalysisTemplateManager, TEMPLATE_CATEGORIES } from './analysis-templates';
import { VesselContextManager } from './vessel-context';
// import { initializeStreamingService, shutdownStreamingService } from './index';

// Shared analyzer instance to maintain conversation state across requests
let sharedAnalyzer: ClaudeAnalyzer | null = null;

/**
 * Get or create the shared Claude analyzer instance
 */
function getSharedAnalyzer(config: any, app: ServerAPI, getDataDir: () => string, state: PluginState): ClaudeAnalyzer {
  if (!sharedAnalyzer) {
    sharedAnalyzer = new ClaudeAnalyzer({
      apiKey: config.claudeIntegration.apiKey,
      model: migrateClaudeModel(config.claudeIntegration.model, app) as any,
      maxTokens: config.claudeIntegration.maxTokens || 4000,
      temperature: config.claudeIntegration.temperature || 0.3
    }, app, getDataDir(), state);
    app.debug('🔧 Created shared Claude analyzer instance');
  }
  return sharedAnalyzer;
}

// AWS S3 for testing connection
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let ListObjectsV2Command: any;

// Helper function to migrate deprecated Claude model names
function migrateClaudeModel(model?: string, app?: ServerAPI): string {
  const currentModel = model || 'claude-sonnet-4-20250514';
  
  // Migration mapping for deprecated models
  const migrations: Record<string, string> = {
    'claude-3-sonnet-20240229': 'claude-sonnet-4-20250514',
    'claude-3-5-sonnet-20241022': 'claude-sonnet-4-20250514',
    'claude-3-7-sonnet-20250219': 'claude-sonnet-4-20250514',
    'claude-3-5-haiku-20241022': 'claude-sonnet-4-20250514',
    'claude-3-haiku-20240307': 'claude-sonnet-4-20250514',
  };
  
  if (migrations[currentModel]) {
    const newModel = migrations[currentModel];
    app?.debug(`Auto-migrated deprecated Claude model ${currentModel} to ${newModel}`);
    return newModel;
  }
  
  // Validate that the model is in our supported list
  const supportedModels = [
    'claude-opus-4-1-20250805',
    'claude-opus-4-20250514', 
    'claude-sonnet-4-20250514'
  ];

  // If model is not in supported list, fall back to default
  if (!supportedModels.includes(currentModel)) {
    app?.debug(`Unknown Claude model ${currentModel}, falling back to default`);
    return 'claude-sonnet-4-20250514';
  }

  return currentModel;
}

export function registerApiRoutes(
  router: Router,
  state: PluginState,
  app: ServerAPI
): void {
  // Serve static files from public directory
  const publicPath = path.join(__dirname, '../public');
  if (fs.existsSync(publicPath)) {
    router.use(express.static(publicPath));
  }

  // Get the current configuration for data directory
  const getDataDir = (): string => {
    // Use the user-configured output directory, fallback to SignalK default
    return state.currentConfig?.outputDirectory || app.getDataDirPath();
  };


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

  // Get available SignalK paths
  router.get(
    '/api/paths',
    (_: TypedRequest, res: TypedResponse<PathsApiResponse>) => {
      try {
        const dataDir = getDataDir();
        const paths = getAvailablePaths(dataDir, app);

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
            const selfContextPath = toContextFilePath(
              app.selfContext as Context
            );
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
              const filePath = toParquetFilePath(
                dataDir,
                selfContextPath,
                quotedPath
              );
              processedQuery = processedQuery.replace(match, `'${filePath}'`);
            }
          });
        }


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

        if (!ListObjectsV2Command || !state.s3Client) {
          // Try to import S3 SDK dynamically
          try {
            const awsS3 = await import('@aws-sdk/client-s3');
            ListObjectsV2Command = awsS3.ListObjectsV2Command;
          } catch (importError) {
            return res.status(503).json({
              success: false,
              error: 'S3 client not available or not initialized',
            });
          }
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
        const webAppConfig = loadWebAppConfig(app);
        return res.json({
          success: true,
          paths: webAppConfig.paths,
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

        // Load current configuration
        const webAppConfig = loadWebAppConfig(app);
        const currentPaths = webAppConfig.paths;
        const currentCommands = webAppConfig.commands;

        // Add to current paths
        currentPaths.push(newPath);

        // Save to web app configuration
        saveWebAppConfig(currentPaths, currentCommands, app);

        // Update subscriptions
        if (state.currentConfig) {
          updateDataSubscriptions(currentPaths, state, state.currentConfig, app);
        }

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

        // Load current configuration
        const webAppConfig = loadWebAppConfig(app);
        const currentPaths = webAppConfig.paths;
        const currentCommands = webAppConfig.commands;

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
        saveWebAppConfig(currentPaths, currentCommands, app);

        // Update subscriptions
        if (state.currentConfig) {
          updateDataSubscriptions(currentPaths, state, state.currentConfig, app);
        }

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

        // Load current configuration
        const webAppConfig = loadWebAppConfig(app);
        const currentPaths = webAppConfig.paths;
        const currentCommands = webAppConfig.commands;

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
        saveWebAppConfig(currentPaths, currentCommands, app);

        // Update subscriptions
        if (state.currentConfig) {
          updateDataSubscriptions(currentPaths, state, state.currentConfig, app);
        }


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
        const commands = getCurrentCommands();
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
        const { command, description, keywords } = req.body;

        if (!command || !/^[a-zA-Z0-9_]+$/.test(command) || command.length === 0 || command.length > 50) {
          return res.status(400).json({
            success: false,
            error:
              'Invalid command name. Must be alphanumeric with underscores, 1-50 characters.',
          });
        }

        const result = registerCommand(command, description, keywords);

        if (result.state === 'COMPLETED') {
          // Update webapp config
          const webAppConfig = loadWebAppConfig(app);
          const currentCommands = getCurrentCommands();
          saveWebAppConfig(webAppConfig.paths, currentCommands, app);

          const commandState = getCommandState();
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
          return res.json({
            success: true,
            command: command,
            value: value,
            executed: true,
            timestamp: result.timestamp,
          });
        } else {
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
          // Update webapp config
          const webAppConfig = loadWebAppConfig(app);
          const currentCommands = getCurrentCommands();
          saveWebAppConfig(webAppConfig.paths, currentCommands, app);

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

  // Update command (PUT)
  router.put(
    '/api/commands/:command',
    (
      req: TypedRequest<{ description?: string; keywords?: string[] }>,
      res: TypedResponse<CommandApiResponse>
    ) => {
      try {
        const { command } = req.params;
        const { description, keywords } = req.body;

        const result = updateCommand(command, description, keywords);
        if (result.state === 'COMPLETED') {
          // Update webapp config
          const webAppConfig = loadWebAppConfig(app);
          const currentCommands = getCurrentCommands();
          saveWebAppConfig(webAppConfig.paths, currentCommands, app);
          
          return res.json({
            success: true,
            message: result.message,
          });
        } else {
          return res.status(result.statusCode || 400).json({
            success: false,
            error: result.message,
          });
        }
      } catch (error) {
        app.error(`Error updating command: ${error}`);
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
        const commandHistory = getCommandHistory();
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
        const commandState = getCommandState();
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

  // Streaming Control API endpoints - DISABLED

  // // Enable streaming at runtime
  // router.post('/api/streaming/enable', async (req: TypedRequest, res: TypedResponse) => {
  //   try {
  //     if (state.streamingService) {
  //       return res.json({
  //         success: true,
  //         message: 'Streaming service is already running',
  //         enabled: true
  //       });
  //     }

  //     // Check if streaming is enabled in config
  //     if (!state.currentConfig?.enableStreaming) {
  //       return res.status(400).json({
  //         success: false,
  //         error: 'Streaming is disabled in plugin configuration. Enable it in plugin settings first.',
  //         enabled: false
  //       });
  //     }

  //     const result = await initializeStreamingService(state, app);
      
  //     if (result.success) {
  //       return res.json({
  //         success: true,
  //         message: 'Streaming service enabled successfully',
  //         enabled: true
  //       });
  //     } else {
  //       return res.status(500).json({
  //         success: false,
  //         error: result.error || 'Failed to enable streaming service',
  //         enabled: false
  //       });
  //     }
  //   } catch (error) {
  //     app.error(`Error enabling streaming: ${error}`);
  //     return res.status(500).json({
  //       success: false,
  //       error: (error as Error).message,
  //       enabled: false
  //     });
  //   }
  // });

  // // Disable streaming at runtime  
  // router.post('/api/streaming/disable', (req: TypedRequest, res: TypedResponse) => {
  //   try {
  //     if (!state.streamingService) {
  //       return res.json({
  //         success: true,
  //         message: 'Streaming service is not running',
  //         enabled: false
  //       });
  //     }

  //     const result = shutdownStreamingService(state, app);
      
  //     if (result.success) {
  //       return res.json({
  //         success: true,
  //         message: 'Streaming service disabled successfully',
  //         enabled: false
  //       });
  //     } else {
  //       return res.status(500).json({
  //         success: false,
  //         error: result.error || 'Failed to disable streaming service',
  //         enabled: true
  //       });
  //     }
  //   } catch (error) {
  //     app.error(`Error disabling streaming: ${error}`);
  //     return res.status(500).json({
  //       success: false,
  //       error: (error as Error).message,
  //       enabled: true
  //     });
  //   }
  // });

  // // Get current streaming status
  // router.get('/api/streaming/status', (req: TypedRequest, res: TypedResponse) => {
  //   try {
  //     const isEnabled = !!state.streamingService;
  //     const configEnabled = state.currentConfig?.enableStreaming ?? false;
      
  //     // Get streaming service statistics if available
  //     let stats = {};
  //     if (state.streamingService && state.streamingService.getActiveSubscriptions) {
  //       const subscriptions = state.streamingService.getActiveSubscriptions();
  //       stats = {
  //         activeSubscriptions: subscriptions.length,
  //         subscriptions: subscriptions
  //       };
  //     }

  //     res.json({
  //       success: true,
  //       enabled: isEnabled,
  //       configEnabled: configEnabled,
  //       canEnable: configEnabled && !isEnabled,
  //       canDisable: isEnabled,
  //       ...stats
  //     });
  //   } catch (error) {
  //     app.error(`Error getting streaming status: ${error}`);
  //     res.status(500).json({
  //       success: false,
  //       error: (error as Error).message
  //     });
  //   }
  // });

  // Health check
  router.get(
    '/api/health',
    (_: TypedRequest, res: TypedResponse<HealthApiResponse>) => {
      return res.json({
        success: true,
        status: 'healthy',
        timestamp: new Date().toISOString(),
      });
    }
  );

  // ===========================================
  // CLAUDE AI ANALYSIS API ROUTES
  // ===========================================

  // Get available analysis templates
  router.get(
    '/api/analyze/templates',
    (_: TypedRequest, res: TypedResponse<AnalysisApiResponse>) => {
      try {
        const templates = TEMPLATE_CATEGORIES.map(category => ({
          ...category,
          templates: category.templates.map(template => ({
            id: template.id,
            name: template.name,
            description: template.description,
            category: template.category,
            icon: template.icon,
            complexity: template.complexity,
            estimatedTime: template.estimatedTime,
            requiredPaths: template.requiredPaths
          }))
        }));

        res.json({
          success: true,
          templates: templates as any
        });
      } catch (error) {
        app.error(`Template retrieval failed: ${(error as Error).message}`);
        res.status(500).json({
          success: false,
          error: 'Failed to retrieve analysis templates'
        });
      }
    }
  );

  // Test Claude connection
  router.post(
    '/api/analyze/test-connection',
    async (_req: TypedRequest, res: TypedResponse<ClaudeConnectionTestResponse>) => {
      try {
        const config = state.currentConfig;
        if (!config?.claudeIntegration?.enabled || !config.claudeIntegration.apiKey) {
          return res.status(400).json({
            success: false,
            error: 'Claude integration is not configured or enabled'
          });
        }

        const analyzer = getSharedAnalyzer(config, app, getDataDir, state);

        const startTime = Date.now();
        const testResult = await analyzer.testConnection();
        const responseTime = Date.now() - startTime;

        if (testResult.success) {
          return res.json({
            success: true,
            model: migrateClaudeModel(config.claudeIntegration.model, app),
            responseTime,
            tokenUsage: 50 // Approximate for test
          });
        } else {
          return res.status(400).json({
            success: false,
            error: testResult.error || 'Connection test failed'
          });
        }
      } catch (error) {
        app.error(`Claude connection test failed: ${(error as Error).message}`);
        return res.status(500).json({
          success: false,
          error: 'Claude connection test failed'
        });
      }
    }
  );

  // Main analysis endpoint
  router.post(
    '/api/analyze',
    async (req: TypedRequest<{
      dataPath: string;
      analysisType?: string;
      templateId?: string;
      customPrompt?: string;
      timeRange?: { start: string; end: string };
      aggregationMethod?: string;
      resolution?: string;
      claudeModel?: string;
      useDatabaseAccess?: boolean;
    }>, res: TypedResponse<AnalysisApiResponse>) => {
      try {
        const config = state.currentConfig;
        if (!config?.claudeIntegration?.enabled || !config.claudeIntegration.apiKey) {
          return res.status(400).json({
            success: false,
            error: 'Claude integration is not configured or enabled'
          });
        }

        const { dataPath, analysisType, templateId, customPrompt, timeRange, aggregationMethod, resolution, claudeModel, useDatabaseAccess } = req.body;

        console.log(`🧠 ANALYSIS REQUEST: dataPath=${dataPath}, templateId=${templateId}, analysisType=${analysisType}, aggregationMethod=${aggregationMethod}, model=${claudeModel || 'config-default'}`);
        console.log(`🔍 CUSTOM PROMPT DEBUG: "${customPrompt}" (type: ${typeof customPrompt}, length: ${customPrompt?.length || 0})`);

        if (!dataPath) {
          return res.status(400).json({
            success: false,
            error: 'Data path is required'
          });
        }

        // Use shared analyzer instance to maintain conversation state
        const analyzer = getSharedAnalyzer(config, app, getDataDir, state);

        // Build analysis request
        let analysisRequest: AnalysisRequest;

        if (templateId) {
          // Use template
          const parsedTimeRange = timeRange ? {
            start: new Date(timeRange.start),
            end: new Date(timeRange.end)
          } : undefined;

          const templateRequest = AnalysisTemplateManager.createAnalysisRequest(
            templateId,
            dataPath,
            customPrompt,
            parsedTimeRange
          );

          if (!templateRequest) {
            return res.status(400).json({
              success: false,
              error: `Template not found: ${templateId}`
            });
          }
          
          analysisRequest = templateRequest;
        } else {
          // Custom analysis
          analysisRequest = {
            dataPath,
            analysisType: (analysisType as any) || 'custom',
            customPrompt: customPrompt || 'Analyze this maritime data and provide insights',
            timeRange: timeRange ? {
              start: new Date(timeRange.start),
              end: new Date(timeRange.end)
            } : undefined,
            aggregationMethod,
            resolution,
            useDatabaseAccess: useDatabaseAccess || false
          };
        }

        // Execute analysis
        app.debug(`Starting Claude analysis: ${analysisRequest.analysisType} for ${dataPath}`);
        const result = await analyzer.analyzeData(analysisRequest);

        return res.json({
          success: true,
          data: {
            id: result.id,
            analysis: result.analysis,
            insights: result.insights,
            recommendations: result.recommendations,
            anomalies: result.anomalies?.map(a => ({
              timestamp: a.timestamp,
              value: a.value,
              expectedRange: a.expectedRange,
              severity: a.severity,
              description: a.description,
              confidence: a.confidence
            })),
            confidence: result.confidence,
            dataQuality: result.dataQuality,
            timestamp: result.timestamp,
            metadata: result.metadata
          },
          usage: result.usage
        });

      } catch (error) {
        app.error(`Analysis failed: ${(error as Error).message}`);
        return res.status(500).json({
          success: false,
          error: `Analysis failed: ${(error as Error).message}`
        });
      }
    }
  );

  // Follow-up question endpoint
  router.post(
    '/api/analyze/followup',
    async (req: TypedRequest<{ conversationId: string; question: string }>, res: TypedResponse<AnalysisApiResponse>) => {
      try {
        const config = state.currentConfig;
        if (!config?.claudeIntegration?.enabled || !config.claudeIntegration.apiKey) {
          return res.status(400).json({
            success: false,
            error: 'Claude integration is not configured or enabled'
          });
        }

        const { conversationId, question } = req.body;

        if (!conversationId || !question) {
          return res.status(400).json({
            success: false,
            error: 'Both conversationId and question are required'
          });
        }

        console.log(`🔄 FOLLOW-UP REQUEST: conversationId=${conversationId}, question=${question.substring(0, 100)}...`);

        // Use shared analyzer instance to access stored conversations
        const analyzer = getSharedAnalyzer(config, app, getDataDir, state);

        // Process follow-up question
        const followUpRequest = {
          conversationId,
          question
        };

        const analysisResult = await analyzer.askFollowUp(followUpRequest);

        return res.json({
          success: true,
          data: analysisResult,
          usage: analysisResult.usage
        });

      } catch (error) {
        app.error(`Follow-up question failed: ${(error as Error).message}`);
        return res.status(500).json({
          success: false,
          error: `Follow-up question failed: ${(error as Error).message}`
        });
      }
    }
  );

  // Get analysis history
  router.get(
    '/api/analyze/history',
    async (req: TypedRequest<any> & { query: { limit?: string } }, res: TypedResponse<AnalysisApiResponse>) => {
      try {
        const config = state.currentConfig;
        if (!config?.claudeIntegration?.enabled || !config.claudeIntegration.apiKey) {
          return res.status(400).json({
            success: false,
            error: 'Claude integration is not configured or enabled'
          });
        }

        const limit = parseInt(req.query.limit || '20', 10);
        
        const analyzer = getSharedAnalyzer(config, app, getDataDir, state);

        const history = await analyzer.getAnalysisHistory(limit);
        
        return res.json({
          success: true,
          data: history.map(h => ({
            id: h.id,
            analysis: h.analysis,
            insights: h.insights,
            recommendations: h.recommendations,
            anomalies: h.anomalies?.map(a => ({
              timestamp: a.timestamp,
              value: a.value,
              expectedRange: a.expectedRange,
              severity: a.severity,
              description: a.description,
              confidence: a.confidence
            })),
            confidence: h.confidence,
            dataQuality: h.dataQuality,
            timestamp: h.timestamp,
            metadata: h.metadata
          }))
        });

      } catch (error) {
        app.error(`Analysis history retrieval failed: ${(error as Error).message}`);
        return res.status(500).json({
          success: false,
          error: 'Failed to retrieve analysis history'
        });
      }
    }
  );

  // Delete analysis from history
  router.delete(
    '/api/analyze/history/:id',
    async (req: TypedRequest<any> & { params: { id: string } }, res: TypedResponse<any>) => {
      try {
        const config = state.currentConfig;
        if (!config?.claudeIntegration?.enabled || !config.claudeIntegration.apiKey) {
          return res.status(400).json({
            success: false,
            error: 'Claude integration is not configured or enabled'
          });
        }

        const analysisId = req.params.id;
        
        const analyzer = getSharedAnalyzer(config, app, getDataDir, state);

        const result = await analyzer.deleteAnalysis(analysisId);
        
        if (result.success) {
          return res.json({
            success: true,
            message: 'Analysis deleted successfully'
          });
        } else {
          return res.status(404).json({
            success: false,
            error: result.error
          });
        }

      } catch (error) {
        app.error(`Analysis deletion failed: ${(error as Error).message}`);
        return res.status(500).json({
          success: false,
          error: 'Failed to delete analysis'
        });
      }
    }
  );

  // ===========================================
  // VESSEL CONTEXT API ROUTES
  // ===========================================

  // Get vessel context
  router.get(
    '/api/vessel-context',
    async (_: TypedRequest, res: TypedResponse<any>) => {
      try {
        const contextManager = new VesselContextManager(app, getDataDir());
        const context = await contextManager.getVesselContext();
        
        return res.json({
          success: true,
          data: context
        });

      } catch (error) {
        app.error(`Failed to get vessel context: ${(error as Error).message}`);
        return res.status(500).json({
          success: false,
          error: 'Failed to get vessel context'
        });
      }
    }
  );

  // Update vessel context
  router.post(
    '/api/vessel-context',
    async (req: TypedRequest<{
      vesselInfo?: any;
      customContext?: string;
    }>, res: TypedResponse<any>) => {
      try {
        const { vesselInfo, customContext } = req.body;
        
        const contextManager = new VesselContextManager(app, getDataDir());
        const updatedContext = await contextManager.updateVesselContext(
          vesselInfo,
          customContext,
          false // Not auto-extracted since it's from user input
        );
        
        return res.json({
          success: true,
          data: updatedContext,
          message: 'Vessel context updated successfully'
        });

      } catch (error) {
        app.error(`Failed to update vessel context: ${(error as Error).message}`);
        return res.status(500).json({
          success: false,
          error: 'Failed to update vessel context'
        });
      }
    }
  );

  // Refresh vessel context from SignalK
  router.post(
    '/api/vessel-context/refresh',
    async (_: TypedRequest, res: TypedResponse<any>) => {
      try {
        const contextManager = new VesselContextManager(app, getDataDir());
        const refreshedContext = await contextManager.refreshVesselInfo();
        
        return res.json({
          success: true,
          data: refreshedContext,
          message: 'Vessel context refreshed from SignalK data'
        });

      } catch (error) {
        app.error(`Failed to refresh vessel context: ${(error as Error).message}`);
        return res.status(500).json({
          success: false,
          error: 'Failed to refresh vessel context from SignalK'
        });
      }
    }
  );

  // Get vessel data paths for UI
  router.get(
    '/api/vessel-context/data-paths',
    (_: TypedRequest, res: TypedResponse<any>) => {
      try {
        const dataPaths = VesselContextManager.getVesselDataPaths();
        
        return res.json({
          success: true,
          data: dataPaths
        });

      } catch (error) {
        app.error(`Failed to get vessel data paths: ${(error as Error).message}`);
        return res.status(500).json({
          success: false,
          error: 'Failed to get vessel data paths'
        });
      }
    }
  );

  // Generate Claude context preview
  router.get(
    '/api/vessel-context/claude-preview',
    async (_: TypedRequest, res: TypedResponse<any>) => {
      try {
        const contextManager = new VesselContextManager(app, getDataDir());
        // Ensure context is loaded before generating preview
        await contextManager.getVesselContext();
        const claudeContext = contextManager.generateClaudeContext();
        
        return res.json({
          success: true,
          data: {
            contextText: claudeContext,
            length: claudeContext.length
          }
        });

      } catch (error) {
        app.error(`Failed to generate Claude context preview: ${(error as Error).message}`);
        return res.status(500).json({
          success: false,
          error: 'Failed to generate Claude context preview'
        });
      }
    }
  );

  // ===========================================
  // END VESSEL CONTEXT API ROUTES
  // ===========================================

  // ===========================================
  // END CLAUDE AI ANALYSIS API ROUTES
  // ===========================================

  // Test endpoint
  router.get('/api/test', (_: express.Request, res: express.Response) => {
    res.json({
      message: 'SignalK Parquet Plugin API is working',
      timestamp: new Date().toISOString(),
      config: state.currentConfig ? 'loaded' : 'not loaded',
    });
  });

  // Version endpoint
  router.get('/api/version', (_: express.Request, res: express.Response) => {
    const packagePath = path.join(__dirname, '..', 'package.json');
    try {
      const packageJson = JSON.parse(fs.readFileSync(packagePath, 'utf8'));
      res.json({
        name: packageJson.name,
        version: packageJson.version,
        description: packageJson.description,
      });
    } catch (error) {
      res.status(500).json({ error: 'Failed to read version information' });
    }
  });

  // Historical streaming test endpoints - DISABLED
  // router.post('/api/historical/trigger/:path', (req: express.Request, res: express.Response) => {
  //   try {
  //     const path = req.params.path;
  //     if (state.historicalStreamingService) {
  //       state.historicalStreamingService.triggerHistoricalStream(path);
  //       res.json({
  //         success: true,
  //         message: `Triggered historical stream for path: ${path}`,
  //         timestamp: new Date().toISOString()
  //       });
  //     } else {
  //       res.status(500).json({
  //         success: false,
  //         error: 'Historical streaming service not initialized'
  //       });
  //     }
  //   } catch (error) {
  //     res.status(500).json({
  //       success: false,
  //       error: error instanceof Error ? error.message : String(error)
  //     });
  //   }
  // });

  // router.get('/api/historical/subscriptions', (_: express.Request, res: express.Response) => {
  //   try {
  //     if (state.historicalStreamingService) {
  //       const subscriptions = state.historicalStreamingService.getActiveSubscriptions();
  //       res.json({
  //         success: true,
  //         subscriptions,
  //         count: subscriptions.length
  //       });
  //     } else {
  //       res.status(500).json({
  //         success: false,
  //         error: 'Historical streaming service not initialized'
  //       });
  //     }
  //   } catch (error) {
  //     res.status(500).json({
  //       success: false,
  //       error: error instanceof Error ? error.message : String(error)
  //     });
  //   }
  // });

  // Stream Management API endpoints - DISABLED
  
  // // Get all streams
  // router.get('/api/streams', (_: express.Request, res: express.Response) => {
  //   try {
  //     if (state.historicalStreamingService) {
  //       const streams = state.historicalStreamingService.getAllStreams();
  //       res.json({
  //         success: true,
  //         streams
  //       });
  //     } else {
  //       res.status(500).json({
  //         success: false,
  //         error: 'Historical streaming service not initialized'
  //       });
  //     }
  //   } catch (error) {
  //     res.status(500).json({
  //       success: false,
  //       error: error instanceof Error ? error.message : String(error)
  //     });
  //   }
  // });

  // // Create new stream
  // router.post('/api/streams', (req: express.Request, res: express.Response) => {
  //   try {
  //     if (!state.historicalStreamingService) {
  //       res.status(500).json({
  //         success: false,
  //         error: 'Historical streaming service not initialized'
  //       });
  //       return;
  //     }

  //     const streamConfig = req.body;
      
  //     // Validate required fields
  //     if (!streamConfig.name || !streamConfig.path) {
  //       res.status(400).json({
  //         success: false,
  //         error: 'Stream name and path are required'
  //       });
  //       return;
  //     }

  //     // Validate aggregation method if provided
  //     const validAggregationMethods = ['average', 'min', 'max', 'first', 'last', 'mid', 'middle_index'];
  //     if (streamConfig.aggregateMethod && !validAggregationMethods.includes(streamConfig.aggregateMethod)) {
  //       res.status(400).json({
  //         success: false,
  //         error: `Invalid aggregation method. Valid options: ${validAggregationMethods.join(', ')}`
  //       });
  //       return;
  //     }

  //     // Validate window size if provided
  //     if (streamConfig.windowSize && (typeof streamConfig.windowSize !== 'number' || streamConfig.windowSize < 1 || streamConfig.windowSize > 1000)) {
  //       res.status(400).json({
  //         success: false,
  //         error: 'Window size must be a number between 1 and 1000'
  //       });
  //       return;
  //     }

  //     const stream = state.historicalStreamingService.createStream(streamConfig);
      
  //     // Handle auto-start if requested
  //     if (streamConfig.autoStart) {
  //       const startResult = state.historicalStreamingService.startStream(stream.id);
  //       if (startResult.success) {
  //         stream.status = 'running';
  //       }
  //     }
      
  //     res.json({
  //       success: true,
  //       stream,
  //       message: `Stream '${streamConfig.name}' created successfully${streamConfig.autoStart ? ' and started' : ''}`
  //     });
  //   } catch (error) {
  //     res.status(500).json({
  //       success: false,
  //       error: error instanceof Error ? error.message : String(error)
  //     });
  //   }
  // });

  // // Update stream configuration
  // router.put('/api/streams/:id', (req: express.Request, res: express.Response) => {
  //   try {
  //     if (!state.historicalStreamingService) {
  //       res.status(500).json({
  //         success: false,
  //         error: 'Historical streaming service not initialized'
  //       });
  //       return;
  //     }

  //     const streamId = req.params.id;
  //     const streamConfig = req.body;

  //     // Validate required fields
  //     if (!streamConfig.name || !streamConfig.path) {
  //       res.status(400).json({
  //         success: false,
  //         error: 'Stream name and path are required'
  //       });
  //       return;
  //     }

  //     // Validate window size if provided
  //     if (streamConfig.windowSize && (typeof streamConfig.windowSize !== 'number' || streamConfig.windowSize < 1 || streamConfig.windowSize > 1000)) {
  //       res.status(400).json({
  //         success: false,
  //         error: 'Window size must be a number between 1 and 1000'
  //       });
  //       return;
  //     }

  //     const success = state.historicalStreamingService.updateStream(streamId, streamConfig);
  //     if (success) {
  //       res.json({
  //         success: true,
  //         message: `Stream '${streamConfig.name}' updated successfully`
  //       });
  //     } else {
  //       res.status(404).json({
  //         success: false,
  //         error: 'Stream not found'
  //       });
  //     }
  //   } catch (error) {
  //     res.status(500).json({
  //       success: false,
  //       error: error instanceof Error ? error.message : String(error)
  //     });
  //   }
  // });

  // // Start stream
  // router.put('/api/streams/:id/start', (req: express.Request, res: express.Response) => {
  //   try {
  //     if (!state.historicalStreamingService) {
  //       res.status(500).json({
  //         success: false,
  //         error: 'Historical streaming service not initialized'
  //       });
  //       return;
  //     }

  //     const streamId = req.params.id;
  //     const result = state.historicalStreamingService.startStream(streamId);
      
  //     if (result.success) {
  //       res.json({
  //         success: true,
  //         message: `Stream started successfully`
  //       });
  //     } else {
  //       res.status(404).json({
  //         success: false,
  //         error: result.error || 'Stream not found'
  //       });
  //     }
  //   } catch (error) {
  //     res.status(500).json({
  //       success: false,
  //       error: error instanceof Error ? error.message : String(error)
  //     });
  //   }
  // });

  // // Pause stream
  // router.put('/api/streams/:id/pause', (req: express.Request, res: express.Response) => {
  //   try {
  //     if (!state.historicalStreamingService) {
  //       res.status(500).json({
  //         success: false,
  //         error: 'Historical streaming service not initialized'
  //       });
  //       return;
  //     }

  //     const streamId = req.params.id;
  //     const result = state.historicalStreamingService.pauseStream(streamId);
      
  //     if (result.success) {
  //       res.json({
  //         success: true,
  //         message: `Stream ${result.paused ? 'paused' : 'resumed'} successfully`
  //       });
  //     } else {
  //       res.status(404).json({
  //         success: false,
  //         error: result.error || 'Stream not found'
  //       });
  //     }
  //   } catch (error) {
  //     res.status(500).json({
  //       success: false,
  //       error: error instanceof Error ? error.message : String(error)
  //     });
  //   }
  // });

  // // Stop stream
  // router.put('/api/streams/:id/stop', (req: express.Request, res: express.Response) => {
  //   try {
  //     if (!state.historicalStreamingService) {
  //       res.status(500).json({
  //         success: false,
  //         error: 'Historical streaming service not initialized'
  //       });
  //       return;
  //     }

  //     const streamId = req.params.id;
  //     const result = state.historicalStreamingService.stopStream(streamId);
      
  //     if (result.success) {
  //       res.json({
  //         success: true,
  //         message: `Stream stopped successfully`
  //       });
  //     } else {
  //       res.status(404).json({
  //         success: false,
  //         error: result.error || 'Stream not found'
  //       });
  //     }
  //   } catch (error) {
  //     res.status(500).json({
  //       success: false,
  //       error: error instanceof Error ? error.message : String(error)
  //     });
  //   }
  // });

  // // Delete stream
  // router.delete('/api/streams/:id', (req: express.Request, res: express.Response) => {
  //   try {
  //     if (!state.historicalStreamingService) {
  //       res.status(500).json({
  //         success: false,
  //         error: 'Historical streaming service not initialized'
  //       });
  //       return;
  //     }

  //     const streamId = req.params.id;
  //     const result = state.historicalStreamingService.deleteStream(streamId);
      
  //     if (result.success) {
  //       res.json({
  //         success: true,
  //         message: `Stream deleted successfully`
  //       });
  //     } else {
  //       res.status(404).json({
  //         success: false,
  //         error: result.error || 'Stream not found'
  //       });
  //     }
  //   } catch (error) {
  //     res.status(500).json({
  //       success: false,
  //       error: error instanceof Error ? error.message : String(error)
  //     });
  //   }
  // });

  // // Get stream statistics
  // router.get('/api/streams/stats', (_: express.Request, res: express.Response) => {
  //   try {
  //     if (state.historicalStreamingService) {
  //       const stats = state.historicalStreamingService.getStreamStats();
  //       res.json({
  //         success: true,
  //         stats
  //       });
  //     } else {
  //       res.status(500).json({
  //         success: false,
  //         error: 'Historical streaming service not initialized'
  //       });
  //     }
  //   } catch (error) {
  //     res.status(500).json({
  //       success: false,
  //       error: error instanceof Error ? error.message : String(error)
  //     });
  //   }
  // });

  // // Get recent time-series data for a specific stream
  // router.get('/api/streams/:id/data', async (req: express.Request, res: express.Response) => {
  //   try {
  //     if (!state.historicalStreamingService) {
  //       res.status(500).json({
  //         success: false,
  //         error: 'Historical streaming service not initialized'
  //       });
  //       return;
  //     }

  //     const streamId = req.params.id;
  //     const limit = parseInt(req.query.limit as string) || 50;
      
  //     const timeSeriesData = await (state.historicalStreamingService as any).getStreamTimeSeriesData(streamId, limit);
      
  //     // Always return success, even if no data (return empty array)
  //     res.json({
  //       success: true,
  //       streamId: streamId,
  //       data: timeSeriesData || []
  //     });
  //   } catch (error) {
  //     res.status(500).json({
  //       success: false,
  //       error: error instanceof Error ? error.message : String(error)
  //     });
  //   }
  // });

  // ===========================================
  // SCHEMA VALIDATION API ROUTES
  // ===========================================

  // Validate parquet schemas
  router.post(
    '/api/validate-schemas',
    async (_: TypedRequest, res: TypedResponse<ValidationApiResponse>) => {
      try {
        app.debug('🔍 Starting schema validation...');

        // Import required modules for parquet reading
        const parquet = require('@dsnp/parquetjs');
        const glob = require('glob');
        const path = require('path');

        // Get the data directory from plugin config - use SignalK's main data dir, not plugin config dir
        const configOutputDir = state.currentConfig?.outputDirectory || 'data';
        const pluginDataPath = app.getDataDirPath(); // /Users/.../plugin-config-data/signalk-parquet
        const signalkDataDir = path.dirname(path.dirname(pluginDataPath)); // Go up 2 levels to get to .signalk
        const dataDir = path.join(signalkDataDir, configOutputDir);

        let totalFiles = 0;
        let schemasFound = 0;
        let correctSchemas = 0;
        let violationSchemas = 0;
        const vessels = new Set<string>();
        const violationDetails: string[] = [];
        const debugMessages: string[] = [];

        // Helper function to add debug messages
        const addDebug = (message: string) => {
          debugMessages.push(message);
          app.debug(message);
        };

        // Search for all parquet files
        const searchPattern = path.join(dataDir, 'vessels', '**', '*.parquet');
        addDebug(`🔍 Data directory: ${dataDir}`);
        addDebug(`🔍 Searching pattern: ${searchPattern}`);

        const files = glob.sync(searchPattern, {
          ignore: [
            `${dataDir}/**/processed/**`,
            `${dataDir}/**/repaired/**`,
            `${dataDir}/**/quarantine/**`,
            `${dataDir}/**/claude-schemas/**`,
            `${dataDir}/**/failed/**`
          ]
        });
        addDebug(`📄 Found ${files.length} parquet files`);

        // Process each file
        for (const filePath of files) {
          // Skip quarantined files and processed directories
          if (path.basename(filePath).includes('quarantine') || path.basename(filePath).includes('corrupted') || filePath.includes('/processed/') || filePath.includes('/repaired/')) {
            continue;
          }

          // Extract vessel from path (vessels/[vessel]/)
          const pathParts = filePath.split(path.sep);
          const vesselsIndex = pathParts.findIndex((part: string) => part === 'vessels');
          if (vesselsIndex !== -1 && vesselsIndex + 1 < pathParts.length) {
            vessels.add(pathParts[vesselsIndex + 1]);
          }

          totalFiles++;

          try {
            const reader = await parquet.ParquetReader.openFile(filePath);
            const cursor = reader.getCursor();
            const schema = cursor.schema;

            if (schema && schema.schema) {
              schemasFound++;
              const fields = schema.schema;

              // Check timestamps
              const receivedTimestamp = fields.received_timestamp ? fields.received_timestamp.type : 'MISSING';
              const signalkTimestamp = fields.signalk_timestamp ? fields.signalk_timestamp.type : 'MISSING';

              // Find all value fields
              const valueFields: { [key: string]: string } = {};
              Object.keys(fields).forEach(fieldName => {
                if (fieldName.startsWith('value_') || fieldName === 'value') {
                  valueFields[fieldName] = fields[fieldName].type;
                }
              });

              // Extract SignalK path for metadata lookup
              const relativePath = path.relative(dataDir, filePath);
              const pathMatch = relativePath.match(/vessels\/[^/]+\/(.+?)\/[^/]*\.parquet$/);
              const signalkPath = pathMatch ? pathMatch[1].replace(/\//g, '.') : '';

              // Check for schema violations
              let hasViolations = false;
              const violations: string[] = [];

              // Rule 1: Timestamps should be UTF8/VARCHAR
              if (receivedTimestamp !== 'UTF8' && receivedTimestamp !== 'MISSING') {
                violations.push(`received_timestamp should be UTF8, got ${receivedTimestamp}`);
                hasViolations = true;
              }
              if (signalkTimestamp !== 'UTF8' && signalkTimestamp !== 'MISSING') {
                violations.push(`signalk_timestamp should be UTF8, got ${signalkTimestamp}`);
                hasViolations = true;
              }

              // Rule 2: Check value fields using TWO-STEP PROCESS (matches repair logic)
              // Determine if this is an exploded file
              const isExplodedFile = Object.keys(valueFields).some(fieldName =>
                fieldName.startsWith('value_') && fieldName !== 'value' && fieldName !== 'value_json'
              );
              addDebug(`🔍 Validation: isExplodedFile = ${isExplodedFile}`);

              // Read sample data for content analysis (STEP 1)
              let sampleRecords = [];
              try {
                if (!parquet) {
                  throw new Error('ParquetJS not available');
                }
                const reader = await parquet.ParquetReader.openFile(filePath);
                const cursor = reader.getCursor();
                let record: any;
                let count = 0;
                while ((record = await cursor.next()) && count < 100) {
                  sampleRecords.push(record);
                  count++;
                }
                await reader.close();
              } catch (error) {
                addDebug(`⚠️ Could not read sample data for validation: ${(error as Error).message}`);
                sampleRecords = [];
              }

              for (const [fieldName, fieldType] of Object.entries(valueFields)) {
                // Always skip value_json (matches repair logic)
                if (fieldName === 'value_json') {
                  addDebug(`⏭️ ${fieldName}: Skipped entirely (always ignored)`);
                  continue;
                }

                // Skip value field in exploded files (matches repair logic)
                if (isExplodedFile && fieldName === 'value') {
                  addDebug(`⏭️ ${fieldName}: Skipped in exploded file (always empty)`);
                  continue;
                }

                if (fieldType === 'UTF8' || fieldType === 'VARCHAR') {
                  let shouldBeNumeric = false;

                  // STEP 1: LOOK AT THE STRING AND SEE WHAT IT IS (matches repair logic)
                  if (sampleRecords.length > 0) {
                    const values = sampleRecords
                      .map(r => r[fieldName])
                      .filter(v => v !== null && v !== undefined);

                    if (values.length > 0) {
                      let allNumeric = true;
                      let allBoolean = true;

                      for (const value of values) {
                        const str = String(value).trim();
                        if (str === 'true' || str === 'false') {
                          allNumeric = false;
                        } else if (!isNaN(Number(str)) && str !== '') {
                          allBoolean = false;
                        } else {
                          allNumeric = false;
                          allBoolean = false;
                          break;
                        }
                      }

                      if (allNumeric && values.length > 0) {
                        shouldBeNumeric = true;
                        violations.push(`${fieldName} contains numbers but is ${fieldType}, should be DOUBLE`);
                        hasViolations = true;
                        addDebug(`🔍 ${fieldName}: VARCHAR contains numbers, flagged as violation`);
                      } else if (allBoolean && values.length > 0) {
                        violations.push(`${fieldName} contains booleans but is ${fieldType}, should be BOOLEAN`);
                        hasViolations = true;
                        addDebug(`🔍 ${fieldName}: VARCHAR contains booleans, flagged as violation`);
                      }
                    }
                  }

                  // STEP 2: LOOK AT METADATA (SKIP IF EXPLODED) - only if step 1 can't determine (matches repair logic)
                  if (!shouldBeNumeric && sampleRecords.length === 0) {
                    const isExplodedField = fieldName.startsWith('value_');

                    if (!isExplodedField && signalkPath) {
                      addDebug(`🔍 ${fieldName}: Using metadata fallback (matches repair logic)`);
                      try {
                        const response = await fetch(`http://localhost:3000/signalk/v1/api/vessels/self/${signalkPath.replace(/\./g, '/')}/meta`);
                        if (response.ok) {
                          const metadata = await response.json() as any;
                          if (metadata && metadata.units &&
                              (metadata.units === 'm' || metadata.units === 'deg' || metadata.units === 'm/s' ||
                               metadata.units === 'rad' || metadata.units === 'K' || metadata.units === 'Pa' ||
                               metadata.units === 'V' || metadata.units === 'A' || metadata.units === 'Hz' ||
                               metadata.units === 'ratio' || metadata.units === 'kg' || metadata.units === 'J')) {
                            violations.push(`${fieldName} has numeric units (${metadata.units}) but is ${fieldType}, should be DOUBLE`);
                            hasViolations = true;
                            addDebug(`🔍 ${fieldName}: Metadata indicates numeric (${metadata.units}), flagged as violation`);
                          }
                        }
                      } catch (metadataError) {
                        addDebug(`🔍 ${fieldName}: Metadata lookup failed, no violation flagged`);
                      }
                    } else {
                      addDebug(`🔍 ${fieldName}: Exploded field or no path, skipping metadata (matches repair logic)`);
                    }
                  }
                }
              }

              if (hasViolations) {
                violationSchemas++;
                const shortPath = path.relative(dataDir, filePath);
                violationDetails.push(`[${totalFiles}] ${shortPath}: ${violations.join(', ')}`);
              } else {
                correctSchemas++;
              }

              if (typeof reader.close === 'function') reader.close();
            }

          } catch (error) {
            app.debug(`Error processing ${filePath}: ${(error as Error).message}`);
            // Count as violation since we couldn't read the schema
            violationSchemas++;
            const shortPath = path.relative(dataDir, filePath);
            violationDetails.push(`[${totalFiles}] ${shortPath}: ERROR - ${(error as Error).message}`);
          }
        }

        addDebug(`📊 Validation completed: ${totalFiles} files, ${vessels.size} vessels, ${correctSchemas} correct, ${violationSchemas} violations`);

        res.json({
          success: true,
          totalFiles,
          totalVessels: vessels.size,
          correctSchemas,
          violations: violationSchemas,
          violationDetails,
          debugMessages
        });

      } catch (error) {
        app.error(`Error during schema validation: ${error}`);
        res.status(500).json({
          success: false,
          error: (error as Error).message
        });
      }
    }
  );

  // Repair schema violations endpoint
  router.post(
    '/api/repair-schemas',
    async (_: TypedRequest, res: any) => {
      try {
        app.debug('🔧 Starting schema repair...');

        // Import required modules
        const parquet = require('@dsnp/parquetjs');
        const glob = require('glob');
        const path = require('path');
        // fs-extra already imported at top of file

        // Get the data directory
        const configOutputDir = state.currentConfig?.outputDirectory || 'data';
        const pluginDataPath = app.getDataDirPath();
        const signalkDataDir = path.dirname(path.dirname(pluginDataPath));
        const dataDir = path.join(signalkDataDir, configOutputDir);

        let repairedFiles = 0;
        let backedUpFiles = 0;

        app.debug(`🔧 Starting repair process in ${dataDir}`);

        // Find all parquet files using the configured filename prefix
        const filenamePrefix = state.currentConfig?.filenamePrefix || 'signalk_data';
        const parquetFiles = glob.sync(`${dataDir}/vessels/**/${filenamePrefix}_*.parquet`, {
          ignore: [
            `${dataDir}/vessels/**/processed/**`,
            `${dataDir}/vessels/**/repaired/**`,
            `${dataDir}/vessels/**/quarantine/**`,
            `${dataDir}/vessels/**/claude-schemas/**`,
            `${dataDir}/vessels/**/failed/**`
          ]
        });

        app.debug(`🔧 Found ${parquetFiles.length} parquet files to check`);

        for (let i = 0; i < parquetFiles.length; i++) {
          const filePath = parquetFiles[i];
          app.debug(`🔧 Processing file ${i + 1}/${parquetFiles.length}: ${path.basename(filePath)}`);

          // Check file size and move corrupted files to quarantine
          try {
            const stats = await fs.stat(filePath);
            if (stats.size < 100) {
              app.debug(`❌ File too small (${stats.size} bytes), moving to quarantine: ${path.basename(filePath)}`);

              // Move to quarantine
              const quarantineDir = path.join(path.dirname(filePath), 'quarantine');
              await fs.ensureDir(quarantineDir);
              const quarantineFile = path.join(quarantineDir, path.basename(filePath));
              await fs.move(filePath, quarantineFile);

              // Log quarantine entry
              const logFile = path.join(quarantineDir, 'quarantine.log');
              const logEntry = `${new Date().toISOString()} | repair | ${stats.size} bytes | File too small (corrupted) | ${filePath}\n`;
              await fs.appendFile(logFile, logEntry);

              app.debug(`📋 Quarantined: ${stats.size.toString().padStart(12, ' ')}  ${filePath}`);
              continue;
            }
          } catch (statError) {
            app.debug(`❌ Error checking file size: ${filePath} - ${(statError as Error).message}`);
            continue;
          }

          try {
            // Read parquet file schema using DuckDB
            const { spawn } = require('child_process');
            const duckdbResult = await new Promise<string>((resolve, reject) => {
              const duckdb = spawn('duckdb', ['-c', `DESCRIBE SELECT * FROM '${filePath}';`]);
              let output = '';
              let error = '';

              duckdb.stdout.on('data', (data: Buffer) => {
                output += data.toString();
              });

              duckdb.stderr.on('data', (data: Buffer) => {
                error += data.toString();
              });

              duckdb.on('close', (code: number) => {
                if (code === 0) {
                  resolve(output);
                } else {
                  reject(new Error(`DuckDB error: ${error}`));
                }
              });
            });

            // Parse schema from DuckDB output
            const lines = duckdbResult.trim().split('\n');
            const valueFields: { [key: string]: string } = {};
            let signalkPath = '';

            for (const line of lines) {
              const match = line.match(/│\s*(\w+)\s*│\s*(\w+)/);
              if (match) {
                const [, fieldName, fieldType] = match;
                if (fieldName === 'value' || fieldName.startsWith('value_')) {
                  valueFields[fieldName] = fieldType;
                }
                if (fieldName === 'path') {
                  // Extract SignalK path from file path using the configured prefix
                  const pathRegex = new RegExp(`/vessels/[^/]+/(.+)/${filenamePrefix}_`);
                  const pathMatch = filePath.match(pathRegex);
                  if (pathMatch) {
                    signalkPath = pathMatch[1].replace(/\//g, '.'); // Convert directory separators to dots
                    app.debug(`🔍 Extracted SignalK path: ${signalkPath} from ${path.basename(filePath)}`);
                  } else {
                    app.debug(`🔍 Could not extract SignalK path from ${path.basename(filePath)} using prefix ${filenamePrefix}`);
                  }
                }
              }
            }

            // Check if file needs repair
            let needsRepair = false;

            /*
            ┌─────────────────────────────────────────────────────────────────────────────────────────┐
            │                                 REPAIR LOGIC PATHWAYS                                   │
            ├─────────────────────────────────────────────────────────────────────────────────────────┤
            │ INITIAL SETUP:                                                                          │
            │ - Sample 100 records from parquet file                                                 │
            │ - Extract schema to get valueFields (field names and types)                            │
            │ - Detect exploded file: hasExplodedFields = Object.keys(valueFields).some(             │
            │   fieldName => fieldName.startsWith('value_') && fieldName !== 'value' &&             │
            │   fieldName !== 'value_json')                                                          │
            │ - Set isExplodedFile = hasExplodedFields                                               │
            │                                                                                         │
            │ MAIN LOOP - for each field in valueFields:                                             │
            │                                                                                         │
            │ Path 1: Always skip value_json                                                         │
            │ - IF fieldName === 'value_json' → continue (skip entirely)                            │
            │                                                                                         │
            │ Path 2: Skip value field in exploded files                                             │
            │ - IF isExplodedFile AND fieldName === 'value' → continue (skip entirely)              │
            │                                                                                         │
            │ Path 3: Process VARCHAR/UTF8 fields                                                    │
            │ - IF fieldType === 'UTF8' OR fieldType === 'VARCHAR'                                  │
            │ - Extract sample values, initialize typeDetected = false                               │
            │   Path 3a: STEP 1 - Content analysis with data                                         │
            │   - IF fieldValues.length > 0:                                                         │
            │     - Analyze each value: numeric? boolean? string?                                     │
            │     - IF all numeric → needsRepair = true, typeDetected = true, BREAK main loop       │
            │     - IF all boolean → needsRepair = true, typeDetected = true, BREAK main loop       │
            │     - IF mixed/strings → typeDetected = true (no repair)                               │
            │   Path 3b: STEP 2 - Metadata fallback                                                  │
            │   - IF !typeDetected:                                                                   │
            │     - Set isExplodedField = fieldName.startsWith('value_')                             │
            │     - IF !isExplodedField AND signalkPath exists:                                      │
            │       - Fetch SignalK metadata from HTTP API                                           │
            │       - IF response ok AND metadata has numeric units → needsRepair = true, BREAK     │
            │       - IF metadata request fails → log error, continue                                │
            │                                                                                         │
            │ Path 4: Process BIGINT fields                                                          │
            │ - IF fieldType === 'BIGINT' → needsRepair = true, BREAK main loop                     │
            │                                                                                         │
            │ Path 5: Skip other field types                                                         │
            │ - IF field type is neither VARCHAR/UTF8 nor BIGINT → continue to next field           │
            │                                                                                         │
            │ FINAL PATHS:                                                                            │
            │ Path 6: IF needsRepair === true → backup original and create repaired version          │
            │ Path 7: IF needsRepair === false → skip this file entirely                             │
            └─────────────────────────────────────────────────────────────────────────────────────────┘
            */

            // STEP 1: LOOK AT THE STRING AND SEE WHAT IT IS
            // Read sample data once for all fields
            let sampleRecords = [];
            const reader = await parquet.ParquetReader.openFile(filePath);
            const cursor = reader.getCursor();
            let record = null;
            let sampleCount = 0;

            // Sample first 100 records for content analysis
            while ((record = await cursor.next()) && sampleCount < 100) {
              sampleRecords.push(record);
              sampleCount++;
            }
            await reader.close();

            // Determine if this is an exploded file
            const hasExplodedFields = Object.keys(valueFields).some(fieldName => fieldName.startsWith('value_') && fieldName !== 'value' && fieldName !== 'value_json');
            const isExplodedFile = hasExplodedFields;

            for (const [fieldName, fieldType] of Object.entries(valueFields)) {
              // Always skip value_json
              if (fieldName === 'value_json') {
                continue;
              }

              // Skip value field in exploded files
              if (isExplodedFile && fieldName === 'value') {
                continue;
              }

              if (fieldType === 'UTF8' || fieldType === 'VARCHAR') {
                // Analyze content of this field
                const fieldValues = sampleRecords.map(r => r[fieldName]).filter(v => v !== null && v !== undefined);

                let typeDetected = false;

                if (fieldValues.length > 0) {
                  let allNumeric = true;
                  let allBoolean = true;

                  for (const value of fieldValues) {
                    const str = String(value).trim();
                    if (str === 'true' || str === 'false') {
                      allNumeric = false;
                    } else if (!isNaN(Number(str)) && str !== '') {
                      allBoolean = false;
                    } else {
                      allNumeric = false;
                      allBoolean = false;
                      break;
                    }
                  }

                  if (allNumeric && fieldValues.length > 0) {
                    needsRepair = true;
                    typeDetected = true;
                    app.debug(`🔧 File needs repair: ${path.basename(filePath)} (${fieldName} contains numbers, should be DOUBLE, not ${fieldType})`);
                    break;
                  } else if (allBoolean && fieldValues.length > 0) {
                    needsRepair = true;
                    typeDetected = true;
                    app.debug(`🔧 File needs repair: ${path.basename(filePath)} (${fieldName} contains booleans, should be BOOLEAN, not ${fieldType})`);
                    break;
                  } else if (fieldValues.length > 0) {
                    // Contains strings - mark as OK
                    typeDetected = true;
                  }
                }

                // STEP 2: LOOK AT METADATA (SKIP IF EXPLODED) - only if step 1 can't determine
                if (!typeDetected) {
                  const isExplodedField = fieldName.startsWith('value_');

                  if (!isExplodedField && signalkPath) {
                    try {
                      const response = await fetch(`http://localhost:3000/signalk/v1/api/vessels/self/${signalkPath.replace(/\./g, '/')}/meta`);
                      if (response.ok) {
                        const metadata = await response.json() as any;
                        if (metadata && metadata.units &&
                            (metadata.units === 'm' || metadata.units === 'deg' || metadata.units === 'm/s' ||
                             metadata.units === 'rad' || metadata.units === 'K' || metadata.units === 'Pa' ||
                             metadata.units === 'V' || metadata.units === 'A' || metadata.units === 'Hz' ||
                             metadata.units === 'ratio' || metadata.units === 'kg' || metadata.units === 'J')) {
                          needsRepair = true;
                          app.debug(`🔧 File needs repair: ${path.basename(filePath)} (${fieldName} should be DOUBLE per metadata, not ${fieldType})`);
                          break;
                        }
                      }
                    } catch (metadataError) {
                      app.debug(`🔧 Metadata check failed for ${fieldName}: ${(metadataError as Error).message}`);
                    }
                  }
                }
              } else if (fieldType === 'BIGINT') {
                // BIGINTS ARE FORBIDDEN - automatically repair
                needsRepair = true;
                app.debug(`🔧 File needs repair: ${path.basename(filePath)} (${fieldName} is BIGINT, converting to DOUBLE)`);
                break;
              }
            }

            if (needsRepair) {
              // Create backup directory
              const backupDir = path.join(path.dirname(filePath), 'repaired');
              await fs.mkdir(backupDir, { recursive: true });

              // Backup original file
              const backupPath = path.join(backupDir, path.basename(filePath));
              await fs.copyFile(filePath, backupPath);
              backedUpFiles++;
              app.debug(`🔧 Backed up: ${path.basename(filePath)}`);

              // Read original data
              const reader = await parquet.ParquetReader.openFile(filePath);
              const cursor = reader.getCursor();
              const records: any[] = [];
              let record = null;
              while (record = await cursor.next()) {
                records.push(record);
              }
              await reader.close();

              // Create writer instance with correct schema detection
              const { ParquetWriter } = require('./parquet-writer');
              const writer = new ParquetWriter({ format: 'parquet', app });

              // Use the corrected schema detection with HTTP metadata
              const correctedSchema = await writer.createParquetSchema(records, signalkPath);

              // Write corrected file
              const parquetWriter = await parquet.ParquetWriter.openFile(correctedSchema, filePath);
              for (const record of records) {
                const preparedRecord = writer.prepareRecordForParquet(record, correctedSchema);
                await parquetWriter.appendRow(preparedRecord);
              }
              await parquetWriter.close();

              repairedFiles++;
              app.debug(`🔧 ✅ Repaired: ${path.basename(filePath)}`);
            }

          } catch (fileError) {
            app.debug(`🔧 ❌ Error processing ${path.basename(filePath)}: ${(fileError as Error).message}`);
          }
        }

        const message = `Repaired ${repairedFiles} files, backed up ${backedUpFiles} originals to 'repaired' folders`;
        app.debug(`🔧 Repair completed: ${message}`);

        // Send completion response
        res.json({
          success: true,
          repairedFiles,
          backedUpFiles,
          message
        });

      } catch (error) {
        app.debug(`❌ Schema repair failed: ${(error as Error).message}`);
        res.status(500).json({
          success: false,
          message: `Repair failed: ${(error as Error).message}`
        });
      }
    }
  );

}