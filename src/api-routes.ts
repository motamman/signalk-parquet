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
} from './types';
import {
  loadWebAppConfig,
  saveWebAppConfig,
  registerCommand,
  unregisterCommand,
  executeCommand,
  getCurrentCommands,
  getCommandHistory,
  getCommandState,
} from './commands';
import { updateDataSubscriptions } from './data-handler';
import { toContextFilePath, toParquetFilePath } from './utils/path-helpers';
import { ServerAPI, Context } from '@signalk/server-api';
import { initializeStreamingService, shutdownStreamingService } from './index';

// AWS S3 for testing connection
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let ListObjectsV2Command: any;

export function registerApiRoutes(
  router: Router,
  state: PluginState,
  app: ServerAPI
): void {
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
        const { command, description } = req.body;

        if (!command || !/^[a-zA-Z0-9_]+$/.test(command) || command.length === 0 || command.length > 50) {
          return res.status(400).json({
            success: false,
            error:
              'Invalid command name. Must be alphanumeric with underscores, 1-50 characters.',
          });
        }

        const result = registerCommand(command, description);

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

  // Streaming Control API endpoints

  // Enable streaming at runtime
  router.post('/api/streaming/enable', async (req: TypedRequest, res: TypedResponse) => {
    try {
      if (state.streamingService) {
        return res.json({
          success: true,
          message: 'Streaming service is already running',
          enabled: true
        });
      }

      // Check if streaming is enabled in config
      if (!state.currentConfig?.enableStreaming) {
        return res.status(400).json({
          success: false,
          error: 'Streaming is disabled in plugin configuration. Enable it in plugin settings first.',
          enabled: false
        });
      }

      const result = await initializeStreamingService(state, app);
      
      if (result.success) {
        return res.json({
          success: true,
          message: 'Streaming service enabled successfully',
          enabled: true
        });
      } else {
        return res.status(500).json({
          success: false,
          error: result.error || 'Failed to enable streaming service',
          enabled: false
        });
      }
    } catch (error) {
      app.error(`Error enabling streaming: ${error}`);
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
        enabled: false
      });
    }
  });

  // Disable streaming at runtime  
  router.post('/api/streaming/disable', (req: TypedRequest, res: TypedResponse) => {
    try {
      if (!state.streamingService) {
        return res.json({
          success: true,
          message: 'Streaming service is not running',
          enabled: false
        });
      }

      const result = shutdownStreamingService(state, app);
      
      if (result.success) {
        return res.json({
          success: true,
          message: 'Streaming service disabled successfully',
          enabled: false
        });
      } else {
        return res.status(500).json({
          success: false,
          error: result.error || 'Failed to disable streaming service',
          enabled: true
        });
      }
    } catch (error) {
      app.error(`Error disabling streaming: ${error}`);
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
        enabled: true
      });
    }
  });

  // Get current streaming status
  router.get('/api/streaming/status', (req: TypedRequest, res: TypedResponse) => {
    try {
      const isEnabled = !!state.streamingService;
      const configEnabled = state.currentConfig?.enableStreaming ?? false;
      
      // Get streaming service statistics if available
      let stats = {};
      if (state.streamingService && state.streamingService.getActiveSubscriptions) {
        const subscriptions = state.streamingService.getActiveSubscriptions();
        stats = {
          activeSubscriptions: subscriptions.length,
          subscriptions: subscriptions
        };
      }

      res.json({
        success: true,
        enabled: isEnabled,
        configEnabled: configEnabled,
        canEnable: configEnabled && !isEnabled,
        canDisable: isEnabled,
        ...stats
      });
    } catch (error) {
      app.error(`Error getting streaming status: ${error}`);
      res.status(500).json({
        success: false,
        error: (error as Error).message
      });
    }
  });

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

  // Test endpoint
  router.get('/api/test', (_: express.Request, res: express.Response) => {
    res.json({
      message: 'SignalK Parquet Plugin API is working',
      timestamp: new Date().toISOString(),
      config: state.currentConfig ? 'loaded' : 'not loaded',
    });
  });

  // Historical streaming test endpoints
  router.post('/api/historical/trigger/:path', (req: express.Request, res: express.Response) => {
    try {
      const path = req.params.path;
      if (state.historicalStreamingService) {
        state.historicalStreamingService.triggerHistoricalStream(path);
        res.json({
          success: true,
          message: `Triggered historical stream for path: ${path}`,
          timestamp: new Date().toISOString()
        });
      } else {
        res.status(500).json({
          success: false,
          error: 'Historical streaming service not initialized'
        });
      }
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  });

  router.get('/api/historical/subscriptions', (_: express.Request, res: express.Response) => {
    try {
      if (state.historicalStreamingService) {
        const subscriptions = state.historicalStreamingService.getActiveSubscriptions();
        res.json({
          success: true,
          subscriptions,
          count: subscriptions.length
        });
      } else {
        res.status(500).json({
          success: false,
          error: 'Historical streaming service not initialized'
        });
      }
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  });

  // Stream Management API endpoints
  
  // Get all streams
  router.get('/api/streams', (_: express.Request, res: express.Response) => {
    try {
      if (state.historicalStreamingService) {
        const streams = state.historicalStreamingService.getAllStreams();
        res.json({
          success: true,
          streams
        });
      } else {
        res.status(500).json({
          success: false,
          error: 'Historical streaming service not initialized'
        });
      }
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  });

  // Create new stream
  router.post('/api/streams', (req: express.Request, res: express.Response) => {
    try {
      if (!state.historicalStreamingService) {
        res.status(500).json({
          success: false,
          error: 'Historical streaming service not initialized'
        });
        return;
      }

      const streamConfig = req.body;
      
      // Validate required fields
      if (!streamConfig.name || !streamConfig.path) {
        res.status(400).json({
          success: false,
          error: 'Stream name and path are required'
        });
        return;
      }

      // Validate aggregation method if provided
      const validAggregationMethods = ['average', 'min', 'max', 'first', 'last', 'mid', 'middle_index'];
      if (streamConfig.aggregateMethod && !validAggregationMethods.includes(streamConfig.aggregateMethod)) {
        res.status(400).json({
          success: false,
          error: `Invalid aggregation method. Valid options: ${validAggregationMethods.join(', ')}`
        });
        return;
      }

      // Validate window size if provided
      if (streamConfig.windowSize && (typeof streamConfig.windowSize !== 'number' || streamConfig.windowSize < 1 || streamConfig.windowSize > 1000)) {
        res.status(400).json({
          success: false,
          error: 'Window size must be a number between 1 and 1000'
        });
        return;
      }

      const stream = state.historicalStreamingService.createStream(streamConfig);
      
      // Handle auto-start if requested
      if (streamConfig.autoStart) {
        const startResult = state.historicalStreamingService.startStream(stream.id);
        if (startResult.success) {
          stream.status = 'running';
        }
      }
      
      res.json({
        success: true,
        stream,
        message: `Stream '${streamConfig.name}' created successfully${streamConfig.autoStart ? ' and started' : ''}`
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  });

  // Update stream configuration
  router.put('/api/streams/:id', (req: express.Request, res: express.Response) => {
    try {
      if (!state.historicalStreamingService) {
        res.status(500).json({
          success: false,
          error: 'Historical streaming service not initialized'
        });
        return;
      }

      const streamId = req.params.id;
      const streamConfig = req.body;

      // Validate required fields
      if (!streamConfig.name || !streamConfig.path) {
        res.status(400).json({
          success: false,
          error: 'Stream name and path are required'
        });
        return;
      }

      // Validate window size if provided
      if (streamConfig.windowSize && (typeof streamConfig.windowSize !== 'number' || streamConfig.windowSize < 1 || streamConfig.windowSize > 1000)) {
        res.status(400).json({
          success: false,
          error: 'Window size must be a number between 1 and 1000'
        });
        return;
      }

      const success = state.historicalStreamingService.updateStream(streamId, streamConfig);
      if (success) {
        res.json({
          success: true,
          message: `Stream '${streamConfig.name}' updated successfully`
        });
      } else {
        res.status(404).json({
          success: false,
          error: 'Stream not found'
        });
      }
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  });

  // Start stream
  router.put('/api/streams/:id/start', (req: express.Request, res: express.Response) => {
    try {
      if (!state.historicalStreamingService) {
        res.status(500).json({
          success: false,
          error: 'Historical streaming service not initialized'
        });
        return;
      }

      const streamId = req.params.id;
      const result = state.historicalStreamingService.startStream(streamId);
      
      if (result.success) {
        res.json({
          success: true,
          message: `Stream started successfully`
        });
      } else {
        res.status(404).json({
          success: false,
          error: result.error || 'Stream not found'
        });
      }
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  });

  // Pause stream
  router.put('/api/streams/:id/pause', (req: express.Request, res: express.Response) => {
    try {
      if (!state.historicalStreamingService) {
        res.status(500).json({
          success: false,
          error: 'Historical streaming service not initialized'
        });
        return;
      }

      const streamId = req.params.id;
      const result = state.historicalStreamingService.pauseStream(streamId);
      
      if (result.success) {
        res.json({
          success: true,
          message: `Stream ${result.paused ? 'paused' : 'resumed'} successfully`
        });
      } else {
        res.status(404).json({
          success: false,
          error: result.error || 'Stream not found'
        });
      }
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  });

  // Stop stream
  router.put('/api/streams/:id/stop', (req: express.Request, res: express.Response) => {
    try {
      if (!state.historicalStreamingService) {
        res.status(500).json({
          success: false,
          error: 'Historical streaming service not initialized'
        });
        return;
      }

      const streamId = req.params.id;
      const result = state.historicalStreamingService.stopStream(streamId);
      
      if (result.success) {
        res.json({
          success: true,
          message: `Stream stopped successfully`
        });
      } else {
        res.status(404).json({
          success: false,
          error: result.error || 'Stream not found'
        });
      }
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  });

  // Delete stream
  router.delete('/api/streams/:id', (req: express.Request, res: express.Response) => {
    try {
      if (!state.historicalStreamingService) {
        res.status(500).json({
          success: false,
          error: 'Historical streaming service not initialized'
        });
        return;
      }

      const streamId = req.params.id;
      const result = state.historicalStreamingService.deleteStream(streamId);
      
      if (result.success) {
        res.json({
          success: true,
          message: `Stream deleted successfully`
        });
      } else {
        res.status(404).json({
          success: false,
          error: result.error || 'Stream not found'
        });
      }
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  });

  // Get stream statistics
  router.get('/api/streams/stats', (_: express.Request, res: express.Response) => {
    try {
      if (state.historicalStreamingService) {
        const stats = state.historicalStreamingService.getStreamStats();
        res.json({
          success: true,
          stats
        });
      } else {
        res.status(500).json({
          success: false,
          error: 'Historical streaming service not initialized'
        });
      }
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  });

  // Get recent time-series data for a specific stream
  router.get('/api/streams/:id/data', async (req: express.Request, res: express.Response) => {
    try {
      if (!state.historicalStreamingService) {
        res.status(500).json({
          success: false,
          error: 'Historical streaming service not initialized'
        });
        return;
      }

      const streamId = req.params.id;
      const limit = parseInt(req.query.limit as string) || 50;
      
      const timeSeriesData = await (state.historicalStreamingService as any).getStreamTimeSeriesData(streamId, limit);
      
      if (timeSeriesData) {
        res.json({
          success: true,
          streamId: streamId,
          data: timeSeriesData
        });
      } else {
        res.status(404).json({
          success: false,
          error: 'Stream not found or no data available'
        });
      }
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  });

  app.debug('Webapp API routes registered');
}