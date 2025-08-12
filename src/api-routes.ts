import * as fs from 'fs-extra';
import * as path from 'path';
import express, { Router } from 'express';
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
        const webAppConfig = loadWebAppConfig();
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
        const webAppConfig = loadWebAppConfig();
        const currentPaths = webAppConfig.paths;
        const currentCommands = webAppConfig.commands;

        // Add to current paths
        currentPaths.push(newPath);

        // Save to web app configuration
        saveWebAppConfig(currentPaths, currentCommands);

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
        const webAppConfig = loadWebAppConfig();
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
        saveWebAppConfig(currentPaths, currentCommands);

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
        const webAppConfig = loadWebAppConfig();
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
        saveWebAppConfig(currentPaths, currentCommands);

        // Update subscriptions
        if (state.currentConfig) {
          updateDataSubscriptions(currentPaths, state, state.currentConfig, app);
        }

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
          const webAppConfig = loadWebAppConfig();
          const currentCommands = getCurrentCommands();
          saveWebAppConfig(webAppConfig.paths, currentCommands);

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
          const webAppConfig = loadWebAppConfig();
          const currentCommands = getCurrentCommands();
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

  app.debug('Webapp API routes registered');
}