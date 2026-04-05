import * as fs from 'fs-extra';
import * as path from 'path';
import { glob } from 'glob';
import express, { Router } from 'express';
import { getAvailablePaths } from './utils/path-discovery';
import { DuckDBInstance } from '@duckdb/node-api';
import { DuckDBPool } from './utils/duckdb-pool';
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
  ValidationViolation,
} from './types';
import { SchemaService } from './schema-service';
import {
  ProcessType,
  ProcessState,
  ProcessStatusApiResponse,
  ProcessCancelApiResponse,
} from './types';
import { MigrationService } from './services/migration-service';
import { AggregationService } from './services/aggregation-service';
import { AggregationTier, HivePathBuilder } from './utils/hive-path-builder';
import { isAngularPath } from './utils/angular-paths';
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
  setManualOverride,
  updatePluginConfig,
} from './commands';
import { updateDataSubscriptions } from './data-handler';
import { toContextFilePath, toParquetFilePath } from './utils/path-helpers';
import { ServerAPI, Context } from '@signalk/server-api';
import {
  ClaudeAnalyzer,
  AnalysisRequest,
  FollowUpRequest,
} from './claude-analyzer';
import {
  AnalysisTemplateManager,
  TEMPLATE_CATEGORIES,
} from './analysis-templates';
import { VesselContextManager } from './vessel-context';
// import { initializeStreamingService, shutdownStreamingService } from './index';

// Progress tracking for validation jobs
interface ValidationProgress {
  jobId: string;
  status: 'running' | 'cancelling' | 'completed' | 'cancelled' | 'error';
  processed: number;
  total: number;
  percent: number;
  startTime: Date;
  currentFile?: string;
  currentVessel?: string;
  currentRelativePath?: string;
  cancelRequested?: boolean;
  error?: string;
  completedAt?: Date;
  result?: ValidationApiResponse;
}

const validationJobs = new Map<string, ValidationProgress>();

let lastValidationViolations: ValidationViolation[] = [];

interface RepairProgress {
  jobId: string;
  status: 'running' | 'cancelling' | 'completed' | 'cancelled' | 'error';
  processed: number;
  total: number;
  percent: number;
  startTime: Date;
  currentFile?: string;
  message?: string;
  cancelRequested?: boolean;
  completedAt?: Date;
  result?: {
    success: boolean;
    repairedFiles: number;
    backedUpFiles: number;
    skippedFiles: string[];
    quarantinedFiles: string[];
    errors: string[];
    message?: string;
  };
}

const repairJobs = new Map<string, RepairProgress>();

const VALIDATION_JOB_TTL_MS = 10 * 60 * 1000; // Retain job metadata for 10 minutes

function scheduleValidationJobCleanup(jobId: string) {
  setTimeout(() => {
    const job = validationJobs.get(jobId);
    if (job && job.status !== 'running') {
      validationJobs.delete(jobId);
    }
  }, VALIDATION_JOB_TTL_MS);
}

const REPAIR_JOB_TTL_MS = 10 * 60 * 1000;

function scheduleRepairJobCleanup(jobId: string) {
  setTimeout(() => {
    const job = repairJobs.get(jobId);
    if (job && job.status !== 'running') {
      repairJobs.delete(jobId);
    }
  }, REPAIR_JOB_TTL_MS);
}

// Shared analyzer instance to maintain conversation state across requests
let sharedAnalyzer: ClaudeAnalyzer | null = null;

/**
 * Get or create the shared Claude analyzer instance
 */
function getSharedAnalyzer(
  config: any,
  app: ServerAPI,
  dataDir: string,
  state: PluginState
): ClaudeAnalyzer {
  if (!sharedAnalyzer) {
    sharedAnalyzer = new ClaudeAnalyzer(
      {
        apiKey: config.claudeIntegration.apiKey,
        model: migrateClaudeModel(config.claudeIntegration.model, app) as any,
        maxTokens: config.claudeIntegration.maxTokens || 4000,
        temperature: config.claudeIntegration.temperature || 0.3,
      },
      app,
      dataDir,
      state
    );
    app.debug('🔧 Created shared Claude analyzer instance');
  }
  return sharedAnalyzer;
}

// AWS S3 for testing connection
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let ListObjectsV2Command: any;

import { getValidClaudeModel } from './claude-models';

// Helper function to migrate deprecated Claude model names
function migrateClaudeModel(model?: string, app?: ServerAPI): string {
  const validatedModel = getValidClaudeModel(model);
  if (model && validatedModel !== model) {
    app?.debug(`Auto-migrated Claude model ${model} to ${validatedModel}`);
  }
  return validatedModel;
}

// ===========================================
// PROCESS MANAGEMENT UTILITIES
// ===========================================

function startProcess(
  state: PluginState,
  type: ProcessType,
  totalFiles?: number
): boolean {
  // Check if another process is already running
  if (state.currentProcess?.isRunning) {
    return false; // Cannot start, another process is active
  }

  // Initialize new process
  state.currentProcess = {
    type,
    isRunning: true,
    startTime: new Date(),
    totalFiles,
    processedFiles: 0,
    cancelRequested: false,
    abortController: new AbortController(),
  };

  return true;
}

function updateProcessProgress(
  state: PluginState,
  processedFiles: number,
  currentFile?: string
): void {
  if (state.currentProcess?.isRunning) {
    state.currentProcess.processedFiles = processedFiles;
    state.currentProcess.currentFile = currentFile;
  }
}

function finishProcess(state: PluginState): void {
  if (state.currentProcess) {
    state.currentProcess.isRunning = false;
    // Keep the process data for a short time for status queries
    setTimeout(() => {
      if (state.currentProcess && !state.currentProcess.isRunning) {
        state.currentProcess = undefined;
      }
    }, 30000); // Clear after 30 seconds
  }
}

function cancelProcess(state: PluginState): boolean {
  if (state.currentProcess?.isRunning) {
    state.currentProcess.cancelRequested = true;
    state.currentProcess.abortController?.abort();
    return true;
  }
  return false;
}

function getProcessStatus(state: PluginState): ProcessStatusApiResponse {
  if (!state.currentProcess) {
    return {
      success: true,
      isRunning: false,
    };
  }

  const process = state.currentProcess;
  const progress =
    process.totalFiles && process.processedFiles !== undefined
      ? Math.round((process.processedFiles / process.totalFiles) * 100)
      : undefined;

  return {
    success: true,
    isRunning: process.isRunning,
    processType: process.type,
    startTime: process.startTime.toISOString(),
    totalFiles: process.totalFiles,
    processedFiles: process.processedFiles,
    currentFile: process.currentFile,
    progress,
  };
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
        const dataDir = state.getDataDirPath();
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
        const dataDir = state.getDataDirPath();
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
        const dataDir = state.getDataDirPath();
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
        const query = `SELECT * FROM read_parquet('${sampleFile.path}', union_by_name=true) LIMIT ${limit}`;

        // Get connection from pool (spatial extension already loaded)
        const connection = await DuckDBPool.getConnection();

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

  // Query parquet data (raw SQL - disabled by default for security)
  // Enable with: SIGNALK_PARQUET_RAW_SQL=true
  router.post(
    '/api/query',
    async (
      req: TypedRequest<QueryRequest>,
      res: TypedResponse<QueryApiResponse>
    ) => {
      try {
        // Security: Raw SQL is disabled by default
        const rawSqlEnabled =
          process.env.SIGNALK_PARQUET_RAW_SQL === 'true' ||
          state.currentConfig?.enableRawSql === true;
        if (!rawSqlEnabled) {
          return res.status(403).json({
            success: false,
            error:
              'Raw SQL queries are disabled. Enable in plugin settings or set SIGNALK_PARQUET_RAW_SQL=true.',
          });
        }

        const { query } = req.body;

        if (!query) {
          return res.status(400).json({
            success: false,
            error: 'Query is required',
          });
        }

        const dataDir = state.getDataDirPath();

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
            } else if (quotedPath.includes('.') && !quotedPath.includes('/')) {
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

        // Get connection from pool (spatial extension already loaded)
        const connection = await DuckDBPool.getConnection();

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

  // Check if raw SQL queries are enabled (for UI visibility)
  router.get('/api/query/enabled', (_req, res) => {
    const rawSqlEnabled =
      process.env.SIGNALK_PARQUET_RAW_SQL === 'true' ||
      state.currentConfig?.enableRawSql === true;
    return res.json({
      success: true,
      enabled: rawSqlEnabled,
    });
  });

  // Test cloud connection (S3 or R2)
  router.post(
    '/api/test-cloud',
    async (_: TypedRequest, res: TypedResponse<S3TestApiResponse>) => {
      try {
        if (!state.currentConfig) {
          return res.status(500).json({
            success: false,
            error: 'Plugin not started or configuration not available',
          });
        }

        const cloud = state.currentConfig.cloudUpload;
        if (cloud.provider === 'none') {
          return res.status(400).json({
            success: false,
            error: 'Cloud upload is not enabled in configuration',
          });
        }

        if (!ListObjectsV2Command || !state.cloudClient) {
          try {
            const awsS3 = await import('@aws-sdk/client-s3');
            ListObjectsV2Command = awsS3.ListObjectsV2Command;
          } catch (importError) {
            return res.status(503).json({
              success: false,
              error: 'Cloud client not available or not initialized',
            });
          }
        }

        const listCommand = new ListObjectsV2Command({
          Bucket: cloud.bucket,
          MaxKeys: 1,
        });

        await state.cloudClient.send(listCommand);

        const label = cloud.provider.toUpperCase();
        return res.json({
          success: true,
          message: `${label} connection successful`,
          provider: cloud.provider,
          bucket: cloud.bucket,
          region:
            cloud.provider === 's3' ? cloud.region || 'us-east-1' : undefined,
          accountId: cloud.provider === 'r2' ? cloud.accountId : undefined,
          keyPrefix: cloud.keyPrefix || 'none',
        });
      } catch (error) {
        return res.status(500).json({
          success: false,
          error: (error as Error).message || 'Cloud connection failed',
        });
      }
    }
  );

  // Cloud compare job storage
  interface CloudCompareJob {
    id: string;
    status:
      | 'scanning_local'
      | 'scanning_cloud'
      | 'comparing'
      | 'completed'
      | 'error';
    phase: string;
    localFilesScanned: number;
    localFilesTotal: number;
    cloudObjectsScanned: number;
    progress: number;
    result?: {
      provider: string;
      summary: {
        localTotal: number;
        cloudTotal: number;
        synced: number;
        localOnly: number;
        cloudOnly: number;
        localOnlySizeMB: string;
      };
      localOnly: Array<{ key: string; size: number }>;
      cloudOnly: string[];
      hasMore: boolean;
    };
    error?: string;
  }

  const cloudCompareJobs = new Map<string, CloudCompareJob>();

  // Start cloud compare job
  router.post('/api/cloud/compare', async (_req, res) => {
    try {
      const cloud = state.currentConfig?.cloudUpload;
      if (!cloud || cloud.provider === 'none') {
        return res.status(400).json({
          success: false,
          error: 'Cloud upload is not enabled',
        });
      }

      if (!state.cloudClient || !ListObjectsV2Command) {
        try {
          const awsS3 = await import('@aws-sdk/client-s3');
          ListObjectsV2Command = awsS3.ListObjectsV2Command;
        } catch {
          return res.status(503).json({
            success: false,
            error: 'Cloud client not available',
          });
        }
      }

      const label = cloud.provider.toUpperCase();
      const jobId = `cloud-compare-${Date.now()}`;
      const job: CloudCompareJob = {
        id: jobId,
        status: 'scanning_local',
        phase: 'Scanning local files...',
        localFilesScanned: 0,
        localFilesTotal: 0,
        cloudObjectsScanned: 0,
        progress: 0,
      };

      cloudCompareJobs.set(jobId, job);

      (async () => {
        try {
          const config = state.currentConfig!;
          const dataDir = state.getDataDirPath();

          job.phase = 'Discovering local files...';
          // Only scan hive-partitioned files (tier=X/context=Y/path=Z/year=YYYY/day=DDD/)
          const excludedDirs = ['/processed/', '/repaired/', '/failed/', '/quarantine/'];
          const allLocalFiles = await glob(
            path.join(dataDir, 'tier=*', '**', '*.parquet')
          );
          const localFiles = allLocalFiles.filter(f => !excludedDirs.some(dir => f.includes(dir)));
          job.localFilesTotal = localFiles.length;

          const localKeys = new Map<string, { path: string; size: number }>();

          for (let i = 0; i < localFiles.length; i++) {
            const filePath = localFiles[i];
            const relativePath = path.relative(dataDir, filePath);
            let cloudKey = relativePath;
            if (cloud.keyPrefix) {
              const prefix = cloud.keyPrefix.endsWith('/')
                ? cloud.keyPrefix
                : `${cloud.keyPrefix}/`;
              cloudKey = `${prefix}${relativePath}`;
            }
            const stats = await fs.stat(filePath);
            localKeys.set(cloudKey, { path: filePath, size: stats.size });

            job.localFilesScanned = i + 1;
            job.progress = Math.round(((i + 1) / localFiles.length) * 40);
            job.phase = `Scanning local files: ${i + 1}/${localFiles.length}`;
          }

          job.status = 'scanning_cloud';
          job.phase = `Listing ${label} objects...`;
          const cloudKeys = new Set<string>();
          let continuationToken: string | undefined;
          let batches = 0;

          do {
            const listCommand = new ListObjectsV2Command({
              Bucket: cloud.bucket,
              Prefix: cloud.keyPrefix || undefined,
              ContinuationToken: continuationToken,
            });

            const response = await state.cloudClient.send(listCommand);
            batches++;

            if (response.Contents) {
              for (const obj of response.Contents) {
                if (obj.Key?.endsWith('.parquet')) {
                  cloudKeys.add(obj.Key);
                }
              }
            }

            job.cloudObjectsScanned = cloudKeys.size;
            job.progress = 40 + Math.min(batches * 5, 40);
            job.phase = `Listing ${label} objects: ${cloudKeys.size} found...`;

            continuationToken = response.IsTruncated
              ? response.NextContinuationToken
              : undefined;
          } while (continuationToken);

          job.status = 'comparing';
          job.phase = 'Comparing files...';
          job.progress = 85;

          const localOnly: Array<{ key: string; size: number }> = [];
          const cloudOnly: string[] = [];
          const synced: string[] = [];

          for (const [key, info] of localKeys) {
            if (cloudKeys.has(key)) {
              synced.push(key);
            } else {
              localOnly.push({ key, size: info.size });
            }
          }

          for (const key of cloudKeys) {
            if (!localKeys.has(key)) {
              cloudOnly.push(key);
            }
          }

          const totalLocalSize = localOnly.reduce((sum, f) => sum + f.size, 0);

          job.status = 'completed';
          job.phase = 'Complete';
          job.progress = 100;
          job.result = {
            provider: cloud.provider,
            summary: {
              localTotal: localKeys.size,
              cloudTotal: cloudKeys.size,
              synced: synced.length,
              localOnly: localOnly.length,
              cloudOnly: cloudOnly.length,
              localOnlySizeMB: (totalLocalSize / 1024 / 1024).toFixed(2),
            },
            localOnly: localOnly.slice(0, 100),
            cloudOnly: cloudOnly.slice(0, 100),
            hasMore: localOnly.length > 100 || cloudOnly.length > 100,
          };

          setTimeout(() => cloudCompareJobs.delete(jobId), 5 * 60 * 1000);
        } catch (error) {
          job.status = 'error';
          job.error = (error as Error).message;
        }
      })();

      return res.json({ success: true, jobId });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // Get cloud compare job status
  router.get('/api/cloud/compare/:jobId', async (req, res) => {
    const job = cloudCompareJobs.get(req.params.jobId);
    if (!job) {
      return res.status(404).json({ success: false, error: 'Job not found' });
    }
    return res.json({ success: true, ...job });
  });

  // Cloud sync job storage
  interface CloudSyncJob {
    id: string;
    status: 'preparing' | 'uploading' | 'completed' | 'error';
    phase: string;
    filesTotal: number;
    filesUploaded: number;
    filesFailed: number;
    currentFile: string;
    progress: number;
    errors: string[];
  }

  const cloudSyncJobs = new Map<string, CloudSyncJob>();

  // Start cloud sync job
  router.post('/api/cloud/sync', async (req, res) => {
    try {
      const cloud = state.currentConfig?.cloudUpload;
      if (!cloud || cloud.provider === 'none') {
        return res.status(400).json({
          success: false,
          error: 'Cloud upload is not enabled',
        });
      }

      if (!state.cloudClient) {
        return res.status(503).json({
          success: false,
          error: 'Cloud client not initialized',
        });
      }

      let PutObjectCommand: any;
      try {
        const awsS3 = await import('@aws-sdk/client-s3');
        PutObjectCommand = awsS3.PutObjectCommand;
        if (!ListObjectsV2Command) {
          ListObjectsV2Command = awsS3.ListObjectsV2Command;
        }
      } catch {
        return res.status(503).json({
          success: false,
          error: 'S3 SDK not available',
        });
      }

      const label = cloud.provider.toUpperCase();
      const jobId = `cloud-sync-${Date.now()}`;
      const job: CloudSyncJob = {
        id: jobId,
        status: 'preparing',
        phase: 'Finding files to sync...',
        filesTotal: 0,
        filesUploaded: 0,
        filesFailed: 0,
        currentFile: '',
        progress: 0,
        errors: [],
      };

      cloudSyncJobs.set(jobId, job);

      const config = state.currentConfig!;
      const dataDir = state.getDataDirPath();
      const { keys } = req.body as { keys?: string[] };

      (async () => {
        try {
          let filesToSync: Array<{ key: string; localPath: string }> = [];

          if (keys && keys.length > 0) {
            for (const key of keys) {
              const relativePath = cloud.keyPrefix
                ? key.replace(new RegExp(`^${cloud.keyPrefix}/?`), '')
                : key;
              const localPath = path.join(dataDir, relativePath);
              if (await fs.pathExists(localPath)) {
                filesToSync.push({ key, localPath });
              }
            }
          } else {
            job.phase = 'Scanning local files...';
            // Only sync hive-partitioned files (tier=X/context=Y/path=Z/year=YYYY/day=DDD/)
            const excludedDirs = ['/processed/', '/repaired/', '/failed/', '/quarantine/'];
            const allLocalFiles = await glob(
              path.join(dataDir, 'tier=*', '**', '*.parquet')
            );
            const localFiles = allLocalFiles.filter(f => !excludedDirs.some(dir => f.includes(dir)));

            job.phase = `Listing ${label} objects...`;
            job.progress = 10;
            const cloudKeys = new Set<string>();
            let continuationToken: string | undefined;

            do {
              const listCommand = new ListObjectsV2Command({
                Bucket: cloud.bucket,
                Prefix: cloud.keyPrefix || undefined,
                ContinuationToken: continuationToken,
              });

              const response = await state.cloudClient.send(listCommand);

              if (response.Contents) {
                for (const obj of response.Contents) {
                  if (obj.Key) cloudKeys.add(obj.Key);
                }
              }

              continuationToken = response.IsTruncated
                ? response.NextContinuationToken
                : undefined;
            } while (continuationToken);

            job.phase = 'Comparing files...';
            job.progress = 20;

            for (const localPath of localFiles) {
              const relativePath = path.relative(dataDir, localPath);
              let cloudKey = relativePath;
              if (cloud.keyPrefix) {
                const prefix = cloud.keyPrefix.endsWith('/')
                  ? cloud.keyPrefix
                  : `${cloud.keyPrefix}/`;
                cloudKey = `${prefix}${relativePath}`;
              }

              if (!cloudKeys.has(cloudKey)) {
                filesToSync.push({ key: cloudKey, localPath });
              }
            }
          }

          job.filesTotal = filesToSync.length;
          job.status = 'uploading';
          job.progress = 25;

          if (filesToSync.length === 0) {
            job.status = 'completed';
            job.phase = 'No files to sync';
            job.progress = 100;
            setTimeout(() => cloudSyncJobs.delete(jobId), 5 * 60 * 1000);
            return;
          }

          for (let i = 0; i < filesToSync.length; i++) {
            const { key, localPath } = filesToSync[i];
            job.currentFile = path.basename(localPath);
            job.phase = `Uploading ${i + 1}/${filesToSync.length}: ${job.currentFile}`;
            job.progress = 25 + Math.round(((i + 1) / filesToSync.length) * 75);

            try {
              const fileContent = await fs.readFile(localPath);
              const command = new PutObjectCommand({
                Bucket: cloud.bucket,
                Key: key,
                Body: fileContent,
                ContentType: 'application/octet-stream',
              });

              await state.cloudClient.send(command);
              job.filesUploaded++;
              app.debug(`Synced to ${label}: ${key}`);
            } catch (err) {
              job.filesFailed++;
              job.errors.push(`${key}: ${(err as Error).message}`);
            }
          }

          job.status = 'completed';
          job.phase = 'Complete';
          job.progress = 100;
          job.currentFile = '';

          setTimeout(() => cloudSyncJobs.delete(jobId), 5 * 60 * 1000);
        } catch (error) {
          job.status = 'error';
          job.phase = (error as Error).message;
        }
      })();

      return res.json({ success: true, jobId });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // Get cloud sync job status
  router.get('/api/cloud/sync/:jobId', async (req, res) => {
    const job = cloudSyncJobs.get(req.params.jobId);
    if (!job) {
      return res.status(404).json({ success: false, error: 'Job not found' });
    }
    return res.json({ success: true, ...job });
  });

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
          updateDataSubscriptions(
            currentPaths,
            state,
            state.currentConfig,
            app
          );
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
          updateDataSubscriptions(
            currentPaths,
            state,
            state.currentConfig,
            app
          );
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
          updateDataSubscriptions(
            currentPaths,
            state,
            state.currentConfig,
            app
          );
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

  // ===========================================
  // HOME PORT CONFIGURATION ENDPOINTS
  // ===========================================

  // Get home port configuration
  router.get('/api/config/homeport', (_req, res) => {
    try {
      if (!state.currentConfig) {
        return res.status(500).json({
          success: false,
          error: 'Plugin configuration not available',
        });
      }

      return res.json({
        success: true,
        latitude: state.currentConfig.homePortLatitude || null,
        longitude: state.currentConfig.homePortLongitude || null,
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // Update home port configuration
  router.put('/api/config/homeport', (req, res): void => {
    try {
      const { latitude, longitude } = req.body;

      if (!state.currentConfig) {
        res.status(500).json({
          success: false,
          error: 'Plugin configuration not available',
        });
        return;
      }

      // Validate latitude and longitude
      if (typeof latitude !== 'number' || typeof longitude !== 'number') {
        res.status(400).json({
          success: false,
          error: 'Latitude and longitude must be numbers',
        });
        return;
      }

      if (latitude < -90 || latitude > 90) {
        res.status(400).json({
          success: false,
          error: 'Latitude must be between -90 and 90',
        });
        return;
      }

      if (longitude < -180 || longitude > 180) {
        res.status(400).json({
          success: false,
          error: 'Longitude must be between -180 and 180',
        });
        return;
      }

      // Update the config
      state.currentConfig.homePortLatitude = latitude;
      state.currentConfig.homePortLongitude = longitude;

      // Update the cached config in commands module so thresholds use new home port
      updatePluginConfig(state.currentConfig);

      // Save to plugin options
      app.savePluginOptions(state.currentConfig, (err?: unknown) => {
        if (err) {
          app.error(`Failed to save home port: ${err}`);
          res.status(500).json({
            success: false,
            error: 'Failed to save home port configuration',
          });
          return;
        }

        app.debug(`✅ Home port updated: ${latitude}, ${longitude}`);
        res.json({
          success: true,
          latitude,
          longitude,
        });
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // Get current vessel position
  router.get('/api/position/current', (_req, res) => {
    try {
      // Cast to any for compatibility with different @signalk/server-api versions
      const position = app.getSelfPath('navigation.position') as any;
      if (position && position.value) {
        return res.json({
          success: true,
          latitude: position.value.latitude,
          longitude: position.value.longitude,
        });
      }
      return res.status(404).json({
        success: false,
        error: 'Current position not available',
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // ===========================================
  // PATH TYPE DETECTION ENDPOINT
  // ===========================================

  // Get data type information for a SignalK path
  router.get('/api/paths/:path/type', async (req, res) => {
    try {
      const pathParam = req.params.path;
      const { detectPathType } = await import('./utils/type-detector');

      const typeInfo = await detectPathType(pathParam, app);

      return res.json({
        success: true,
        ...typeInfo,
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // ===========================================
  // COMMAND MANAGEMENT ENDPOINTS
  // ===========================================

  // Register a new command
  router.post(
    '/api/commands',
    (
      req: TypedRequest<CommandRegistrationRequest>,
      res: TypedResponse<CommandApiResponse>
    ) => {
      try {
        const { command, description, keywords, defaultState, thresholds } =
          req.body;

        if (
          !command ||
          !/^[a-zA-Z0-9_]+$/.test(command) ||
          command.length === 0 ||
          command.length > 50
        ) {
          return res.status(400).json({
            success: false,
            error:
              'Invalid command name. Must be alphanumeric with underscores, 1-50 characters.',
          });
        }

        const result = registerCommand(
          command,
          description,
          keywords,
          defaultState,
          thresholds
        );

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
      req: TypedRequest<{
        description?: string;
        keywords?: string[];
        defaultState?: boolean;
        thresholds?: any[];
      }>,
      res: TypedResponse<CommandApiResponse>
    ) => {
      try {
        const { command } = req.params;
        const { description, keywords, defaultState, thresholds } = req.body;

        const result = updateCommand(
          command,
          description,
          keywords,
          defaultState,
          thresholds
        );
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

  // Manual override endpoint
  router.put(
    '/api/commands/:command/override',
    (
      req: TypedRequest<{ override: boolean; expiryMinutes?: number }>,
      res: TypedResponse<CommandApiResponse>
    ) => {
      try {
        const { command } = req.params;
        const { override, expiryMinutes } = req.body;

        const result = setManualOverride(command, override, expiryMinutes);
        if (result.success) {
          // Update webapp config
          const webAppConfig = loadWebAppConfig(app);
          const currentCommands = getCurrentCommands();
          saveWebAppConfig(webAppConfig.paths, currentCommands, app);

          return res.json({
            success: true,
            message: result.message,
          });
        } else {
          return res.status(400).json({
            success: false,
            error: result.message,
          });
        }
      } catch (error) {
        app.error(`Error setting manual override: ${error}`);
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
        // Cast to any for compatibility with different @signalk/server-api versions
        const currentValue = app.getSelfPath(`commands.${command}`) as any;

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
            requiredPaths: template.requiredPaths,
          })),
        }));

        res.json({
          success: true,
          templates: templates as any,
        });
      } catch (error) {
        app.error(`Template retrieval failed: ${(error as Error).message}`);
        res.status(500).json({
          success: false,
          error: 'Failed to retrieve analysis templates',
        });
      }
    }
  );

  // Test Claude connection
  router.post(
    '/api/analyze/test-connection',
    async (
      _req: TypedRequest,
      res: TypedResponse<ClaudeConnectionTestResponse>
    ) => {
      try {
        const config = state.currentConfig;
        if (
          !config?.claudeIntegration?.enabled ||
          !config.claudeIntegration.apiKey
        ) {
          return res.status(400).json({
            success: false,
            error: 'Claude integration is not configured or enabled',
          });
        }

        const analyzer = getSharedAnalyzer(config, app, state.getDataDirPath(), state);

        const startTime = Date.now();
        const testResult = await analyzer.testConnection();
        const responseTime = Date.now() - startTime;

        if (testResult.success) {
          return res.json({
            success: true,
            model: migrateClaudeModel(config.claudeIntegration.model, app),
            responseTime,
            tokenUsage: 50, // Approximate for test
          });
        } else {
          return res.status(400).json({
            success: false,
            error: testResult.error || 'Connection test failed',
          });
        }
      } catch (error) {
        app.error(`Claude connection test failed: ${(error as Error).message}`);
        return res.status(500).json({
          success: false,
          error: 'Claude connection test failed',
        });
      }
    }
  );

  // Main analysis endpoint
  router.post(
    '/api/analyze',
    async (
      req: TypedRequest<{
        dataPath: string;
        analysisType?: string;
        templateId?: string;
        customPrompt?: string;
        timeRange?: { start: string; end: string };
        aggregationMethod?: string;
        resolution?: string;
        claudeModel?: string;
        useDatabaseAccess?: boolean;
      }>,
      res: TypedResponse<AnalysisApiResponse>
    ) => {
      try {
        const config = state.currentConfig;
        if (
          !config?.claudeIntegration?.enabled ||
          !config.claudeIntegration.apiKey
        ) {
          return res.status(400).json({
            success: false,
            error: 'Claude integration is not configured or enabled',
          });
        }

        const {
          dataPath,
          analysisType,
          templateId,
          customPrompt,
          timeRange,
          aggregationMethod,
          resolution,
          claudeModel,
          useDatabaseAccess,
        } = req.body;

        console.log(
          `🧠 ANALYSIS REQUEST: dataPath=${dataPath}, templateId=${templateId}, analysisType=${analysisType}, aggregationMethod=${aggregationMethod}, model=${claudeModel || 'config-default'}`
        );
        console.log(
          `🔍 CUSTOM PROMPT DEBUG: "${customPrompt}" (type: ${typeof customPrompt}, length: ${customPrompt?.length || 0})`
        );

        if (!dataPath) {
          return res.status(400).json({
            success: false,
            error: 'Data path is required',
          });
        }

        // Use shared analyzer instance to maintain conversation state
        const analyzer = getSharedAnalyzer(config, app, state.getDataDirPath(), state);

        // Build analysis request
        let analysisRequest: AnalysisRequest;

        if (templateId) {
          // Use template
          const parsedTimeRange = timeRange
            ? {
                start: new Date(timeRange.start),
                end: new Date(timeRange.end),
              }
            : undefined;

          const templateRequest = AnalysisTemplateManager.createAnalysisRequest(
            templateId,
            dataPath,
            customPrompt,
            parsedTimeRange
          );

          if (!templateRequest) {
            return res.status(400).json({
              success: false,
              error: `Template not found: ${templateId}`,
            });
          }

          analysisRequest = templateRequest;
        } else {
          // Custom analysis
          analysisRequest = {
            dataPath,
            analysisType: (analysisType as any) || 'custom',
            customPrompt:
              customPrompt || 'Analyze this maritime data and provide insights',
            timeRange: timeRange
              ? {
                  start: new Date(timeRange.start),
                  end: new Date(timeRange.end),
                }
              : undefined,
            aggregationMethod,
            resolution,
            useDatabaseAccess: useDatabaseAccess || false,
          };
        }

        // Execute analysis
        app.debug(
          `Starting Claude analysis: ${analysisRequest.analysisType} for ${dataPath}`
        );
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
              confidence: a.confidence,
            })),
            confidence: result.confidence,
            dataQuality: result.dataQuality,
            timestamp: result.timestamp,
            metadata: result.metadata,
          },
          usage: result.usage,
        });
      } catch (error) {
        app.error(`Analysis failed: ${(error as Error).message}`);
        return res.status(500).json({
          success: false,
          error: `Analysis failed: ${(error as Error).message}`,
        });
      }
    }
  );

  // Follow-up question endpoint
  router.post(
    '/api/analyze/followup',
    async (
      req: TypedRequest<{ conversationId: string; question: string }>,
      res: TypedResponse<AnalysisApiResponse>
    ) => {
      try {
        const config = state.currentConfig;
        if (
          !config?.claudeIntegration?.enabled ||
          !config.claudeIntegration.apiKey
        ) {
          return res.status(400).json({
            success: false,
            error: 'Claude integration is not configured or enabled',
          });
        }

        const { conversationId, question } = req.body;

        if (!conversationId || !question) {
          return res.status(400).json({
            success: false,
            error: 'Both conversationId and question are required',
          });
        }

        console.log(
          `🔄 FOLLOW-UP REQUEST: conversationId=${conversationId}, question=${question.substring(0, 100)}...`
        );

        // Use shared analyzer instance to access stored conversations
        const analyzer = getSharedAnalyzer(config, app, state.getDataDirPath(), state);

        // Process follow-up question
        const followUpRequest = {
          conversationId,
          question,
        };

        const analysisResult = await analyzer.askFollowUp(followUpRequest);

        return res.json({
          success: true,
          data: analysisResult,
          usage: analysisResult.usage,
        });
      } catch (error) {
        app.error(`Follow-up question failed: ${(error as Error).message}`);
        return res.status(500).json({
          success: false,
          error: `Follow-up question failed: ${(error as Error).message}`,
        });
      }
    }
  );

  // Get analysis history
  router.get(
    '/api/analyze/history',
    async (
      req: TypedRequest<any> & { query: { limit?: string } },
      res: TypedResponse<AnalysisApiResponse>
    ) => {
      try {
        const config = state.currentConfig;
        if (
          !config?.claudeIntegration?.enabled ||
          !config.claudeIntegration.apiKey
        ) {
          return res.status(400).json({
            success: false,
            error: 'Claude integration is not configured or enabled',
          });
        }

        const limit = parseInt(req.query.limit || '20', 10);

        const analyzer = getSharedAnalyzer(config, app, state.getDataDirPath(), state);

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
              confidence: a.confidence,
            })),
            confidence: h.confidence,
            dataQuality: h.dataQuality,
            timestamp: h.timestamp,
            metadata: h.metadata,
          })),
        });
      } catch (error) {
        app.error(
          `Analysis history retrieval failed: ${(error as Error).message}`
        );
        return res.status(500).json({
          success: false,
          error: 'Failed to retrieve analysis history',
        });
      }
    }
  );

  // Delete analysis from history
  router.delete(
    '/api/analyze/history/:id',
    async (
      req: TypedRequest<any> & { params: { id: string } },
      res: TypedResponse<any>
    ) => {
      try {
        const config = state.currentConfig;
        if (
          !config?.claudeIntegration?.enabled ||
          !config.claudeIntegration.apiKey
        ) {
          return res.status(400).json({
            success: false,
            error: 'Claude integration is not configured or enabled',
          });
        }

        const analysisId = req.params.id;

        const analyzer = getSharedAnalyzer(config, app, state.getDataDirPath(), state);

        const result = await analyzer.deleteAnalysis(analysisId);

        if (result.success) {
          return res.json({
            success: true,
            message: 'Analysis deleted successfully',
          });
        } else {
          return res.status(404).json({
            success: false,
            error: result.error,
          });
        }
      } catch (error) {
        app.error(`Analysis deletion failed: ${(error as Error).message}`);
        return res.status(500).json({
          success: false,
          error: 'Failed to delete analysis',
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
        const contextManager = new VesselContextManager(app, state.getDataDirPath());
        const context = await contextManager.getVesselContext();

        return res.json({
          success: true,
          data: context,
        });
      } catch (error) {
        app.error(`Failed to get vessel context: ${(error as Error).message}`);
        return res.status(500).json({
          success: false,
          error: 'Failed to get vessel context',
        });
      }
    }
  );

  // Update vessel context
  router.post(
    '/api/vessel-context',
    async (
      req: TypedRequest<{
        vesselInfo?: any;
        customContext?: string;
      }>,
      res: TypedResponse<any>
    ) => {
      try {
        const { vesselInfo, customContext } = req.body;

        const contextManager = new VesselContextManager(app, state.getDataDirPath());
        const updatedContext = await contextManager.updateVesselContext(
          vesselInfo,
          customContext,
          false // Not auto-extracted since it's from user input
        );

        return res.json({
          success: true,
          data: updatedContext,
          message: 'Vessel context updated successfully',
        });
      } catch (error) {
        app.error(
          `Failed to update vessel context: ${(error as Error).message}`
        );
        return res.status(500).json({
          success: false,
          error: 'Failed to update vessel context',
        });
      }
    }
  );

  // Refresh vessel context from SignalK
  router.post(
    '/api/vessel-context/refresh',
    async (_: TypedRequest, res: TypedResponse<any>) => {
      try {
        const contextManager = new VesselContextManager(app, state.getDataDirPath());
        const refreshedContext = await contextManager.refreshVesselInfo();

        return res.json({
          success: true,
          data: refreshedContext,
          message: 'Vessel context refreshed from SignalK data',
        });
      } catch (error) {
        app.error(
          `Failed to refresh vessel context: ${(error as Error).message}`
        );
        return res.status(500).json({
          success: false,
          error: 'Failed to refresh vessel context from SignalK',
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
          data: dataPaths,
        });
      } catch (error) {
        app.error(
          `Failed to get vessel data paths: ${(error as Error).message}`
        );
        return res.status(500).json({
          success: false,
          error: 'Failed to get vessel data paths',
        });
      }
    }
  );

  // Generate Claude context preview
  router.get(
    '/api/vessel-context/claude-preview',
    async (_: TypedRequest, res: TypedResponse<any>) => {
      try {
        const contextManager = new VesselContextManager(app, state.getDataDirPath());
        // Ensure context is loaded before generating preview
        await contextManager.getVesselContext();
        const claudeContext = contextManager.generateClaudeContext();

        return res.json({
          success: true,
          data: {
            contextText: claudeContext,
            length: claudeContext.length,
          },
        });
      } catch (error) {
        app.error(
          `Failed to generate Claude context preview: ${(error as Error).message}`
        );
        return res.status(500).json({
          success: false,
          error: 'Failed to generate Claude context preview',
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
  // PROCESS MANAGEMENT API ROUTES
  // ===========================================

  // Get current process status
  router.get(
    '/api/process-status',
    (_: TypedRequest, res: TypedResponse<ProcessStatusApiResponse>) => {
      const status = getProcessStatus(state);
      res.json(status);
    }
  );

  // Cancel current process
  router.post(
    '/api/cancel-process',
    (_: TypedRequest, res: TypedResponse<ProcessCancelApiResponse>) => {
      const cancelled = cancelProcess(state);

      if (cancelled) {
        res.json({
          success: true,
          message: `${state.currentProcess?.type || 'Process'} cancellation requested`,
        });
      } else {
        res.json({
          success: false,
          message: 'No active process to cancel',
        });
      }
    }
  );

  // ===========================================
  // SCHEMA VALIDATION API ROUTES
  // ===========================================

  // Get validation progress
  router.get(
    '/api/validate-schemas/progress/:jobId',
    async (req: TypedRequest, res: TypedResponse<ValidationProgress>) => {
      const { jobId } = req.params;
      const progress = validationJobs.get(jobId);

      if (!progress) {
        return res.status(404).json({
          success: false,
          error: 'Validation job not found',
        } as any);
      }

      res.set(
        'Cache-Control',
        'no-store, no-cache, must-revalidate, proxy-revalidate'
      );
      res.set('Pragma', 'no-cache');
      res.set('Expires', '0');
      res.set('Surrogate-Control', 'no-store');

      return res.json(progress);
    }
  );

  router.post(
    '/api/validate-schemas/cancel/:jobId',
    async (
      req: TypedRequest,
      res: TypedResponse<
        ValidationApiResponse & {
          jobId: string;
          status: ValidationProgress['status'];
        }
      >
    ) => {
      const { jobId } = req.params;
      const job = validationJobs.get(jobId);

      if (!job) {
        return res.status(404).json({
          success: false,
          error: 'Validation job not found',
          jobId,
          status: 'error',
        } as any);
      }

      if (
        job.status === 'completed' ||
        job.status === 'cancelled' ||
        job.status === 'error'
      ) {
        return res.json({
          success: true,
          message: `Validation job already ${job.status}`,
          jobId,
          status: job.status,
        });
      }

      job.cancelRequested = true;
      job.status = 'cancelling';
      app.debug(`⏹️ Cancellation requested for validation job: ${jobId}`);

      return res.json({
        success: true,
        message: 'Cancellation requested',
        jobId,
        status: job.status,
      });
    }
  );

  router.get(
    '/api/repair-schemas/progress/:jobId',
    async (req: TypedRequest, res: TypedResponse<RepairProgress>) => {
      const { jobId } = req.params;
      const progress = repairJobs.get(jobId);

      if (!progress) {
        return res.status(404).json({
          success: false,
          message: 'Repair job not found',
        } as any);
      }

      res.set(
        'Cache-Control',
        'no-store, no-cache, must-revalidate, proxy-revalidate'
      );
      res.set('Pragma', 'no-cache');
      res.set('Expires', '0');
      res.set('Surrogate-Control', 'no-store');

      return res.json(progress);
    }
  );

  router.post(
    '/api/repair-schemas/cancel/:jobId',
    async (
      req: TypedRequest,
      res: TypedResponse<{
        success: boolean;
        message: string;
        jobId: string;
        status: RepairProgress['status'];
      }>
    ) => {
      const { jobId } = req.params;
      const job = repairJobs.get(jobId);

      if (!job) {
        return res.status(404).json({
          success: false,
          message: 'Repair job not found',
          jobId,
          status: 'error',
        });
      }

      if (
        job.status === 'completed' ||
        job.status === 'cancelled' ||
        job.status === 'error'
      ) {
        return res.json({
          success: true,
          message: `Repair job already ${job.status}`,
          jobId,
          status: job.status,
        });
      }

      job.cancelRequested = true;
      job.status = 'cancelling';
      job.message = 'Cancellation requested';
      app.debug(`⏹️ Cancellation requested for repair job: ${jobId}`);

      return res.json({
        success: true,
        message: 'Cancellation requested',
        jobId,
        status: job.status,
      });
    }
  );

  // Validate parquet schemas
  router.post(
    '/api/validate-schemas',
    async (
      _req: TypedRequest,
      res: TypedResponse<
        ValidationApiResponse & { jobId: string; status?: string }
      >
    ) => {
      const jobId = `val_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      const progressJob: ValidationProgress = {
        jobId,
        status: 'running',
        processed: 0,
        total: 0,
        percent: 0,
        startTime: new Date(),
        cancelRequested: false,
      };

      validationJobs.set(jobId, progressJob);
      lastValidationViolations = [];

      const runValidationJob = async () => {
        let violationFiles: ValidationViolation[] = [];
        try {
          app.debug('🔍 Starting schema validation...');
          app.debug(`📋 Created validation job: ${jobId}`);

          const parquet = require('@dsnp/parquetjs');
          const glob = require('glob');
          const path = require('path');

          const configOutputDir =
            state.currentConfig?.outputDirectory || 'data';
          const pluginDataPath = app.getDataDirPath();
          const signalkDataDir = path.dirname(path.dirname(pluginDataPath));
          const dataDir = path.join(signalkDataDir, configOutputDir);

          let totalFiles = 0;
          let correctSchemas = 0;
          let violationSchemas = 0;
          const vessels = new Set<string>();
          const violationDetails: string[] = [];
          const debugMessages: string[] = [];

          const addDebug = (message: string) => {
            debugMessages.push(message);
            app.debug(message);
          };

          const searchPattern = path.join(
            dataDir,
            'vessels',
            '**',
            '*.parquet'
          );
          addDebug(`🔍 Data directory: ${dataDir}`);
          addDebug(`🔍 Searching pattern: ${searchPattern}`);

          const files = glob.sync(searchPattern, {
            ignore: [
              `${dataDir}/**/processed/**`,
              `${dataDir}/**/repaired/**`,
              `${dataDir}/**/quarantine/**`,
              `${dataDir}/**/claude-schemas/**`,
              `${dataDir}/**/failed/**`,
            ],
          });
          addDebug(`📄 Found ${files.length} parquet files`);

          progressJob.total = files.length;
          progressJob.percent = files.length === 0 ? 100 : 0;

          addDebug('⏸️ Pausing for 10 seconds before processing...');
          await new Promise(resolve => setTimeout(resolve, 10000));
          addDebug('▶️ Resuming processing after 10 second pause');

          for (let fileIndex = 0; fileIndex < files.length; fileIndex++) {
            if (progressJob.cancelRequested) {
              addDebug(
                '⏹️ Validation cancellation requested, stopping processing'
              );
              progressJob.status = 'cancelled';
              break;
            }

            const filePath = files[fileIndex];

            progressJob.processed = Math.min(fileIndex + 1, files.length);
            progressJob.percent =
              files.length > 0
                ? Math.round((progressJob.processed / files.length) * 100)
                : 100;
            progressJob.currentFile = path.basename(filePath);
            progressJob.currentRelativePath = path.relative(dataDir, filePath);

            const pathParts = filePath.split(path.sep);
            const vesselsIndex = pathParts.findIndex(
              (part: string) => part === 'vessels'
            );
            const vesselName =
              vesselsIndex !== -1 && vesselsIndex + 1 < pathParts.length
                ? pathParts[vesselsIndex + 1]
                : undefined;
            progressJob.currentVessel = vesselName;

            if (vesselName) {
              vessels.add(vesselName);
            }

            if (
              path.basename(filePath).includes('quarantine') ||
              path.basename(filePath).includes('corrupted') ||
              filePath.includes('/processed/') ||
              filePath.includes('/repaired/')
            ) {
              continue;
            }

            totalFiles++;

            try {
              const reader = await parquet.ParquetReader.openFile(filePath);
              const cursor = reader.getCursor();
              const schema = cursor.schema;

              if (schema && schema.schema) {
                const fields = schema.schema;

                const receivedTimestamp = fields.received_timestamp
                  ? fields.received_timestamp.type
                  : 'MISSING';
                const signalkTimestamp = fields.signalk_timestamp
                  ? fields.signalk_timestamp.type
                  : 'MISSING';

                const valueFields: { [key: string]: string } = {};
                Object.keys(fields).forEach(fieldName => {
                  if (fieldName.startsWith('value_') || fieldName === 'value') {
                    valueFields[fieldName] = fields[fieldName].type;
                  }
                });

                const relativePath =
                  progressJob.currentRelativePath ||
                  path.relative(dataDir, filePath);
                const pathMatch = relativePath.match(
                  /vessels\/[^/]+\/(.+?)\/[^/]*\.parquet$/
                );
                const signalkPath = pathMatch
                  ? pathMatch[1].replace(/\//g, '.')
                  : '';

                let hasViolations = false;
                const violations: string[] = [];

                if (
                  receivedTimestamp !== 'UTF8' &&
                  receivedTimestamp !== 'MISSING'
                ) {
                  violations.push(
                    `received_timestamp should be UTF8, got ${receivedTimestamp}`
                  );
                  hasViolations = true;
                }
                if (
                  signalkTimestamp !== 'UTF8' &&
                  signalkTimestamp !== 'MISSING'
                ) {
                  violations.push(
                    `signalk_timestamp should be UTF8, got ${signalkTimestamp}`
                  );
                  hasViolations = true;
                }

                const isExplodedFile = Object.keys(valueFields).some(
                  fieldName =>
                    fieldName.startsWith('value_') &&
                    fieldName !== 'value' &&
                    fieldName !== 'value_json'
                );
                addDebug(`🔍 Validation: isExplodedFile = ${isExplodedFile}`);

                let sampleRecords = [];
                try {
                  if (!parquet) {
                    throw new Error('ParquetJS not available');
                  }
                  const sampleReader =
                    await parquet.ParquetReader.openFile(filePath);
                  const sampleCursor = sampleReader.getCursor();
                  let record: any;
                  let count = 0;
                  while ((record = await sampleCursor.next()) && count < 100) {
                    sampleRecords.push(record);
                    count++;
                  }
                  await sampleReader.close();
                } catch (error) {
                  addDebug(
                    `⚠️ Could not read sample data for validation: ${(error as Error).message}`
                  );
                  sampleRecords = [];
                }

                for (const [fieldName, fieldType] of Object.entries(
                  valueFields
                )) {
                  if (fieldName === 'value_json') {
                    addDebug(
                      `⏭️ ${fieldName}: Skipped entirely (always ignored)`
                    );
                    continue;
                  }

                  if (isExplodedFile && fieldName === 'value') {
                    addDebug(
                      `⏭️ ${fieldName}: Skipped in exploded file (always empty)`
                    );
                    continue;
                  }

                  if (fieldType === 'UTF8' || fieldType === 'VARCHAR') {
                    let shouldBeNumeric = false;

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
                          violations.push(
                            `${fieldName} contains numbers but is ${fieldType}, should be DOUBLE`
                          );
                          hasViolations = true;
                          addDebug(
                            `🔍 ${fieldName}: VARCHAR contains numbers, flagged as violation`
                          );
                        } else if (allBoolean && values.length > 0) {
                          violations.push(
                            `${fieldName} contains booleans but is ${fieldType}, should be BOOLEAN`
                          );
                          hasViolations = true;
                          addDebug(
                            `🔍 ${fieldName}: VARCHAR contains booleans, flagged as violation`
                          );
                        }
                      }
                    }

                    if (!shouldBeNumeric && sampleRecords.length === 0) {
                      const isExplodedField = fieldName.startsWith('value_');

                      if (!isExplodedField && signalkPath) {
                        addDebug(
                          `🔍 ${fieldName}: Using metadata fallback (matches repair logic)`
                        );
                        try {
                          const metadata = app.getMetadata(signalkPath) as any;
                          if (
                            metadata &&
                            metadata.units &&
                            (metadata.units === 'm' ||
                              metadata.units === 'deg' ||
                              metadata.units === 'm/s' ||
                              metadata.units === 'rad' ||
                              metadata.units === 'K' ||
                              metadata.units === 'Pa' ||
                              metadata.units === 'V' ||
                              metadata.units === 'A' ||
                              metadata.units === 'Hz' ||
                              metadata.units === 'ratio' ||
                              metadata.units === 'kg' ||
                              metadata.units === 'J')
                          ) {
                            violations.push(
                              `${fieldName} has numeric units (${metadata.units}) but is ${fieldType}, should be DOUBLE`
                            );
                            hasViolations = true;
                            addDebug(
                              `🔍 ${fieldName}: Metadata indicates numeric (${metadata.units}), flagged as violation`
                            );
                          }
                        } catch (metadataError) {
                          addDebug(
                            `🔍 ${fieldName}: Metadata lookup failed, no violation flagged`
                          );
                        }
                      } else {
                        addDebug(
                          `🔍 ${fieldName}: Exploded field or no path, skipping metadata (matches repair logic)`
                        );
                      }
                    }
                  }
                }

                if (hasViolations) {
                  violationSchemas++;
                  const shortPath = path.relative(dataDir, filePath);
                  violationDetails.push(
                    `[${totalFiles}] ${shortPath}: ${violations.join(', ')}`
                  );
                  violationFiles.push({
                    file: shortPath,
                    vessel: vesselName,
                    issues: [...violations],
                  });
                } else {
                  correctSchemas++;
                }

                if (typeof reader.close === 'function') reader.close();
              }
            } catch (error) {
              app.debug(
                `Error processing ${filePath}: ${(error as Error).message}`
              );
              violationSchemas++;
              const shortPath = path.relative(dataDir, filePath);
              violationDetails.push(
                `[${totalFiles}] ${shortPath}: ERROR - ${(error as Error).message}`
              );
              violationFiles.push({
                file: shortPath,
                vessel: progressJob.currentVessel,
                issues: [`ERROR - ${(error as Error).message}`],
              });
            }
          }

          if (
            progressJob.cancelRequested ||
            progressJob.status === 'cancelled'
          ) {
            addDebug(
              `⏹️ Validation cancelled by user after processing ${totalFiles} files`
            );
            progressJob.status = 'cancelled';
            progressJob.completedAt = new Date();
            progressJob.currentFile = undefined;
            progressJob.currentVessel = undefined;
            progressJob.currentRelativePath = undefined;
            progressJob.result = {
              success: false,
              error: 'Validation cancelled by user',
              cancelled: true,
              totalFiles,
              totalVessels: vessels.size,
              correctSchemas,
              violations: violationSchemas,
              violationDetails,
              violationFiles,
              debugMessages,
              jobId,
            };
            lastValidationViolations = violationFiles;
            return;
          }

          addDebug(
            `📊 Validation completed: ${totalFiles} files, ${vessels.size} vessels, ${correctSchemas} correct, ${violationSchemas} violations`
          );

          progressJob.status = 'completed';
          progressJob.processed = files.length;
          progressJob.percent = 100;
          progressJob.completedAt = new Date();
          progressJob.currentFile = undefined;
          progressJob.currentVessel = undefined;
          progressJob.currentRelativePath = undefined;
          progressJob.result = {
            success: true,
            totalFiles,
            totalVessels: vessels.size,
            correctSchemas,
            violations: violationSchemas,
            violationDetails,
            violationFiles,
            debugMessages,
            jobId,
          };
          lastValidationViolations = violationFiles;
        } catch (error) {
          app.error(`Error during schema validation: ${error}`);
          progressJob.status = 'error';
          progressJob.error = (error as Error).message;
          progressJob.completedAt = new Date();
          progressJob.result = {
            success: false,
            error: (error as Error).message,
            violationFiles,
            jobId,
          };
          lastValidationViolations = violationFiles;
        } finally {
          progressJob.currentFile = undefined;
          progressJob.currentVessel = undefined;
          progressJob.currentRelativePath = undefined;
          scheduleValidationJobCleanup(jobId);
        }
      };

      setImmediate(() => {
        runValidationJob().catch(error => {
          app.error(`Unhandled validation job error: ${error}`);
        });
      });

      return res.json({
        success: true,
        status: 'started',
        jobId,
      });
    }
  );

  // Repair schema violations endpoint
  router.post('/api/repair-schemas', async (_: TypedRequest, res: any) => {
    try {
      app.debug('🔧 Starting schema repair...');

      const parquet = require('@dsnp/parquetjs');
      const path = require('path');

      const configOutputDir = state.currentConfig?.outputDirectory || 'data';
      const pluginDataPath = app.getDataDirPath();
      const signalkDataDir = path.dirname(path.dirname(pluginDataPath));
      const dataDir = path.join(signalkDataDir, configOutputDir);
      const filenamePrefix =
        state.currentConfig?.filenamePrefix || 'signalk_data';

      const uniqueViolations = new Map<string, ValidationViolation>();
      for (const violation of lastValidationViolations) {
        if (violation?.file) {
          uniqueViolations.set(violation.file, violation);
        }
      }

      if (uniqueViolations.size === 0) {
        const message =
          'No validation violations available for repair. Run validation first.';
        app.debug(`🔧 ${message}`);
        return res.status(400).json({ success: false, message });
      }

      const targetFiles: {
        relative: string;
        absolute: string;
        violation?: ValidationViolation;
      }[] = [];
      for (const [relativePath, violation] of uniqueViolations) {
        const absolutePath = path.isAbsolute(relativePath)
          ? relativePath
          : path.join(dataDir, relativePath);

        if (!absolutePath.startsWith(dataDir)) {
          app.debug(
            `❌ Skipping flagged file outside data directory: ${relativePath}`
          );
          continue;
        }

        if (await fs.pathExists(absolutePath)) {
          targetFiles.push({
            relative: relativePath,
            absolute: absolutePath,
            violation,
          });
        } else {
          app.debug(`❌ Flagged file no longer exists: ${relativePath}`);
        }
      }

      if (targetFiles.length === 0) {
        const message =
          'Validation flagged files are missing; nothing to repair.';
        app.debug(`🔧 ${message}`);
        return res.status(400).json({ success: false, message });
      }

      targetFiles.sort((a, b) => a.relative.localeCompare(b.relative));

      const jobId = `rep_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      const job: RepairProgress = {
        jobId,
        status: 'running',
        processed: 0,
        total: targetFiles.length,
        percent: targetFiles.length === 0 ? 100 : 0,
        startTime: new Date(),
        message: `Preparing to repair ${targetFiles.length} files`,
        cancelRequested: false,
      };

      repairJobs.set(jobId, job);

      const runRepairJob = async () => {
        let repairedFiles = 0;
        let backedUpFiles = 0;
        const skippedFiles: string[] = [];
        const quarantinedFiles: string[] = [];
        const errors: string[] = [];
        const handledRelativePaths = new Set<string>();

        try {
          app.debug(
            `🔧 Repair job ${jobId}: targeting ${targetFiles.length} files`
          );

          job.message = 'Pausing briefly before repair';
          await new Promise(resolve => setTimeout(resolve, 10000));
          job.message = 'Starting repair process';

          if (job.cancelRequested) {
            app.debug(`🔧 Repair job ${jobId}: cancelled before processing`);
            job.status = 'cancelled';
            job.message = 'Repair cancelled before start';
            job.completedAt = new Date();
            job.result = {
              success: false,
              repairedFiles: 0,
              backedUpFiles: 0,
              skippedFiles: [],
              quarantinedFiles: [],
              errors: [],
              message: 'Repair cancelled before start',
            };
            return;
          }

          for (let i = 0; i < targetFiles.length; i++) {
            if (job.cancelRequested) {
              app.debug(`🔧 Repair job ${jobId}: cancellation detected`);
              job.status = 'cancelled';
              job.message = 'Repair cancelled by user';
              break;
            }

            const {
              absolute: filePath,
              relative: relativePath,
              violation,
            } = targetFiles[i];
            job.currentFile = relativePath;
            job.processed = Math.min(i + 1, job.total);
            job.percent =
              job.total > 0 ? Math.round(((i + 1) / job.total) * 100) : 100;
            job.message = `Repairing (${Math.min(i + 1, job.total)}/${job.total}): ${relativePath}`;

            let needsRepair =
              Array.isArray(violation?.issues) && violation.issues.length > 0;

            app.debug(`🔧 Repair job ${jobId}: processing ${relativePath}`);

            try {
              const stats = await fs.stat(filePath);
              if (stats.size < 100) {
                app.debug(
                  `❌ File too small (${stats.size} bytes), moving to quarantine: ${path.basename(filePath)}`
                );

                const quarantineDir = path.join(
                  path.dirname(filePath),
                  'quarantine'
                );
                await fs.ensureDir(quarantineDir);
                const quarantineFile = path.join(
                  quarantineDir,
                  path.basename(filePath)
                );
                await fs.move(filePath, quarantineFile, { overwrite: true });

                const logFile = path.join(quarantineDir, 'quarantine.log');
                const logEntry = `${new Date().toISOString()} | repair | ${stats.size} bytes | File too small (corrupted) | ${filePath}\n`;
                await fs.appendFile(logFile, logEntry);

                quarantinedFiles.push(relativePath);
                handledRelativePaths.add(relativePath);
                job.processed = i + 1;
                job.percent =
                  job.total > 0 ? Math.round(((i + 1) / job.total) * 100) : 100;
                job.message = `Quarantined: ${relativePath}`;
                continue;
              }
            } catch (statError) {
              const message = `Error checking file size for ${relativePath}: ${(statError as Error).message}`;
              app.debug(`❌ ${message}`);
              errors.push(message);
              job.processed = i + 1;
              job.percent =
                job.total > 0 ? Math.round(((i + 1) / job.total) * 100) : 100;
              job.message = `Skipped due to error: ${relativePath}`;
              continue;
            }

            try {
              // Get schema and sample data directly from parquet file (same as validation)
              const parquetReader =
                await parquet.ParquetReader.openFile(filePath);
              const parquetCursor = parquetReader.getCursor();
              const schema = parquetCursor.schema;

              const valueFields: { [key: string]: string } = {};
              let signalkPath = '';

              // Extract schema fields
              if (schema && schema.schema) {
                const fields = schema.schema;

                // Get value fields
                for (const [fieldName, fieldInfo] of Object.entries(fields)) {
                  if (fieldName === 'value' || fieldName.startsWith('value_')) {
                    valueFields[fieldName] =
                      (fieldInfo as any).type || 'UNKNOWN';
                  }
                }

                // Extract SignalK path from file path (same logic as before)
                const pathRegex = new RegExp(
                  `/vessels/[^/]+/(.+)/${filenamePrefix}_`
                );
                const pathMatch = filePath.match(pathRegex);
                if (pathMatch) {
                  signalkPath = pathMatch[1].replace(/\//g, '.');
                  app.debug(
                    `🔍 Extracted SignalK path: ${signalkPath} from ${path.basename(filePath)}`
                  );
                } else {
                  app.debug(
                    `🔍 Could not extract SignalK path from ${path.basename(filePath)} using prefix ${filenamePrefix}`
                  );
                }
              }

              // Read sample records for analysis
              const sampleRecords: any[] = [];
              try {
                let record: any;
                let count = 0;
                while ((record = await parquetCursor.next()) && count < 100) {
                  sampleRecords.push(record);
                  count++;
                }
              } catch (sampleError) {
                app.debug(
                  `🔧 Could not read sample data for repair: ${(sampleError as Error).message}`
                );
              }
              await parquetReader.close();

              const fieldEntries = Object.entries(valueFields);
              const hasExplodedFields = fieldEntries.some(
                ([fieldName]) =>
                  fieldName.startsWith('value_') &&
                  fieldName !== 'value' &&
                  fieldName !== 'value_json'
              );

              for (const [fieldName, fieldType] of fieldEntries) {
                if (fieldName === 'value_json') continue;
                if (hasExplodedFields && fieldName === 'value') continue;

                if (fieldType === 'UTF8' || fieldType === 'VARCHAR') {
                  const values = sampleRecords
                    .map(r => r[fieldName])
                    .filter(v => v !== null && v !== undefined)
                    .map(v => String(v).trim());

                  if (values.length > 0) {
                    const allNumeric = values.every(
                      v =>
                        v !== '' &&
                        !isNaN(Number(v)) &&
                        v !== 'true' &&
                        v !== 'false'
                    );
                    const allBoolean = values.every(
                      v => v === 'true' || v === 'false'
                    );

                    if (allNumeric) {
                      needsRepair = true;
                      app.debug(
                        `🔧 File needs repair: ${relativePath} (${fieldName} contains numbers, should be DOUBLE, not ${fieldType})`
                      );
                      break;
                    }
                    if (allBoolean) {
                      needsRepair = true;
                      app.debug(
                        `🔧 File needs repair: ${relativePath} (${fieldName} contains booleans, should be BOOLEAN, not ${fieldType})`
                      );
                      break;
                    }
                  }

                  if (
                    !needsRepair &&
                    signalkPath &&
                    !fieldName.startsWith('value_')
                  ) {
                    try {
                      const metadata = app.getMetadata(signalkPath) as any;
                      if (
                        metadata &&
                        metadata.units &&
                        (metadata.units === 'm' ||
                          metadata.units === 'deg' ||
                          metadata.units === 'm/s' ||
                          metadata.units === 'rad' ||
                          metadata.units === 'K' ||
                          metadata.units === 'Pa' ||
                          metadata.units === 'V' ||
                          metadata.units === 'A' ||
                          metadata.units === 'Hz' ||
                          metadata.units === 'ratio' ||
                          metadata.units === 'kg' ||
                          metadata.units === 'J')
                      ) {
                        needsRepair = true;
                        app.debug(
                          `🔧 File needs repair: ${relativePath} (${fieldName} should be DOUBLE per metadata, not ${fieldType})`
                        );
                        break;
                      }
                    } catch (metadataError) {
                      app.debug(
                        `🔧 Metadata check failed for ${fieldName}: ${(metadataError as Error).message}`
                      );
                    }
                  }
                } else if (fieldType === 'BIGINT') {
                  needsRepair = true;
                  app.debug(
                    `🔧 File needs repair: ${relativePath} (${fieldName} is BIGINT, converting to DOUBLE)`
                  );
                  break;
                }
              }

              if (!needsRepair) {
                skippedFiles.push(relativePath);
                handledRelativePaths.add(relativePath);
                job.message = `Skipped (no repair needed): ${relativePath}`;
                job.processed = i + 1;
                job.percent =
                  job.total > 0 ? Math.round(((i + 1) / job.total) * 100) : 100;
                continue;
              }

              const backupDir = path.join(path.dirname(filePath), 'repaired');
              await fs.mkdir(backupDir, { recursive: true });

              const backupPath = path.join(backupDir, path.basename(filePath));
              await fs.copyFile(filePath, backupPath);
              backedUpFiles++;
              app.debug(`🔧 Backed up: ${path.basename(filePath)}`);

              const reader = await parquet.ParquetReader.openFile(filePath);
              const cursor = reader.getCursor();
              const records: any[] = [];
              let record: any;
              while ((record = await cursor.next())) {
                records.push(record);
              }
              await reader.close();
              const { ParquetWriter } = require('./parquet-writer');
              const writer = new ParquetWriter({ format: 'parquet', app });
              const correctedSchema = await writer.createParquetSchema(
                records,
                signalkPath
              );

              const parquetWriter = await parquet.ParquetWriter.openFile(
                correctedSchema,
                filePath
              );
              for (const row of records) {
                const prepared = writer.prepareRecordForParquet(
                  row,
                  correctedSchema
                );
                await parquetWriter.appendRow(prepared);
              }
              await parquetWriter.close();
              repairedFiles++;
              handledRelativePaths.add(relativePath);
              job.message = `Repaired: ${relativePath}`;
              app.debug(`🔧 ✅ Repaired: ${path.basename(filePath)}`);
            } catch (fileError) {
              const message = `Error processing ${relativePath}: ${(fileError as Error).message}`;
              app.debug(`🔧 ❌ ${message}`);
              errors.push(message);
              job.message = message;
              job.processed = i + 1;
              job.percent =
                job.total > 0 ? Math.round(((i + 1) / job.total) * 100) : 100;
            }
          }

          if (handledRelativePaths.size > 0) {
            lastValidationViolations = lastValidationViolations.filter(
              violation => !handledRelativePaths.has(violation.file)
            );
          }

          if (job.status === 'cancelled') {
            job.completedAt = new Date();
            job.result = {
              success: false,
              repairedFiles,
              backedUpFiles,
              skippedFiles,
              quarantinedFiles,
              errors,
              message: 'Repair cancelled',
            };
          } else {
            job.status = errors.length > 0 ? 'error' : 'completed';
            job.message =
              errors.length > 0
                ? 'Repair completed with errors'
                : 'Repair completed successfully';
            job.completedAt = new Date();
            job.result = {
              success: errors.length === 0,
              repairedFiles,
              backedUpFiles,
              skippedFiles,
              quarantinedFiles,
              errors,
              message: job.message,
            };
          }
        } catch (jobError) {
          job.status = 'error';
          job.message = `Repair job failed: ${(jobError as Error).message}`;
          job.completedAt = new Date();
          job.result = {
            success: false,
            repairedFiles: 0,
            backedUpFiles: 0,
            skippedFiles: [],
            quarantinedFiles: [],
            errors: [(jobError as Error).message],
          };
        } finally {
          job.currentFile = undefined;
          job.processed = job.total;
          job.percent = 100;
          scheduleRepairJobCleanup(jobId);
        }
      };

      setImmediate(() => {
        runRepairJob().catch(error => {
          app.debug(`❌ Unhandled repair job error (${jobId}): ${error}`);
        });
      });

      return res.json({
        success: true,
        status: 'started',
        jobId,
        totalFiles: targetFiles.length,
      });
    } catch (error) {
      app.debug(`❌ Schema repair failed: ${(error as Error).message}`);
      return res.status(500).json({
        success: false,
        message: `Repair failed: ${(error as Error).message}`,
      });
    }
  });

  // ===========================================
  // PARQUET STORE STATS API
  // ===========================================

  router.get('/api/store/stats', async (_req, res) => {
    try {
      const dataDir = state.getDataDirPath();
      const hiveBuilder = new HivePathBuilder();
      const tiers: Array<{ tier: string; fileCount: number }> = [];
      const contexts: Array<{
        name: string;
        pathCount: number;
        fileCount: number;
      }> = [];
      let totalFiles = 0;
      let totalPaths = 0;
      let earliestDate: Date | null = null;
      let latestDate: Date | null = null;

      // Helper to count parquet files recursively
      const countFiles = (dir: string): number => {
        let count = 0;
        try {
          const items = fs.readdirSync(dir);
          for (const item of items) {
            const fullPath = path.join(dir, item);
            const stat = fs.statSync(fullPath);
            if (stat.isDirectory()) {
              if (
                !['processed', 'failed', 'quarantine', 'repaired'].includes(
                  item
                )
              ) {
                count += countFiles(fullPath);
              }
            } else if (item.endsWith('.parquet')) {
              count++;
            }
          }
        } catch {
          // skip unreadable dirs
        }
        return count;
      };

      // Helper to find earliest/latest year+day in a path directory
      const scanDateRange = (
        pathDir: string
      ): { earliest: Date | null; latest: Date | null } => {
        let minYear = Infinity,
          minDay = Infinity;
        let maxYear = -Infinity,
          maxDay = -Infinity;
        try {
          const yearDirs = fs
            .readdirSync(pathDir)
            .filter((d: string) => d.startsWith('year='));
          for (const yearDir of yearDirs) {
            const year = parseInt(yearDir.split('=')[1], 10);
            if (isNaN(year)) continue;
            const yearPath = path.join(pathDir, yearDir);
            const dayDirs = fs
              .readdirSync(yearPath)
              .filter((d: string) => d.startsWith('day='));
            for (const dayDir of dayDirs) {
              const day = parseInt(dayDir.split('=')[1], 10);
              if (isNaN(day)) continue;
              const dayPath = path.join(yearPath, dayDir);
              const hasFiles = fs
                .readdirSync(dayPath)
                .some((f: string) => f.endsWith('.parquet'));
              if (!hasFiles) continue;
              if (year < minYear || (year === minYear && day < minDay)) {
                minYear = year;
                minDay = day;
              }
              if (year > maxYear || (year === maxYear && day > maxDay)) {
                maxYear = year;
                maxDay = day;
              }
            }
          }
        } catch {
          // skip
        }
        return {
          earliest:
            minYear !== Infinity
              ? hiveBuilder.dateFromDayOfYear(minYear, minDay)
              : null,
          latest:
            maxYear !== -Infinity
              ? hiveBuilder.dateFromDayOfYear(maxYear, maxDay)
              : null,
        };
      };

      // Scan all tiers
      const tierNames = ['raw', '5s', '60s', '1h'];
      const contextMap = new Map<
        string,
        { pathCount: number; fileCount: number }
      >();

      for (const tierName of tierNames) {
        const tierDir = path.join(dataDir, `tier=${tierName}`);
        let tierFileCount = 0;

        if (!fs.existsSync(tierDir)) {
          tiers.push({ tier: tierName, fileCount: 0 });
          continue;
        }

        const contextDirs = fs
          .readdirSync(tierDir)
          .filter((d: string) => d.startsWith('context='));

        for (const contextDir of contextDirs) {
          const sanitizedCtx = contextDir.replace('context=', '');
          const ctxName = hiveBuilder.unsanitizeContext(sanitizedCtx);
          const ctxFullPath = path.join(tierDir, contextDir);

          const pathDirs = fs
            .readdirSync(ctxFullPath)
            .filter((d: string) => d.startsWith('path='));

          let ctxFileCount = 0;
          const ctxPaths = new Set<string>();

          for (const pathDir of pathDirs) {
            const pathFullPath = path.join(ctxFullPath, pathDir);
            const fileCount = countFiles(pathFullPath);
            if (fileCount > 0) {
              ctxPaths.add(pathDir);
              ctxFileCount += fileCount;

              // Scan date range (only on raw tier for efficiency)
              if (tierName === 'raw') {
                const range = scanDateRange(pathFullPath);
                if (
                  range.earliest &&
                  (!earliestDate || range.earliest < earliestDate)
                ) {
                  earliestDate = range.earliest;
                }
                if (
                  range.latest &&
                  (!latestDate || range.latest > latestDate)
                ) {
                  latestDate = range.latest;
                }
              }
            }
          }

          tierFileCount += ctxFileCount;

          // Aggregate context stats across tiers
          const existing = contextMap.get(ctxName);
          if (existing) {
            existing.fileCount += ctxFileCount;
            // Only count paths from raw tier to avoid double-counting
            if (tierName === 'raw') {
              existing.pathCount += ctxPaths.size;
            }
          } else {
            contextMap.set(ctxName, {
              pathCount: tierName === 'raw' ? ctxPaths.size : 0,
              fileCount: ctxFileCount,
            });
          }
        }

        tiers.push({ tier: tierName, fileCount: tierFileCount });
        totalFiles += tierFileCount;
      }

      // Build contexts array and totals
      for (const [name, data] of contextMap) {
        contexts.push({
          name,
          pathCount: data.pathCount,
          fileCount: data.fileCount,
        });
        totalPaths += data.pathCount;
      }
      contexts.sort((a, b) => b.pathCount - a.pathCount);

      return res.json({
        success: true,
        stats: {
          totalContexts: contexts.length,
          totalPaths,
          totalFiles,
          earliestDate: earliestDate ? earliestDate.toISOString() : null,
          latestDate: latestDate ? latestDate.toISOString() : null,
          contexts,
          tiers,
        },
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error:
          error instanceof Error ? error.message : 'Failed to get store stats',
      });
    }
  });

  // ===========================================
  // SQLITE BUFFER API ROUTES
  // ===========================================

  // Get buffer statistics
  router.get('/api/buffer/stats', (_req, res) => {
    try {
      if (!state.sqliteBuffer) {
        return res.json({
          success: true,
          enabled: false,
          message: 'SQLite buffer is not enabled',
          error: state.sqliteBufferError || undefined,
        });
      }

      const stats = state.sqliteBuffer.getStats();
      const exportStatus = state.exportService?.getStatus();

      return res.json({
        success: true,
        enabled: true,
        stats,
        exportService: exportStatus,
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // Force export pending records
  router.post('/api/buffer/export', async (_req, res) => {
    try {
      if (!state.exportService) {
        return res.status(400).json({
          success: false,
          error: 'SQLite buffer is not enabled',
        });
      }

      const result = await state.exportService.forceExport();
      return res.json({
        success: true,
        ...result,
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // Buffer health check
  router.get('/api/buffer/health', (_req, res) => {
    try {
      if (!state.exportService) {
        return res.json({
          success: true,
          enabled: false,
          healthy: true,
          message: 'SQLite buffer is not enabled (using in-memory buffer)',
        });
      }

      const health = state.exportService.getHealth();
      return res.json({
        success: true,
        enabled: true,
        ...health,
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // ===========================================
  // MIGRATION API ROUTES
  // ===========================================

  const migrationService = new MigrationService(app);

  // Scan for files to migrate
  router.post('/api/migrate/scan', async (req, res) => {
    try {
      const { sourceDirectory } = req.body;
      const source = sourceDirectory || state.getDataDirPath();

      const result = await migrationService.scan(source);

      // Convert Map to array for JSON serialization
      const byPathArray = Array.from(result.byPath.entries()).map(
        ([path, stats]) => ({
          path,
          ...stats,
        })
      );

      return res.json({
        success: true,
        totalFiles: result.totalFiles,
        totalSize: result.totalSize,
        totalSizeMB: (result.totalSize / 1024 / 1024).toFixed(2),
        byPath: byPathArray,
        estimatedTimeSeconds: result.estimatedTime,
        sourceStyle: result.sourceStyle,
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // Start migration
  router.post('/api/migrate', async (req, res) => {
    try {
      const {
        sourceDirectory,
        targetDirectory,
        targetTier = 'raw',
        deleteSource = false,
        triggerAggregation = true,
      } = req.body;

      const source = sourceDirectory || state.getDataDirPath();
      const target = targetDirectory || state.getDataDirPath();

      // Validate tier
      const validTiers: AggregationTier[] = ['raw', '5s', '60s', '1h'];
      if (!validTiers.includes(targetTier)) {
        return res.status(400).json({
          success: false,
          error: `Invalid tier: ${targetTier}. Must be one of: ${validTiers.join(', ')}`,
        });
      }

      const jobId = await migrationService.migrate({
        sourceDirectory: source,
        targetDirectory: target,
        targetTier,
        deleteSourceAfterMigration: deleteSource,
        triggerAggregation,
      });

      return res.json({
        success: true,
        jobId,
        message: 'Migration started',
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // Get migration progress
  router.get('/api/migrate/progress/:jobId', (req, res) => {
    try {
      const { jobId } = req.params;
      const progress = migrationService.getProgress(jobId);

      if (!progress) {
        return res.status(404).json({
          success: false,
          error: 'Migration job not found',
        });
      }

      return res.json({
        success: true,
        ...progress,
        bytesProcessedMB: (progress.bytesProcessed / 1024 / 1024).toFixed(2),
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // Cancel migration
  router.post('/api/migrate/cancel/:jobId', (req, res) => {
    try {
      const { jobId } = req.params;
      const cancelled = migrationService.cancel(jobId);

      if (!cancelled) {
        return res.status(400).json({
          success: false,
          error: 'Migration job not found or not running',
        });
      }

      return res.json({
        success: true,
        message: 'Cancellation requested',
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // List all migration jobs
  router.get('/api/migrate/jobs', (_req, res) => {
    try {
      const jobIds = migrationService.getJobIds();
      const jobs = jobIds
        .map(id => migrationService.getProgress(id))
        .filter(Boolean);

      return res.json({
        success: true,
        jobs,
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // ===========================================
  // AGGREGATION API ROUTES
  // ===========================================

  // Initialize aggregation service
  const aggregationService = new AggregationService(
    {
      outputDirectory: state.getDataDirPath(),
      filenamePrefix: state.currentConfig?.filenamePrefix || 'signalk_data',
      retentionDays: {
        raw: state.currentConfig?.retentionDays || 7,
        '5s': (state.currentConfig?.retentionDays || 7) * 2,
        '60s': (state.currentConfig?.retentionDays || 7) * 4,
        '1h': (state.currentConfig?.retentionDays || 7) * 12,
      },
    },
    app
  );

  // Run aggregation for a specific date
  router.post('/api/aggregate', async (req, res) => {
    try {
      const { date } = req.body;

      // Use yesterday if no date provided
      const targetDate = date
        ? new Date(date)
        : new Date(Date.now() - 24 * 60 * 60 * 1000);

      if (isNaN(targetDate.getTime())) {
        return res.status(400).json({
          success: false,
          error: 'Invalid date format',
        });
      }

      app.debug(
        `Starting aggregation for ${targetDate.toISOString().slice(0, 10)}`
      );

      const results = await aggregationService.aggregateDate(targetDate);

      return res.json({
        success: true,
        date: targetDate.toISOString().slice(0, 10),
        results: results.map(r => ({
          sourceTier: r.sourceTier,
          targetTier: r.targetTier,
          filesProcessed: r.filesProcessed,
          recordsAggregated: r.recordsAggregated,
          filesCreated: r.filesCreated,
          durationMs: r.duration,
          errors: r.errors,
        })),
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // Run aggregation for a specific tier only
  router.post('/api/aggregate/:sourceTier/:targetTier', async (req, res) => {
    try {
      const { sourceTier, targetTier } = req.params;
      const { date } = req.body;

      const validTiers: AggregationTier[] = ['raw', '5s', '60s', '1h'];
      if (
        !validTiers.includes(sourceTier as AggregationTier) ||
        !validTiers.includes(targetTier as AggregationTier)
      ) {
        return res.status(400).json({
          success: false,
          error: `Invalid tier. Must be one of: ${validTiers.join(', ')}`,
        });
      }

      const targetDate = date
        ? new Date(date)
        : new Date(Date.now() - 24 * 60 * 60 * 1000);

      if (isNaN(targetDate.getTime())) {
        return res.status(400).json({
          success: false,
          error: 'Invalid date format',
        });
      }

      app.debug(
        `Starting aggregation ${sourceTier} -> ${targetTier} for ${targetDate.toISOString().slice(0, 10)}`
      );

      const result = await aggregationService.aggregateTier(
        sourceTier as AggregationTier,
        targetTier as AggregationTier,
        targetDate
      );

      return res.json({
        success: true,
        date: targetDate.toISOString().slice(0, 10),
        sourceTier: result.sourceTier,
        targetTier: result.targetTier,
        filesProcessed: result.filesProcessed,
        recordsAggregated: result.recordsAggregated,
        filesCreated: result.filesCreated,
        durationMs: result.duration,
        errors: result.errors,
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // Cleanup old aggregated data
  router.post('/api/aggregate/cleanup', async (_req, res) => {
    try {
      const result = await aggregationService.cleanupOldData();

      return res.json({
        success: true,
        deletedFiles: result.deletedFiles,
        freedMB: (result.freedBytes / 1024 / 1024).toFixed(2),
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // ===========================================
  // BULK AGGREGATION API ROUTES
  // ===========================================

  // Start bulk aggregation for all dates in tier=raw
  router.post('/api/aggregate/bulk', async (req, res) => {
    try {
      const { startDate, endDate } = req.body;

      const start = startDate ? new Date(startDate) : undefined;
      const end = endDate ? new Date(endDate) : undefined;

      if (start && isNaN(start.getTime())) {
        return res.status(400).json({
          success: false,
          error: 'Invalid startDate format',
        });
      }
      if (end && isNaN(end.getTime())) {
        return res.status(400).json({
          success: false,
          error: 'Invalid endDate format',
        });
      }

      const jobId = aggregationService.startBulkAggregation(start, end);

      return res.json({
        success: true,
        jobId,
        message: 'Bulk aggregation started',
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // Get bulk aggregation progress
  router.get('/api/aggregate/bulk/:jobId', (req, res) => {
    try {
      const { jobId } = req.params;
      const progress = aggregationService.getBulkProgress(jobId);

      if (!progress) {
        return res.status(404).json({
          success: false,
          error: 'Bulk aggregation job not found',
        });
      }

      return res.json({
        success: true,
        ...progress,
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // Cancel bulk aggregation
  router.post('/api/aggregate/bulk/cancel/:jobId', (req, res) => {
    try {
      const { jobId } = req.params;
      const cancelled = aggregationService.cancelBulk(jobId);

      return res.json({
        success: cancelled,
        message: cancelled
          ? 'Cancel requested'
          : 'Job not found or not running',
      });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // ===========================================
  // VECTOR AVERAGING MIGRATION API ROUTES
  // ===========================================

  const vectorAvgJobs = new Map<
    string,
    {
      id: string;
      status: 'running' | 'completed' | 'cancelled' | 'error';
      dryRun: boolean;
      angularPaths: string[];
      datesProcessed: number;
      totalDates: number;
      startTime: Date;
      completedAt?: Date;
      error?: string;
      results: Array<{
        date: string;
        tiersReaggregated: number;
        errors: string[];
      }>;
    }
  >();

  // Start vector averaging migration job
  router.post('/api/migrate/vector-averaging', async (req, res) => {
    try {
      const { dryRun = false } = req.body || {};
      const jobId = `vecavg_${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;

      const job: {
        id: string;
        status: 'running' | 'completed' | 'cancelled' | 'error';
        dryRun: boolean;
        angularPaths: string[];
        datesProcessed: number;
        totalDates: number;
        startTime: Date;
        completedAt?: Date;
        error?: string;
        results: Array<{
          date: string;
          tiersReaggregated: number;
          errors: string[];
        }>;
      } = {
        id: jobId,
        status: 'running',
        dryRun,
        angularPaths: [],
        datesProcessed: 0,
        totalDates: 0,
        startTime: new Date(),
        results: [],
      };
      vectorAvgJobs.set(jobId, job);

      // Fire async job (not awaited — runs in background)
      (async () => {
        try {
          const dataDir = state.getDataDirPath();
          const aggregatedTiers: AggregationTier[] = ['5s', '60s', '1h'];

          // Scan for angular paths in aggregated tiers
          const angularPathsFound = new Set<string>();
          const datesToProcess = new Set<string>();

          for (const tier of aggregatedTiers) {
            const tierDir = path.join(dataDir, `tier=${tier}`);
            if (!(await fs.pathExists(tierDir))) continue;

            const contextDirs = await glob(path.join(tierDir, 'context=*'));
            for (const contextDir of contextDirs) {
              const context = path
                .basename(contextDir)
                .replace('context=', '')
                .replace(/__/g, '.');
              const pathDirs = await glob(path.join(contextDir, 'path=*'));
              for (const pathDir of pathDirs) {
                const signalkPath = path
                  .basename(pathDir)
                  .replace('path=', '')
                  .replace(/__/g, '.');
                if (isAngularPath(signalkPath, app, context)) {
                  angularPathsFound.add(signalkPath);
                  // Find dates with data for this path
                  const yearDirs = await glob(path.join(pathDir, 'year=*'));
                  for (const yearDir of yearDirs) {
                    const dayDirs = await glob(path.join(yearDir, 'day=*'));
                    for (const dayDir of dayDirs) {
                      const yearStr = path
                        .basename(yearDir)
                        .replace('year=', '');
                      const dayStr = path.basename(dayDir).replace('day=', '');
                      datesToProcess.add(`${yearStr}-${dayStr}`);
                    }
                  }
                }
              }
            }
          }

          job.angularPaths = Array.from(angularPathsFound);
          job.totalDates = datesToProcess.size;

          app.debug(
            `Vector averaging migration: found ${angularPathsFound.size} angular paths, ${datesToProcess.size} dates to process`
          );

          if (dryRun) {
            job.status = 'completed';
            job.completedAt = new Date();
            return;
          }

          // Re-aggregate each date (this will now use vector averaging for angular paths)
          for (const dateKey of datesToProcess) {
            if (job.status === 'cancelled') break;

            const [yearStr, dayStr] = dateKey.split('-');
            const year = parseInt(yearStr);
            const dayOfYear = parseInt(dayStr);
            const date = new Date(Date.UTC(year, 0, dayOfYear));

            try {
              const results = await aggregationService.aggregateDate(date);
              const errors = results.flatMap(r => r.errors);
              const tiersReaggregated = results.filter(
                r => r.filesCreated > 0
              ).length;
              job.results.push({
                date: date.toISOString().slice(0, 10),
                tiersReaggregated,
                errors,
              });
            } catch (error) {
              job.results.push({
                date: date.toISOString().slice(0, 10),
                tiersReaggregated: 0,
                errors: [(error as Error).message],
              });
            }

            job.datesProcessed++;
          }

          job.status = 'completed';
          job.completedAt = new Date();
        } catch (error) {
          job.status = 'error';
          job.error = (error as Error).message;
          job.completedAt = new Date();
        }
      })();

      return res.json({ success: true, jobId, dryRun });
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: (error as Error).message,
      });
    }
  });

  // Poll vector averaging migration progress
  router.get('/api/migrate/vector-averaging/:jobId', (req, res) => {
    const job = vectorAvgJobs.get(req.params.jobId);
    if (!job) {
      return res.status(404).json({ success: false, error: 'Job not found' });
    }
    return res.json({
      success: true,
      ...job,
    });
  });

  // Cancel vector averaging migration
  router.post('/api/migrate/vector-averaging/cancel/:jobId', (req, res) => {
    const job = vectorAvgJobs.get(req.params.jobId);
    if (!job || job.status !== 'running') {
      return res
        .status(400)
        .json({ success: false, error: 'Job not found or not running' });
    }
    job.status = 'cancelled';
    job.completedAt = new Date();
    return res.json({ success: true, jobId: job.id });
  });
}
