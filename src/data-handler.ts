import * as fs from 'fs-extra';
import * as path from 'path';
import { glob as globOriginal } from 'glob';
import { promisify } from 'util';

// Wrap glob for compatibility with both glob@7.x (callbacks) and glob@11.x (promises)
const glob = async (pattern: string, options?: object): Promise<string[]> => {
  // If glob returns a Promise (glob@11.x), use it directly
  const result = globOriginal(pattern, options || {}) as unknown;
  if (result && typeof (result as Promise<string[]>).then === 'function') {
    return result as Promise<string[]>;
  }
  // Otherwise use promisify for glob@7.x callback style
  const globPromise = promisify(
    globOriginal as (
      pattern: string,
      options: object,
      cb: (err: Error | null, matches: string[]) => void
    ) => void
  );
  return globPromise(pattern, options || {}) as Promise<string[]>;
};
import {
  PluginConfig,
  PathConfig,
  DataRecord,
  PluginState,
  NormalizedDelta,
} from './types';
import { extractCommandName } from './commands';
import {
  Context,
  Delta,
  hasValues,
  Path,
  PathValue,
  ServerAPI,
  Update,
} from '@signalk/server-api';

// AWS S3 for file upload
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let S3Client: any,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  PutObjectCommand: any,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ListObjectsV2Command: any;

let appInstance: ServerAPI;

// Generic cloud target for S3-compatible uploads (S3, R2, etc.)
export interface CloudTarget {
  client: any;
  bucket: string;
  keyPrefix: string;
  deleteAfterUpload: boolean;
  label: string; // "S3" or "R2" for logging
}

export async function initializeCloudSDK(
  config: PluginConfig,
  app: ServerAPI
): Promise<void> {
  appInstance = app;

  if (config.cloudUpload.provider !== 'none') {
    try {
      if (!S3Client) {
        const awsS3 = await import('@aws-sdk/client-s3');
        S3Client = awsS3.S3Client;
        PutObjectCommand = awsS3.PutObjectCommand;
        ListObjectsV2Command = awsS3.ListObjectsV2Command;
      }
    } catch (importError) {
      S3Client = undefined;
    }
  }
}

// Legacy alias
export const initializeS3 = initializeCloudSDK;

export function createCloudClient(config: PluginConfig, app: ServerAPI): any {
  const cloud = config.cloudUpload;
  if (cloud.provider === 'none' || !S3Client) {
    return undefined;
  }

  try {
    if (cloud.provider === 'r2') {
      if (!cloud.accountId) {
        app.error('R2 upload enabled but no account ID configured');
        return undefined;
      }
      return new S3Client({
        region: 'auto',
        endpoint: `https://${cloud.accountId}.r2.cloudflarestorage.com`,
        credentials:
          cloud.accessKeyId && cloud.secretAccessKey
            ? {
                accessKeyId: cloud.accessKeyId,
                secretAccessKey: cloud.secretAccessKey,
              }
            : undefined,
      });
    }

    // S3 provider
    const s3Config: {
      region: string;
      credentials?: { accessKeyId: string; secretAccessKey: string };
    } = {
      region: cloud.region || 'us-east-1',
    };

    if (cloud.accessKeyId && cloud.secretAccessKey) {
      s3Config.credentials = {
        accessKeyId: cloud.accessKeyId,
        secretAccessKey: cloud.secretAccessKey,
      };
    }

    return new S3Client(s3Config);
  } catch (error) {
    app.error(
      `Failed to create ${cloud.provider.toUpperCase()} client: ${(error as Error).message}`
    );
    return undefined;
  }
}

// Legacy aliases
export const createS3Client = createCloudClient;
export const createR2Client = createCloudClient;

// Build cloud target from config and state
export function getCloudTarget(
  config: PluginConfig,
  state: PluginState
): CloudTarget | null {
  const cloud = config.cloudUpload;
  if (cloud.provider === 'none' || !state.cloudClient) {
    return null;
  }

  return {
    client: state.cloudClient,
    bucket: cloud.bucket || '',
    keyPrefix: cloud.keyPrefix || '',
    deleteAfterUpload: cloud.deleteAfterUpload || false,
    label: cloud.provider.toUpperCase(),
  };
}

// Subscribe to command paths that control regimens using proper subscription manager
export function subscribeToCommandPaths(
  currentPaths: PathConfig[],
  state: PluginState,
  config: PluginConfig,
  app: ServerAPI
): void {
  const commandPaths = currentPaths.filter(
    (pathConfig: PathConfig) =>
      pathConfig &&
      pathConfig.path &&
      pathConfig.path.startsWith('commands.') &&
      pathConfig.enabled
  );

  if (commandPaths.length === 0) return;

  // Create Map for O(1) lookup instead of O(n) .find()
  // This eliminates O(n²) nested loop on every delta message
  const commandPathsMap = new Map<string, PathConfig>(
    commandPaths.map(pathConfig => [pathConfig.path, pathConfig])
  );

  const commandSubscription = {
    context: 'vessels.self' as Context,
    subscribe: commandPaths.map((pathConfig: PathConfig) => ({
      path: pathConfig.path,
      period: 1000, // Check commands every second
      policy: 'fixed' as const,
    })),
  };

  app.subscriptionmanager.subscribe(
    commandSubscription,
    state.unsubscribes,
    (subscriptionError: unknown) => {},
    (delta: Delta) => {
      // Process each update in the delta
      if (delta.updates) {
        delta.updates.forEach((update: Update) => {
          if (hasValues(update)) {
            update.values.forEach((valueUpdate: PathValue) => {
              // O(1) Map lookup instead of O(n) array.find()
              const pathConfig = commandPathsMap.get(valueUpdate.path);
              if (pathConfig) {
                handleCommandMessage(
                  valueUpdate,
                  pathConfig,
                  config,
                  update,
                  state,
                  app
                );
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
  update: Update,
  state: PluginState,
  app: ServerAPI
): void {
  try {
    // Check source filter if specified for commands too
    if (pathConfig.source && pathConfig.source.trim() !== '') {
      const messageSource =
        update.$source || (update.source ? update.source.label : null);
      if (messageSource !== pathConfig.source.trim()) {
        return;
      }
    }

    if (valueUpdate.value !== undefined) {
      const commandName = extractCommandName(pathConfig.path);
      const isActive = Boolean(valueUpdate.value);

      if (isActive) {
        state.activeRegimens.add(commandName);
      } else {
        state.activeRegimens.delete(commandName);
      }

      // Debug active regimens state

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
          source: update.source || undefined, // Store as object, serialize at write time
          source_label:
            update.$source || (update.source ? update.source.label : undefined),
          source_type: update.source ? update.source.type : undefined,
          source_pgn: update.source ? update.source.pgn : undefined,
          source_src: update.source ? update.source.src : undefined,
        },
        config,
        state,
        app
      );
    }
  } catch (error) {}
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
  pathConfig: PathConfig,
  app: ServerAPI
): boolean {
  if (!pathConfig.excludeMMSI || pathConfig.excludeMMSI.length === 0) {
    return false; // No exclusions specified
  }

  try {
    // For vessels.self, use getSelfPath
    // Cast to any for compatibility with different @signalk/server-api versions
    if (vesselContext === 'vessels.self') {
      const mmsiData = app.getSelfPath('mmsi') as any;
      if (mmsiData && mmsiData.value) {
        const mmsi = String(mmsiData.value);
        return pathConfig.excludeMMSI.includes(mmsi);
      }
    } else {
      // For other vessels, we would need to get their MMSI from the delta or other means
      // For now, we'll skip MMSI filtering for other vessels
    }
  } catch (error) {}

  return false; // Don't exclude if we can't determine MMSI
}

// Update data path subscriptions based on active regimens
export function updateDataSubscriptions(
  currentPaths: PathConfig[],
  state: PluginState,
  config: PluginConfig,
  app: ServerAPI
): void {
  // First, unsubscribe from all existing subscriptions
  state.unsubscribes.forEach(unsubscribe => {
    if (typeof unsubscribe === 'function') {
      unsubscribe();
    }
  });
  state.unsubscribes = [];
  state.subscribedPaths.clear();

  // Re-subscribe to command paths
  subscribeToCommandPaths(currentPaths, state, config, app);

  // Now subscribe to data paths using currentPaths
  const dataPaths = currentPaths.filter(
    (pathConfig: PathConfig) =>
      pathConfig && pathConfig.path && !pathConfig.path.startsWith('commands.')
  );

  const shouldSubscribePaths = dataPaths.filter((pathConfig: PathConfig) =>
    shouldSubscribeToPath(pathConfig, state, app)
  );

  // Handle wildcard contexts (like vessels.*)
  const processedPaths: PathConfig[] = shouldSubscribePaths.map(pathConfig =>
    handleWildcardContext(pathConfig)
  );

  if (processedPaths.length === 0) {
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

  // Context filter helper — reused for both normal and root-level paths
  function passesContextFilter(
    normalizedDelta: NormalizedDelta,
    pathConfig: PathConfig
  ): boolean {
    // Filter by source if specified
    if (pathConfig.source && pathConfig.source.trim() !== '') {
      if (normalizedDelta.$source !== pathConfig.source.trim()) return false;
    }

    // Filter by context
    const targetContext = pathConfig.context || 'vessels.self';
    if (targetContext === 'vessels.*') {
      if (!normalizedDelta.context.startsWith('vessels.')) return false;
    } else if (targetContext === 'vessels.self') {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const selfContext = app.selfContext;
      const selfVessel = (app.getSelfPath('') as any) || {};
      const selfMMSI = selfVessel.mmsi;
      const selfUuid = app.getSelfPath('uuid') as any;

      const isSelfVessel =
        normalizedDelta.context === 'vessels.self' ||
        normalizedDelta.context === selfContext ||
        (selfMMSI && normalizedDelta.context.includes(selfMMSI)) ||
        (selfUuid && normalizedDelta.context.includes(selfUuid));

      if (!isSelfVessel) return false;
    } else {
      if (normalizedDelta.context !== targetContext) return false;
    }

    // MMSI exclusion filtering
    if (pathConfig.excludeMMSI && pathConfig.excludeMMSI.length > 0) {
      if (
        pathConfig.excludeMMSI.some(mmsi =>
          normalizedDelta.context.includes(mmsi)
        )
      )
        return false;
    }

    // Skip meta deltas
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    if ((normalizedDelta as any).isMeta) return false;

    return true;
  }

  // Use app.streambundle approach as recommended by SignalK developer
  // This avoids server arbitration and provides true source filtering
  contextGroups.forEach((pathConfigs, context) => {
    // Root-level paths (no dots, e.g., "name", "mmsi") arrive on the root bus ("")
    // as part of a bundled object update, not as individual path deltas
    const rootPaths = pathConfigs.filter(
      (pc: PathConfig) => !pc.path.includes('.')
    );
    const normalPaths = pathConfigs.filter(
      (pc: PathConfig) => pc.path.includes('.')
    );

    // Subscribe to root bus for root-level paths
    if (rootPaths.length > 0) {
      const rootPathMap = new Map<string, PathConfig>();
      rootPaths.forEach((pc: PathConfig) => rootPathMap.set(pc.path, pc));

      const rootStream = app.streambundle
        .getBus('' as Path)
        .filter((normalizedDelta: NormalizedDelta) => {
          if (
            !normalizedDelta.value ||
            typeof normalizedDelta.value !== 'object'
          )
            return false;
          const valueObj = normalizedDelta.value as Record<string, unknown>;
          return rootPaths.some(
            (pc: PathConfig) => valueObj[pc.path] !== undefined
          );
        })
        .debounceImmediate(5000) // Root properties are static, debounce aggressively
        .onValue((normalizedDelta: NormalizedDelta) => {
          const valueObj = normalizedDelta.value as Record<string, unknown>;
          for (const [pathName, pathConfig] of rootPathMap) {
            if (valueObj[pathName] === undefined) continue;
            if (!passesContextFilter(normalizedDelta, pathConfig)) continue;
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const syntheticDelta = {
              ...normalizedDelta,
              path: pathName,
              value: valueObj[pathName],
            } as any as NormalizedDelta;
            handleStreamData(syntheticDelta, pathConfig, config, state, app);
          }
        });

      state.streamSubscriptions = state.streamSubscriptions || [];
      state.streamSubscriptions.push(rootStream);
      rootPaths.forEach((pc: PathConfig) =>
        state.subscribedPaths.add(pc.path)
      );
    }

    // Normal dotted paths — individual stream per path
    normalPaths.forEach((pathConfig: PathConfig) => {
      const stream = app.streambundle
        .getBus(pathConfig.path as Path)
        .filter((normalizedDelta: NormalizedDelta) =>
          passesContextFilter(normalizedDelta, pathConfig)
        )
        .debounceImmediate(1000)
        .onValue((normalizedDelta: NormalizedDelta) => {
          handleStreamData(normalizedDelta, pathConfig, config, state, app);
        });

      state.streamSubscriptions = state.streamSubscriptions || [];
      state.streamSubscriptions.push(stream);
      state.subscribedPaths.add(pathConfig.path);
    });
  });
}

// Determine if we should subscribe to a path based on regimens
function shouldSubscribeToPath(
  pathConfig: PathConfig,
  state: PluginState,
  app: ServerAPI
): boolean {
  // Always subscribe if explicitly enabled
  if (pathConfig.enabled) {
    return true;
  }

  // Check if any required regimens are active
  if (pathConfig.regimen) {
    const requiredRegimens = pathConfig.regimen.split(',').map(r => r.trim());
    const hasActiveRegimen = requiredRegimens.some(regimen =>
      state.activeRegimens.has(regimen)
    );
    return hasActiveRegimen;
  }

  return false;
}

// New handler for streambundle data (developer's recommended approach)
function handleStreamData(
  normalizedDelta: NormalizedDelta,
  pathConfig: PathConfig,
  config: PluginConfig,
  state: PluginState,
  app: ServerAPI
): void {
  try {
    // Retrieve metadata for this path
    let metadata: object | undefined;
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const pathMetadata = (app as any).getMetadata?.(normalizedDelta.path);
      if (pathMetadata) {
        metadata = pathMetadata; // Store as object, serialize at write time
      }
    } catch (error) {
      // Metadata retrieval failed, continue without it
    }

    const record: DataRecord = {
      received_timestamp: new Date().toISOString(),
      signalk_timestamp: normalizedDelta.timestamp || new Date().toISOString(),
      context: normalizedDelta.context || pathConfig.context || 'vessels.self',
      path: normalizedDelta.path,
      value: null,
      value_json: undefined,
      source: normalizedDelta.source || undefined, // Store as object, serialize at write time
      source_label: normalizedDelta.$source || undefined,
      source_type: normalizedDelta.source
        ? normalizedDelta.source.type
        : undefined,
      source_pgn: normalizedDelta.source
        ? normalizedDelta.source.pgn
        : undefined,
      source_src: normalizedDelta.source
        ? normalizedDelta.source.src
        : undefined,
      meta: metadata,
    };

    // Handle complex values
    if (
      typeof normalizedDelta.value === 'object' &&
      normalizedDelta.value !== null
    ) {
      const valueObj = normalizedDelta.value as Record<string, unknown>;
      const objKeys = Object.keys(valueObj);

      // Skip if this looks like a meta-only update (only has units, meta, description keys)
      // These are metadata updates, not actual data values
      const metaOnlyKeys = [
        'units',
        'meta',
        'description',
        'displayUnits',
        'zones',
        'timeout',
      ];
      const isMetaOnly =
        objKeys.length > 0 && objKeys.every(k => metaOnlyKeys.includes(k));

      if (isMetaOnly) {
        // This is a metadata update, not real data - skip it
        return;
      }

      record.value_json = normalizedDelta.value; // Store as object, serialize at write time
      // Extract key properties as columns for easier querying
      Object.entries(normalizedDelta.value).forEach(([key, val]) => {
        if (
          typeof val === 'string' ||
          typeof val === 'number' ||
          typeof val === 'boolean'
        ) {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          (record as any)[`value_${key}`] = val;
        }
      });
    } else {
      record.value = normalizedDelta.value;
    }

    // Use actual context + path as buffer key to separate data from different vessels
    const bufferKey = `${normalizedDelta.context}:${pathConfig.path}`;
    bufferData(bufferKey, record, config, state, app);
  } catch (error) {}
}

// Buffer data and trigger save if buffer is full
function bufferData(
  signalkPath: string,
  record: DataRecord,
  config: PluginConfig,
  state: PluginState,
  app: ServerAPI
): void {
  // Use SQLite buffer if enabled
  if (config.useSqliteBuffer) {
    if (!state.sqliteBuffer) {
      app.error(
        `[DataHandler] SQLite buffer is enabled but not initialized! Data for ${signalkPath} will be lost.`
      );
      return;
    }
    if (!state.sqliteBuffer.isOpen()) {
      app.error(
        `[DataHandler] SQLite buffer is closed! Data for ${signalkPath} will be lost.`
      );
      return;
    }
    state.sqliteBuffer.insert(record);
    return;
  }

  // LRU cache only used when SQLite is disabled
  if (!state.dataBuffers.has(signalkPath)) {
    state.dataBuffers.set(signalkPath, []);
  }

  const buffer = state.dataBuffers.get(signalkPath)!;
  buffer.push(record);

  if (buffer.length >= config.bufferSize) {
    // Extract the actual SignalK path from the buffer key (context:path format)
    // Find the separator between context and path - look for the last colon followed by a valid SignalK path
    const pathMatch = signalkPath.match(/^.*:([a-zA-Z][a-zA-Z0-9._]*)$/);
    const actualPath = pathMatch ? pathMatch[1] : signalkPath;
    const urnMatch = signalkPath.match(/^([^:]+):/);
    const urn = urnMatch ? urnMatch[1] : 'vessels.self';
    saveBufferToParquet(actualPath, buffer, config, state, app);
    state.dataBuffers.set(signalkPath, []); // Clear buffer
  }
}

// Save all buffers (called periodically and on shutdown)
export function saveAllBuffers(
  config: PluginConfig,
  state: PluginState,
  app: ServerAPI
): void {
  state.dataBuffers.forEach((buffer, signalkPath) => {
    if (buffer.length > 0) {
      // Extract the actual SignalK path from the buffer key (context:path format)
      // Find the separator between context and path - look for the last colon followed by a valid SignalK path
      const pathMatch = signalkPath.match(/^.*:([a-zA-Z][a-zA-Z0-9._]*)$/);
      const actualPath = pathMatch ? pathMatch[1] : signalkPath;
      const urnMatch = signalkPath.match(/^([^:]+):/);
      const urn = urnMatch ? urnMatch[1] : 'vessels.self';
      saveBufferToParquet(actualPath, buffer, config, state, app);
      state.dataBuffers.delete(signalkPath); // Delete buffer to free memory
    }
  });
}

// Save buffer to Parquet file
async function saveBufferToParquet(
  signalkPath: string,
  buffer: DataRecord[],
  config: PluginConfig,
  state: PluginState,
  app: ServerAPI
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
    const savedPath = await state.parquetWriter!.writeRecords(filepath, buffer);
  } catch (error) {}
}

// Initialize regimen states from current API values at startup
export function initializeRegimenStates(
  currentPaths: PathConfig[],
  state: PluginState,
  app: ServerAPI
): void {
  const commandPaths = currentPaths.filter(
    (pathConfig: PathConfig) =>
      pathConfig &&
      pathConfig.path &&
      pathConfig.path.startsWith('commands.') &&
      pathConfig.enabled
  );

  commandPaths.forEach((pathConfig: PathConfig) => {
    try {
      // Get current value from SignalK API
      // Cast to any for compatibility with different @signalk/server-api versions
      const currentData = app.getSelfPath(pathConfig.path) as any;

      if (currentData !== undefined && currentData !== null) {
        // Check if there's source information
        const shouldProcess = true;

        // If source filter is specified, check it
        if (pathConfig.source && pathConfig.source.trim() !== '') {
          // For startup, we need to check the API source info
          // This is a simplified check - in real deltas we get more source info
          // For now, we'll process the value if it exists and log a warning
          // In practice, you might want to check the source here too
        }

        if (shouldProcess && currentData.value !== undefined) {
          const commandName = extractCommandName(pathConfig.path);
          const isActive = Boolean(currentData.value);

          if (isActive) {
            state.activeRegimens.add(commandName);
          } else {
            state.activeRegimens.delete(commandName);
          }
        }
      } else {
      }
    } catch (error) {}
  });
}

// REMOVED: consolidateMissedDays() and consolidateYesterday()
// These functions have been replaced by the daily export system in parquet-export-service.ts
// The new exportDayToParquet() creates consolidated daily files directly

// List all existing keys in cloud bucket (paginated)
async function listCloudKeys(
  client: any,
  bucket: string,
  prefix?: string
): Promise<Set<string>> {
  const keys = new Set<string>();
  if (!ListObjectsV2Command) return keys;

  let continuationToken: string | undefined;
  do {
    const response = await client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix || undefined,
        ContinuationToken: continuationToken,
      })
    );
    if (response.Contents) {
      for (const obj of response.Contents) {
        if (obj.Key) keys.add(obj.Key);
      }
    }
    continuationToken = response.IsTruncated
      ? response.NextContinuationToken
      : undefined;
  } while (continuationToken);

  return keys;
}

// Upload a single file to cloud (no HEAD check — caller handles dedup)
async function putToCloud(
  filePath: string,
  target: CloudTarget,
  config: PluginConfig
): Promise<boolean> {
  if (!target.client || !PutObjectCommand) return false;

  try {
    const relativePath = path.relative(config.outputDirectory, filePath);
    let cloudKey = relativePath;
    if (target.keyPrefix) {
      const prefix = target.keyPrefix.endsWith('/')
        ? target.keyPrefix
        : `${target.keyPrefix}/`;
      cloudKey = `${prefix}${relativePath}`;
    }

    const fileContent = await fs.readFile(filePath);
    await target.client.send(
      new PutObjectCommand({
        Bucket: target.bucket,
        Key: cloudKey,
        Body: fileContent,
        ContentType: 'application/octet-stream',
      })
    );

    if (target.deleteAfterUpload) {
      await fs.unlink(filePath);
    }
    return true;
  } catch {
    return false;
  }
}

// Get cloud key for a local file path
function getCloudKey(filePath: string, target: CloudTarget, config: PluginConfig): string {
  const relativePath = path.relative(config.outputDirectory, filePath);
  if (target.keyPrefix) {
    const prefix = target.keyPrefix.endsWith('/')
      ? target.keyPrefix
      : `${target.keyPrefix}/`;
    return `${prefix}${relativePath}`;
  }
  return relativePath;
}

// Upload missing hive files to cloud with batch concurrency
async function uploadMissingFiles(
  localFiles: string[],
  existingKeys: Set<string>,
  target: CloudTarget,
  config: PluginConfig,
  app: ServerAPI,
  concurrency = 3
): Promise<number> {
  const excludedDirs = ['/processed/', '/repaired/', '/failed/', '/quarantine/'];
  const filtered = localFiles.filter(f => !excludedDirs.some(dir => f.includes(dir)));
  const missing = filtered.filter(
    f => !existingKeys.has(getCloudKey(f, target, config))
  );

  if (missing.length === 0) return 0;

  app.debug(`[CloudSync] ${missing.length} files to upload (${concurrency} concurrent)`);

  let uploaded = 0;
  for (let i = 0; i < missing.length; i += concurrency) {
    const batch = missing.slice(i, i + concurrency);
    const results = await Promise.all(
      batch.map(f => putToCloud(f, target, config))
    );
    uploaded += results.filter(Boolean).length;
    // Yield to event loop between batches
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  return uploaded;
}

// Upload all hive-partitioned parquet files to cloud (7-day lookback)
export async function uploadAllConsolidatedFilesToS3(
  config: PluginConfig,
  state: PluginState,
  app: ServerAPI
): Promise<void> {
  const target = getCloudTarget(config, state);
  if (!target) return;

  try {
    const daysToCheck = 7;
    const today = new Date();

    // Gather local files for the lookback window first (fast, local I/O)
    const allLocalFiles: string[] = [];
    for (let daysAgo = 1; daysAgo <= daysToCheck; daysAgo++) {
      const targetDate = new Date(today);
      targetDate.setUTCDate(today.getUTCDate() - daysAgo);
      const year = targetDate.getUTCFullYear();
      const dayOfYear = String(
        Math.floor(
          (targetDate.getTime() - Date.UTC(year, 0, 1)) / 86400000
        ) + 1
      ).padStart(3, '0');

      const files = await glob(`tier=*/**/year=${year}/day=${dayOfYear}/*.parquet`, {
        cwd: config.outputDirectory,
        absolute: true,
        nodir: true,
      });
      allLocalFiles.push(...files);
    }

    app.debug(`[StartupSync] Found ${allLocalFiles.length} local files in last ${daysToCheck} days`);
    if (allLocalFiles.length === 0) return;

    // List only raw-tier prefixes in R2 to find which context/path/year/day combos are synced
    const prefixSet = new Set<string>();
    const basePrefix = target.keyPrefix
      ? (target.keyPrefix.endsWith('/') ? target.keyPrefix : `${target.keyPrefix}/`)
      : '';
    for (const file of allLocalFiles) {
      const rel = path.relative(config.outputDirectory, file);
      if (!rel.startsWith('tier=raw')) continue;
      const dirPart = path.dirname(rel);
      prefixSet.add(`${basePrefix}${dirPart}/`);
    }

    app.debug(`[StartupSync] Listing ${target.label} objects for ${prefixSet.size} raw-tier prefixes...`);

    // Build set of synced directories (context/path/year/day) from raw tier
    // e.g. "context=X/path=Y/year=2026/day=073"
    const syncedDirs = new Set<string>();
    for (const prefix of prefixSet) {
      const keys = await listCloudKeys(target.client, target.bucket, prefix);
      if (keys.size > 0) {
        // Extract context/path/year/day from the prefix (strip basePrefix and tier=raw/)
        const withoutBase = prefix.startsWith(basePrefix)
          ? prefix.slice(basePrefix.length)
          : prefix;
        // withoutBase = "tier=raw/context=X/path=Y/year=YYYY/day=DDD/"
        const withoutTier = withoutBase.replace(/^tier=[^/]+\//, '');
        syncedDirs.add(withoutTier);
      }
    }
    app.debug(`[StartupSync] Found ${syncedDirs.size} synced directories in ${target.label}`);

    // Filter local files: skip any file whose context/path/year/day is already synced
    const excludedDirs = ['/processed/', '/repaired/', '/failed/', '/quarantine/'];
    const filesToUpload = allLocalFiles.filter(f => {
      if (excludedDirs.some(dir => f.includes(dir))) return false;
      const rel = path.relative(config.outputDirectory, f);
      // Strip tier segment to get context/path/year/day/
      const withoutTier = rel.replace(/^tier=[^/]+\//, '');
      const dirPart = path.dirname(withoutTier) + '/';
      return !syncedDirs.has(dirPart);
    });

    if (filesToUpload.length === 0) {
      app.debug(`[StartupSync] All files already synced`);
      return;
    }

    app.debug(`[CloudSync] ${filesToUpload.length} files to upload (3 concurrent)`);
    let uploaded = 0;
    for (let i = 0; i < filesToUpload.length; i += 3) {
      const batch = filesToUpload.slice(i, i + 3);
      const results = await Promise.all(
        batch.map(f => putToCloud(f, target, config))
      );
      uploaded += results.filter(Boolean).length;
      await new Promise(resolve => setTimeout(resolve, 10));
    }

    if (uploaded > 0) {
      app.debug(
        `[StartupSync] Uploaded ${uploaded} files to ${target.label}`
      );
    }
  } catch (error) {
    app.error(`[StartupSync] Failed: ${(error as Error).message}`);
  }
}

// Upload hive-partitioned parquet files for a specific date to cloud
export async function uploadConsolidatedFilesToS3(
  config: PluginConfig,
  date: Date,
  state: PluginState,
  app: ServerAPI
): Promise<void> {
  const target = getCloudTarget(config, state);
  if (!target) return;

  try {
    const year = date.getUTCFullYear();
    const dayOfYear = String(
      Math.floor(
        (date.getTime() - Date.UTC(year, 0, 1)) / 86400000
      ) + 1
    ).padStart(3, '0');
    const dateStr = date.toISOString().slice(0, 10);

    const localFiles = await glob(`tier=*/**/year=${year}/day=${dayOfYear}/*.parquet`, {
      cwd: config.outputDirectory,
      absolute: true,
      nodir: true,
    });

    if (localFiles.length === 0) return;

    // List existing keys and only upload missing
    const existingKeys = await listCloudKeys(
      target.client,
      target.bucket,
      target.keyPrefix || undefined
    );

    const uploaded = await uploadMissingFiles(
      localFiles,
      existingKeys,
      target,
      config,
      app
    );

    if (uploaded > 0) {
      app.debug(
        `${target.label}: Uploaded ${uploaded} hive files for ${dateStr}`
      );
    }
  } catch (error) {
    app.error(
      `Cloud upload failed for date ${date.toISOString().slice(0, 10)}: ${(error as Error).message}`
    );
  }
}

