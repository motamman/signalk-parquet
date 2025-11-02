import * as fs from 'fs-extra';
import * as path from 'path';
import { glob } from 'glob';
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
  ListObjectsV2Command: any,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  HeadObjectCommand: any;

let appInstance: ServerAPI;

export async function initializeS3(config: PluginConfig, app: ServerAPI): Promise<void> {
  appInstance = app;
  
  // Initialize S3 client if enabled
  
  if (config.s3Upload.enabled) {
    // Wait for AWS SDK import to complete
    try {
      if (!S3Client) {
        const awsS3 = await import('@aws-sdk/client-s3');
        S3Client = awsS3.S3Client;
        PutObjectCommand = awsS3.PutObjectCommand;
        ListObjectsV2Command = awsS3.ListObjectsV2Command;
        HeadObjectCommand = awsS3.HeadObjectCommand;
      }
    } catch (importError) {
      S3Client = undefined;
    }
  }
}

export function createS3Client(config: PluginConfig, app: ServerAPI): any {
  if (!config.s3Upload.enabled || !S3Client) {
    return undefined;
  }

  try {
    const s3Config: {
      region: string;
      credentials?: { accessKeyId: string; secretAccessKey: string };
    } = {
      region: config.s3Upload.region || 'us-east-1',
    };

    // Add credentials if provided
    if (
      config.s3Upload.accessKeyId &&
      config.s3Upload.secretAccessKey
    ) {
      s3Config.credentials = {
        accessKeyId: config.s3Upload.accessKeyId,
        secretAccessKey: config.s3Upload.secretAccessKey,
      };
    }

    const s3Client = new S3Client(s3Config);
    return s3Client;
  } catch (error) {
    return undefined;
  }
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
    (subscriptionError: unknown) => {
    },
    (delta: Delta) => {
      // Process each update in the delta
      if (delta.updates) {
        delta.updates.forEach((update: Update) => {
          if (hasValues(update)) {
            update.values.forEach((valueUpdate: PathValue) => {
              // O(1) Map lookup instead of O(n) array.find()
              const pathConfig = commandPathsMap.get(valueUpdate.path);
              if (pathConfig) {
                handleCommandMessage(valueUpdate, pathConfig, config, update, state, app);
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
          source: update.source ? JSON.stringify(update.source) : undefined,
          source_label:
            update.$source ||
            (update.source ? update.source.label : undefined),
          source_type: update.source ? update.source.type : undefined,
          source_pgn: update.source ? update.source.pgn : undefined,
          source_src: update.source ? update.source.src : undefined,
        },
        config,
        state,
        app
      );
    }
  } catch (error) {
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
  pathConfig: PathConfig,
  app: ServerAPI
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
    }
  } catch (error) {
  }

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
      pathConfig &&
      pathConfig.path &&
      !pathConfig.path.startsWith('commands.')
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

  // Use app.streambundle approach as recommended by SignalK developer
  // This avoids server arbitration and provides true source filtering
  contextGroups.forEach((pathConfigs, context) => {

    pathConfigs.forEach((pathConfig: PathConfig) => {
      // Show MMSI exclusion config for troubleshooting
      if (pathConfig.excludeMMSI && pathConfig.excludeMMSI.length > 0) {
      }

      // Create individual stream for each path (developer's recommended approach)
      const stream = app.streambundle
        .getBus(pathConfig.path as Path)
        .filter((normalizedDelta: NormalizedDelta) => {
          // Filter by source if specified
          if (pathConfig.source && pathConfig.source.trim() !== '') {
            const expectedSource = pathConfig.source.trim();
            const actualSource = normalizedDelta.$source;

            if (actualSource !== expectedSource) {
              return false;
            }
          }

          // Filter by context
          const targetContext = pathConfig.context || 'vessels.self';
          if (targetContext === 'vessels.*') {
            // For wildcard, accept any vessel context
            if (!normalizedDelta.context.startsWith('vessels.')) {
              return false;
            }
          } else if (targetContext === 'vessels.self') {
            // For vessels.self, check if this is the server's own vessel
            const selfContext = app.selfContext;
            const selfVessel = app.getSelfPath('') || {};
            const selfMMSI = selfVessel.mmsi;
            const selfUuid = app.getSelfPath('uuid');

            // Check if the context matches the server's self vessel
            let isSelfVessel = false;

            if (normalizedDelta.context === 'vessels.self') {
              isSelfVessel = true;
            } else if (normalizedDelta.context === selfContext) {
              isSelfVessel = true;
            } else if (
              selfMMSI &&
              normalizedDelta.context.includes(selfMMSI)
            ) {
              isSelfVessel = true;
            } else if (
              selfUuid &&
              normalizedDelta.context.includes(selfUuid)
            ) {
              isSelfVessel = true;
            }

            if (!isSelfVessel) {
              return false;
            }
          } else {
            // For specific context, match exactly
            if (normalizedDelta.context !== targetContext) {
              return false;
            }
          }

          // MMSI exclusion filtering
          if (pathConfig.excludeMMSI && pathConfig.excludeMMSI.length > 0) {
            const contextHasExcludedMMSI = pathConfig.excludeMMSI.some(mmsi =>
              normalizedDelta.context.includes(mmsi)
            );
            if (contextHasExcludedMMSI) {
              return false;
            }
          }

          return true;
        })
        .debounceImmediate(1000) // Built-in debouncing as recommended
        .onValue((normalizedDelta: NormalizedDelta) => {
          handleStreamData(normalizedDelta, pathConfig, config, state, app);
        });

      // Store stream reference for cleanup (instead of unsubscribe functions)
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
    let metadata: string | undefined;
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const pathMetadata = (app as any).getMetadata?.(normalizedDelta.path);
      if (pathMetadata) {
        metadata = JSON.stringify(pathMetadata);
      }
    } catch (error) {
      // Metadata retrieval failed, continue without it
    }

    const record: DataRecord = {
      received_timestamp: new Date().toISOString(),
      signalk_timestamp:
        normalizedDelta.timestamp || new Date().toISOString(),
      context:
        normalizedDelta.context || pathConfig.context || 'vessels.self',
      path: normalizedDelta.path,
      value: null,
      value_json: undefined,
      source: normalizedDelta.source
        ? JSON.stringify(normalizedDelta.source)
        : undefined,
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
      record.value_json = JSON.stringify(normalizedDelta.value);
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
  } catch (error) {
  }
}

// Buffer data and trigger save if buffer is full
function bufferData(
  signalkPath: string,
  record: DataRecord,
  config: PluginConfig,
  state: PluginState,
  app: ServerAPI
): void {
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
export function saveAllBuffers(config: PluginConfig, state: PluginState, app: ServerAPI): void {
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
    const savedPath = await state.parquetWriter!.writeRecords(
      filepath,
      buffer
    );

    // Upload to S3 if enabled and timing is real-time
    if (config.s3Upload.enabled && config.s3Upload.timing === 'realtime') {
      await uploadToS3(savedPath, config, state, app);
    }
  } catch (error) {
  }
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
      const currentData = app.getSelfPath(pathConfig.path);

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
    } catch (error) {
    }
  });

}

// Startup consolidation for missed previous days (last 7 days, excludes current day)
export async function consolidateMissedDays(config: PluginConfig, state: PluginState, app: ServerAPI): Promise<void> {
  try {

    // Get list of all date directories that exist
    const outputDir = config.outputDirectory;
    if (!(await fs.pathExists(outputDir))) {
      return;
    }

    // Find all non-consolidated files from the last 1 day (excluding today)
    const today = new Date();
    today.setUTCHours(0, 0, 0, 0);

    const sevenDaysAgo = new Date(today);
    sevenDaysAgo.setUTCDate(today.getUTCDate() - 1);

    const pattern = path.join(outputDir, '**/*.parquet');
    const files = await glob(pattern);

    // Extract dates from files and find days that need consolidation
    const datesNeedingConsolidation = new Set<string>();

    for (const file of files) {
      // Skip already consolidated files
      if (file.includes('_consolidated.parquet')) {
        continue;
      }

      // Extract date from filename (format: signalk_data_2025-07-14T1847.parquet)
      const filename = path.basename(file);
      const dateMatch = filename.match(
        /(\d{4})-(\d{2})-(\d{2})T\d{4}\.parquet$/
      );

      if (dateMatch) {
        const year = parseInt(dateMatch[1]);
        const month = parseInt(dateMatch[2]) - 1; // Month is 0-based
        const day = parseInt(dateMatch[3]);
        const fileDate = new Date(year, month, day);
        fileDate.setUTCHours(0, 0, 0, 0);

        // Only consolidate if file is from before today and within last 7 days
        if (fileDate < today && fileDate >= sevenDaysAgo) {
          const dateStr = `${year}-${month + 1 < 10 ? '0' : ''}${month + 1}-${day < 10 ? '0' : ''}${day}`;
          datesNeedingConsolidation.add(dateStr);
        }
      }
    }

    // Consolidate each missed day
    for (const dateStr of datesNeedingConsolidation) {
      // Parse date string format: 2025-07-14
      const [yearStr, monthStr, dayStr] = dateStr.split('-');
      const date = new Date(
        parseInt(yearStr),
        parseInt(monthStr) - 1,
        parseInt(dayStr)
      );


      const consolidatedCount = await state.parquetWriter!.consolidateDaily(
        config.outputDirectory,
        date,
        config.filenamePrefix
      );


      if (consolidatedCount > 0) {

        // Upload consolidated files to S3 if enabled and timing is consolidation
        if (
          config.s3Upload.enabled &&
          config.s3Upload.timing === 'consolidation'
        ) {
          await uploadConsolidatedFilesToS3(config, date, state, app);
        } else {
        }
      }
    }

    if (datesNeedingConsolidation.size > 0) {
    } else {
    }
  } catch (error) {
    app.error(`Failed to consolidate missed days: ${(error as Error).message}`);
    app.debug(`Consolidation error stack: ${(error as Error).stack}`);
  }
}

// Daily consolidation function
export async function consolidateYesterday(config: PluginConfig, state: PluginState, app: ServerAPI): Promise<void> {
  try {
    const yesterday = new Date();
    yesterday.setUTCDate(yesterday.getUTCDate() - 1);

    const consolidatedCount = await state.parquetWriter!.consolidateDaily(
      config.outputDirectory,
      yesterday,
      config.filenamePrefix
    );


    if (consolidatedCount > 0) {

      // Upload consolidated files to S3 if enabled and timing is consolidation
      if (
        config.s3Upload.enabled &&
        config.s3Upload.timing === 'consolidation'
      ) {
        await uploadConsolidatedFilesToS3(config, yesterday, state, app);
      }
    }
  } catch (error) {
    app.error(`Failed to consolidate yesterday's files: ${(error as Error).message}`);
    app.debug(`Consolidation error stack: ${(error as Error).stack}`);
  }
}

// Upload all existing consolidated files to S3 (for catching up after BigInt fix)
export async function uploadAllConsolidatedFilesToS3(
  config: PluginConfig,
  state: PluginState,
  app: ServerAPI
): Promise<void> {
  try {

    // Find all consolidated parquet files
    const consolidatedPattern = `**/*_consolidated.parquet`;
    const consolidatedFiles = await glob(consolidatedPattern, {
      cwd: config.outputDirectory,
      absolute: true,
      nodir: true,
    });


    let uploadedCount = 0;
    for (const filePath of consolidatedFiles) {
      const success = await uploadToS3(filePath, config, state, app);
      if (success) uploadedCount++;
    }

  } catch (error) {
  }
}

// Upload consolidated files to S3
async function uploadConsolidatedFilesToS3(
  config: PluginConfig,
  date: Date,
  state: PluginState,
  app: ServerAPI
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


    // Upload each consolidated file
    for (const filePath of consolidatedFiles) {
      await uploadToS3(filePath, config, state, app);
    }
  } catch (error) {
  }
}

// S3 upload function
async function uploadToS3(
  filePath: string,
  config: PluginConfig,
  state: PluginState,
  app: ServerAPI
): Promise<boolean> {
  if (!config.s3Upload.enabled || !state.s3Client || !PutObjectCommand) {
    return false;
  }

  try {
    // Generate S3 key first
    const relativePath = path.relative(config.outputDirectory, filePath);
    let s3Key = relativePath;
    if (config.s3Upload.keyPrefix) {
      const prefix = config.s3Upload.keyPrefix.endsWith('/')
        ? config.s3Upload.keyPrefix
        : `${config.s3Upload.keyPrefix}/`;
      s3Key = `${prefix}${relativePath}`;
    }

    // Check if file exists in S3 and compare timestamps
    const localStats = await fs.stat(filePath);
    let shouldUpload = true;

    try {
      if (HeadObjectCommand) {
        const headCommand = new HeadObjectCommand({
          Bucket: config.s3Upload.bucket,
          Key: s3Key,
        });
        const s3Object = await state.s3Client.send(headCommand);

        if (s3Object.LastModified) {
          const s3LastModified = new Date(s3Object.LastModified);
          const localLastModified = new Date(localStats.mtime);

          if (localLastModified <= s3LastModified) {
            shouldUpload = false;
          } else {
          }
        }
      }
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (headError: any) {
      if (
        headError.name === 'NotFound' ||
        headError.$metadata?.httpStatusCode === 404
      ) {
        shouldUpload = true;
      } else {
        shouldUpload = true;
      }
    }

    if (!shouldUpload) {
      return true; // Consider it successful since file is already up to date
    }

    // Read the file
    const fileContent = await fs.readFile(filePath);

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

    // Delete local file if configured
    if (config.s3Upload.deleteAfterUpload) {
      await fs.unlink(filePath);
    }

    return true;
  } catch (error) {
    return false;
  }
}