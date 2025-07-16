import { Request, Response, Router } from 'express';

// SignalK App Interface
export interface SignalKApp {
  debug: (message: string, ...args: any[]) => void;
  error: (message: string, ...args: any[]) => void;
  getSelfPath: (path: string) => any;
  getDataDirPath: () => string;
  selfContext: string;
  subscriptionmanager: {
    subscribe: (
      subscription: SignalKSubscription,
      unsubscribes: Array<() => void>,
      errorCallback: (error: any) => void,
      deltaCallback: (delta: SignalKDelta) => void
    ) => void;
  };
  savePluginOptions: (options: any, callback: (error?: Error) => void) => void;
  handleMessage: (pluginId: string, delta: SignalKDelta) => void;
}

// SignalK Plugin Interface
export interface SignalKPlugin {
  id: string;
  name: string;
  description: string;
  schema: any;
  start: (options: Partial<PluginConfig>) => void;
  stop: () => void;
  registerWithRouter?: (router: Router) => void;
}

// Plugin Configuration
export interface PluginConfig {
  bufferSize: number;
  saveIntervalSeconds: number;
  outputDirectory: string;
  filenamePrefix: string;
  retentionDays: number;
  fileFormat: 'json' | 'csv' | 'parquet';
  vesselMMSI: string;
  s3Upload: S3UploadConfig;
}

export interface PathConfig {
  path: string;
  name?: string;
  enabled?: boolean;
  regimen?: string;
  source?: string;
  context?: string;
}

// Command Registration Types
export interface CommandConfig {
  command: string;
  path: string;
  registered: string;
  description?: string;
  active?: boolean;
  lastExecuted?: string;
}

export interface CommandRegistrationState {
  registeredCommands: Map<string, CommandConfig>;
  putHandlers: Map<string, CommandPutHandler>;
}

export interface CommandExecutionRequest {
  command: string;
  value: boolean;
  timestamp?: string;
}

export interface CommandRegistrationRequest {
  command: string;
  description?: string;
}

// Web App Configuration (stored separately from plugin config)
export interface WebAppPathConfig {
  paths: PathConfig[];
  commands: CommandConfig[];
}

export interface S3UploadConfig {
  enabled: boolean;
  timing?: 'realtime' | 'consolidation';
  bucket?: string;
  region?: string;
  keyPrefix?: string;
  accessKeyId?: string;
  secretAccessKey?: string;
  deleteAfterUpload?: boolean;
}

// SignalK Data Structures
export interface SignalKSubscription {
  context: string;
  subscribe: Array<{
    path: string;
    period: number;
  }>;
}

export interface SignalKDelta {
  context: string;
  updates: SignalKUpdate[];
}

export interface SignalKUpdate {
  source?: {
    label?: string;
    type?: string;
    pgn?: number;
    src?: string;
  };
  $source?: string;
  timestamp: string;
  values: SignalKValue[];
}

export interface SignalKValue {
  path: string;
  value: any;
  meta?: any;
}

// Data Record Structure
export interface DataRecord {
  received_timestamp: string;
  signalk_timestamp: string;
  context: string;
  path: string;
  value: any;
  value_json?: string;
  source?: string;
  source_label?: string;
  source_type?: string;
  source_pgn?: number;
  source_src?: string;
  meta?: string;
  [key: string]: any; // For flattened object properties
}

// Parquet Writer Options
export interface ParquetWriterOptions {
  format: 'json' | 'csv' | 'parquet';
  app?: SignalKApp;
}

// File System Related
export interface FileInfo {
  name: string;
  path: string;
  size: number;
  modified: string;
}

export interface PathInfo {
  path: string;
  directory: string;
  fileCount: number;
}

// API Response Types
export interface ApiResponse<T = any> {
  success: boolean;
  data?: T;
  error?: string;
  message?: string;
}

// Command API Response Types
export interface CommandApiResponse extends ApiResponse {
  commands?: CommandConfig[];
  command?: CommandConfig;
  count?: number;
}

export interface CommandExecutionResponse extends ApiResponse {
  command?: string;
  value?: boolean;
  executed?: boolean;
  timestamp?: string;
}

export interface PathsApiResponse extends ApiResponse {
  dataDirectory?: string;
  paths?: PathInfo[];
}

export interface FilesApiResponse extends ApiResponse {
  path?: string;
  directory?: string;
  files?: FileInfo[];
}

export interface QueryApiResponse extends ApiResponse {
  query?: string;
  rowCount?: number;
  data?: any[];
}

export interface SampleApiResponse extends ApiResponse {
  path?: string;
  file?: string;
  columns?: string[];
  rowCount?: number;
  data?: any[];
}

export interface ConfigApiResponse extends ApiResponse {
  paths?: PathConfig[];
}

export interface HealthApiResponse extends ApiResponse {
  status?: string;
  timestamp?: string;
  duckdb?: string;
}

export interface S3TestApiResponse extends ApiResponse {
  bucket?: string;
  region?: string;
  keyPrefix?: string;
}

// Express Router Types
export interface TypedRequest<T = any> extends Request {
  body: T;
  params: { [key: string]: string };
  query: { [key: string]: string };
}

export interface TypedResponse<T = any> extends Response {
  json: (body: T) => this;
  status: (code: number) => this;
}

// Internal Plugin State
export interface PluginState {
  unsubscribes: Array<() => void>;
  dataBuffers: Map<string, DataRecord[]>;
  activeRegimens: Set<string>;
  subscribedPaths: Set<string>;
  saveInterval?: NodeJS.Timeout;
  consolidationInterval?: NodeJS.Timeout;
  parquetWriter?: ParquetWriter;
  s3Client?: any;
  currentConfig?: PluginConfig;
  commandState: CommandRegistrationState;
}

// Parquet Writer Class Interface
export interface ParquetWriter {
  writeRecords(filepath: string, records: DataRecord[]): Promise<string>;
  writeJSON(filepath: string, records: DataRecord[]): Promise<string>;
  writeCSV(filepath: string, records: DataRecord[]): Promise<string>;
  writeParquet(filepath: string, records: DataRecord[]): Promise<string>;
  consolidateDaily(outputDirectory: string, date: Date, filenamePrefix: string): Promise<number>;
}

// DuckDB Related Types
export interface DuckDBConnection {
  runAndReadAll(query: string): Promise<DuckDBResult>;
  disconnectSync(): void;
}

export interface DuckDBResult {
  getRowObjects(): any[];
}

export interface DuckDBInstance {
  connect(): Promise<DuckDBConnection>;
}

// S3 Related Types
export interface S3Config {
  region: string;
  credentials?: {
    accessKeyId: string;
    secretAccessKey: string;
  };
}

// Query Request/Response Types
export interface QueryRequest {
  query: string;
}

export interface PathConfigRequest {
  path: string;
  name?: string;
  enabled?: boolean;
  regimen?: string;
  source?: string;
  context?: string;
}

// Command Types
export type CommandPutHandler = (
  context: string,
  path: string,
  value: any,
  callback: (result: CommandExecutionResult) => void
) => void;

export interface CommandExecutionResult {
  state: 'COMPLETED' | 'PENDING' | 'FAILED';
  statusCode?: number;
  message?: string;
  timestamp: string;
}

export interface CommandHistoryEntry {
  command: string;
  action: 'EXECUTE' | 'STOP' | 'REGISTER' | 'UNREGISTER';
  value?: boolean;
  timestamp: string;
  success: boolean;
  error?: string;
}

export enum CommandStatus {
  ACTIVE = 'ACTIVE',
  INACTIVE = 'INACTIVE',
  PENDING = 'PENDING',
  ERROR = 'ERROR'
}

// Utility Types
export type FileFormat = 'json' | 'csv' | 'parquet';
export type UploadTiming = 'realtime' | 'consolidation';
export type BufferKey = string; // Format: "context:path"

// Error Types
export interface PluginError extends Error {
  code?: string;
  details?: any;
}

// Consolidation Types
export interface ConsolidationOptions {
  outputDirectory: string;
  date: Date;
  filenamePrefix: string;
}

export interface ConsolidationResult {
  processedPaths: number;
  consolidatedFiles: string[];
  errors: string[];
}

// Schema Definition Types
export interface ParquetField {
  type: string;
  optional?: boolean;
  repeated?: boolean;
}

export interface ParquetSchema {
  [fieldName: string]: ParquetField;
}

// File Processing Types
export interface ProcessingStats {
  totalBuffers: number;
  buffersWithData: number;
  totalRecords: number;
  processedPaths: string[];
}