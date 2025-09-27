import {
  Context,
  Path,
  ServerAPI,
  NormalizedDelta,
  SourceRef,
} from '@signalk/server-api';

// Re-export SignalK types for convenience
export { NormalizedDelta, SourceRef };
import { Request, Response, Router } from 'express';

// SignalK Plugin Interface
export interface SignalKPlugin {
  id: string;
  name: string;
  description: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
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
  enableStreaming?: boolean; // Enable WebSocket streaming functionality
  claudeIntegration?: ClaudeIntegrationConfig;
}

export interface ClaudeIntegrationConfig {
  enabled: boolean;
  apiKey?: string;
  model?: 'claude-opus-4-1-20250805' | 'claude-opus-4-20250514' | 'claude-sonnet-4-20250514';
  maxTokens?: number;
  temperature?: number;
  autoAnalysis?: {
    daily: boolean;
    anomaly: boolean;
    threshold: number;
  };
  cacheEnabled?: boolean;
  templates?: string[];
}

// Vessel Context Document for Claude AI Analysis
export interface VesselContext {
  vesselInfo: VesselInfo;
  customContext: string;
  lastUpdated: string;
  autoExtracted: boolean;
}

export interface VesselInfo {
  // Basic vessel identification
  name?: string;
  callsign?: string;
  mmsi?: string;
  
  // Physical characteristics
  length?: number;        // Length Overall (LOA) in meters
  beam?: number;          // Beam in meters
  draft?: number;         // Draft in meters
  height?: number;        // Height/air draft in meters
  displacement?: number;  // Weight/displacement in tons
  
  // Vessel classification
  vesselType?: string;    // Type of vessel (sailboat, motorboat, cargo, etc.)
  classification?: string; // Classification society info
  flag?: string;          // Flag state
  
  // Technical specifications
  grossTonnage?: number;
  netTonnage?: number;
  deadWeight?: number;
  
  // Build information
  builder?: string;
  buildYear?: number;
  hullNumber?: string;
  
  // Contact information
  ownerName?: string;
  port?: string;          // Port of registry
  
  // Additional context
  notes?: string;
}

export interface VesselContextExtraction {
  path: string;
  signalkPath: string;
  displayName: string;
  unit?: string;
  category: 'identification' | 'physical' | 'classification' | 'technical' | 'build' | 'contact';
}

export interface PathConfig {
  path: Path;
  name?: string;
  enabled?: boolean;
  regimen?: string;
  source?: string;
  context?: Context;
  excludeMMSI?: string[]; // Array of MMSI numbers to exclude when using vessels.*
}

// Command Registration Types
export interface CommandConfig {
  command: string;
  path: string;
  registered: string;
  description?: string;
  keywords?: string[];      // For Claude context matching
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
  keywords?: string[];
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

// Data Record Structure
export interface DataRecord {
  received_timestamp: string;
  signalk_timestamp: string;
  context: string;
  path: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  value: any;
  value_json?: string;
  source?: string;
  source_label?: string;
  source_type?: string;
  source_pgn?: number;
  source_src?: string;
  meta?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [key: string]: any; // For flattened object properties
}

// Parquet Writer Options
export interface ParquetWriterOptions {
  format: 'json' | 'csv' | 'parquet';
  app?: ServerAPI;
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
// eslint-disable-next-line @typescript-eslint/no-explicit-any
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
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data?: any[];
}

export interface SampleApiResponse extends ApiResponse {
  path?: string;
  file?: string;
  columns?: string[];
  rowCount?: number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
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

export interface ValidationApiResponse extends ApiResponse {
  totalFiles?: number;
  totalVessels?: number;
  correctSchemas?: number;
  violations?: number;
  violationDetails?: string[];
  debugMessages?: string[];
  processedFiles?: number;
  processedVessels?: number;
  progress?: string;
}

// Claude Analysis API Response Types
export interface AnalysisApiResponse extends ApiResponse {
  analysis?: AnalysisResult;
  history?: AnalysisResult[];
  templates?: AnalysisTemplateInfo[];
  usage?: {
    input_tokens: number;
    output_tokens: number;
  };
}

export interface AnalysisResult {
  id: string;
  analysis: string;
  insights: string[];
  recommendations?: string[];
  anomalies?: AnomalyInfo[];
  confidence: number;
  dataQuality: string;
  timestamp: string;
  metadata: AnalysisMetadata;
}

export interface AnomalyInfo {
  timestamp: string;
  value: any;
  expectedRange: { min: number; max: number };
  severity: 'low' | 'medium' | 'high';
  description: string;
  confidence: number;
}

export interface AnalysisMetadata {
  dataPath: string;
  analysisType: string;
  recordCount: number;
  timeRange?: { start: Date; end: Date };
  templateUsed?: string;
}

export interface AnalysisTemplateInfo {
  id: string;
  name: string;
  description: string;
  category: string;
  icon: string;
  complexity: string;
  estimatedTime: string;
  requiredPaths: string[];
}

export interface ClaudeConnectionTestResponse extends ApiResponse {
  model?: string;
  responseTime?: number;
  tokenUsage?: number;
}

// Express Router Types
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface TypedRequest<T = any> extends Request {
  body: T;
  params: { [key: string]: string };
  query: { [key: string]: string };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface TypedResponse<T = any> extends Response {
  json: (body: T) => this;
  status: (code: number) => this;
}

// Internal Plugin State
export interface PluginState {
  unsubscribes: Array<() => void>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  streamSubscriptions?: any[]; // Store streambundle stream references for cleanup
  historicalStreamingService?: any; // HistoricalStreamingService - avoiding circular import
  streamingService?: any; // WebSocket streaming service for runtime control
  streamingEnabled?: boolean; // Runtime control separate from config
  restoredSubscriptions?: Map<string, any>; // Track active subscriptions
  dataBuffers: Map<string, DataRecord[]>;
  activeRegimens: Set<string>;
  subscribedPaths: Set<string>;
  saveInterval?: NodeJS.Timeout;
  consolidationInterval?: NodeJS.Timeout;
  parquetWriter?: ParquetWriter;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
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
  consolidateDaily(
    outputDirectory: string,
    date: Date,
    filenamePrefix: string
  ): Promise<number>;
}

// DuckDB Related Types
export interface DuckDBConnection {
  runAndReadAll(query: string): Promise<DuckDBResult>;
  disconnectSync(): void;
}

export interface DuckDBResult {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
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
  path: Path;
  name?: string;
  enabled?: boolean;
  regimen?: string;
  source?: string;
  context?: Context;
}
//FIXME https://github.com/SignalK/signalk-server/pull/2043
// Command Types
export type CommandPutHandler = (
  context: string,
  path: string,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  value: any,
  callback?: (result: CommandExecutionResult) => void
) => CommandExecutionResult;

export interface CommandExecutionResult {
  state: 'COMPLETED' | 'PENDING' | 'FAILED';
  statusCode?: number;
  message?: string;
  timestamp: string;
}

export interface CommandHistoryEntry {
  command: string;
  action: 'EXECUTE' | 'STOP' | 'REGISTER' | 'UNREGISTER' | 'UPDATE';
  value?: boolean;
  timestamp: string;
  success: boolean;
  error?: string;
}

export enum CommandStatus {
  ACTIVE = 'ACTIVE',
  INACTIVE = 'INACTIVE',
  PENDING = 'PENDING',
  ERROR = 'ERROR',
}

// Utility Types
export type FileFormat = 'json' | 'csv' | 'parquet';
export type UploadTiming = 'realtime' | 'consolidation';
export type BufferKey = string; // Format: "context:path"

// Error Types
export interface PluginError extends Error {
  code?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
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

// Data Analysis Related Types
export interface DataSummary {
  rowCount: number;
  timeRange: { start: Date; end: Date };
  columns: ColumnInfo[];
  statisticalSummary: Record<string, Statistics>;
  dataQuality: DataQualityMetrics;
}

export interface ColumnInfo {
  name: string;
  type: string;
  nullCount: number;
  uniqueCount: number;
  sampleValues: any[];
}

export interface Statistics {
  count: number;
  mean?: number;
  median?: number;
  min?: any;
  max?: any;
  stdDev?: number;
}

export interface DataQualityMetrics {
  completeness: number; // Percentage of non-null values
  consistency: number;  // Data format consistency
  timeliness: number;   // Data freshness
  accuracy: number;     // Estimated data accuracy
}

// File Processing Types
export interface ProcessingStats {
  totalBuffers: number;
  buffersWithData: number;
  totalRecords: number;
  processedPaths: string[];
}
