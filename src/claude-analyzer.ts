import Anthropic from '@anthropic-ai/sdk';
import { ServerAPI } from '@signalk/server-api';
import { DataRecord, PluginState } from './types';
import { VesselContextManager } from './vessel-context';
import { getAvailablePaths } from './utils/path-discovery';
import * as fs from 'fs-extra';
import * as path from 'path';
import { DuckDBInstance } from '@duckdb/node-api';

// Claude AI Integration Types
export interface ClaudeAnalyzerConfig {
  apiKey: string;
  model: 'claude-opus-4-1-20250805' | 'claude-opus-4-20250514' | 'claude-sonnet-4-20250514';
  maxTokens: number;
  temperature: number;
}

export interface AnalysisRequest {
  dataPath: string;
  analysisType: 'summary' | 'anomaly' | 'trend' | 'correlation' | 'custom';
  timeRange?: { start: Date; end: Date };
  customPrompt?: string;
  context?: Record<string, any>;
  aggregationMethod?: string;
  resolution?: string;
  useDatabaseAccess?: boolean;
}

export interface FollowUpRequest {
  conversationId: string;
  question: string;
}

export interface AnomalyDetection {
  timestamp: string;
  value: any;
  expectedRange: { min: number; max: number };
  severity: 'low' | 'medium' | 'high';
  description: string;
  confidence: number;
}

export interface AnalysisResponse {
  id: string;
  analysis: string;
  insights: string[];
  recommendations?: string[];
  anomalies?: AnomalyDetection[];
  confidence: number;
  dataQuality: string;
  timestamp: string;
  usage?: {
    input_tokens: number;
    output_tokens: number;
  };
  metadata: {
    dataPath: string;
    analysisType: string;
    recordCount: number;
    timeRange?: { start: Date; end: Date };
    useDatabaseAccess?: boolean;
  };
}

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

export class ClaudeAnalyzer {
  private client: Anthropic;
  private config: ClaudeAnalyzerConfig;
  private app?: ServerAPI;
  private dataDirectory?: string;
  private vesselContextManager: VesselContextManager;
  private activeConversations: Map<string, Array<any>> = new Map();
  private state?: PluginState;

  constructor(config: ClaudeAnalyzerConfig, app?: ServerAPI, dataDirectory?: string, state?: PluginState) {
    this.config = config;
    this.app = app;
    this.dataDirectory = dataDirectory;
    this.state = state;
    this.vesselContextManager = new VesselContextManager(app, dataDirectory);
    
    if (!config.apiKey) {
      throw new Error('Claude API key is required for analysis functionality');
    }

    this.client = new Anthropic({
      apiKey: config.apiKey,
      defaultHeaders: {
        'anthropic-version': '2023-06-01'
      }
    });
  }

  /**
   * Main analysis method - analyzes data and returns structured insights
   */
  async analyzeData(request: AnalysisRequest): Promise<AnalysisResponse> {
    try {
      this.app?.debug(`Starting Claude analysis: ${request.analysisType} for ${request.dataPath}${request.useDatabaseAccess ? ' (DATABASE ACCESS MODE)' : ' (SAMPLING MODE)'}`);
      
      // Route to appropriate analysis system
      if (request.useDatabaseAccess) {
        return await this.analyzeWithDatabaseAccess(request);
      }
      
      // Legacy system: Prepare data for analysis
      const data = await this.prepareDataForAnalysis(request);
      
      // Build analysis prompt with data structure guidance
      const prompt = this.buildAnalysisPrompt(data, request);
      
      // Call Claude API
      const response = await this.client.messages.create({
        model: this.config.model,
        max_tokens: this.config.maxTokens,
        temperature: this.config.temperature,
        messages: [{
          role: 'user',
          content: prompt
        }]
      });

      // Parse response
      const analysisResult = this.parseAnalysisResponse(response, request, data);
      
      // Save analysis to history
      await this.saveAnalysisToHistory(analysisResult);
      
      this.app?.debug(`Claude analysis completed: ${analysisResult.id}`);
      return analysisResult;
      
    } catch (error) {
      const errorMessage = (error as Error).message;
      this.app?.error(`Claude analysis failed: ${errorMessage}`);
      
      // Prevent recursive error messages
      if (errorMessage.includes('Analysis failed:')) {
        throw error; // Re-throw original error to avoid nesting
      }
      throw new Error(`Analysis failed: ${errorMessage}`);
    }
  }

  /**
   * Quick analysis using predefined templates
   */
  async quickAnalysis(dataPath: string, analysisType: string, timeRange?: { start: Date; end: Date }): Promise<AnalysisResponse> {
    const request: AnalysisRequest = {
      dataPath,
      analysisType: analysisType as any,
      timeRange,
      // No sampleSize needed - using REST API
    };

    return this.analyzeData(request);
  }

  /**
   * Detect anomalies in the data
   */
  async detectAnomalies(dataPath: string, timeRange?: { start: Date; end: Date }): Promise<AnomalyDetection[]> {
    const request: AnalysisRequest = {
      dataPath,
      analysisType: 'anomaly',
      timeRange,
      customPrompt: 'Focus specifically on detecting anomalies and unusual patterns in this maritime data. Return detailed anomaly information.'
    };

    const result = await this.analyzeData(request);
    return result.anomalies || [];
  }

  /**
   * Prepare data for analysis - includes sampling and summarization
   */
  private async prepareDataForAnalysis(request: AnalysisRequest): Promise<any> {
    try {
      let data: any[];
      
      // Load data from parquet files using existing method
      data = await this.loadDataFromPath(request.dataPath, request.timeRange, request.aggregationMethod, request.resolution);
      
      // Generate statistical summary
      const summary = this.generateDataSummary(data);
      
      // Sample data very aggressively for production systems with lots of data
      const maxSamples = data.length > 10000 ? 20 : 50; // Ultra-aggressive for large datasets
      const sampledData = this.sampleDataForAnalysis(data, maxSamples);
      
      return {
        summary,
        sampleData: sampledData,
        originalCount: data.length
      };
      
    } catch (error) {
      this.app?.error(`Failed to prepare data for analysis: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * Load data from parquet files based on path and time range
   */
  private async loadDataFromPath(dataPath: string, timeRange?: { start: Date; end: Date }, aggregationMethod?: string, resolution?: string): Promise<DataRecord[]> {
    try {
      // Use the existing REST API instead of custom query logic  
      const baseUrl = `http://localhost:3000`; // Use default SignalK port
      
      // Construct paths with aggregation method if provided
      // HistoryAPI supports format: "path:aggregateMethod" (e.g., "environment.outside.tempest.observations.solarRadiation:max")
      // For multiple paths, apply aggregation to each path individually
      const pathsWithAggregation = dataPath.split(',').map(path => {
        const trimmedPath = path.trim();
        return aggregationMethod && aggregationMethod !== 'average' 
          ? `${trimmedPath}:${aggregationMethod}`
          : trimmedPath; // 'average' is the default, so no need to specify it
      }).join(',');
      
      // Build query parameters for the history API (only valid parameters)
      const params = new URLSearchParams({
        paths: pathsWithAggregation,
        from: timeRange ? timeRange.start.toISOString() : new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString(),
        to: timeRange ? timeRange.end.toISOString() : new Date().toISOString()
      });
      
      // Set resolution if provided (empty string = auto/let HistoryAPI choose)
      if (resolution && resolution.trim() !== '') {
        params.set('resolution', resolution);
        console.log(`📊 CLAUDE ANALYZER: Using custom resolution: ${resolution}ms`);
      } else {
        console.log(`📊 CLAUDE ANALYZER: Using auto resolution (HistoryAPI will choose optimal bucketing)`);
      }
      
      const url = `${baseUrl}/api/history/values?${params}`;
      console.log(`🌐 CLAUDE ANALYZER: Making REST API call to ${url}`);
      console.log(`📊 CLAUDE ANALYZER: Using paths "${pathsWithAggregation}" ${aggregationMethod ? `with aggregation method "${aggregationMethod}"` : 'with default aggregation'}`);
      
      // Make HTTP request to the history API
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      
      const apiResult = await response.json() as any;
      console.log(`📊 CLAUDE ANALYZER (REST API): Raw API result:`, Object.keys(apiResult));
      
      // Convert API result to DataRecord format
      // API returns: { context, range, values: [{path, method}, ...], data: [[timestamp, value1, value2, ...], ...] }
      const records: DataRecord[] = [];
      
      if (apiResult.data && Array.isArray(apiResult.data) && apiResult.values && Array.isArray(apiResult.values)) {
        console.log(`🔍 CLAUDE ANALYZER: Processing ${apiResult.data.length} data rows with ${apiResult.values.length} value columns`);
        console.log(`🔍 CLAUDE ANALYZER: Value column info:`, apiResult.values);
        
        // Safety limit to prevent stack overflow
        const maxRows = Math.min(apiResult.data.length, 10000);
        const maxCols = Math.min(apiResult.values.length, 20);
        
        for (let rowIndex = 0; rowIndex < maxRows; rowIndex++) {
          const row = apiResult.data[rowIndex];
          
          if (Array.isArray(row) && row.length >= 2) {
            const timestamp = row[0]; // First column is always timestamp
            
            // Process each data column (starting from index 1)
            for (let colIndex = 1; colIndex < row.length && colIndex <= maxCols; colIndex++) {
              const value = row[colIndex];
              const valueInfo = apiResult.values[colIndex - 1]; // values array is 0-indexed
              
              if (rowIndex < 3) {
                console.log(`🔍 Sample row ${rowIndex}, col ${colIndex}: timestamp=${timestamp}, path=${valueInfo?.path}, method=${valueInfo?.method}, value=${value}`);
              }
              
              records.push({
                received_timestamp: timestamp, // Use the actual timestamp from data as-is
                signalk_timestamp: timestamp,   // Use the actual timestamp from data as-is
                path: valueInfo?.path || 'unknown',
                value: typeof value === 'bigint' ? Number(value) : value,
                context: this.app?.selfContext || 'unknown',
                source: 'rest-api',
                source_label: `REST API (${valueInfo?.method || 'default'})`,
                aggregation_method: valueInfo?.method
              } as DataRecord & { aggregation_method?: string });
            }
          }
        }
      } else {
        console.log(`🔍 CLAUDE ANALYZER: No data array found in API result`);
      }
      
      console.log(`📊 CLAUDE ANALYZER (REST API): Loaded ${records.length} records for analysis from ${pathsWithAggregation}`);
      this.app?.debug(`REST API loaded ${records.length} records for analysis from ${pathsWithAggregation}`);
      return records;
      
    } catch (error) {
      this.app?.error(`Failed to load data from ${dataPath}: ${(error as Error).message}`);
      
      // Fallback to sample data if query fails
      const sampleData: DataRecord[] = [{
        received_timestamp: new Date().toISOString(),
        signalk_timestamp: new Date().toISOString(),
        context: 'vessels.self',
        path: dataPath,
        value: 0,
        source: 'fallback-sample'
      }];
      
      return sampleData;
    }
  }

  /**
   * Generate statistical summary of the data
   */
  private generateDataSummary(data: DataRecord[]): DataSummary {
    if (data.length === 0) {
      return {
        rowCount: 0,
        timeRange: { start: new Date(), end: new Date() },
        columns: [],
        statisticalSummary: {},
        dataQuality: {
          completeness: 0,
          consistency: 0,
          timeliness: 0,
          accuracy: 0
        }
      };
    }

    // Extract time range
    const timestamps = data.map(d => new Date(d.received_timestamp)).sort();
    const timeRange = {
      start: timestamps[0],
      end: timestamps[timestamps.length - 1]
    };

    // Analyze columns
    const columns: ColumnInfo[] = [];
    const allKeys = new Set<string>();
    data.forEach(record => {
      Object.keys(record).forEach(key => allKeys.add(key));
    });

    allKeys.forEach(key => {
      const values = data.map(d => (d as any)[key]).filter(v => v !== null && v !== undefined);
      columns.push({
        name: key,
        type: typeof values[0],
        nullCount: data.length - values.length,
        uniqueCount: new Set(values).size,
        sampleValues: values.slice(0, 5)
      });
    });

    // Calculate statistics for numeric values
    const statisticalSummary: Record<string, Statistics> = {};
    columns.forEach(col => {
      if (col.type === 'number') {
        const values = data.map(d => (d as any)[col.name]).filter(v => typeof v === 'number');
        if (values.length > 0) {
          const sorted = values.sort((a, b) => a - b);
          const sum = values.reduce((a, b) => a + b, 0);
          const mean = sum / values.length;
          const variance = values.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / values.length;
          
          statisticalSummary[col.name] = {
            count: values.length,
            mean,
            median: sorted[Math.floor(sorted.length / 2)],
            min: sorted[0],
            max: sorted[sorted.length - 1],
            stdDev: Math.sqrt(variance)
          };
        }
      } else {
        statisticalSummary[col.name] = {
          count: data.length - col.nullCount,
          min: col.sampleValues[0],
          max: col.sampleValues[col.sampleValues.length - 1]
        };
      }
    });

    // Calculate data quality metrics
    const dataQuality = this.calculateDataQuality(data, columns);

    return {
      rowCount: data.length,
      timeRange,
      columns,
      statisticalSummary,
      dataQuality
    };
  }

  /**
   * Calculate data quality metrics
   */
  private calculateDataQuality(data: DataRecord[], columns: ColumnInfo[]): DataQualityMetrics {
    const totalFields = data.length * columns.length;
    const nullFields = columns.reduce((sum, col) => sum + col.nullCount, 0);
    const completeness = ((totalFields - nullFields) / totalFields) * 100;

    // Simple heuristics for other quality metrics
    const consistency = 85; // Placeholder - would analyze format consistency
    const timeliness = this.calculateTimeliness(data);
    const accuracy = 90; // Placeholder - would need validation data

    return {
      completeness,
      consistency,
      timeliness,
      accuracy
    };
  }

  /**
   * Calculate data timeliness based on timestamps
   */
  private calculateTimeliness(data: DataRecord[]): number {
    if (data.length === 0) return 0;

    const now = new Date();
    const latestRecord = new Date(Math.max(...data.map(d => new Date(d.received_timestamp).getTime())));
    const ageHours = (now.getTime() - latestRecord.getTime()) / (1000 * 60 * 60);
    
    // Timeliness decreases as data gets older
    return Math.max(0, 100 - (ageHours * 2)); // 2% decrease per hour
  }

  /**
   * Sample data for analysis to respect Claude token limits
   */
  private sampleDataForAnalysis(data: DataRecord[], maxSamples: number): DataRecord[] {
    if (data.length <= maxSamples) {
      return data;
    }

    // Reduce max samples to limit token usage - be very aggressive for production
    const tokenSafeMaxSamples = Math.min(maxSamples, 30);

    // Intelligent sampling - take some from beginning, middle, and end
    const step = Math.floor(data.length / tokenSafeMaxSamples);
    const sampled: DataRecord[] = [];
    
    for (let i = 0; i < data.length && sampled.length < tokenSafeMaxSamples; i += step) {
      sampled.push(data[i]);
    }

    // Include very few recent records to save tokens  
    const recentCount = Math.min(5, tokenSafeMaxSamples - sampled.length);
    const recentRecords = data.slice(-recentCount);
    
    return [...sampled, ...recentRecords].slice(0, tokenSafeMaxSamples);
  }

  /**
   * Safely stringify data that may contain BigInt values
   */
  private safeStringify(obj: any, space?: number): string {
    return JSON.stringify(obj, (_, value) => {
      return typeof value === 'bigint' ? value.toString() : value;
    }, space);
  }

  /**
   * Analyze data structure to guide Claude on how to interpret the data
   */
  private analyzeDataStructure(sampleData: any[]): string {
    if (!sampleData || sampleData.length === 0) {
      return "- No sample data available for structure analysis";
    }

    const firstRecord = sampleData[0];
    const notes: string[] = [];

    // Analyze paths and aggregation methods
    const uniquePaths = new Set(sampleData.map(record => record.path).filter(Boolean));
    const aggregationMethods = new Set(sampleData.map(record => record.aggregation_method).filter(Boolean));
    
    if (uniquePaths.size > 1) {
      notes.push(`- Multi-path analysis: ${uniquePaths.size} different SignalK paths`);
      notes.push(`- Paths included: ${Array.from(uniquePaths).join(', ')}`);
    } else {
      notes.push(`- Single path analysis: ${Array.from(uniquePaths)[0] || 'unknown'}`);
    }
    
    if (aggregationMethods.size > 0) {
      notes.push(`- Aggregation methods applied: ${Array.from(aggregationMethods).join(', ')}`);
    }

    // Check for value_json presence
    const hasValueJson = firstRecord.hasOwnProperty('value_json') && firstRecord.value_json !== null;
    const hasDirectValues = Object.keys(firstRecord).some(key => key.startsWith('value_') && key !== 'value_json');

    if (hasValueJson) {
      notes.push("- Data contains JSON objects in 'value_json' column");
      notes.push("- Main data values are stored as JSON objects (e.g., position data with longitude/latitude)");
      
      // Try to parse a sample to show structure
      try {
        const parsed = typeof firstRecord.value_json === 'string' 
          ? JSON.parse(firstRecord.value_json) 
          : firstRecord.value_json;
        const keys = Object.keys(parsed);
        notes.push(`- JSON structure contains: ${keys.join(', ')}`);
      } catch (e) {
        notes.push("- JSON values present but structure varies");
      }
    }

    if (hasDirectValues) {
      const directValueColumns = Object.keys(firstRecord).filter(key => 
        key.startsWith('value_') && key !== 'value_json' && firstRecord[key] !== null
      );
      if (directValueColumns.length > 0) {
        notes.push("- Data also contains direct value columns:");
        notes.push(`  ${directValueColumns.join(', ')}`);
      }
    }

    // Guidance for Claude
    if (hasValueJson && hasDirectValues) {
      notes.push("- ANALYSIS NOTE: Use 'value_json' for the primary data values, direct columns may be supplementary");
    } else if (hasValueJson) {
      notes.push("- ANALYSIS NOTE: Primary data is in 'value_json' objects - parse this for meaningful values");
    } else if (hasDirectValues) {
      notes.push("- ANALYSIS NOTE: Data values are in direct columns (value_longitude, value_latitude, etc.)");
    }

    // Check for other important columns
    const standardColumns = ['received_timestamp', 'timestamp', 'context', 'path', 'source'];
    const presentColumns = standardColumns.filter(col => firstRecord.hasOwnProperty(col));
    if (presentColumns.length > 0) {
      notes.push(`- Standard SignalK columns available: ${presentColumns.join(', ')}`);
    }

    return notes.length > 0 ? notes.join('\n') : "- Standard data structure detected";
  }

  /**
   * Build analysis prompt based on data and request type
   */
  private buildAnalysisPrompt(data: any, request: AnalysisRequest): string {
    const { summary, sampleData } = data;
    
    const dataStructureNote = this.analyzeDataStructure(sampleData);
    const vesselContext = this.vesselContextManager.generateClaudeContext();
    
    let prompt = `You are an expert maritime data analyst. Analyze the following SignalK vessel data and provide insights.

${vesselContext}

DATA SUMMARY:
- Path: ${request.dataPath}
- Records: ${summary.rowCount} (showing sample of ${sampleData.length})
- Time Range: ${summary.timeRange.start.toISOString()} to ${summary.timeRange.end.toISOString()}
- Data Quality: ${Math.round(summary.dataQuality.completeness)}% complete, ${Math.round(summary.dataQuality.accuracy)}% accuracy

DATA STRUCTURE NOTES:
${dataStructureNote}

STATISTICAL SUMMARY:
${this.safeStringify(summary.statisticalSummary, 2)}

SAMPLE DATA:
${this.safeStringify(sampleData, 2)}

`;

    // Add specific analysis instructions based on type
    switch (request.analysisType) {
      case 'summary':
        prompt += `
ANALYSIS REQUEST: Provide a comprehensive summary of this maritime data.
Focus on:
1. Overall trends and patterns
2. Operational insights
3. Performance indicators
4. Notable observations
5. Data quality assessment

Please structure your response as:
- Executive Summary (2-3 sentences)
- Key Insights (bullet points)
- Recommendations (actionable items)
- Data Quality Notes
`;
        break;

      case 'anomaly':
        prompt += `
ANALYSIS REQUEST: Detect anomalies and unusual patterns in this maritime data.
Focus on:
1. Statistical outliers
2. Unusual temporal patterns
3. Operational anomalies
4. Safety concerns
5. Equipment irregularities

For each anomaly found, specify:
- Timestamp
- Value and expected range
- Severity (low/medium/high)
- Description and potential cause
- Confidence level
`;
        break;

      case 'trend':
        prompt += `
ANALYSIS REQUEST: Analyze trends and patterns in this maritime data over time.
Focus on:
1. Temporal trends (increasing/decreasing/cyclical)
2. Seasonal patterns
3. Operational patterns
4. Performance trends
5. Predictive insights

Provide trend analysis with confidence levels and future projections where appropriate.
`;
        break;

      case 'custom':
        prompt += `
ANALYSIS REQUEST: ${request.customPrompt}

IMPORTANT: When analyzing the data, note that:
- This data comes from the SignalK REST API with proper timestamp alignment
- Each record has a 'path' field indicating the SignalK data source
- Multiple paths may be included for correlation analysis (check 'path' field for each record)
- Aggregation methods (like 'max', 'ema', 'sma') are applied and noted in 'aggregation_method' field
- All timestamps are properly synchronized across different data sources
- Focus on the 'value' field for numerical data and 'path' field to distinguish data sources
- If 'value_json' contains objects, extract the meaningful values from these JSON structures
- Consider the SignalK data path context to understand what type of maritime data you're analyzing

Please provide detailed analysis addressing the specific request while considering maritime operations context.
`;
        break;


      default:
        prompt += `
ANALYSIS REQUEST: Analyze this maritime data and provide relevant insights for vessel operations.
`;
    }

    prompt += `
RESPONSE FORMAT:
Please structure your response as JSON with the following format:
{
  "analysis": "Main analysis text",
  "insights": ["insight1", "insight2", ...],
  "recommendations": ["recommendation1", "recommendation2", ...],
  "anomalies": [{"timestamp": "ISO8601", "value": "actual", "expectedRange": {"min": 0, "max": 100}, "severity": "high", "description": "...", "confidence": 0.9}],
  "confidence": 0.85,
  "dataQuality": "assessment of data quality"
}
`;

    return prompt;
  }

  /**
   * Parse Claude's analysis response into structured format
   */
  private parseAnalysisResponse(response: any, request: AnalysisRequest, data: any): AnalysisResponse {
    try {
      let content = '';
      if (response.content && response.content[0] && response.content[0].text) {
        content = response.content[0].text;
      } else if (typeof response === 'string') {
        content = response;
      }

      // Try to extract JSON from the response
      let parsedResponse: any = {};
      const jsonMatch = content.match(/\{[\s\S]*\}/);
      if (jsonMatch) {
        try {
          parsedResponse = JSON.parse(jsonMatch[0]);
        } catch (parseError) {
          // If JSON parsing fails, create structured response from text
          parsedResponse = {
            analysis: content,
            insights: this.extractBulletPoints(content),
            recommendations: [],
            confidence: 0.8,
            dataQuality: "Analysis completed"
          };
        }
      } else {
        parsedResponse = {
          analysis: content,
          insights: this.extractBulletPoints(content),
          recommendations: [],
          confidence: 0.8,
          dataQuality: "Analysis completed"
        };
      }

      // Generate unique ID
      const analysisId = `analysis_${Date.now()}_${Math.random().toString(36).substring(2, 11)}`;

      return {
        id: analysisId,
        analysis: parsedResponse.analysis || content,
        insights: parsedResponse.insights || [],
        recommendations: parsedResponse.recommendations || [],
        anomalies: parsedResponse.anomalies || [],
        confidence: parsedResponse.confidence || 0.8,
        dataQuality: parsedResponse.dataQuality || "Analysis completed",
        timestamp: new Date().toISOString(),
        metadata: {
          dataPath: request.dataPath,
          analysisType: request.analysisType,
          recordCount: data.originalCount || 0,
          timeRange: request.timeRange,
          useDatabaseAccess: request.useDatabaseAccess
        }
      };

    } catch (error) {
      this.app?.error(`Failed to parse Claude response: ${(error as Error).message}`);
      throw new Error(`Response parsing failed: ${(error as Error).message}`);
    }
  }

  /**
   * Extract bullet points from text for insights
   */
  private extractBulletPoints(text: string): string[] {
    const lines = text.split('\n');
    const bulletPoints: string[] = [];

    lines.forEach(line => {
      const trimmed = line.trim();
      if (trimmed.match(/^[-*•]\s/) || trimmed.match(/^\d+\.\s/)) {
        bulletPoints.push(trimmed.replace(/^[-*•]\s*/, '').replace(/^\d+\.\s*/, ''));
      }
    });

    return bulletPoints;
  }

  /**
   * Save analysis to history for later retrieval
   */
  private async saveAnalysisToHistory(analysis: AnalysisResponse): Promise<void> {
    try {
      // Create history directory in plugin's data directory
      if (!this.dataDirectory) {
        throw new Error('No data directory configured for plugin');
      }
      const historyDir = path.join(this.dataDirectory, 'analysis-history');
      await fs.ensureDir(historyDir);

      // Save analysis to file
      const filename = `${analysis.id}.json`;
      const filepath = path.join(historyDir, filename);
      
      await fs.writeJson(filepath, analysis, { spaces: 2 });
      this.app?.debug(`Analysis saved to history: ${filepath}`);

    } catch (error) {
      this.app?.error(`Failed to save analysis to history: ${(error as Error).message}`);
      // Don't throw - this is not critical
    }
  }

  /**
   * Get analysis history
   */
  async getAnalysisHistory(limit: number = 20): Promise<AnalysisResponse[]> {
    try {
      if (!this.dataDirectory) {
        return []; // No data directory configured, return empty history
      }
      const historyDir = path.join(this.dataDirectory, 'analysis-history');
      
      if (!await fs.pathExists(historyDir)) {
        return [];
      }

      const files = await fs.readdir(historyDir);
      const analysisFiles = files.filter(f => f.endsWith('.json')).sort().reverse();
      
      const history: AnalysisResponse[] = [];
      for (let i = 0; i < Math.min(limit, analysisFiles.length); i++) {
        try {
          const analysis = await fs.readJson(path.join(historyDir, analysisFiles[i]));
          history.push(analysis);
        } catch (error) {
          this.app?.debug(`Failed to read analysis file ${analysisFiles[i]}: ${(error as Error).message}`);
        }
      }

      return history;

    } catch (error) {
      this.app?.error(`Failed to get analysis history: ${(error as Error).message}`);
      return [];
    }
  }

  /**
   * Delete an analysis from history
   */
  async deleteAnalysis(analysisId: string): Promise<{ success: boolean; error?: string }> {
    try {
      if (!this.dataDirectory) {
        return { success: false, error: 'No data directory configured for plugin' };
      }
      
      const historyDir = path.join(this.dataDirectory, 'analysis-history');
      const filename = `${analysisId}.json`;
      const filePath = path.join(historyDir, filename);

      if (!await fs.pathExists(filePath)) {
        return { success: false, error: 'Analysis not found' };
      }

      await fs.remove(filePath);
      this.app?.debug(`Deleted analysis: ${analysisId}`);
      
      return { success: true };

    } catch (error) {
      this.app?.error(`Failed to delete analysis: ${(error as Error).message}`);
      return { success: false, error: (error as Error).message };
    }
  }


  /**
   * Tony's approach: Direct database access analysis
   * Claude can query the database interactively during analysis
   */
  async analyzeWithDatabaseAccess(request: AnalysisRequest): Promise<AnalysisResponse> {
    try {
      this.app?.debug('🚀 Using Tony\'s direct database access approach');
      
      // Ensure vessel context is loaded before generating context for Claude
      await this.vesselContextManager.refreshVesselInfo();
      const vesselContext = this.vesselContextManager.generateClaudeContext();
      this.app?.debug(`🛥️ Vessel context for Claude (${vesselContext.length} chars):\n${vesselContext.substring(0, 500)}${vesselContext.length > 500 ? '...' : ''}`);
      const schemaInfo = await this.getEnhancedSchemaForClaude();
      this.app?.debug(`📊 Schema info for Claude (${schemaInfo.length} chars):\n${schemaInfo.substring(0, 1000)}${schemaInfo.length > 1000 ? '...' : ''}`);
      
      // Debug: Log if schema is empty or suspicious
      if (!schemaInfo || schemaInfo.length < 100) {
        this.app?.error(`❌ Schema info appears empty or too short! Length: ${schemaInfo?.length || 0}`);
        this.app?.error(`Schema content: "${schemaInfo}"`);
      }
      
      // Build time range guidance for Claude
      let timeRangeGuidance = '';
      if (request.timeRange) {
        console.log(`🔍 REQUEST TIME RANGE DEBUG:`, {
          userRequested: request.customPrompt || request.analysisType,
          actualStart: request.timeRange.start.toISOString(),
          actualEnd: request.timeRange.end.toISOString(),
          calculatedHours: (request.timeRange.end.getTime() - request.timeRange.start.getTime()) / (1000 * 60 * 60)
        });
        
        timeRangeGuidance = `

ANALYSIS SCOPE: Focus your analysis on data between ${request.timeRange.start.toISOString().replace('.000Z', 'Z')} and ${request.timeRange.end.toISOString().replace('.000Z', 'Z')}.
IMPORTANT: Always include WHERE clauses in your SQL queries to filter results to this time range:
WHERE signalk_timestamp >= '${request.timeRange.start.toISOString().replace('.000Z', 'Z')}' AND signalk_timestamp <= '${request.timeRange.end.toISOString().replace('.000Z', 'Z')}'`;
      } else {
        // Default to recent data if no time range specified
        const now = new Date();
        const sixHoursAgo = new Date(now.getTime() - 6 * 60 * 60 * 1000);
        timeRangeGuidance = `

TIME RANGE FOCUS: Since no specific time range was provided, focus on recent data (last 6 hours).
IMPORTANT: Always include WHERE clauses to limit results to recent data:
WHERE signalk_timestamp >= '${sixHoursAgo.toISOString().replace('.000Z', 'Z')}'`;
      }

      // Get system timezone for timestamp interpretation
      const systemTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
      const now = new Date();
      const timezoneOffset = -now.getTimezoneOffset() / 60; // Convert to hours from UTC
      
      const initialPrompt = `You are an expert maritime data analyst with direct access to a comprehensive database.

IMPORTANT: Please use the vessel context information provided below for all analysis and responses. This vessel information is critical for accurate maritime analysis.

${vesselContext}

CRITICAL TIMESTAMP INFORMATION:
- ALL SignalK timestamps in the database are in UTC (ending with 'Z')
- System timezone: ${systemTimezone} (UTC${timezoneOffset >= 0 ? '+' : ''}${timezoneOffset})
- When interpreting times for the user, convert UTC timestamps to local time (${systemTimezone})
- Example: 2025-09-02T00:24:44Z (UTC) = ${new Date('2025-09-02T00:24:44Z').toLocaleString('en-US', {timeZone: systemTimezone, timeZoneName: 'short'})}

${schemaInfo}${timeRangeGuidance}

ANALYSIS REQUEST: ${request.customPrompt || 'Analyze maritime data and provide insights'}

You can query the database using the query_maritime_database function. Start by exploring the data to understand what's available, then provide comprehensive analysis.

REMEMBER: 
- Always refer to and use the vessel context provided above (vessel name, dimensions, operational details, etc.) when analyzing data and providing recommendations.
- ALWAYS include time range WHERE clauses in your queries to avoid loading excessive historical data.
- Keep query results focused and relevant to the specified time period.
- CRITICAL: ONLY use the exact paths listed in the "AVAILABLE DATA PATHS" section. DO NOT make up or guess path names.
- If a path you want to use is not in the available paths list, it does not exist - inform the user instead of guessing.

CRITICAL FOR TOKEN EFFICIENCY: 
- NEVER query raw individual records - ALWAYS use time bucketing and aggregation
- MANDATORY SQL pattern for all data queries:
  SELECT 
    strftime(date_trunc('hour', signalk_timestamp::TIMESTAMP), '%Y-%m-%dT%H:%M:%SZ') as time_bucket,
    AVG(CAST(value AS DOUBLE)) as avg_value,
    MAX(CAST(value AS DOUBLE)) as max_value, 
    MIN(CAST(value AS DOUBLE)) as min_value,
    COUNT(*) as record_count
  FROM 'path/*.parquet' 
  WHERE signalk_timestamp >= 'start_time' AND signalk_timestamp <= 'end_time'
    AND value IS NOT NULL
  GROUP BY time_bucket 
  ORDER BY time_bucket
- Use date_trunc('minute', ...) for detailed analysis, date_trunc('hour', ...) for overviews
- NEVER return more than 100 time buckets per query

Focus on:
1. Current vessel status and recent activity
2. Patterns in navigation, weather, and performance data  
3. Safety considerations and operational insights
4. Data quality and completeness assessment

Begin your analysis by querying relevant data within the specified time range.`;

      this.app?.debug(`📝 Full prompt for Claude (${initialPrompt.length} chars):\n${initialPrompt.substring(0, 2000)}${initialPrompt.length > 2000 ? '...[TRUNCATED]' : ''}`);
      
      // Save full prompt to file for debugging
      const fs = require('fs');
      const debugFile = `/tmp/claude-prompt-debug-${Date.now()}.txt`;
      fs.writeFileSync(debugFile, `FULL CLAUDE PROMPT (${initialPrompt.length} chars):\n\n${initialPrompt}`);
      this.app?.debug(`📄 Full prompt saved to: ${debugFile}`);

      // Extract system context and user prompt  
      const systemContext = `You are an expert maritime data analyst with direct access to a comprehensive database.

CRITICAL DATA INTEGRITY RULES - VIOLATION OF THESE RULES IS UNACCEPTABLE:
- NEVER fabricate, guess, or make up any data, coordinates, timestamps, or values
- If a query returns no data, you MUST say "No data available" or "Query returned no results"  
- NEVER invent plausible-sounding but false information
- If you don't know something, explicitly state "I don't have this information"
- Financial and navigational decisions depend on accurate data - false information causes real harm
- NEVER create example data, sample values, or hypothetical scenarios when discussing real vessel data
- ALWAYS reference the specific query that provided any data you present
- If insufficient data exists for analysis, explicitly state this rather than creating synthetic data

MANDATORY DATA VALIDATION PROTOCOL - THESE STEPS ARE REQUIRED FOR EVERY RESPONSE:
1. BEFORE presenting ANY data, you MUST explicitly state: "Query executed: [exact SQL]"
2. BEFORE presenting ANY data, you MUST explicitly state: "Rows returned: [exact count]"
3. BEFORE creating ANY chart, you MUST explicitly state: "Data points for chart: [exact count from query]"
4. BEFORE analyzing time ranges, you MUST explicitly state: "Actual time range in data: [start] to [end]"
5. If user requests specific time window and data doesn't match, you MUST state: "Requested: [X], Available: [Y]"

MANDATORY TIME CALCULATION VALIDATION - NEVER USE DEFAULT TIME PERIODS:
1. When user requests specific time periods (72 hours, 3 days, 1 week, etc.), ALWAYS calculate exact timestamps
2. NEVER default to familiar periods like "yesterday" or "24 hours" when user specified different duration
3. ALWAYS verify: "User requested [X hours/days], calculating [X hours/days] back from [current time]"
4. ALWAYS state explicitly: "Time range calculated: [exact start timestamp] to [exact end timestamp]"
5. If user says "72 hours", you MUST calculate 72 hours, not 24 hours or any other duration
6. NEVER assume data might not be available - execute the exact request first, then report actual results
7. NEVER claim "database restrictions" or "time range restrictions" exist without first querying the requested range
8. Execute the full requested query first - only report actual limitations found in the results
9. CRITICAL: If user requests "7 days", you MUST query 7 days back from current time, not 1 day or 24 hours
10. CRITICAL: If user requests "1 week", you MUST query 7 days back from current time, not 1 day or 24 hours
11. NEVER say "this is the complete dataset" unless you actually queried the full requested time period
12. ABSOLUTELY FORBIDDEN: Do not mention "database time range restrictions" - no such restrictions exist
13. ABSOLUTELY FORBIDDEN: Do not say "I need to work within" any restrictions - query the full requested range
14. THERE ARE NO DATABASE LIMITATIONS - the database contains weeks of historical data
15. If user asks for 7 days, query FROM 7 days ago TO now - do not modify this range for any reason

MANDATORY QUERY EXECUTION RULE - NO EXCEPTIONS:
When user specifies ANY time frame (72 hours, 3 days, 1 week, 7 days, etc.):
1. Calculate the EXACT start and end timestamps for that period
2. Execute your database queries using those EXACT timestamps 
3. NEVER modify, restrict, or reduce the time range for any reason
4. Query the database with the full requested range - period, no exceptions

CRITICAL: Before executing ANY query, you MUST:
1. Extract the EXACT time range from user request
2. Query the database to find the ACTUAL available data range
3. Use ONLY the available data range - DO NOT default to 24 hours
4. If requested range exceeds available data, use ALL available data and state the actual range used

QUERY EXECUTION RULES:
- For time-based requests, FIRST run: SELECT MIN(signalk_timestamp), MAX(signalk_timestamp) FROM relevant_table
- Use the full available range, not arbitrary subsets
- State actual data range used in response
- If user asks for "7 days" but only 3 days exist, use all 3 days and explain
- NEVER assume 24-hour periods. ALWAYS query for full available dataset first.

FORBIDDEN ACTIONS - THESE WILL RESULT IN IMMEDIATE FAILURE:
- Creating ASCII charts, text visualizations, or any fake visual representations
- Using terms like "trending", "pattern", or "shows" without showing exact data points
- Making statements about data without first showing the query that produced it
- Creating any visualization that isn't a proper Plotly.js JSON specification
- Presenting analysis conclusions without first showing raw query results

CHART EMBEDDING CAPABILITIES:
When you want to include charts in your response, add a Plotly.js JSON chart specification in a code block like this:
\`\`\`json
{
  "type": "chart",
  "title": "Speed Over Ground Trend",
  "data": [
    {
      "x": ["12:00", "13:00", "14:00", "15:00"],
      "y": [5.2, 6.1, 5.8, 7.3],
      "name": "Speed Over Ground",
      "type": "scatter",
      "mode": "lines+markers",
      "line": {"color": "#1976d2", "width": 2},
      "marker": {"color": "#1976d2", "size": 6}
    }
  ],
  "layout": {
    "title": "Speed Over Ground Trend",
    "xaxis": {"title": "Time"},
    "yaxis": {"title": "Speed (knots)"},
    "showlegend": true
  }
}
\`\`\`

SUPPORTED CHART TYPES:
- **Line Charts**: type: "scatter", mode: "lines+markers" or "lines"
- **Bar Charts**: type: "bar"
- **Scatter Plots**: type: "scatter", mode: "markers"
- **Wind Rose/Radar**: type: "scatterpolar" with r and theta values
- **Multiple Series**: Include multiple objects in the data array
- **Styling**: Use line.color, marker.color, line.width, etc.

Include this JSON when analysis would benefit from visualization.

CRITICAL CHART DATA RULES - CHARTS WITH FAKE DATA ARE FORBIDDEN:
- ONLY use data that comes from actual database query results
- NEVER fabricate, estimate, or interpolate data points
- NEVER extend data beyond what the query returned
- If you don't have enough data points for a meaningful chart, say so explicitly
- All chart data must be traceable to specific query results you executed
- Include a comment in your response explaining which query provided the chart data

MANDATORY CHART VALIDATION - REQUIRED BEFORE ANY CHART:
1. Count exact data points from query result
2. State: "Creating chart with [N] actual data points from query"
3. If query returns 5 rows, chart must have exactly 5 data points - NEVER MORE
4. If user asks for 72-hour window but data spans 24 hours, explicitly state the mismatch
5. NEVER fill gaps or extend trends - use only actual timestamps and values from database
6. Show first 3 and last 3 actual data rows before creating chart
7. Explicitly verify: "Chart data matches query results: [timestamp1: value1], [timestamp2: value2]..."

RESPONSE STRUCTURE REQUIREMENTS:
1. Always start with: "QUERY VALIDATION:"
2. Show the exact SQL executed
3. Show exact row count and time range
4. If creating chart, show sample data points
5. Only then provide analysis using that specific data

IMPORTANT: Please use the vessel context information provided below for all analysis and responses. This vessel information is critical for accurate maritime analysis.

${vesselContext}

${schemaInfo}${timeRangeGuidance}

CRITICAL FOR TOKEN EFFICIENCY: 
- NEVER query raw individual records - ALWAYS use time bucketing and aggregation
- MANDATORY SQL pattern for all data queries:
  SELECT 
    strftime(date_trunc('hour', signalk_timestamp::TIMESTAMP), '%Y-%m-%dT%H:%M:%SZ') as time_bucket,
    AVG(CAST(value AS DOUBLE)) as avg_value,
    MAX(CAST(value AS DOUBLE)) as max_value, 
    MIN(CAST(value AS DOUBLE)) as min_value,
    COUNT(*) as record_count
  FROM 'path/*.parquet' 
  WHERE signalk_timestamp >= 'start_time' AND signalk_timestamp <= 'end_time'
    AND value IS NOT NULL
  GROUP BY time_bucket 
  ORDER BY time_bucket
- Use date_trunc('minute', ...) for detailed analysis, date_trunc('hour', ...) for overviews
- NEVER return more than 100 time buckets per query

REMEMBER: 
- Always refer to and use the vessel context provided above (vessel name, dimensions, operational details, etc.) when analyzing data and providing recommendations.
- ALWAYS include time range WHERE clauses in your queries to avoid loading excessive historical data.
- Keep query results focused and relevant to the specified time period.
- CRITICAL: ONLY use the exact paths listed in the "AVAILABLE DATA PATHS" section. DO NOT make up or guess path names.
- If a path you want to use is not in the available paths list, it does not exist - inform the user instead of guessing.

Focus on:
1. Current vessel status and recent activity
2. Patterns in navigation, weather, and performance data  
3. Safety considerations and operational insights
4. Data quality and completeness assessment`;

      this.app?.debug(`🔧 Final system context (${systemContext.length} chars):`);
      this.app?.debug(`📋 System context preview:\n${systemContext.substring(0, 2000)}${systemContext.length > 2000 ? '...' : ''}`);

      const userPrompt = `${request.customPrompt || 'Analyze maritime data and provide insights'}

Begin your analysis by querying relevant data within the specified time range.`;

      // Start conversation with Claude with function calling capability
      let conversationMessages: Array<any> = [{
        role: 'user',
        content: userPrompt
      }];

      let analysisResult = '';
      let queryCount = 0;
      const maxQueries = 10; // Allow more queries for thorough analysis
      let totalTokenUsage = { input_tokens: 0, output_tokens: 0 };

      // Check if user is requesting real-time data
      const needsRealTimeData = this.checkForRealTimeKeywords(request.customPrompt || '', conversationMessages);
      
      // Identify relevant regimens based on keywords
      const relevantRegimens = this.identifyRelevantRegimens(request.customPrompt || '');
      
      // Build tools array - always include database access
      const availableTools: any[] = [{
        name: 'query_maritime_database',
        description: 'Execute SQL queries against the maritime Parquet database to explore and analyze data',
        input_schema: {
          type: 'object',
          properties: {
            sql: {
              type: 'string',
              description: 'SQL query to execute against the Parquet database'
            },
            purpose: {
              type: 'string',
              description: 'Brief description of what this query is trying to discover'
            }
          },
          required: ['sql', 'purpose']
        }
      }];

      // Add real-time SignalK data tool if keywords detected
      if (needsRealTimeData) {
        availableTools.push({
          name: 'get_current_signalk_data',
          description: 'Get current real-time SignalK data values for specific paths or all available paths from any vessel. Use this when user asks about "now", "current", "real-time" conditions. For queries about "all vessels" or "other vessels", use vesselContext="vessels.*".',
          input_schema: {
            type: 'object',
            properties: {
              paths: {
                type: 'array',
                items: { type: 'string' },
                description: 'Array of SignalK paths to query (e.g., ["navigation.position", "navigation.speedOverGround"]). Leave empty to get all available current values.'
              },
              purpose: {
                type: 'string',
                description: 'Brief description of why you need this real-time data'
              },
              vesselContext: {
                type: 'string',
                description: 'Vessel context to query. Use "vessels.*" for ALL vessels (recommended for multi-vessel queries), "vessels.self" for own vessel, or "vessels.urn:mrn:imo:mmsi:123456789" for specific vessel. Defaults to vessels.self if not specified.'
              }
            },
            required: ['purpose']
          }
        });
        this.app?.debug(`🕐 Real-time keywords detected, adding current SignalK data tool`);
      }

      // Add episode boundary detection tool if regimens were identified
      if (relevantRegimens.length > 0) {
        availableTools.push({
          name: 'find_regimen_episodes',
          description: 'REQUIRED for finding episodes/periods when regimens were active. Detects start/end boundaries from command state changes (false->true->false). Use this instead of query_maritime_database for episode detection.',
          input_schema: {
            type: 'object',
            properties: {
              regimenName: {
                type: 'string',
                description: `Regimen to analyze. Available: ${relevantRegimens.join(', ')}`
              },
              timeRange: {
                type: 'object',
                description: 'Optional time range constraint (start/end ISO timestamps)'
              },
              limit: {
                type: 'number', 
                description: 'Maximum number of episodes to return (default: 10)'
              }
            },
            required: ['regimenName']
          }
        });
        
        this.app?.debug(`🎬 Added episode detection tool for regimens: [${relevantRegimens.join(', ')}]`);
      }

      // Add wind analysis tool for detailed wind rose and analysis prompts
      const windKeywords = ['wind', 'breeze', 'gust', 'rose', 'direction', 'beaufort'];
      const hasWindKeywords = windKeywords.some(keyword => 
        (request.customPrompt || '').toLowerCase().includes(keyword)
      );
      
      if (hasWindKeywords) {
        availableTools.push({
          name: 'generate_wind_analysis',
          description: 'Generate detailed wind analysis prompts with proper Beaufort scale categories and radar chart specifications for professional maritime wind analysis',
          input_schema: {
            type: 'object',
            properties: {
              timeFrame: {
                type: 'string',
                description: 'Time period for analysis (e.g., "24 hours", "3 days", "1 week")',
                default: '48 hours'
              },
              chartType: {
                type: 'string',
                description: 'Type of wind chart to generate (e.g., "wind rose", "trend analysis", "directional frequency")',
                default: 'wind rose'
              },
              windSpeedCategories: {
                type: 'array',
                items: { type: 'string' },
                description: 'Custom wind speed categories (defaults to Beaufort scale if not specified)'
              },
              vesselName: {
                type: 'string',
                description: 'Name of vessel for analysis',
                default: 'Zennora'
              }
            },
            required: []
          }
        });
        this.app?.debug(`🌬️ Wind analysis keywords detected, adding wind analysis tool`);
      }

      while (queryCount < maxQueries) {
        const response = await this.callClaudeWithRetry({
          model: this.config.model,
          max_tokens: Math.max(this.config.maxTokens, 8000), // Increased for comprehensive analysis
          temperature: 0.0,
          system: systemContext,
          tools: availableTools,
          messages: conversationMessages
        });

        // Track token usage
        if (response.usage) {
          totalTokenUsage.input_tokens += response.usage.input_tokens || 0;
          totalTokenUsage.output_tokens += response.usage.output_tokens || 0;
          this.app?.debug(`Updated totalTokenUsage: ${JSON.stringify(totalTokenUsage)}`);
        }

        // Add Claude's response to conversation
        conversationMessages.push({
          role: 'assistant',
          content: response.content
        });

        // Process each tool use in the response
        const toolResults = [];
        for (const contentBlock of response.content) {
          if (contentBlock.type === 'text') {
            const textContent = contentBlock.text;
            this.app?.debug(`📝 Claude response text (${textContent.length} chars): ${textContent.substring(0, 200)}...`);
            
            // Debug: Check for JSON chart specs in the response
            const chartJsonMatches = textContent.match(/```json\s*([\s\S]*?)\s*```/gi);
            if (chartJsonMatches) {
              this.app?.debug(`🔍 FOUND ${chartJsonMatches.length} JSON BLOCKS IN CLAUDE RESPONSE`);
              chartJsonMatches.forEach((match: string, index: number) => {
                const jsonContent = match.replace(/```json\s*/, '').replace(/\s*```/, '');
                this.app?.debug(`📊 JSON Block ${index + 1} - Length: ${jsonContent.length} chars`);
                this.app?.debug(`📊 JSON Block ${index + 1} - Preview: ${jsonContent.substring(0, 100)}...`);
                this.app?.debug(`📊 JSON Block ${index + 1} - Ending: ...${jsonContent.substring(Math.max(0, jsonContent.length - 100))}`);
              });
            }
            
            analysisResult += textContent + '\n\n';
          } else if (contentBlock.type === 'tool_use') {
            const toolCall = contentBlock;
            
            // Ensure every tool_use gets a tool_result, even if processing fails
            try {
              if (toolCall.name === 'query_maritime_database') {
                queryCount++;
              }
              
              const toolResult = await this.processToolCall(toolCall);
              toolResults.push(toolResult);
            
            } catch (toolProcessingError) {
              // Critical: Always provide a tool_result, even if tool processing fails completely
              toolResults.push({
                type: 'tool_result',
                tool_use_id: toolCall.id,
                content: `Tool processing failed: ${(toolProcessingError as Error).message}`
              });
              this.app?.error(`🚨 Tool processing error for ${toolCall.name}: ${(toolProcessingError as Error).message}`);
            }
          }
        }

        // Add all tool results as a single user message
        if (toolResults.length > 0) {
          conversationMessages.push({
            role: 'user',
            content: toolResults
          });
        }

        // If Claude didn't use any tools, we're done
        const hasToolUse = response.content.some((block: any) => block.type === 'tool_use');
        if (!hasToolUse) {
          break;
        }
      }

      const analysisId = `analysis_${Date.now()}_${Math.random().toString(36).substring(2, 11)}`;
      
      // Ensure we have a meaningful analysis result
      const finalAnalysis = analysisResult.trim();
      if (!finalAnalysis) {
        throw new Error('Analysis completed but no results were generated. This may indicate a configuration issue.');
      }

      const analysisResponse = {
        id: analysisId,
        analysis: finalAnalysis,
        insights: [
          'Analysis completed using direct database access',
          `Executed ${queryCount} database queries`,
          'Comprehensive analysis of complete historical dataset'
        ],
        recommendations: [
          'Review the detailed analysis above',
          'Consider setting up automated monitoring for identified patterns',
          'Database access enables deeper historical insights than sampling'
        ],
        anomalies: [],
        confidence: 0.95,
        dataQuality: `Dynamic assessment via ${queryCount} database queries`,
        timestamp: new Date().toISOString(),
        metadata: {
          dataPath: request.dataPath || 'database_access_mode',
          analysisType: request.analysisType,
          recordCount: queryCount, // Number of queries executed
          timeRange: request.timeRange,
          useDatabaseAccess: true
        },
        usage: totalTokenUsage
      };

      this.app?.debug(`Final analysis response usage: ${JSON.stringify(analysisResponse.usage)}`);

      // Store conversation for follow-up questions
      this.activeConversations.set(analysisId, conversationMessages);
      this.app?.debug(`💾 Stored conversation ${analysisId} with ${conversationMessages.length} messages`);

      // Save analysis to history
      await this.saveAnalysisToHistory(analysisResponse);
      
      return analysisResponse;

    } catch (error) {
      this.app?.error(`Database access analysis failed: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * Call Claude API with retry logic for rate limit and overload errors
   */
  private async callClaudeWithRetry(params: any, maxRetries: number = 5): Promise<any> {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const response = await this.client.messages.create(params);
        this.app?.debug(`Claude API response usage: ${JSON.stringify(response.usage)}`);
        return response;
      } catch (error: any) {
        const isRateLimited = error?.status === 429 || 
                             (error?.message && error.message.includes('rate limit')) ||
                             (error?.error?.type === 'rate_limit_error');
                             
        const isOverloaded = error?.status === 529 || 
                           (error?.message && error.message.includes('overloaded')) ||
                           (error?.error?.type === 'overloaded_error');
        
        if ((isRateLimited || isOverloaded) && attempt < maxRetries) {
          // Exponential backoff with jitter for rate limits
          const baseDelay = isRateLimited ? 5000 : 2000; // 5s for rate limit, 2s for overload
          const exponentialDelay = Math.pow(2, attempt - 1) * baseDelay;
          const jitter = Math.random() * 1000; // Add random jitter
          const delayMs = exponentialDelay + jitter;
          
          const errorType = isRateLimited ? 'rate limited' : 'overloaded';
          this.app?.debug(`Claude ${errorType} (attempt ${attempt}/${maxRetries}), retrying in ${Math.round(delayMs)}ms...`);
          await new Promise(resolve => setTimeout(resolve, delayMs));
          continue;
        }
        throw error;
      }
    }
  }

  /**
   * Continue conversation with follow-up question
   */
  async askFollowUp(request: FollowUpRequest): Promise<AnalysisResponse> {
    try {
      this.app?.debug(`🔄 Processing follow-up question for conversation: ${request.conversationId}`);

      // Get the stored conversation
      this.app?.debug(`🔍 Looking for conversation: ${request.conversationId}`);
      this.app?.debug(`📚 Active conversations: ${Array.from(this.activeConversations.keys()).join(', ')}`);
      
      const conversationMessages = this.activeConversations.get(request.conversationId);
      if (!conversationMessages) {
        this.app?.error(`❌ Conversation ${request.conversationId} not found. Available: [${Array.from(this.activeConversations.keys()).join(', ')}]`);
        throw new Error('Conversation not found. Please start a new analysis.');
      }

      // Add user's follow-up question
      conversationMessages.push({
        role: 'user',
        content: request.question
      });

      let analysisResult = '';
      let queryCount = 0;
      const maxQueries = 10; // Allow thorough follow-up analysis
      let totalTokenUsage = { input_tokens: 0, output_tokens: 0 };

      // Check if follow-up question contains real-time keywords
      const needsRealTimeData = this.checkForRealTimeKeywords(request.question, conversationMessages);
      this.app?.debug(`🔍 Follow-up real-time check: "${request.question}" -> ${needsRealTimeData}`);
      
      // Build tools array using same logic as initial analysis - identify regimens and build context-aware tools
      const relevantRegimens = this.identifyRelevantRegimens(request.question, conversationMessages);
      this.app?.debug(`🎯 Follow-up identified ${relevantRegimens.length} relevant regimens: [${relevantRegimens.join(', ')}]`);
      
      // Start with base database access tool
      const followUpTools: any[] = [{
        name: 'query_maritime_database',
        description: 'Execute SQL queries against the maritime Parquet database to explore and analyze data',
        input_schema: {
          type: 'object',
          properties: {
            sql: {
              type: 'string',
              description: 'SQL query to execute against the Parquet database'
            },
            purpose: {
              type: 'string',
              description: 'Brief description of what this query is trying to discover'
            }
          },
          required: ['sql', 'purpose']
        }
      }];

      // Add episode boundary detection tool if regimens were identified
      if (relevantRegimens.length > 0) {
        followUpTools.push({
          name: 'find_regimen_episodes',
          description: 'REQUIRED for finding episodes/periods when regimens were active. Detects start/end boundaries from command state changes (false->true->false). Use this instead of query_maritime_database for episode detection.',
          input_schema: {
            type: 'object',
            properties: {
              regimenName: {
                type: 'string',
                description: `Regimen to analyze. Available: ${relevantRegimens.join(', ')}`
              },
              timeRange: {
                type: 'object',
                description: 'Optional time range constraint (start/end ISO timestamps)'
              },
              limit: {
                type: 'number',
                description: 'Maximum number of episodes to return (default 10)'
              }
            },
            required: ['regimenName']
          }
        });
        this.app?.debug(`🎬 Added episode detection tool for regimens: [${relevantRegimens.join(', ')}]`);
      }

      // Add real-time SignalK data tool if keywords detected in follow-up
      if (needsRealTimeData) {
        followUpTools.push({
          name: 'get_current_signalk_data',
          description: 'Get current real-time SignalK data values for specific paths or all available paths from any vessel. Use this when user asks about "now", "current", "real-time" conditions. For queries about "all vessels" or "other vessels", use vesselContext="vessels.*".',
          input_schema: {
            type: 'object',
            properties: {
              paths: {
                type: 'array',
                items: { type: 'string' },
                description: 'Array of SignalK paths to query (e.g., ["navigation.position", "navigation.speedOverGround"]). Leave empty to get all available current values.'
              },
              purpose: {
                type: 'string',
                description: 'Brief description of why you need this real-time data'
              },
              vesselContext: {
                type: 'string',
                description: 'Vessel context to query. Use "vessels.*" for ALL vessels (recommended for multi-vessel queries), "vessels.self" for own vessel, or "vessels.urn:mrn:imo:mmsi:123456789" for specific vessel. Defaults to vessels.self if not specified.'
              }
            },
            required: ['purpose']
          }
        });
        this.app?.debug(`🕐 Real-time keywords detected in follow-up, adding current SignalK data tool`);
      }

      // Continue the conversation with Claude
      while (queryCount < maxQueries) {
        const response = await this.callClaudeWithRetry({
          model: this.config.model,
          max_tokens: Math.max(this.config.maxTokens, 8000),
          temperature: 0.0,
          tools: followUpTools,
          messages: conversationMessages
        });

        // Track token usage
        if (response.usage) {
          totalTokenUsage.input_tokens += response.usage.input_tokens || 0;
          totalTokenUsage.output_tokens += response.usage.output_tokens || 0;
          this.app?.debug(`Follow-up updated totalTokenUsage: ${JSON.stringify(totalTokenUsage)}`);
        }

        // Add Claude's response to conversation
        conversationMessages.push({
          role: 'assistant',
          content: response.content
        });

        // Process the response
        const toolResults = [];
        for (const contentBlock of response.content) {
          if (contentBlock.type === 'text') {
            const textContent = contentBlock.text;
            this.app?.debug(`📝 Follow-up response text (${textContent.length} chars): ${textContent.substring(0, 200)}...`);
            analysisResult += textContent + '\n\n';
          } else if (contentBlock.type === 'tool_use') {
            const toolCall = contentBlock;
            
            // Ensure every tool_use gets a tool_result, even if processing fails
            try {
              if (toolCall.name === 'query_maritime_database') {
                queryCount++;
              }
              
              const toolResult = await this.processToolCall(toolCall);
              toolResults.push(toolResult);
            
            } catch (toolProcessingError) {
              // Critical: Always provide a tool_result, even if tool processing fails completely
              toolResults.push({
                type: 'tool_result',
                tool_use_id: toolCall.id,
                content: `Follow-up tool processing failed: ${(toolProcessingError as Error).message}`
              });
              this.app?.error(`🚨 Follow-up tool processing error for ${toolCall.name}: ${(toolProcessingError as Error).message}`);
            }
          }
        }

        // Add tool results if any
        if (toolResults.length > 0) {
          conversationMessages.push({
            role: 'user',
            content: toolResults
          });
        }

        // Check if Claude used tools - if not, we're done
        const hasToolUse = response.content.some((block: any) => block.type === 'tool_use');
        if (!hasToolUse) {
          break;
        }
      }

      // Update stored conversation
      this.activeConversations.set(request.conversationId, conversationMessages);

      const followUpResponse = {
        id: request.conversationId, // Keep same conversation ID
        analysis: analysisResult.trim() || 'Follow-up question answered.',
        insights: [
          'Follow-up question processed',
          `Executed ${queryCount} additional database queries`,
          'Conversation continued with database access'
        ],
        recommendations: [
          'Ask more follow-up questions to explore deeper',
          'Use specific questions for targeted analysis'
        ],
        anomalies: [],
        confidence: 0.9,
        dataQuality: `Follow-up with ${queryCount} additional queries`,
        timestamp: new Date().toISOString(),
        metadata: {
          dataPath: 'follow_up_question',
          analysisType: 'custom',
          recordCount: queryCount,
          timeRange: undefined
        },
        usage: totalTokenUsage
      };

      this.app?.debug(`Final follow-up response usage: ${JSON.stringify(followUpResponse.usage)}`);

      // Save follow-up response to history
      await this.saveAnalysisToHistory(followUpResponse);

      return followUpResponse;

    } catch (error) {
      this.app?.error(`Follow-up question failed: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * Get current real-time SignalK data values
   */
  private getCurrentSignalKData(paths?: string[], purpose?: string, vesselContext?: string): any {
    const contextToUse = vesselContext || 'vessels.self';
    this.app?.debug(`🕐 Getting current SignalK data for ${contextToUse}: ${paths?.length ? paths.join(', ') : 'all paths'}. Purpose: ${purpose}`);
    
    try {
      if (!paths || paths.length === 0) {
        // Get all current data for the specified context
        if (contextToUse === 'vessels.self') {
          // Resolve 'self' to actual vessel ID and use getPath with full vessel identifier
          const actualVesselId = this.app?.selfId;
          if (actualVesselId) {
            const vesselData = this.app?.getPath(`vessels.${actualVesselId}`) || {};
            const cleanData = this.cleanSignalKData(vesselData);
            this.app?.debug(`📊 Retrieved ${Object.keys(cleanData).length} current SignalK data points for vessels.${actualVesselId} (resolved from ${contextToUse})`);
            return {
              timestamp: new Date().toISOString(),
              source: 'real-time SignalK',
              context: `vessels.${actualVesselId}`,
              data: cleanData
            };
          } else {
            this.app?.debug(`⚠️ Could not resolve selfId for ${contextToUse}`);
            return {
              timestamp: new Date().toISOString(),
              source: 'real-time SignalK',
              context: contextToUse,
              data: {}
            };
          }
        } else if (contextToUse === 'vessels.*') {
          // Get all vessels using getPath
          const allVessels = this.app?.getPath('vessels') || {};
          const cleanData = this.cleanSignalKData(allVessels);
          this.app?.debug(`📊 Retrieved data from all vessels (${Object.keys(cleanData).length} vessel contexts)`);
          return {
            timestamp: new Date().toISOString(),
            source: 'real-time SignalK',
            context: 'vessels.*',
            data: cleanData
          };
        } else {
          // Get specific vessel data using getPath
          const vesselData = this.app?.getPath(contextToUse) || {};
          const cleanData = this.cleanSignalKData(vesselData);
          this.app?.debug(`📊 Retrieved ${Object.keys(cleanData).length} current SignalK data points for ${contextToUse}`);
          return {
            timestamp: new Date().toISOString(),
            source: 'real-time SignalK',
            context: contextToUse,
            data: cleanData
          };
        }
      } else {
        // Get specific paths
        const pathData: any = {};
        
        if (contextToUse === 'vessels.*') {
          // For wildcard, get paths from all vessels
          const allVessels = this.app?.getPath('vessels') || {};
          for (const vesselId in allVessels) {
            if (vesselId === 'self') continue; // Skip self since it's handled separately
            
            for (const path of paths) {
              try {
                const fullPath = `vessels.${vesselId}.${path}`;
                const value = this.app?.getPath(fullPath);
                
                if (value !== undefined && value !== null) {
                  if (!pathData[path]) pathData[path] = {};
                  pathData[path][vesselId] = value;
                }
              } catch (error) {
                // Skip errors for individual vessels - some may not have all paths
              }
            }
          }
        } else {
          // Handle single vessel (self or specific vessel)
          for (const path of paths) {
            try {
              let value;
              if (contextToUse === 'vessels.self') {
                // Resolve 'self' to actual vessel ID and use getPath with full vessel identifier
                const actualVesselId = this.app?.selfId;
                if (actualVesselId) {
                  const fullPath = `vessels.${actualVesselId}.${path}`;
                  value = this.app?.getPath(fullPath);
                } else {
                  value = null;
                }
              } else {
                // Get from specific vessel using getPath
                const fullPath = `${contextToUse}.${path}`;
                value = this.app?.getPath(fullPath);
              }
              
              pathData[path] = value !== undefined ? value : null;
            } catch (error) {
              pathData[path] = `Error: ${(error as Error).message}`;
            }
          }
        }
        
        this.app?.debug(`📊 Retrieved ${Object.keys(pathData).length} specific SignalK paths`);
        return {
          timestamp: new Date().toISOString(),
          source: 'real-time SignalK',
          context: contextToUse,
          requestedPaths: paths,
          data: pathData
        };
      }
    } catch (error) {
      this.app?.error(`Failed to get current SignalK data: ${(error as Error).message}`);
      return {
        error: `Failed to retrieve SignalK data: ${(error as Error).message}`,
        timestamp: new Date().toISOString()
      };
    }
  }

  /**
   * Get current data from specific vessel buffers
   */
  private getCurrentDataFromBuffers(context: string, paths?: string[]): any {
    if (!this.state) {
      return {
        timestamp: new Date().toISOString(),
        source: 'real-time SignalK buffers',
        context: context,
        data: {},
        error: 'Plugin state not available - cannot access data buffers'
      };
    }

    // DEBUG: Log what's actually in the buffers
    const totalBuffers = this.state.dataBuffers.size;
    const bufferKeys = Array.from(this.state.dataBuffers.keys());
    this.app?.debug(`🔍 DEBUG: Total buffers: ${totalBuffers}`);
    this.app?.debug(`🔍 DEBUG: All buffer keys: ${JSON.stringify(bufferKeys)}`);
    this.app?.debug(`🔍 DEBUG: Looking for context: "${context}"`);

    const currentData: any = {};
    let dataFound = false;
    let matchingKeys: string[] = [];

    // Iterate through data buffers to find matching context
    this.state.dataBuffers.forEach((buffer, bufferKey) => {
      if (bufferKey.startsWith(context + ':')) {
        matchingKeys.push(bufferKey);
        const path = bufferKey.split(':')[1];
        
        // If specific paths requested, only include those
        if (!paths || paths.includes(path)) {
          if (buffer.length > 0) {
            // Get the latest value from the buffer
            const latestRecord = buffer[buffer.length - 1];
            currentData[path] = {
              value: latestRecord.value,
              timestamp: latestRecord.signalk_timestamp || latestRecord.received_timestamp,
              source: latestRecord.source_label
            };
            dataFound = true;
          }
        }
      }
    });

    this.app?.debug(`🔍 DEBUG: Matching keys for "${context}": ${JSON.stringify(matchingKeys.slice(0, 5))}${matchingKeys.length > 5 ? '...' : ''}`);
    this.app?.debug(`📊 Retrieved ${Object.keys(currentData).length} current data points from buffers for ${context}`);
    
    return {
      timestamp: new Date().toISOString(),
      source: 'real-time SignalK buffers',
      context: context,
      requestedPaths: paths,
      data: currentData,
      dataFound: dataFound,
      debug: {
        totalBuffers: totalBuffers,
        matchingKeys: matchingKeys.length,
        sampleBufferKeys: bufferKeys.slice(0, 5)
      }
    };
  }

  /**
   * Get current data from all vessel buffers
   */
  private getAllVesselsCurrentDataFromBuffers(): any {
    if (!this.state) {
      return {
        timestamp: new Date().toISOString(),
        source: 'real-time SignalK buffers',
        context: 'vessels.*',
        data: {},
        error: 'Plugin state not available - cannot access data buffers'
      };
    }

    const allVesselData: any = {};

    // Group buffers by vessel context
    this.state.dataBuffers.forEach((buffer, bufferKey) => {
      if (bufferKey.includes(':')) {
        const [context, path] = bufferKey.split(':', 2);
        
        // Only include vessel contexts
        if (context.startsWith('vessels.')) {
          if (!allVesselData[context]) {
            allVesselData[context] = {};
          }
          
          if (buffer.length > 0) {
            // Get the latest value from the buffer
            const latestRecord = buffer[buffer.length - 1];
            allVesselData[context][path] = {
              value: latestRecord.value,
              timestamp: latestRecord.signalk_timestamp || latestRecord.received_timestamp,
              source: latestRecord.source_label
            };
          }
        }
      }
    });

    const vesselCount = Object.keys(allVesselData).length;
    const totalPaths = Object.values(allVesselData).reduce((sum: number, vesselData: any) => sum + Object.keys(vesselData).length, 0);
    
    this.app?.debug(`📊 Retrieved data from ${vesselCount} vessels with ${totalPaths} total data points from buffers`);
    
    return {
      timestamp: new Date().toISOString(),
      source: 'real-time SignalK buffers',
      context: 'vessels.*',
      data: allVesselData,
      vesselCount: vesselCount,
      totalDataPoints: totalPaths
    };
  }

  /**
   * Clean SignalK data by removing functions and circular references
   */
  private cleanSignalKData(obj: any, maxDepth: number = 3, currentDepth: number = 0): any {
    if (currentDepth >= maxDepth || obj === null || obj === undefined) {
      return obj;
    }

    if (typeof obj === 'function') {
      return '[Function]';
    }

    if (typeof obj !== 'object') {
      return obj;
    }

    if (obj instanceof Date) {
      return obj.toISOString();
    }

    if (Array.isArray(obj)) {
      return obj.map(item => this.cleanSignalKData(item, maxDepth, currentDepth + 1));
    }

    const cleaned: any = {};
    for (const [key, value] of Object.entries(obj)) {
      // Skip common SignalK metadata that's not useful for analysis
      if (['_updateTimes', '_sources', 'meta'].includes(key)) {
        continue;
      }
      
      try {
        cleaned[key] = this.cleanSignalKData(value, maxDepth, currentDepth + 1);
      } catch (error) {
        cleaned[key] = '[Circular Reference]';
      }
    }
    
    return cleaned;
  }

  /**
   * Identify relevant regimens based on user query keywords
   */
  private identifyRelevantRegimens(userQuery: string, conversationMessages?: Array<any>): string[] {
    try {
      const { getCurrentCommands } = require('./commands');
      const commands = getCurrentCommands();
      const queryLower = userQuery.toLowerCase();
      
      this.app?.debug(`🔍 Analyzing query for regimen keywords: "${userQuery}"`);
      
      const relevantRegimens = commands.filter((cmd: any) => {
        if (!cmd.keywords || cmd.keywords.length === 0) return false;
        
        // Check current query
        const hasMatchingKeyword = cmd.keywords.some((keyword: string) => 
          queryLower.includes(keyword.toLowerCase())
        );
        
        // If not found in current query, check conversation messages
        if (!hasMatchingKeyword && conversationMessages) {
          for (const message of conversationMessages.slice(-3)) { // Check last 3 messages
            if (message.content && typeof message.content === 'string') {
              const contentLower = message.content.toLowerCase();
              const hasContextKeyword = cmd.keywords.some((keyword: string) => 
                contentLower.includes(keyword.toLowerCase())
              );
              if (hasContextKeyword) {
                this.app?.debug(`✅ Found matching regimen from conversation context: ${cmd.command} (keyword: ${cmd.keywords.join(', ')})`);
                return true;
              }
            }
          }
        }
        
        if (hasMatchingKeyword) {
          this.app?.debug(`✅ Found matching regimen: ${cmd.command} (keywords: ${cmd.keywords.join(', ')})`);
        }
        
        return hasMatchingKeyword;
      }).map((cmd: any) => cmd.command);
      
      this.app?.debug(`🎯 Identified ${relevantRegimens.length} relevant regimens: [${relevantRegimens.join(', ')}]`);
      
      return relevantRegimens;
    } catch (error) {
      this.app?.error(`Failed to identify relevant regimens: ${(error as Error).message}`);
      return [];
    }
  }

  /**
   * Get paths associated with a regimen
   */
  private getPathsForRegimen(regimenName: string): string[] {
    try {
      const { loadWebAppConfig } = require('./commands');
      const webAppConfig = loadWebAppConfig(this.app);
      
      // Find all paths where regimen matches the command name
      const regimenPaths = webAppConfig.paths.filter((pathConfig: any) => 
        pathConfig.regimen === regimenName
      ).map((pathConfig: any) => pathConfig.path);
      
      this.app?.debug(`📊 Found ${regimenPaths.length} paths for regimen ${regimenName}: [${regimenPaths.join(', ')}]`);
      
      return regimenPaths;
    } catch (error) {
      this.app?.error(`Failed to get paths for regimen ${regimenName}: ${(error as Error).message}`);
      return [];
    }
  }

  /**
   * Check if user request contains real-time keywords
   */
  private checkForRealTimeKeywords(customPrompt: string, conversationMessages: Array<any>): boolean {
    const keywords = ['now', 'current', 'real-time', 'realtime', 'live', 'present', 'right now', 'at this moment'];
    
    // Check custom prompt
    const promptLower = customPrompt.toLowerCase();
    this.app?.debug(`🔍 Checking prompt: "${customPrompt}" (${promptLower})`);
    for (const keyword of keywords) {
      if (promptLower.includes(keyword)) {
        this.app?.debug(`✅ Found real-time keyword: "${keyword}"`);
        return true;
      }
    }
    
    // Check recent conversation messages
    for (const message of conversationMessages.slice(-3)) { // Check last 3 messages
      if (message.content && typeof message.content === 'string') {
        const contentLower = message.content.toLowerCase();
        for (const keyword of keywords) {
          if (contentLower.includes(keyword)) {
            return true;
          }
        }
      }
    }
    
    return false;
  }

  /**
   * Process a single tool call and return the result
   */
  private async processToolCall(toolCall: any): Promise<any> {
    if (toolCall.name === 'query_maritime_database') {
      const { sql, purpose } = toolCall.input as { sql: string; purpose: string };
      
      try {
        const queryResult = await this.executeSQLQuery(sql, purpose);
        const resultSummary = `Query "${purpose}" returned ${queryResult.length} rows:\n\n${JSON.stringify(queryResult, null, 2)}`;
        
        this.app?.debug(`✅ Query executed: ${purpose} - ${queryResult.length} rows returned`);
        
        return {
          type: 'tool_result',
          tool_use_id: toolCall.id,
          content: resultSummary
        };
        
      } catch (queryError) {
        return {
          type: 'tool_result',
          tool_use_id: toolCall.id,
          content: `Query failed: ${(queryError as Error).message}`
        };
      }
    } else if (toolCall.name === 'get_current_signalk_data') {
      const { paths, purpose, vesselContext } = toolCall.input as { paths?: string[]; purpose: string; vesselContext?: string };
      
      try {
        // Ensure paths is always an array or undefined
        const safePaths = paths ? (Array.isArray(paths) ? paths : [paths]) : undefined;
        const currentData = this.getCurrentSignalKData(safePaths, purpose, vesselContext);
        const resultSummary = `Current SignalK data "${purpose}":\n\n${JSON.stringify(currentData, null, 2)}`;
        
        this.app?.debug(`✅ Real-time data retrieved: ${purpose} - ${safePaths?.length || 'all'} paths`);
        
        return {
          type: 'tool_result',
          tool_use_id: toolCall.id,
          content: resultSummary
        };
        
      } catch (realTimeError) {
        return {
          type: 'tool_result',
          tool_use_id: toolCall.id,
          content: `Real-time data retrieval failed: ${(realTimeError as Error).message}`
        };
      }
    } else if (toolCall.name === 'find_regimen_episodes') {
      const { regimenName, timeRange, limit } = toolCall.input as { regimenName: string; timeRange?: any; limit?: number };
      
      try {
        const episodes = await this.findRegimenEpisodes(regimenName, timeRange, limit || 10);
        
        // Configure display limit based on total episodes found and request limit
        const displayLimit = Math.min(limit || 10, episodes.length);
        const showAll = limit && limit >= episodes.length;
        
        const resultSummary = `Found ${episodes.length} episodes for regimen "${regimenName}":\n\n${JSON.stringify(episodes.slice(0, displayLimit), null, 2)}${!showAll && episodes.length > displayLimit ? `\n\n... and ${episodes.length - displayLimit} more episodes` : ''}`;
        
        this.app?.debug(`✅ Episode detection completed: ${regimenName} - ${episodes.length} episodes found, showing ${displayLimit}`);
        
        return {
          type: 'tool_result',
          tool_use_id: toolCall.id,
          content: resultSummary
        };
        
      } catch (episodeError) {
        return {
          type: 'tool_result',
          tool_use_id: toolCall.id,
          content: `Episode detection failed: ${(episodeError as Error).message}`
        };
      }
    } else if (toolCall.name === 'generate_wind_analysis') {
      const { timeFrame, windSpeedCategories, chartType, vesselName } = toolCall.input as { 
        timeFrame?: string; 
        windSpeedCategories?: string[];
        chartType?: string;
        vesselName?: string;
      };
      
      const categories = windSpeedCategories || [
        'Calm (0-1 knots)',
        'Light Air (1-3 knots)', 
        'Light Breeze (4-6 knots)',
        'Gentle Breeze (7-10 knots)',
        'Moderate Breeze (11-15 knots)',
        'Fresh Breeze (16-21 knots)',
        'Strong Breeze (22+ knots)'
      ];
      
      const vessel = vesselName || 'Zennora';
      const period = timeFrame || '48 hours';
      const type = chartType || 'wind rose';
      
      const windAnalysisPrompt = `Query the wind direction and speed data for ${vessel} over the previous ${period}. Create a ${type} chart that shows wind direction frequency by compass sectors (N, NNE, NE, ENE, E, ESE, SE, SSE, S, SSW, SW, WSW, W, WNW, NW, NNW). Group the data into wind speed categories: ${categories.join(', ')}. For each compass direction, count how many hours of wind occurred in each speed category. Display this as a radar chart with ${categories.length} datasets - one for each wind speed range - using different colors (light blue, green, yellow, orange, red, dark red, purple for increasing intensities). The chart should show the frequency distribution of wind directions and intensities as a traditional ${type}.`;
      
      return {
        type: 'tool_result',
        tool_use_id: toolCall.id,
        content: `Generated detailed wind analysis prompt:\n\n${windAnalysisPrompt}\n\nNow executing this analysis...`
      };
    } else {
      // Handle unknown tool calls
      this.app?.debug(`⚠️ Unknown tool called: ${toolCall.name}`);
      return {
        type: 'tool_result',
        tool_use_id: toolCall.id,
        content: `Unknown tool "${toolCall.name}" requested. Available tools: query_maritime_database, get_current_signalk_data, find_regimen_episodes, generate_wind_analysis`
      };
    }
  }

  /**
   * Find episodes for a specific regimen using command state transitions
   */
  private async findRegimenEpisodes(regimenName: string, timeRange?: any, limit: number = 10): Promise<any[]> {
    // Build the episode boundary detection SQL
    let timeConstraint = '';
    if (timeRange?.start && timeRange?.end) {
      timeConstraint = `WHERE signalk_timestamp >= '${timeRange.start}' AND signalk_timestamp <= '${timeRange.end}'`;
    }
    
    const episodeSQL = `
      WITH transitions AS (
        SELECT
          signalk_timestamp,
          CAST(value AS BOOLEAN) as current_value,
          LAG(CAST(value AS BOOLEAN)) OVER (ORDER BY signalk_timestamp) as previous_value
        FROM '${this.dataDirectory}/vessels/*/commands/${regimenName}/*.parquet'
        ${timeConstraint}
        ORDER BY signalk_timestamp
      ),
      episode_boundaries AS (
        SELECT
          signalk_timestamp,
          current_value,
          CASE
            WHEN current_value = true AND (previous_value = false OR previous_value IS NULL) THEN 'start'
            WHEN current_value = false AND previous_value = true THEN 'end'
            ELSE NULL
          END as boundary_type
        FROM transitions
        WHERE current_value = true AND (previous_value = false OR previous_value IS NULL)
           OR current_value = false AND previous_value = true
      ),
      episodes AS (
        SELECT
          starts.signalk_timestamp as start_time,
          ends.signalk_timestamp as end_time,
          CASE 
            WHEN ends.signalk_timestamp IS NULL THEN 'active'
            ELSE 'completed'
          END as status
        FROM 
          (SELECT signalk_timestamp FROM episode_boundaries WHERE boundary_type = 'start') starts
        LEFT JOIN 
          (SELECT signalk_timestamp FROM episode_boundaries WHERE boundary_type = 'end') ends
        ON ends.signalk_timestamp = (
          SELECT MIN(signalk_timestamp) 
          FROM episode_boundaries 
          WHERE boundary_type = 'end' AND signalk_timestamp > starts.signalk_timestamp
        )
      )
      SELECT 
        start_time,
        end_time,
        status,
        CASE 
          WHEN end_time IS NOT NULL THEN 
            (EXTRACT(EPOCH FROM (end_time::TIMESTAMP - start_time::TIMESTAMP)) * 1000)::BIGINT
          ELSE NULL 
        END as duration_ms
      FROM episodes
      ORDER BY start_time DESC
      LIMIT ${limit}
    `;
    
    this.app?.debug(`🎬 Executing episode detection SQL for ${regimenName}`);
    
    try {
      const episodes = await this.executeSQLQuery(episodeSQL, `Episode boundary detection for ${regimenName}`);
      
      // Add regimen info and clean up the results
      return episodes.map(episode => ({
        regimen: regimenName,
        startTime: episode.start_time,
        endTime: episode.end_time,
        status: episode.status,
        durationMs: episode.duration_ms,
        paths: this.getPathsForRegimen(regimenName) // Include associated paths
      }));
    } catch (error) {
      this.app?.error(`Failed to find episodes for ${regimenName}: ${(error as Error).message}`);
      throw error;
    }
  }


  /**
   * Determine the appropriate column to use based on SignalK path type
   */
  private getValueColumn(pathName: string): string {
    // Position data and other complex objects use value_json
    if (pathName.includes('position') || pathName.includes('attitude') || 
        pathName.includes('coordinate') || pathName.includes('navigation.location')) {
      return 'value_json';
    }
    
    // Simple numeric/boolean values use value column
    return 'value';
  }

  /**
   * Auto-correct column usage in SQL queries based on SignalK path patterns
   */
  private correctColumnUsage(sql: string): string {
    // Define path patterns that use value_json (complex objects)
    const jsonPatterns = [
      // File path patterns in FROM clauses
      /FROM\s+['"]*[^'"]*\/position\/[^'"]*['"]/gi,
      /FROM\s+['"]*[^'"]*\/attitude\/[^'"]*['"]/gi,
      /FROM\s+['"]*[^'"]*\/coordinate\/[^'"]*['"]/gi,
      /FROM\s+['"]*[^'"]*\/navigation\/position[^'"]*['"]/gi,
      /FROM\s+['"]*[^'"]*\/navigation\/attitude[^'"]*['"]/gi,
      
      // Path column patterns in WHERE clauses
      /WHERE.*path.*position/gi,
      /WHERE.*path.*attitude/gi,
      /WHERE.*path.*coordinate/gi,
      
      // Direct mention of position/attitude/coordinate paths
      /navigation\.position/gi,
      /navigation\.attitude/gi,
      /navigation\.coordinate/gi
    ];
    
    let correctedSQL = sql;
    
    // Check if query involves JSON object paths
    const hasJsonPath = jsonPatterns.some(pattern => pattern.test(sql));
    if (hasJsonPath) {
      // Replace standalone 'value' references with 'value_json' for object queries
      // Use negative lookbehind/lookahead to avoid replacing value_json, value_latitude, etc.
      correctedSQL = correctedSQL.replace(/\bvalue\b(?!_|\w)/gi, 'value_json');
    }
    
    return correctedSQL;
  }

  /**
   * Execute SQL query safely against Parquet database
   */
  private async executeSQLQuery(sql: string, purpose: string): Promise<any[]> {
    // Auto-correct common column usage patterns
    const correctedSQL = this.correctColumnUsage(sql);
    
    // Validate query is read-only (starts with SELECT or WITH for CTEs)
    const trimmedSQL = correctedSQL.trim().toUpperCase();
    if (!trimmedSQL.startsWith('SELECT') && !trimmedSQL.startsWith('WITH')) {
      throw new Error('Only SELECT and WITH queries are allowed for security');
    }

    // Additional safety checks
    const dangerousKeywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'CREATE', 'ALTER', 'TRUNCATE'];
    for (const keyword of dangerousKeywords) {
      if (trimmedSQL.includes(keyword)) {
        throw new Error(`Dangerous SQL keyword '${keyword}' is not allowed`);
      }
    }

    const instance = await DuckDBInstance.create();
    const connection = await instance.connect();
    
    try {
      this.app?.debug(`🔍 Executing SQL query for: ${purpose}`);
      this.app?.debug(`📝 Original query: ${sql}`);
      if (correctedSQL !== sql) {
        this.app?.debug(`🔧 Corrected query: ${correctedSQL}`);
      }
      
      const result = await connection.runAndReadAll(correctedSQL);
      const data = result.getRowObjects();
      
      // Convert BigInt values to regular numbers to prevent serialization errors
      const cleanedData = data.map(row => {
        const cleanRow = { ...row };
        for (const key in cleanRow) {
          if (typeof cleanRow[key] === 'bigint') {
            cleanRow[key] = Number(cleanRow[key]);
          }
        }
        return cleanRow;
      });
      
      // Limit result size aggressively for production systems to prevent memory and token issues
      const maxRows = cleanedData.length > 1000 ? 500 : 1000; // Smaller limits for large datasets
      const limitedData = cleanedData.slice(0, maxRows);
      
      this.app?.debug(`✅ Query returned ${limitedData.length} rows`);
      return limitedData;
      
    } catch (error) {
      this.app?.error(`SQL query failed: ${(error as Error).message}`);
      throw new Error(`Database query failed: ${(error as Error).message}`);
    } finally {
      // DuckDB connections close automatically when instance is destroyed
    }
  }

  /**
   * Scan a vessel directory for available SignalK paths
   */
  private scanVesselPaths(vesselDir: string): string[] {
    const paths: string[] = [];
    
    function walkPaths(currentPath: string, relativePath: string = ''): void {
      try {
        const items = fs.readdirSync(currentPath);
        items.forEach((item: string) => {
          const fullPath = path.join(currentPath, item);
          const stat = fs.statSync(fullPath);

          if (stat.isDirectory() && item !== 'processed' && item !== 'failed') {
            const newRelativePath = relativePath ? `${relativePath}.${item}` : item;

            // Check if this directory has parquet files
            const hasParquetFiles = fs.readdirSync(fullPath)
              .some((file: string) => file.endsWith('.parquet'));

            if (hasParquetFiles) {
              paths.push(newRelativePath);
            } else {
              // Recurse into subdirectories
              walkPaths(fullPath, newRelativePath);
            }
          }
        });
      } catch (error) {
        // Skip directories that can't be read
      }
    }

    if (fs.existsSync(vesselDir)) {
      walkPaths(vesselDir);
    }
    
    return paths;
  }

  /**
   * Generate enhanced schema information for Claude
   */
  private async getEnhancedSchemaForClaude(): Promise<string> {
    this.app?.debug('🔧 Getting enhanced schema for Claude...');
    const dataDir = this.dataDirectory || '';
    this.app?.debug(`📂 Data directory: "${dataDir}"`);
    
    let selfContextPath = 'vessels/self';
    if (this.app?.selfContext) {
      selfContextPath = this.app.selfContext.replace(/\./g, '/').replace(/:/g, '_');
      this.app?.debug(`🛥️ Self context path: "${selfContextPath}"`);
    }
    
    // Get actual available paths from the filesystem  
    let availablePathsInfo = '';
    let otherVesselsInfo = '';
    
    // Get self context dynamically from SignalK
    const selfContext = this.app?.selfContext || 'vessels.self';
    
    try {
      if (this.app && dataDir) {
        // Get your vessel's paths
        this.app?.debug('📊 Scanning available paths...');
        this.app?.debug(`📍 App.selfContext: "${this.app?.selfContext}"`);
        const paths = getAvailablePaths(dataDir, this.app);
        this.app?.debug(`📈 Found ${paths.length} available paths`);
        if (paths.length === 0) {
          this.app?.debug('⚠️ No available paths found - this could cause Claude to not see schema information');
        }
        const pathList = paths.map(p => p.path).join('\n- ');
        availablePathsInfo = `
CRITICAL: ONLY USE THESE EXACT PATHS - DO NOT MAKE UP OR GUESS PATH NAMES:
- ${pathList}

THESE ARE THE ONLY VALID PATHS (${paths.length} total paths):
${paths.map(p => `- ${p.path} (${p.fileCount} files)`).join('\n')}

DO NOT USE ANY PATH NOT LISTED ABOVE. DO NOT GUESS PATH NAMES LIKE "windAvg" - ONLY USE THE EXACT PATHS PROVIDED.`;

        // Check for other vessels by scanning the vessels directory
        const vesselsDir = path.join(dataDir, 'vessels');
        if (fs.existsSync(vesselsDir)) {
          const vesselDirs = fs.readdirSync(vesselsDir, { withFileTypes: true })
            .filter(dirent => dirent.isDirectory())
            .map(dirent => dirent.name)
            .filter(name => name !== selfContextPath.split('/')[1]); // Exclude own vessel
          
          if (vesselDirs.length > 0) {
            // Scan each other vessel's directory for their available paths
            let otherVesselPaths: string[] = [];
            
            for (const vesselDir of vesselDirs) {
              const vesselPath = path.join(vesselsDir, vesselDir);
              try {
                const vesselPaths = this.scanVesselPaths(vesselPath);
                otherVesselPaths = otherVesselPaths.concat(vesselPaths.map(p => `${vesselDir}: ${p}`));
              } catch (error) {
                // Skip vessels that can't be scanned
              }
            }
            
            otherVesselsInfo = `
OTHER VESSELS: ${vesselDirs.length} vessels detected in area

Distance and proximity data:
- navigation.distanceToSelf: Distance from your vessel in meters
- navigation.closestApproach: Contains distance (meters) and timeTo (seconds) 
  Example: {"distance": 762.6940177184225, "timeTo": -30673.210651733683}

Common vessel data paths (when available):
- name: Vessel name (string)
- mmsi: MMSI number (string)
- navigation.position: GPS coordinates (latitude/longitude)
- navigation.speedOverGround: Speed in meters per second
- navigation.courseOverGroundTrue: Course direction in radians
- navigation.closestApproach: Closest approach calculations
- navigation.distanceToSelf: Distance from your vessel in meters

Query example: SELECT * FROM 'data/vessels/*/navigation/position/*.parquet'`;
          } else {
            otherVesselsInfo = `
OTHER VESSELS: None detected in this dataset`;
          }
        }
      }
    } catch (error) {
      this.app?.error(`❌ Failed to scan filesystem for paths: ${(error as Error).message}`);
      availablePathsInfo = '\nAVAILABLE DATA PATHS: Unable to scan filesystem';
    }
    
    const schemaResult = `MARITIME DATABASE SCHEMA:
Base Directory: ${dataDir}
File Pattern: {contextPath}/{signalk_path}/{filename}.parquet

VESSEL CONTEXTS:
- Your vessel: ${selfContextPath}
${availablePathsInfo}
${otherVesselsInfo}

COLUMN STRUCTURE:
- context (VARCHAR): Vessel/source identifier (e.g., "${selfContext}")
- meta (VARCHAR): Metadata (usually null)
- path (VARCHAR): SignalK data path (e.g., "navigation.position", "environment.wind.speedTrue")
- received_timestamp (VARCHAR): ISO timestamp when data was received (e.g., "YYYY-MM-DDTHH:MM:SS.sssZ")
- signalk_timestamp (VARCHAR): ISO timestamp from SignalK data (e.g., "YYYY-MM-DDTHH:MM:SSZ")
- source (VARCHAR): JSON string with source info (e.g., '{"sentence":"GLL","talker":"GN","type":"NMEA0183"}')
- source_label (VARCHAR): Source device label (e.g., "maiana.GN")
- source_pgn, source_src (VARCHAR): Usually null for NMEA0183
- source_type (VARCHAR): Data source type (e.g., "NMEA0183")
- value (VARCHAR): Simple numeric values (usually null for complex data)
- value_json (VARCHAR): JSON representation of complex values (e.g., '{"longitude":-72.08,"latitude":41.32}')
- value_latitude, value_longitude (DOUBLE): Extracted position coordinates

MANDATORY QUERY SYNTAX - USE EXACT FILE PATHS:
- Recent position: SELECT received_timestamp, value_latitude, value_longitude FROM '${dataDir}/${selfContextPath}/navigation/position/*.parquet' ORDER BY received_timestamp DESC LIMIT 100
- Speed analysis: SELECT AVG(CAST(value AS DOUBLE)) as avg_speed FROM '${dataDir}/${selfContextPath}/navigation/speedOverGround/*.parquet' WHERE signalk_timestamp >= '2024-01-01T00:00:00Z'
- Wind patterns: SELECT DATE_TRUNC('hour', CAST(received_timestamp AS TIMESTAMP)) as hour, AVG(CAST(value AS DOUBLE)) FROM '${dataDir}/${selfContextPath}/environment/wind/speedTrue/*.parquet' GROUP BY hour ORDER BY hour
- Time-based filtering: WHERE signalk_timestamp >= 'YYYY-MM-DDTHH:MM:SSZ' AND signalk_timestamp <= 'YYYY-MM-DDTHH:MM:SSZ'

CRITICAL: Your vessel's data is at: ${dataDir}/${selfContextPath}/
IMPORTANT: Use the vessel's MMSI from the VESSEL CONTEXT section above to filter data by context column.
Example: SELECT * FROM '${dataDir}/${selfContextPath}/navigation/position/*.parquet' WHERE context LIKE '%[MMSI_FROM_VESSEL_CONTEXT]%' LIMIT 5

MULTI-VESSEL QUERIES:
- Find all vessels: SELECT DISTINCT context FROM '${dataDir}/vessels/*/navigation/position/*.parquet'
- All vessels positions: SELECT context, received_timestamp, value_latitude, value_longitude FROM '${dataDir}/vessels/*/navigation/position/*.parquet' ORDER BY received_timestamp DESC
- Specific vessel by MMSI: SELECT * FROM '${dataDir}/vessels/urn_mrn_imo_mmsi_123456789/navigation/position/*.parquet'
- Vessel traffic analysis: SELECT context, COUNT(*) as message_count FROM '${dataDir}/vessels/*/navigation/position/*.parquet' GROUP BY context

IMPORTANT NOTES:
- All timestamps are ISO strings in VARCHAR format, not milliseconds
- Use CAST(signalk_timestamp AS TIMESTAMP) for date functions  
- Use CAST(value AS DOUBLE) to convert string numbers to numeric
- Timestamps have NO milliseconds - format is always YYYY-MM-DDTHH:MM:SSZ
- Position data: use value_latitude/value_longitude columns directly (they're already DOUBLE)
- Complex data: parse value_json for structured data like wind direction/speed
- Always use glob patterns like '*.parquet' for file matching
- Path structure follows SignalK standard (navigation.position, environment.wind.speedTrue, etc.)

DATA LIMITATIONS:
- NO meta/*.parquet files exist - vessel metadata is provided in the vessel context above
- For vessel names/specs, refer to the VESSEL CONTEXT section, not database queries`;

    this.app?.debug(`✅ Generated schema result (${schemaResult.length} chars)`);
    
    // Save the actual schema to a file for inspection
    try {
      if (this.dataDirectory) {
        const schemaDir = path.join(this.dataDirectory, 'claude-schemas');
        await fs.ensureDir(schemaDir);
        
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const filename = `claude-schema-${timestamp}.txt`;
        const filepath = path.join(schemaDir, filename);
        
        await fs.writeFile(filepath, schemaResult, 'utf8');
        this.app?.debug(`📄 Schema saved to: ${filepath}`);
      }
    } catch (error) {
      this.app?.error(`Failed to save schema to file: ${(error as Error).message}`);
      // Don't throw - this is not critical to the analysis
    }
    
    return schemaResult;
  }

  /**
   * Test Claude API connection
   */
  async testConnection(): Promise<{ success: boolean; error?: string }> {
    try {
      const response = await this.client.messages.create({
        model: this.config.model,
        max_tokens: 50,
        messages: [{
          role: 'user',
          content: 'Hello! Please respond with "Claude AI connection successful" to test the connection.'
        }]
      });

      const content = response.content[0] as any;
      if (content && content.text && content.text.includes('successful')) {
        return { success: true };
      } else {
        return { success: false, error: 'Unexpected response from Claude API' };
      }

    } catch (error) {
      return { success: false, error: (error as Error).message };
    }
  }

  /**
   * Get vessel context manager
   */
  getVesselContextManager(): VesselContextManager {
    return this.vesselContextManager;
  }

  /**
   * Refresh vessel information from SignalK
   */
  async refreshVesselContext(): Promise<void> {
    try {
      await this.vesselContextManager.refreshVesselInfo();
      this.app?.debug('Vessel context refreshed from SignalK data');
    } catch (error) {
      this.app?.error(`Failed to refresh vessel context: ${(error as Error).message}`);
      throw error;
    }
  }
}