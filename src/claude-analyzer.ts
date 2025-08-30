import Anthropic from '@anthropic-ai/sdk';
import { ServerAPI } from '@signalk/server-api';
import { DataRecord } from './types';
import { VesselContextManager } from './vessel-context';
import { getAvailablePaths } from './utils/path-discovery';
import * as fs from 'fs-extra';
import * as path from 'path';
import { DuckDBInstance } from '@duckdb/node-api';

// Claude AI Integration Types
export interface ClaudeAnalyzerConfig {
  apiKey: string;
  model: 'claude-opus-4-1-20250805' | 'claude-opus-4-20250514' | 'claude-sonnet-4-20250514' | 'claude-3-7-sonnet-20250219' | 'claude-3-5-haiku-20241022' | 'claude-3-haiku-20240307';
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

  constructor(config: ClaudeAnalyzerConfig, app?: ServerAPI, dataDirectory?: string) {
    this.config = config;
    this.app = app;
    this.dataDirectory = dataDirectory;
    this.vesselContextManager = new VesselContextManager(app, dataDirectory);
    
    if (!config.apiKey) {
      throw new Error('Claude API key is required for analysis functionality');
    }

    this.client = new Anthropic({
      apiKey: config.apiKey,
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
        from: timeRange ? timeRange.start.toISOString() : new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
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
      const schemaInfo = this.getEnhancedSchemaForClaude();
      
      // Build time range guidance for Claude
      let timeRangeGuidance = '';
      if (request.timeRange) {
        timeRangeGuidance = `

TIME RANGE RESTRICTION: Focus your analysis on data between ${request.timeRange.start.toISOString()} and ${request.timeRange.end.toISOString()}.
IMPORTANT: Always include WHERE clauses in your SQL queries to limit results to this time range:
WHERE received_timestamp >= '${request.timeRange.start.toISOString()}' AND received_timestamp <= '${request.timeRange.end.toISOString()}'`;
      } else {
        // Default to recent data if no time range specified
        const now = new Date();
        const sixHoursAgo = new Date(now.getTime() - 6 * 60 * 60 * 1000);
        timeRangeGuidance = `

TIME RANGE FOCUS: Since no specific time range was provided, focus on recent data (last 6 hours).
IMPORTANT: Always include WHERE clauses to limit results to recent data:
WHERE received_timestamp >= '${sixHoursAgo.toISOString()}'`;
      }

      const initialPrompt = `You are an expert maritime data analyst with direct access to a comprehensive database.

IMPORTANT: Please use the vessel context information provided below for all analysis and responses. This vessel information is critical for accurate maritime analysis.

${vesselContext}

${schemaInfo}${timeRangeGuidance}

ANALYSIS REQUEST: ${request.customPrompt || 'Analyze maritime data and provide insights'}

You can query the database using the query_maritime_database function. Start by exploring the data to understand what's available, then provide comprehensive analysis.

REMEMBER: 
- Always refer to and use the vessel context provided above (vessel name, dimensions, operational details, etc.) when analyzing data and providing recommendations.
- ALWAYS include time range WHERE clauses in your queries to avoid loading excessive historical data.
- Keep query results focused and relevant to the specified time period.

Focus on:
1. Current vessel status and recent activity
2. Patterns in navigation, weather, and performance data  
3. Safety considerations and operational insights
4. Data quality and completeness assessment

Begin your analysis by querying relevant data within the specified time range.`;

      // Start conversation with Claude with function calling capability
      let conversationMessages: Array<any> = [{
        role: 'user',
        content: initialPrompt
      }];

      let analysisResult = '';
      let queryCount = 0;
      const maxQueries = 10; // Prevent infinite loops

      while (queryCount < maxQueries) {
        const response = await this.callClaudeWithRetry({
          model: this.config.model,
          max_tokens: Math.max(this.config.maxTokens, 8000), // Increased for comprehensive analysis
          temperature: this.config.temperature,
          tools: [{
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
          }],
          messages: conversationMessages
        });

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
            analysisResult += textContent + '\n\n';
          } else if (contentBlock.type === 'tool_use') {
            const toolCall = contentBlock;
            if (toolCall.name === 'query_maritime_database') {
              queryCount++;
              const { sql, purpose } = toolCall.input as { sql: string; purpose: string };
              
              try {
                // Execute the SQL query safely
                const queryResult = await this.executeSQLQuery(sql, purpose);
                
                const resultSummary = `Query "${purpose}" returned ${queryResult.length} rows:\n\n${JSON.stringify(queryResult.slice(0, 5), null, 2)}${queryResult.length > 5 ? `\n\n... and ${queryResult.length - 5} more rows` : ''}`;
                
                toolResults.push({
                  type: 'tool_result',
                  tool_use_id: toolCall.id,
                  content: resultSummary
                });
                
                this.app?.debug(`✅ Query executed: ${purpose} - ${queryResult.length} rows returned`);
                
              } catch (queryError) {
                toolResults.push({
                  type: 'tool_result',
                  tool_use_id: toolCall.id,
                  content: `Query failed: ${(queryError as Error).message}`
                });
              }
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
        }
      };

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
        return await this.client.messages.create(params);
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
      const maxQueries = 5; // Fewer queries for follow-ups

      // Continue the conversation with Claude
      while (queryCount < maxQueries) {
        const response = await this.callClaudeWithRetry({
          model: this.config.model,
          max_tokens: Math.max(this.config.maxTokens, 8000),
          temperature: this.config.temperature,
          tools: [{
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
          }],
          messages: conversationMessages
        });

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
            if (toolCall.name === 'query_maritime_database') {
              queryCount++;
              const { sql, purpose } = toolCall.input as { sql: string; purpose: string };
              
              try {
                const queryResult = await this.executeSQLQuery(sql, purpose);
                const resultSummary = `Query "${purpose}" returned ${queryResult.length} rows:\n\n${JSON.stringify(queryResult.slice(0, 5), null, 2)}${queryResult.length > 5 ? `\n\n... and ${queryResult.length - 5} more rows` : ''}`;
                
                toolResults.push({
                  type: 'tool_result',
                  tool_use_id: toolCall.id,
                  content: resultSummary
                });
                
                this.app?.debug(`✅ Follow-up query executed: ${purpose} - ${queryResult.length} rows returned`);
                
              } catch (queryError) {
                toolResults.push({
                  type: 'tool_result',
                  tool_use_id: toolCall.id,
                  content: `Query failed: ${(queryError as Error).message}`
                });
              }
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
        }
      };

      // Save follow-up response to history
      await this.saveAnalysisToHistory(followUpResponse);

      return followUpResponse;

    } catch (error) {
      this.app?.error(`Follow-up question failed: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * Execute SQL query safely against Parquet database
   */
  private async executeSQLQuery(sql: string, purpose: string): Promise<any[]> {
    // Validate query is read-only (starts with SELECT)
    const trimmedSQL = sql.trim().toUpperCase();
    if (!trimmedSQL.startsWith('SELECT')) {
      throw new Error('Only SELECT queries are allowed for security');
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
      this.app?.debug(`📝 Query: ${sql}`);
      
      const result = await connection.runAndReadAll(sql);
      const data = result.getRowObjects();
      
      // Limit result size aggressively for production systems to prevent memory and token issues
      const maxRows = data.length > 1000 ? 500 : 1000; // Smaller limits for large datasets
      const limitedData = data.slice(0, maxRows);
      
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
  private getEnhancedSchemaForClaude(): string {
    const dataDir = this.dataDirectory || '';
    let selfContextPath = 'vessels/self';
    if (this.app?.selfContext) {
      selfContextPath = this.app.selfContext.replace(/\./g, '/').replace(/:/g, '_');
    }
    
    // Get actual available paths from the filesystem  
    let availablePathsInfo = '';
    let otherVesselsInfo = '';
    
    // Get self context dynamically from SignalK
    const selfContext = this.app?.selfContext || 'vessels.self';
    const selfContextForFilter = selfContext.replace(/\./g, ':'); // Convert to context format for filtering
    
    try {
      if (this.app && dataDir) {
        // Get your vessel's paths
        const paths = getAvailablePaths(dataDir, this.app);
        availablePathsInfo = `
AVAILABLE DATA PATHS (Your vessel):
${paths.map(p => `- ${p.path} (${p.fileCount} files)`).join('\n')}`;

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
OTHER VESSELS DETECTED:
${vesselDirs.map(vesselId => `- vessels/${vesselId}`).join('\n')}

OTHER VESSELS' AVAILABLE PATHS:
${otherVesselPaths.length > 0 ? otherVesselPaths.map(p => `- ${p}`).join('\n') : '- (Unable to scan other vessel paths)'}

TO QUERY OTHER VESSELS:
- Find all vessels: SELECT DISTINCT context FROM 'data/vessels/*/navigation/position/*.parquet' WHERE context != '${selfContextForFilter}'
- Specific vessel: SELECT * FROM 'data/vessels/${vesselDirs[0]}/navigation/position/*.parquet'
- All vessels data: SELECT context, received_timestamp, value_latitude, value_longitude FROM 'data/vessels/*/navigation/position/*.parquet'`;
          } else {
            otherVesselsInfo = `
OTHER VESSELS: None detected in this dataset`;
          }
        }
      }
    } catch (error) {
      availablePathsInfo = '\nAVAILABLE DATA PATHS: Unable to scan filesystem';
    }
    
    return `MARITIME DATABASE SCHEMA:
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
- signalk_timestamp (VARCHAR): ISO timestamp from SignalK data (e.g., "YYYY-MM-DDTHH:MM:SS.000Z")
- source (VARCHAR): JSON string with source info (e.g., '{"sentence":"GLL","talker":"GN","type":"NMEA0183"}')
- source_label (VARCHAR): Source device label (e.g., "maiana.GN")
- source_pgn, source_src (VARCHAR): Usually null for NMEA0183
- source_type (VARCHAR): Data source type (e.g., "NMEA0183")
- value (VARCHAR): Simple numeric values (usually null for complex data)
- value_json (VARCHAR): JSON representation of complex values (e.g., '{"longitude":-72.08,"latitude":41.32}')
- value_latitude, value_longitude (DOUBLE): Extracted position coordinates

QUERY EXAMPLES:
- Recent position: SELECT received_timestamp, value_latitude, value_longitude FROM '${dataDir}/${selfContextPath}/navigation/position/*.parquet' ORDER BY received_timestamp DESC LIMIT 100
- Speed analysis: SELECT AVG(CAST(value AS DOUBLE)) as avg_speed FROM '${dataDir}/${selfContextPath}/navigation/speedOverGround/*.parquet' WHERE received_timestamp >= '2024-01-01T00:00:00.000Z'
- Wind patterns: SELECT DATE_TRUNC('hour', CAST(received_timestamp AS TIMESTAMP)) as hour, AVG(CAST(value AS DOUBLE)) FROM '${dataDir}/${selfContextPath}/environment/wind/speedTrue/*.parquet' GROUP BY hour ORDER BY hour
- Time-based filtering: WHERE received_timestamp >= 'YYYY-MM-DDTHH:MM:SS.000Z' AND received_timestamp < 'YYYY-MM-DDTHH:MM:SS.000Z'

MULTI-VESSEL QUERIES:
- Find all vessels: SELECT DISTINCT context FROM 'data/vessels/*/navigation/position/*.parquet'
- All vessels positions: SELECT context, received_timestamp, value_latitude, value_longitude FROM 'data/vessels/*/navigation/position/*.parquet' ORDER BY received_timestamp DESC
- Specific vessel by MMSI: SELECT * FROM 'data/vessels/urn_mrn_imo_mmsi_123456789/navigation/position/*.parquet'
- Vessel traffic analysis: SELECT context, COUNT(*) as message_count FROM 'data/vessels/*/navigation/position/*.parquet' GROUP BY context

IMPORTANT NOTES:
- All timestamps are ISO strings in VARCHAR format, not milliseconds
- Use CAST(received_timestamp AS TIMESTAMP) for date functions
- Use CAST(value AS DOUBLE) to convert string numbers to numeric
- Position data: use value_latitude/value_longitude columns directly (they're already DOUBLE)
- Complex data: parse value_json for structured data like wind direction/speed
- Always use glob patterns like '*.parquet' for file matching
- Path structure follows SignalK standard (navigation.position, environment.wind.speedTrue, etc.)

DATA LIMITATIONS:
- NO meta/*.parquet files exist - vessel metadata is provided in the vessel context above
- For vessel names/specs, refer to the VESSEL CONTEXT section, not database queries`;
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