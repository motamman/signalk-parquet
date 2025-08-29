import Anthropic from '@anthropic-ai/sdk';
import { ServerAPI } from '@signalk/server-api';
import { DataRecord } from './types';
import { VesselContextManager } from './vessel-context';
import * as fs from 'fs-extra';
import * as path from 'path';

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
      this.app?.debug(`Starting Claude analysis: ${request.analysisType} for ${request.dataPath}`);
      
      // Prepare data for analysis
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
      
      // Sample data if too large (Claude token limits)
      const sampledData = this.sampleDataForAnalysis(data, 500);
      
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

    // Intelligent sampling - take some from beginning, middle, and end
    const step = Math.floor(data.length / maxSamples);
    const sampled: DataRecord[] = [];
    
    for (let i = 0; i < data.length && sampled.length < maxSamples; i += step) {
      sampled.push(data[i]);
    }

    // Always include the most recent records
    const recentCount = Math.min(50, maxSamples - sampled.length);
    const recentRecords = data.slice(-recentCount);
    
    return [...sampled, ...recentRecords].slice(0, maxSamples);
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
          timeRange: request.timeRange
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