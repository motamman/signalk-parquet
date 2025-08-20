import { Observable, BehaviorSubject, combineLatest, timer } from 'rxjs';
import { map, switchMap, distinctUntilChanged, shareReplay, startWith } from 'rxjs/operators';
import { HistoryAPI } from './HistoryAPI';
import { HistoryAPIValue, Context } from './HistoryAPI-types';

export interface DataSourceConfig {
  path: string;
  timeWindow?: string | [string, string]; // "30s" or [from, to]
  resolution?: string; // "1s", "5s", "1m"
  aggregates?: ('current' | 'min' | 'max' | 'average' | 'first' | 'last' | 'median')[];
  context?: string; // Changed from Context to string for simplicity
  refreshInterval?: number; // milliseconds, default 1000
}

export interface StreamValue {
  path: string;
  timestamp: string;
  value: any;
  aggregates?: {
    min?: any;
    max?: any;
    average?: any;
    first?: any;
    last?: any;
    median?: any;
  };
}

export interface StreamResponse {
  path: string;
  values: StreamValue[];
  meta?: {
    timeWindow: string | [string, string];
    resolution?: string;
    count: number;
  };
}

export class UniversalDataSource {
  private config: DataSourceConfig;
  private historyAPI: HistoryAPI;
  private refreshSubject: BehaviorSubject<number>;
  
  constructor(config: DataSourceConfig, historyAPI: HistoryAPI) {
    this.config = {
      refreshInterval: 1000,
      aggregates: ['current'],
      context: 'vessels.self',
      ...config
    };
    this.historyAPI = historyAPI;
    this.refreshSubject = new BehaviorSubject(0);
  }

  /**
   * Creates an Observable stream that emits data updates
   */
  stream(): Observable<StreamResponse> {
    const refreshTimer = timer(0, this.config.refreshInterval!);
    
    return refreshTimer.pipe(
      switchMap(() => {
        return this.fetchData();
      }),
      distinctUntilChanged((prev: StreamResponse, curr: StreamResponse) => {
        // For streaming, we want to emit regularly even if data is the same
        // Only filter out if the values array is empty (no data available)
        const prevEmpty = !prev.values || prev.values.length === 0;
        const currEmpty = !curr.values || curr.values.length === 0;
        
        // If both are empty, they're the same (filter out)
        if (prevEmpty && currEmpty) {
          return true;
        }
        
        // Always emit if we have data (even if same values)
        return false;
      }),
      shareReplay(1)
    );
  }

  /**
   * Get historical data as a Promise (one-time query)
   */
  async query(from?: string, to?: string): Promise<StreamResponse> {
    return this.fetchData(from, to);
  }

  /**
   * Manually trigger a refresh
   */
  refresh(): void {
    this.refreshSubject.next(Date.now());
  }

  /**
   * Update configuration and restart stream
   */
  updateConfig(newConfig: Partial<DataSourceConfig>): void {
    this.config = { ...this.config, ...newConfig };
    this.refresh();
  }

  private async fetchData(from?: string, to?: string): Promise<StreamResponse> {
    try {
      // Determine time window
      let fromTime: string;
      let toTime: string;

      if (from && to) {
        fromTime = from;
        toTime = to;
      } else if (Array.isArray(this.config.timeWindow)) {
        [fromTime, toTime] = this.config.timeWindow;
      } else if (this.config.timeWindow) {
        // Convert duration to time range
        toTime = new Date().toISOString();
        fromTime = this.calculateFromTime(this.config.timeWindow);
      } else {
        // Default to last hour
        toTime = new Date().toISOString();
        fromTime = this.calculateFromTime('1h');
      }

      // Query history API using the streaming-friendly method
      const historyData = await this.historyAPI.queryForStreaming(
        this.config.path,
        fromTime,
        toTime,
        this.config.context
      );

      // Find data for our specific path
      const pathData = historyData.find((item: any) => item.path === this.config.path);
      
      if (!pathData || !pathData.values) {
        return {
          path: this.config.path,
          values: [],
          meta: {
            timeWindow: this.config.timeWindow || [fromTime, toTime],
            resolution: this.config.resolution,
            count: 0
          }
        };
      }

      // Transform data to our format with aggregates
      const values = await this.transformValues(pathData.values, fromTime, toTime);

      return {
        path: this.config.path,
        values,
        meta: {
          timeWindow: this.config.timeWindow || [fromTime, toTime],
          resolution: this.config.resolution,
          count: values.length
        }
      };

    } catch (error) {
      console.error(`UniversalDataSource error for path ${this.config.path}:`, error);
      return {
        path: this.config.path,
        values: [],
        meta: {
          timeWindow: this.config.timeWindow || '1h',
          count: 0
        }
      };
    }
  }

  private async transformValues(values: HistoryAPIValue[], from: string, to: string): Promise<StreamValue[]> {
    const aggregates = this.config.aggregates || ['current'];
    
    // Safety checks
    if (!values || !Array.isArray(values) || values.length === 0) return [];
    if (!aggregates || !Array.isArray(aggregates) || aggregates.length === 0) return [];
    
    // Limit processing to reasonable data size to prevent memory issues
    const maxValues = 10000;
    const processedValues = values.length > maxValues ? values.slice(-maxValues) : values;

    const result: StreamValue[] = [];
    const currentTime = new Date().toISOString();

    // Handle each requested aggregate type
    for (const aggregate of aggregates) {
      switch (aggregate) {
        case 'current': {
          const latest = processedValues[processedValues.length - 1];
          if (latest) {
            result.push({
              path: this.config.path,
              timestamp: latest.timestamp,
              value: latest.value
            });
          }
          break;
        }
        
        case 'min': {
          const numericValues = processedValues
            .map(v => typeof v.value === 'number' ? v.value : null)
            .filter(v => v !== null) as number[];
          
          if (numericValues.length > 0) {
            const minValue = Math.min(...numericValues);
            result.push({
              path: this.config.path,
              timestamp: currentTime,
              value: minValue
            });
          }
          break;
        }
        
        case 'max': {
          const numericValues = processedValues
            .map(v => typeof v.value === 'number' ? v.value : null)
            .filter(v => v !== null) as number[];
          
          if (numericValues.length > 0) {
            const maxValue = Math.max(...numericValues);
            result.push({
              path: this.config.path,
              timestamp: currentTime,
              value: maxValue
            });
          }
          break;
        }
        
        case 'average': {
          const numericValues = processedValues
            .map(v => typeof v.value === 'number' ? v.value : null)
            .filter(v => v !== null) as number[];
          
          if (numericValues.length > 0) {
            const avgValue = numericValues.reduce((a, b) => a + b, 0) / numericValues.length;
            result.push({
              path: this.config.path,
              timestamp: currentTime,
              value: Math.round(avgValue * 100) / 100 // Round to 2 decimal places
            });
          }
          break;
        }
        
        case 'first': {
          const first = processedValues[0];
          if (first) {
            result.push({
              path: this.config.path,
              timestamp: first.timestamp,
              value: first.value
            });
          }
          break;
        }
        
        case 'last': {
          const last = processedValues[processedValues.length - 1];
          if (last) {
            result.push({
              path: this.config.path,
              timestamp: last.timestamp,
              value: last.value
            });
          }
          break;
        }
        
        case 'median': {
          const numericValues = processedValues
            .map(v => typeof v.value === 'number' ? v.value : null)
            .filter(v => v !== null) as number[];
          
          if (numericValues.length > 0) {
            const sorted = [...numericValues].sort((a, b) => a - b);
            const mid = Math.floor(sorted.length / 2);
            const medianValue = sorted.length % 2 === 0 
              ? (sorted[mid - 1] + sorted[mid]) / 2 
              : sorted[mid];
            
            result.push({
              path: this.config.path,
              timestamp: currentTime,
              value: Math.round(medianValue * 100) / 100 // Round to 2 decimal places
            });
          }
          break;
        }
      }
    }

    return result;
  }

  private calculateFromTime(duration: string): string {
    const now = new Date();
    const match = duration.match(/^(\d+)([smhd])$/);
    
    if (!match) {
      throw new Error(`Invalid duration format: ${duration}`);
    }

    const amount = parseInt(match[1]);
    const unit = match[2];

    switch (unit) {
      case 's':
        now.setSeconds(now.getSeconds() - amount);
        break;
      case 'm':
        now.setMinutes(now.getMinutes() - amount);
        break;
      case 'h':
        now.setHours(now.getHours() - amount);
        break;
      case 'd':
        now.setDate(now.getDate() - amount);
        break;
      default:
        throw new Error(`Unsupported time unit: ${unit}`);
    }

    return now.toISOString();
  }
}

// Factory function for easy creation
export function createDataSource(config: DataSourceConfig, historyAPI: HistoryAPI): UniversalDataSource {
  return new UniversalDataSource(config, historyAPI);
}

// Utility function to combine multiple data sources
export function combineDataSources(...sources: UniversalDataSource[]): Observable<StreamResponse[]> {
  const streams = sources.map(source => source.stream());
  return combineLatest(streams);
}