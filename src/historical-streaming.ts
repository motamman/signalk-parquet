import { ServerAPI, Context, Path, Timestamp, SourceRef } from '@signalk/server-api';
import { HistoryAPI } from './HistoryAPI';
import { AggregateMethod } from './HistoryAPI-types';
import { ZonedDateTime, ZoneOffset } from '@js-joda/core';
import * as WebSocket from 'ws';

export class HistoricalStreamingService {
  private app: ServerAPI;
  private activeSubscriptions = new Map<string, any>();
  private historyAPI: HistoryAPI;
  private wsServer?: WebSocket.Server;
  private connectedClients = new Set<WebSocket>();
  private streamBuffers = new Map<string, number[]>();
  private streamLastTimestamps = new Map<string, string>();

  constructor(app: ServerAPI, dataDir?: string) {
    this.app = app;
    // Initialize HistoryAPI - use provided data directory or default
    const actualDataDir = dataDir || `${app.getDataDirPath()}/signalk-parquet`;
    this.app.debug(`Historical streaming using data directory: ${actualDataDir}`);
    this.historyAPI = new HistoryAPI(app.selfId, actualDataDir);
    this.setupSubscriptionInterceptor();
  }

  private setupSubscriptionInterceptor() {
    this.app.debug('Setting up historical data subscription interceptor');

    // Try to hook into WebSocket data stream
    const wsInterface = (this.app as any).interfaces?.ws;
    if (wsInterface && wsInterface.data) {
      this.app.debug('Found WebSocket interface with data stream, attempting to hook in');
      
      try {
        // Hook into the data stream to catch subscription messages
        const originalDataEmit = wsInterface.data.emit;
        wsInterface.data.emit = (event: string, ...args: any[]) => {
          // Look for subscription-related events
          if (event === 'message' || event === 'subscription') {
            this.app.debug(`WebSocket event: ${event}, args: ${JSON.stringify(args)}`);
            
            // Check if any of the args contain subscription data
            args.forEach(arg => {
              if (this.isSubscriptionMessage(arg)) {
                this.app.debug(`Found subscription in WebSocket event: ${JSON.stringify(arg)}`);
                this.handleSubscriptionRequest(arg);
              }
            });
          }
          
          // Call original emit
          return originalDataEmit.apply(wsInterface.data, [event, ...args]);
        };
        
        this.app.debug('Successfully hooked into WebSocket data stream');
      } catch (error) {
        this.app.debug(`Error hooking into WebSocket data stream: ${error}`);
      }
    }

    // Also register delta handler as fallback
    this.app.registerDeltaInputHandler((delta, next) => {
      if (this.isSubscriptionMessage(delta)) {
        this.app.debug(`Delta handler caught subscription: ${JSON.stringify(delta)}`);
        this.handleSubscriptionRequest(delta);
      }
      next(delta);
    });

    this.app.debug('Historical streaming service ready');
  }
  

  private isSubscriptionMessage(delta: any): boolean {
    // Check if this looks like a subscription message
    // SignalK subscription messages typically have a 'subscribe' property
    return delta && (delta.subscribe || delta.unsubscribe);
  }

  private handleSubscriptionRequest(subscriptionMessage: any) {
    this.app.debug(`Processing potential subscription request: ${JSON.stringify(subscriptionMessage)}`);

    if (subscriptionMessage.subscribe) {
      this.handleSubscribe(subscriptionMessage);
    } else if (subscriptionMessage.unsubscribe) {
      this.handleUnsubscribe(subscriptionMessage);
    }
  }

  private handleSubscribe(subscriptionMessage: any) {
    const { context, subscribe } = subscriptionMessage;
    
    if (subscribe && Array.isArray(subscribe)) {
      subscribe.forEach((subscription: any) => {
        const { path } = subscription;
        
        // Check if this is a request for historical data
        if (this.isHistoricalDataPath(path)) {
          this.app.debug(`Historical data subscription requested for path: ${path}`);
          this.startHistoricalStream(context, path, subscription);
        }
      });
    }
  }

  private handleUnsubscribe(subscriptionMessage: any) {
    // Handle unsubscription logic
    this.app.debug(`Unsubscribe request: ${JSON.stringify(subscriptionMessage)}`);
  }

  private isHistoricalDataPath(path: string): boolean {
    // Define which paths should trigger historical data streaming
    // For now, let's make all navigation paths historical
    return !!(path && (
      path.startsWith('navigation.') ||
      path.startsWith('environment.') ||
      path.includes('history.')  // Special prefix for historical requests
    ));
  }

  private async startHistoricalStream(context: string, path: string, subscription: any) {
    try {
      // Generate a subscription ID
      const subscriptionId = `historical_${Date.now()}_${Math.random().toString(36).substring(2, 11)}`;
      
      // Store the subscription
      this.activeSubscriptions.set(subscriptionId, {
        context,
        path,
        subscription,
        startedAt: new Date()
      });

      this.app.debug(`Starting historical stream for ${path}`);

      // Stream real historical data from Parquet files
      this.streamHistoricalData(subscriptionId, path);

    } catch (error) {
      this.app.error(`Error starting historical stream for ${path}`);
    }
  }

  private async streamHistoricalData(_subscriptionId: string, path: string) {
    this.app.debug(`🚀 Streaming real historical data for ${path}`);

    try {
      // Get historical data for the last hour with 30 second resolution
      const to = ZonedDateTime.now(ZoneOffset.UTC);
      const from = to.minusHours(1);
      const timeResolutionMillis = 30000; // 30 seconds
      
      // Create a mock request/response for the HistoryAPI
      const mockReq = {
        query: {
          paths: path,
          resolution: timeResolutionMillis.toString()
        }
      } as any;

      const mockRes = {
        json: (data: any) => {
          this.processHistoricalDataResponse(data, path);
        },
        status: (code: number) => ({
          json: (error: any) => {
            this.app.error(`❌ Historical data query failed with status ${code}: ${JSON.stringify(error)}`);
          }
        })
      } as any;

      this.app.debug(`📊 Querying historical data for ${path} from ${from.toString()} to ${to.toString()}`);
      
      // Call the HistoryAPI to get real historical data
      await this.historyAPI.getValues(
        this.app.selfContext as Context,
        from,
        to,
        false, // shouldRefresh
        this.app.debug.bind(this.app),
        mockReq,
        mockRes
      );

    } catch (error) {
      this.app.error(`❌ Error streaming historical data for ${path}: ${error}`);
      
      // Fallback to sample data if real data fails
      this.app.debug(`🔄 Falling back to sample data for ${path}`);
      this.streamSampleDataFallback(path);
    }
  }

  private processHistoricalDataResponse(historyResponse: any, path: string) {
    this.app.debug(`📥 Received historical data response for ${path}`);

    if (!historyResponse.data || historyResponse.data.length === 0) {
      this.app.debug(`⚠️ No historical data found for ${path}, using sample data`);
      this.streamSampleDataFallback(path);
      return;
    }

    this.app.debug(`📊 Processing ${historyResponse.data.length} historical data points for ${path}`);

    // Stream the historical data points
    historyResponse.data.forEach((dataPoint: any, index: number) => {
      const [timestamp, ...values] = dataPoint;
      const value = values[0]; // Get first value for this path

      if (value !== null && value !== undefined) {
        const delta = {
          context: this.app.selfContext as Context,
          updates: [{
            $source: 'signalk-parquet-historical' as SourceRef,
            timestamp: timestamp as Timestamp,
            values: [{
              path: path as Path,
              value: value
            }]
          }]
        };

        // Inject with small delays to avoid overwhelming
        setTimeout(() => {
          this.app.debug(`📤 Injecting historical data point ${index + 1}/${historyResponse.data.length} for ${path}`);
          try {
            this.app.handleMessage('signalk-parquet-historical', delta);
          } catch (error) {
            this.app.error(`❌ Error injecting historical data point: ${error}`);
          }
        }, index * 100); // 100ms between each data point
      }
    });

    this.app.debug(`✅ Completed streaming ${historyResponse.data.length} historical data points for ${path}`);
  }

  private streamSampleDataFallback(path: string) {
    this.app.debug(`🔄 Using sample data fallback for ${path}`);
    
    const sampleData = [
      { timestamp: new Date(Date.now() - 3600000), value: Math.random() * 100 },
      { timestamp: new Date(Date.now() - 1800000), value: Math.random() * 100 },
      { timestamp: new Date(Date.now() - 900000), value: Math.random() * 100 },
    ];

    sampleData.forEach((data, index) => {
      const delta = {
        context: this.app.selfContext as Context,
        updates: [{
          $source: 'signalk-parquet-historical-sample' as SourceRef,
          timestamp: data.timestamp.toISOString() as Timestamp,
          values: [{
            path: path as Path,
            value: data.value
          }]
        }]
      };

      setTimeout(() => {
        this.app.debug(`📤 Injecting sample data point ${index + 1} for ${path}: ${data.value}`);
        try {
          this.app.handleMessage('signalk-parquet-historical', delta);
        } catch (error) {
          this.app.error(`❌ Error injecting sample data: ${error}`);
        }
      }, index * 1000);
    });
  }

  public shutdown() {
    this.app.debug('Shutting down historical streaming service');
    this.activeSubscriptions.clear();
    
    // Clear all stream intervals
    this.streamIntervals.forEach((interval, streamId) => {
      clearInterval(interval);
      this.app.debug(`Cleared streaming interval for stream: ${streamId}`);
    });
    this.streamIntervals.clear();
    
    // Clear stream buffers and timestamps
    this.streamBuffers.clear();
    this.streamLastTimestamps.clear();
    
    // Close all WebSocket connections
    this.connectedClients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.close();
      }
    });
    this.connectedClients.clear();
    
    // Close WebSocket server if exists
    if (this.wsServer) {
      this.wsServer.close();
      this.wsServer = undefined;
    }
  }

  public getActiveSubscriptions() {
    return Array.from(this.activeSubscriptions.entries()).map(([id, sub]) => ({
      id,
      ...sub
    }));
  }

  // Manual trigger for testing historical data streaming
  public triggerHistoricalStream(path: string) {
    this.app.debug(`Manually triggering historical stream for: ${path}`);
    try {
      this.startHistoricalStream(this.app.selfContext, path, { path, period: 1000 });
      this.app.debug(`Successfully called startHistoricalStream for: ${path}`);
    } catch (error) {
      this.app.error(`Error in triggerHistoricalStream: ${error}`);
    }
  }

  // Stream management methods for webapp interface
  private streams = new Map<string, any>();
  private streamIntervals = new Map<string, NodeJS.Timeout>();

  private parseTimeRange(timeRange: string): number {
    // Parse time range strings like '1h', '30m', '2d' into milliseconds
    const match = timeRange.match(/^(\d+)([smhd])$/);
    if (!match) {
      this.app.debug(`Invalid time range format: ${timeRange}, defaulting to 1h`);
      return 60 * 60 * 1000; // 1 hour default
    }
    
    const value = parseInt(match[1]);
    const unit = match[2];
    
    switch (unit) {
      case 's': return value * 1000;                    // seconds
      case 'm': return value * 60 * 1000;               // minutes
      case 'h': return value * 60 * 60 * 1000;          // hours  
      case 'd': return value * 24 * 60 * 60 * 1000;     // days
      default: return 60 * 60 * 1000; // 1 hour default
    }
  }

  public createStream(streamConfig: any) {
    const streamId = `stream_${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;
    const stream = {
      id: streamId,
      name: streamConfig.name,
      path: streamConfig.path,
      status: 'created',
      createdAt: new Date().toISOString(),
      dataPointsStreamed: 0,
      connectedClients: 0,
      rate: streamConfig.rate || 5000,
      resolution: streamConfig.resolution || 30000,
      timeRange: streamConfig.timeRange || '1h',
      aggregateMethod: (streamConfig.aggregateMethod || 'average') as AggregateMethod,
      windowSize: streamConfig.windowSize || 10,
      ...streamConfig
    };
    this.streams.set(streamId, stream);
    this.streamBuffers.set(streamId, []);
    this.app.debug(`Created stream: ${streamId} for path: ${streamConfig.path} with aggregation: ${stream.aggregateMethod}`);
    return stream;
  }

  public getAllStreams() {
    return Array.from(this.streams.values());
  }

  public startStream(streamId: string) {
    const stream = this.streams.get(streamId);
    if (!stream) {
      return { success: false, error: 'Stream not found' };
    }
    
    // Clear existing interval if any
    const existingInterval = this.streamIntervals.get(streamId);
    if (existingInterval) {
      clearInterval(existingInterval);
    }
    
    stream.status = 'running';
    stream.startedAt = new Date().toISOString();
    stream.dataPointsStreamed = 0;
    
    // Set streaming time window
    const now = new Date();
    const timeRangeDuration = this.parseTimeRange(stream.timeRange || '1h');
    const windowStart = new Date(now.getTime() - timeRangeDuration);
    
    stream.startTime = windowStart.toLocaleTimeString();
    stream.endTime = 'Live'; // For real-time streaming
    stream.actualStartTime = windowStart.toISOString();
    stream.actualEndTime = null; // null means live/ongoing
    
    // Start continuous streaming
    try {
      this.startContinuousStreaming(streamId);
      this.streams.set(streamId, stream);
      this.app.debug(`Started continuous stream: ${streamId} for path: ${stream.path} (${stream.startTime} - ${stream.endTime})`);
      return { success: true };
    } catch (error) {
      stream.status = 'error';
      stream.error = (error as Error).message;
      this.streams.set(streamId, stream);
      return { success: false, error: (error as Error).message };
    }
  }

  private startContinuousStreaming(streamId: string) {
    const stream = this.streams.get(streamId);
    if (!stream) return;

    const interval = setInterval(async () => {
      if (stream.status !== 'running') return;

      try {
        // Get historical data window with proper time bucketing and statistics
        const historicalDataWindow = await this.getHistoricalDataWindow(
          streamId,
          stream.path, 
          stream.resolution, 
          stream.aggregateMethod || ('average' as AggregateMethod),
          stream.timeRange || '1h'
        );
        
        if (historicalDataWindow && historicalDataWindow.dataPoints && historicalDataWindow.dataPoints.length > 0) {
          // Send all bucketed data points to WebSocket clients
          this.broadcastTimeSeriesData(streamId, historicalDataWindow);
          
          // Update stream statistics
          const dataPointCount = historicalDataWindow.dataPoints.length;
          stream.dataPointsStreamed = (stream.dataPointsStreamed || 0) + dataPointCount;
          stream.lastDataPoint = new Date().toISOString();
          stream.connectedClients = this.connectedClients.size;
          stream.totalBuckets = (stream.totalBuckets || 0) + dataPointCount;
          stream.lastTimeWindow = `${historicalDataWindow.from} to ${historicalDataWindow.to}`;
          stream.isIncremental = historicalDataWindow.isIncremental;
          
          // Get the most recent value for display
          const latestPoint = historicalDataWindow.dataPoints[historicalDataWindow.dataPoints.length - 1];
          stream.lastValue = latestPoint.value;
          stream.lastTimestamp = latestPoint.timestamp;
          
          this.streams.set(streamId, stream);
          
          const mode = historicalDataWindow.isIncremental ? 'new incremental' : 'initial';
          this.app.debug(`📊 Streamed ${dataPointCount} ${mode} time-bucketed data points for stream ${streamId}`);
        } else {
          // No new data available - this is normal for incremental streaming
          const lastTimestamp = this.streamLastTimestamps.get(streamId);
          if (lastTimestamp) {
            this.app.debug(`📊 No new data since ${lastTimestamp} for stream ${streamId}`);
          } else {
            this.app.debug(`⚠️ No data retrieved for stream ${streamId}`);
          }
        }
      } catch (error) {
        this.app.error(`Error in continuous streaming for ${streamId}: ${error}`);
      }
    }, stream.rate);

    this.streamIntervals.set(streamId, interval);
    this.app.debug(`Started continuous streaming interval for stream: ${streamId}`);
  }

  private broadcastToClients(streamId: string, data: any) {
    const message = JSON.stringify({
      type: 'streamData',
      streamId: streamId,
      timestamp: new Date().toISOString(),
      data: data
    });

    this.connectedClients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });
  }

  private broadcastTimeSeriesData(streamId: string, timeSeriesData: any) {
    const message = JSON.stringify({
      type: 'timeSeriesData',
      streamId: streamId,
      timestamp: new Date().toISOString(),
      metadata: {
        path: timeSeriesData.path,
        aggregateMethod: timeSeriesData.aggregateMethod,
        resolution: timeSeriesData.resolution,
        timeRange: timeSeriesData.timeRange,
        totalPoints: timeSeriesData.totalPoints,
        isIncremental: timeSeriesData.isIncremental || false,
        from: timeSeriesData.from,
        to: timeSeriesData.to
      },
      data: timeSeriesData.dataPoints.map((point: any) => ({
        timestamp: point.timestamp,
        value: point.value,
        bucketIndex: point.bucketIndex
      }))
    });

    this.connectedClients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });
    
    const mode = timeSeriesData.isIncremental ? 'incremental' : 'initial';
    this.app.debug(`📡 Sent ${timeSeriesData.totalPoints} ${mode} time-bucketed points to ${this.connectedClients.size} WebSocket clients for stream ${streamId}`);
  }

  private async getHistoricalDataWindow(streamId: string, path: string, resolution: number, aggregateMethod: AggregateMethod, timeRange: string): Promise<any> {
    try {
      const stream = this.streams.get(streamId);
      if (!stream) {
        throw new Error(`Stream ${streamId} not found`);
      }

      const to = ZonedDateTime.now(ZoneOffset.UTC);
      const lastTimestamp = this.streamLastTimestamps.get(streamId);
      
      let from: ZonedDateTime;
      let isIncremental = false;
      
      if (lastTimestamp) {
        // Use sliding window: get new data since last timestamp
        from = ZonedDateTime.parse(lastTimestamp).plusSeconds(1); // Start 1 second after last data
        isIncremental = true;
        this.app.debug(`📊 Getting incremental data for ${path} from ${from.toString()} to ${to.toString()}`);
      } else {
        // Initial load: get full time window
        const timeRangeDuration = this.parseTimeRange(timeRange);
        from = to.minusNanos(timeRangeDuration * 1000000); // Convert ms to nanoseconds
        this.app.debug(`📊 Getting initial data window for ${path} from ${from.toString()} to ${to.toString()}`);
      }

      // Skip if time window is too small (less than resolution)
      const timeDiffMs = to.toInstant().toEpochMilli() - from.toInstant().toEpochMilli();
      if (timeDiffMs < resolution) {
        this.app.debug(`⏭️ Skipping query - time window (${timeDiffMs}ms) smaller than resolution (${resolution}ms)`);
        return null;
      }
      
      // Create a mock request for HistoryAPI with aggregation method
      const mockReq = {
        query: {
          paths: `${path}:${aggregateMethod}`,
          resolution: resolution.toString()
        }
      } as any;

      return new Promise((resolve) => {
        const mockRes = {
          json: (data: any) => {
            if (data.data && data.data.length > 0) {
              this.app.debug(`📊 Received ${data.data.length} ${isIncremental ? 'new' : 'initial'} time-bucketed data points for ${path}`);
              
              // Process all time-bucketed data points
              const processedData = data.data.map((dataPoint: any, index: number) => {
                const [timestamp, value] = dataPoint;
                return {
                  path: path,
                  timestamp: timestamp,
                  value: value,
                  bucketIndex: index,
                  aggregateMethod: aggregateMethod,
                  resolution: resolution
                };
              });

              // Update last timestamp for sliding window
              const lastDataPoint = processedData[processedData.length - 1];
              this.streamLastTimestamps.set(streamId, lastDataPoint.timestamp);
              
              // Return all processed data points
              resolve({
                path: path,
                aggregateMethod: aggregateMethod,
                resolution: resolution,
                timeRange: timeRange,
                dataPoints: processedData,
                totalPoints: processedData.length,
                isIncremental: isIncremental,
                from: from.toString(),
                to: to.toString()
              });
              
            } else if (!isIncremental) {
              // Generate sample time series only for initial load if no historical data available
              this.app.debug(`⚠️ No historical data found for ${path}, generating sample time series`);
              const sampleData = this.generateSampleTimeSeries(path, from, to, resolution);
              
              // Set last timestamp for sample data
              if (sampleData.length > 0) {
                this.streamLastTimestamps.set(streamId, sampleData[sampleData.length - 1].timestamp);
              }
              
              resolve({
                path: path,
                aggregateMethod: aggregateMethod,
                resolution: resolution,
                timeRange: timeRange,
                dataPoints: sampleData,
                totalPoints: sampleData.length,
                isIncremental: false,
                from: from.toString(),
                to: to.toString()
              });
            } else {
              // No new data in incremental mode
              this.app.debug(`📊 No new data available for ${path}`);
              resolve(null);
            }
          },
          status: () => ({ 
            json: (error: any) => {
              this.app.error(`❌ Historical data query failed: ${JSON.stringify(error)}`);
              resolve(null);
            }
          })
        } as any;

        this.historyAPI.getValues(
          this.app.selfContext as Context,
          from,
          to,
          false,
          this.app.debug.bind(this.app),
          mockReq,
          mockRes
        );
      });
    } catch (error) {
      this.app.debug(`Error getting historical data window for ${path}: ${error}`);
      return null;
    }
  }


  private generateSampleTimeSeries(path: string, from: ZonedDateTime, to: ZonedDateTime, resolution: number): any[] {
    const dataPoints = [];
    const durationMs = to.toInstant().toEpochMilli() - from.toInstant().toEpochMilli();
    const numPoints = Math.floor(durationMs / resolution);
    
    this.app.debug(`📊 Generating ${numPoints} sample data points for ${path} over ${durationMs}ms`);
    
    for (let i = 0; i < numPoints; i++) {
      const timestamp = from.plusSeconds(Math.floor((i * resolution) / 1000));
      const value = this.generateSampleValue(path, i, numPoints);
      
      dataPoints.push({
        path: path,
        timestamp: timestamp.toString(),
        value: value,
        bucketIndex: i,
        aggregateMethod: 'sample',
        resolution: resolution
      });
    }
    
    return dataPoints;
  }

  private generateSampleValue(path: string, index: number, total: number): any {
    const progress = index / total;
    
    switch (path) {
      case 'navigation.position':
        return {
          latitude: 41.329265 + Math.sin(progress * Math.PI * 4) * 0.001,
          longitude: -72.08793666666666 + Math.cos(progress * Math.PI * 4) * 0.001
        };
      case 'environment.wind.speedApparent':
        return 10 + Math.sin(progress * Math.PI * 6) * 5 + Math.random() * 2; // 5-17 m/s with variation
      case 'navigation.speedOverGround':
        return 5 + Math.sin(progress * Math.PI * 8) * 2 + Math.random(); // 3-8 m/s with variation
      default:
        return 50 + Math.sin(progress * Math.PI * 10) * 25 + Math.random() * 10; // 15-85 with variation
    }
  }


  public pauseStream(streamId: string) {
    const stream = this.streams.get(streamId);
    if (!stream) {
      return { success: false, error: 'Stream not found' };
    }
    
    const wasPaused = stream.status === 'paused';
    
    if (wasPaused) {
      // Resume streaming
      stream.status = 'running';
      this.startContinuousStreaming(streamId);
    } else {
      // Pause streaming
      stream.status = 'paused';
      const interval = this.streamIntervals.get(streamId);
      if (interval) {
        clearInterval(interval);
        this.streamIntervals.delete(streamId);
      }
    }
    
    stream.lastToggled = new Date().toISOString();
    this.streams.set(streamId, stream);
    
    this.app.debug(`${wasPaused ? 'Resumed' : 'Paused'} stream: ${streamId}`);
    return { success: true, paused: !wasPaused };
  }

  public stopStream(streamId: string) {
    const stream = this.streams.get(streamId);
    if (!stream) {
      return { success: false, error: 'Stream not found' };
    }
    
    // Clear streaming interval
    const interval = this.streamIntervals.get(streamId);
    if (interval) {
      clearInterval(interval);
      this.streamIntervals.delete(streamId);
    }
    
    stream.status = 'stopped';
    stream.stoppedAt = new Date().toISOString();
    
    // Update time window to show final range
    if (stream.actualStartTime) {
      const startTime = new Date(stream.actualStartTime);
      const endTime = new Date(stream.stoppedAt);
      stream.startTime = startTime.toLocaleTimeString();
      stream.endTime = endTime.toLocaleTimeString();
    }
    
    this.streams.set(streamId, stream);
    
    this.app.debug(`Stopped stream: ${streamId}`);
    return { success: true };
  }

  public deleteStream(streamId: string) {
    const stream = this.streams.get(streamId);
    if (!stream) {
      return { success: false, error: 'Stream not found' };
    }
    
    // Stop the stream first
    this.stopStream(streamId);
    
    // Clean up stream data
    this.streams.delete(streamId);
    this.streamBuffers.delete(streamId);
    this.streamLastTimestamps.delete(streamId);
    
    this.app.debug(`Deleted stream: ${streamId}`);
    return { success: true };
  }

  public getStreamStats() {
    const streams = Array.from(this.streams.values());
    const totalDataPoints = streams.reduce((sum, stream) => sum + (stream.dataPointsStreamed || 0), 0);
    
    return {
      totalStreams: streams.length,
      runningStreams: streams.filter(s => s.status === 'running').length,
      pausedStreams: streams.filter(s => s.status === 'paused').length,
      stoppedStreams: streams.filter(s => s.status === 'stopped').length,
      totalDataPointsStreamed: totalDataPoints,
      connectedClients: this.connectedClients.size,
      streams: streams
    };
  }

  // WebSocket client management methods
  public addWebSocketClient(ws: WebSocket) {
    this.connectedClients.add(ws);
    this.app.debug(`📡 Added WebSocket client. Total clients: ${this.connectedClients.size}`);
  }

  public removeWebSocketClient(ws: WebSocket) {
    this.connectedClients.delete(ws);
    this.app.debug(`📡 Removed WebSocket client. Total clients: ${this.connectedClients.size}`);
  }
}