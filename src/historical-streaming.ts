import { ServerAPI, Context, Path, Timestamp, SourceRef } from '@signalk/server-api';
import { HistoryAPI } from './HistoryAPI';
import { AggregateMethod } from './HistoryAPI-types';
import { ZonedDateTime, ZoneOffset } from '@js-joda/core';
import * as WebSocket from 'ws';
import * as fs from 'fs';
import * as path from 'path';

export class HistoricalStreamingService {
  private app: ServerAPI;
  private activeSubscriptions = new Map<string, any>();
  private historyAPI: HistoryAPI;
  private wsServer?: WebSocket.Server;
  private connectedClients = new Set<WebSocket>();
  private streamBuffers = new Map<string, number[]>();
  private streamLastTimestamps = new Map<string, string>();
  private streamTimeSeriesData = new Map<string, any[]>();
  private streamsConfigPath: string;
  private streamsAlreadyLoaded = false;

  constructor(app: ServerAPI, dataDir?: string) {
    this.app = app;
    // Initialize HistoryAPI - use provided data directory or default
    const actualDataDir = dataDir || `${app.getDataDirPath()}/signalk-parquet`;
    this.historyAPI = new HistoryAPI(app.selfId, actualDataDir);
    this.streamsConfigPath = path.join(actualDataDir, 'streams-config.json');
    this.loadPersistedStreams();
    this.setupSubscriptionInterceptor();
  }

  private setupSubscriptionInterceptor() {

    // Try to hook into WebSocket data stream
    const wsInterface = (this.app as any).interfaces?.ws;
    if (wsInterface && wsInterface.data) {
      
      try {
        // Hook into the data stream to catch subscription messages
        const originalDataEmit = wsInterface.data.emit;
        wsInterface.data.emit = (event: string, ...args: any[]) => {
          // Look for subscription-related events
          if (event === 'message' || event === 'subscription') {
            
            // Check if any of the args contain subscription data
            args.forEach(arg => {
              if (this.isSubscriptionMessage(arg)) {
                this.handleSubscriptionRequest(arg);
              }
            });
          }
          
          // Call original emit
          return originalDataEmit.apply(wsInterface.data, [event, ...args]);
        };
        
      } catch (error) {
      }
    }

    // Also register delta handler as fallback
    this.app.registerDeltaInputHandler((delta, next) => {
      if (this.isSubscriptionMessage(delta)) {
        this.handleSubscriptionRequest(delta);
      }
      next(delta);
    });

  }
  

  private isSubscriptionMessage(delta: any): boolean {
    // Check if this looks like a subscription message
    // SignalK subscription messages typically have a 'subscribe' property
    return delta && (delta.subscribe || delta.unsubscribe);
  }

  private handleSubscriptionRequest(subscriptionMessage: any) {

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
          this.startHistoricalStream(context, path, subscription);
        }
      });
    }
  }

  private handleUnsubscribe(subscriptionMessage: any) {
    // Handle unsubscription logic
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


      // Stream real historical data from Parquet files
      this.streamHistoricalData(subscriptionId, path);

    } catch (error) {
      this.app.error(`Error starting historical stream for ${path}`);
    }
  }

  private async streamHistoricalData(_subscriptionId: string, path: string) {

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
      this.streamSampleDataFallback(path);
    }
  }

  private processHistoricalDataResponse(historyResponse: any, path: string) {

    if (!historyResponse.data || historyResponse.data.length === 0) {
      this.streamSampleDataFallback(path);
      return;
    }


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
          try {
            this.app.handleMessage('signalk-parquet-historical', delta);
          } catch (error) {
            this.app.error(`❌ Error injecting historical data point: ${error}`);
          }
        }, index * 100); // 100ms between each data point
      }
    });

  }

  private streamSampleDataFallback(path: string) {
    
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
        try {
          this.app.handleMessage('signalk-parquet-historical', delta);
        } catch (error) {
          this.app.error(`❌ Error injecting sample data: ${error}`);
        }
      }, index * 1000);
    });
  }

  public shutdown() {
    this.activeSubscriptions.clear();
    
    // Clear all stream intervals
    this.streamIntervals.forEach((interval, streamId) => {
      clearInterval(interval);
    });
    this.streamIntervals.clear();
    
    // Clear stream buffers and timestamps
    this.streamBuffers.clear();
    this.streamLastTimestamps.clear();
    this.streamTimeSeriesData.clear();
    
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
    try {
      this.startHistoricalStream(this.app.selfContext, path, { path, period: 1000 });
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

  // Stream persistence methods
  private loadPersistedStreams() {
    if (this.streamsAlreadyLoaded) {
      return;
    }
    this.streamsAlreadyLoaded = true;
    
    try {
      if (fs.existsSync(this.streamsConfigPath)) {
        const data = fs.readFileSync(this.streamsConfigPath, 'utf8');
        const persistedStreams = JSON.parse(data);
        
        if (Array.isArray(persistedStreams)) {
          persistedStreams.forEach((streamConfig, index) => {
            // Restore stream but keep status as stopped initially
            const stream = {
              ...streamConfig,
              status: 'stopped',
              dataPointsStreamed: 0,
              connectedClients: 0,
              restoredAt: new Date().toISOString()
            };
            
            this.streams.set(stream.id, stream);
            this.streamBuffers.set(stream.id, []);
            // Clear timestamp and data to force initial data load after restart
            this.streamLastTimestamps.delete(stream.id);
            this.streamTimeSeriesData.delete(stream.id);
            
            // Debug: Verify timestamp was cleared
            const timestampAfterClear = this.streamLastTimestamps.get(stream.id);
            
            // Auto-start streams that were running when server stopped
            if (streamConfig.autoRestart === true) {
              setTimeout(() => {
                const currentStream = this.streams.get(stream.id);
                if (currentStream && currentStream.status !== 'running') {
                  this.startStream(stream.id);
                } else {
                }
              }, 2000); // Wait 2 seconds after startup
            }
          });
          
        }
      }
    } catch (error) {
    }
  }

  private saveStreamsConfig() {
    try {
      // Ensure directory exists
      const dir = path.dirname(this.streamsConfigPath);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }

      // Save all streams with their current state
      const streamsToSave = Array.from(this.streams.values()).map(stream => ({
        ...stream,
        autoRestart: stream.status === 'running' // Mark running streams for auto-restart
      }));
      
      fs.writeFileSync(this.streamsConfigPath, JSON.stringify(streamsToSave, null, 2), 'utf8');
    } catch (error) {
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
    this.saveStreamsConfig();
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
    
    // Prevent duplicate starts
    if (stream.status === 'running') {
      return { success: true, message: 'Stream already running' };
    }
    
    // Clear existing interval if any
    const existingInterval = this.streamIntervals.get(streamId);
    if (existingInterval) {
      clearInterval(existingInterval);
    }
    
    stream.status = 'running';
    stream.startedAt = new Date().toISOString();
    stream.dataPointsStreamed = 0;
    this.saveStreamsConfig();
    
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
          // Store time-series data points for API access
          this.storeTimeSeriesData(streamId, historicalDataWindow.dataPoints, historicalDataWindow.isIncremental);
          
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
          
          const mode = historicalDataWindow.isIncremental ? 'incremental' : 'initial';
          
          // Mark that initial data was successfully loaded after restoration
          if (!historicalDataWindow.isIncremental && stream.restoredAt) {
            stream.hasInitialDataAfterRestore = true;
            this.streams.set(streamId, stream);
          }
          
        } else {
          // No new data available - this is normal for incremental streaming
          const lastTimestamp = this.streamLastTimestamps.get(streamId);
          if (lastTimestamp) {
          } else {
          }
        }
      } catch (error) {
        this.app.error(`Error in continuous streaming for ${streamId}: ${error}`);
      }
    }, stream.rate);

    this.streamIntervals.set(streamId, interval);
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
    
    // Also emit SignalK delta messages for other apps to subscribe to
    this.emitSignalKStreamData(streamId, timeSeriesData);
    
    const mode = timeSeriesData.isIncremental ? 'incremental' : 'initial';
  }

  private emitSignalKStreamData(streamId: string, timeSeriesData: any) {
    const stream = this.streams.get(streamId);
    if (!stream) return;

    // Emit each data point as a SignalK delta message
    timeSeriesData.dataPoints.forEach((point: any, index: number) => {
      // Create a SignalK path for the stream data
      const streamPath = `streaming.${stream.name.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase()}.${stream.aggregateMethod}` as Path;
      
      const delta = {
        context: this.app.selfContext as Context,
        updates: [{
          $source: `signalk-parquet-stream-${streamId}` as SourceRef,
          timestamp: point.timestamp as Timestamp,
          values: [{
            path: streamPath,
            value: {
              // Original path being streamed
              sourcePath: timeSeriesData.path,
              // Statistical aggregate value
              statisticalValue: point.value,
              // Aggregation method used
              method: timeSeriesData.aggregateMethod,
              // Time bucket information
              bucketIndex: point.bucketIndex,
              resolution: timeSeriesData.resolution,
              // Stream metadata
              streamId: streamId,
              streamName: stream.name,
              isIncremental: timeSeriesData.isIncremental || false,
              // Time range info
              windowFrom: timeSeriesData.from,
              windowTo: timeSeriesData.to
            }
          }]
        }]
      };

      // Emit with small delay to avoid overwhelming the SignalK bus
      setTimeout(() => {
        try {
          this.app.handleMessage(`signalk-parquet-stream-${streamId}`, delta);
        } catch (error) {
        }
      }, index * 10); // 10ms delay between points to avoid overwhelming
    });

    // Also emit stream status/metadata as a separate path
    const statusPath = `streaming.${stream.name.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase()}.status` as Path;
    const statusDelta = {
      context: this.app.selfContext as Context,
      updates: [{
        $source: `signalk-parquet-stream-${streamId}` as SourceRef,
        timestamp: new Date().toISOString() as Timestamp,
        values: [{
          path: statusPath,
          value: {
            streamId: streamId,
            streamName: stream.name,
            status: stream.status,
            sourcePath: timeSeriesData.path,
            aggregateMethod: timeSeriesData.aggregateMethod,
            resolution: timeSeriesData.resolution,
            totalPoints: timeSeriesData.totalPoints,
            dataPointsStreamed: stream.dataPointsStreamed || 0,
            lastUpdate: new Date().toISOString()
          }
        }]
      }]
    };

    try {
      this.app.handleMessage(`signalk-parquet-stream-${streamId}`, statusDelta);
    } catch (error) {
    }
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
      
      // Debug: Check if stream was restored and should start fresh
      const currentStream = this.streams.get(streamId);
      const wasRestored = currentStream && currentStream.restoredAt && !currentStream.hasInitialDataAfterRestore;
      
      if (lastTimestamp) {
        // Use sliding window: get new data since last timestamp
        from = ZonedDateTime.parse(lastTimestamp).plusSeconds(1); // Start 1 second after last data
        isIncremental = true;
      } else {
        // Initial load: get full time window
        const timeRangeDuration = this.parseTimeRange(timeRange);
        from = to.minusNanos(timeRangeDuration * 1000000); // Convert ms to nanoseconds
      }

      // Skip if time window is too small (less than resolution)
      const timeDiffMs = to.toInstant().toEpochMilli() - from.toInstant().toEpochMilli();
      if (timeDiffMs < resolution) {
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
      return null;
    }
  }


  private generateSampleTimeSeries(path: string, from: ZonedDateTime, to: ZonedDateTime, resolution: number): any[] {
    const dataPoints = [];
    const durationMs = to.toInstant().toEpochMilli() - from.toInstant().toEpochMilli();
    const numPoints = Math.floor(durationMs / resolution);
    
    
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
      this.saveStreamsConfig();
    } else {
      // Pause streaming
      stream.status = 'paused';
      this.saveStreamsConfig();
      const interval = this.streamIntervals.get(streamId);
      if (interval) {
        clearInterval(interval);
        this.streamIntervals.delete(streamId);
      }
    }
    
    stream.lastToggled = new Date().toISOString();
    this.streams.set(streamId, stream);
    
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
    this.saveStreamsConfig();
    
    // Update time window to show final range
    if (stream.actualStartTime) {
      const startTime = new Date(stream.actualStartTime);
      const endTime = new Date(stream.stoppedAt);
      stream.startTime = startTime.toLocaleTimeString();
      stream.endTime = endTime.toLocaleTimeString();
    }
    
    this.streams.set(streamId, stream);
    
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
    this.streamTimeSeriesData.delete(streamId);
    this.saveStreamsConfig();
    
    return { success: true };
  }

  public updateStream(streamId: string, streamConfig: any): boolean {
    const stream = this.streams.get(streamId);
    if (!stream) {
      return false;
    }

    // Stop the stream if it's currently running
    const wasRunning = stream.status === 'running';
    if (wasRunning) {
      this.stopStream(streamId);
    }

    // Update stream configuration
    const updatedStream = {
      ...stream,
      name: streamConfig.name,
      path: streamConfig.path,
      timeRange: streamConfig.timeRange,
      resolution: streamConfig.resolution || 30000,
      rate: streamConfig.rate || 1000,
      aggregateMethod: streamConfig.aggregateMethod || 'average',
      windowSize: streamConfig.windowSize || 50
    };

    // Handle custom time range
    if (streamConfig.timeRange === 'custom') {
      updatedStream.startTime = streamConfig.startTime;
      updatedStream.endTime = streamConfig.endTime;
    }

    this.streams.set(streamId, updatedStream);
    this.saveStreamsConfig();

    // Restart the stream if it was running before the update
    if (wasRunning && streamConfig.autoRestart !== false) {
      this.startStream(streamId);
    }

    return true;
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
  }

  public removeWebSocketClient(ws: WebSocket) {
    this.connectedClients.delete(ws);
  }

  // Time-series data storage and retrieval methods
  private storeTimeSeriesData(streamId: string, dataPoints: any[], isIncremental: boolean) {
    const stream = this.streams.get(streamId);
    if (!stream) return;

    let storedData = this.streamTimeSeriesData.get(streamId) || [];
    
    // Add metadata to each data point
    const enrichedDataPoints = dataPoints.map(point => ({
      ...point,
      streamId: streamId,
      streamName: stream.name,
      aggregateMethod: stream.aggregateMethod,
      resolution: stream.resolution,
      isIncremental: isIncremental,
      deliveryTime: new Date().toISOString()
    }));

    if (isIncremental) {
      // Add new points to the beginning (newest first)
      storedData = [...enrichedDataPoints, ...storedData];
    } else {
      // For initial load, replace existing data
      storedData = enrichedDataPoints;
    }

    // Keep only the most recent 200 data points per stream
    if (storedData.length > 200) {
      storedData = storedData.slice(0, 200);
    }

    this.streamTimeSeriesData.set(streamId, storedData);
    
  }

  public getStreamTimeSeriesData(streamId: string, limit: number = 50): any[] | null {
    const data = this.streamTimeSeriesData.get(streamId);
    if (!data) return null;

    const limitedData = data.slice(0, limit);
    
    return limitedData;
  }
}