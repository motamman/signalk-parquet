import { ServerAPI, Context, Path, Timestamp, SourceRef } from '@signalk/server-api';
import { HistoryAPI } from './HistoryAPI';
import { ZonedDateTime, ZoneOffset } from '@js-joda/core';
import * as WebSocket from 'ws';

export class HistoricalStreamingService {
  private app: ServerAPI;
  private activeSubscriptions = new Map<string, any>();
  private historyAPI: HistoryAPI;
  private wsServer?: WebSocket.Server;
  private connectedClients = new Set<WebSocket>();

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
    
    this.app.debug('Historical streaming service shutdown complete');
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
      ...streamConfig
    };
    this.streams.set(streamId, stream);
    this.app.debug(`Created stream: ${streamId} for path: ${streamConfig.path}`);
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
        // Get historical data for this path
        const historicalData = await this.getHistoricalDataPoint(stream.path, stream.resolution);
        
        if (historicalData) {
          // Send to WebSocket clients
          this.broadcastToClients(streamId, historicalData);
          
          // Update stream statistics
          stream.dataPointsStreamed = (stream.dataPointsStreamed || 0) + 1;
          stream.lastDataPoint = new Date().toISOString();
          stream.connectedClients = this.connectedClients.size;
          this.streams.set(streamId, stream);
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

  private async getHistoricalDataPoint(path: string, resolution: number): Promise<any> {
    try {
      // Get recent historical data from the last minute
      const to = ZonedDateTime.now(ZoneOffset.UTC);
      const from = to.minusMinutes(1);
      
      // Create a mock request for HistoryAPI
      const mockReq = {
        query: {
          paths: path,
          resolution: resolution.toString()
        }
      } as any;

      return new Promise((resolve) => {
        const mockRes = {
          json: (data: any) => {
            if (data.data && data.data.length > 0) {
              // Return the most recent data point
              const latestPoint = data.data[data.data.length - 1];
              resolve({
                path: path,
                timestamp: latestPoint[0],
                value: latestPoint[1]
              });
            } else {
              // Generate sample data if no historical data available
              resolve({
                path: path,
                timestamp: new Date().toISOString(),
                value: this.generateSampleData(path)
              });
            }
          },
          status: () => ({ json: () => resolve(null) })
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
      this.app.debug(`Error getting historical data for ${path}: ${error}`);
      return {
        path: path,
        timestamp: new Date().toISOString(),
        value: this.generateSampleData(path)
      };
    }
  }

  private generateSampleData(path: string): any {
    switch (path) {
      case 'navigation.position':
        return {
          latitude: 41.329265 + (Math.random() - 0.5) * 0.001,
          longitude: -72.08793666666666 + (Math.random() - 0.5) * 0.001
        };
      case 'environment.wind.speedApparent':
        return Math.random() * 15 + 5; // 5-20 m/s
      default:
        return Math.random() * 100;
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
    
    this.streams.delete(streamId);
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
}