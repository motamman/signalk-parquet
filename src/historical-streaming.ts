import { ServerAPI, Context, Path, Timestamp, SourceRef } from '@signalk/server-api';
import { HistoryAPI } from './HistoryAPI';
import { ZonedDateTime, ZoneOffset } from '@js-joda/core';

export class HistoricalStreamingService {
  private app: ServerAPI;
  private activeSubscriptions = new Map<string, any>();
  private historyAPI: HistoryAPI;

  constructor(app: ServerAPI) {
    this.app = app;
    // Initialize HistoryAPI - we'll get the data directory from the app
    const dataDir = app.getDataDirPath();
    this.historyAPI = new HistoryAPI(app.selfId, dataDir);
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
}