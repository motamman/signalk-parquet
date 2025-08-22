import { ServerAPI, Context, Path, Timestamp } from '@signalk/server-api';

export class HistoricalStreamingService {
  private app: ServerAPI;
  private activeSubscriptions = new Map<string, any>();

  constructor(app: ServerAPI) {
    this.app = app;
    this.setupSubscriptionInterceptor();
  }

  private setupSubscriptionInterceptor() {
    this.app.debug('Setting up historical data subscription interceptor');

    // Try to hook into WebSocket server directly
    try {
      const server = (this.app as any).server;
      if (server && server.interfaces && server.interfaces.ws) {
        this.app.debug('Found WebSocket interface, attempting to hook into message handling');
        
        // Hook into WebSocket message handling
        const originalMessageHandler = server.interfaces.ws.handleMessage;
        if (originalMessageHandler) {
          server.interfaces.ws.handleMessage = (ws: any, message: any) => {
            // Check if this is a subscription message
            try {
              const parsed = typeof message === 'string' ? JSON.parse(message) : message;
              if (this.isSubscriptionMessage(parsed)) {
                this.app.debug(`Intercepted WebSocket subscription: ${JSON.stringify(parsed)}`);
                this.handleSubscriptionRequest(parsed);
              }
            } catch (e) {
              // Ignore parsing errors
            }
            
            // Call original handler
            return originalMessageHandler.call(server.interfaces.ws, ws, message);
          };
          this.app.debug('Successfully hooked into WebSocket message handler');
        }
      } else {
        this.app.debug('WebSocket interface not found, using delta handler fallback');
        // Fallback to delta handler
        this.app.registerDeltaInputHandler((delta, next) => {
          if (this.isSubscriptionMessage(delta)) {
            this.handleSubscriptionRequest(delta);
          }
          next(delta);
        });
      }
    } catch (error) {
      this.app.debug(`Error setting up WebSocket hook: ${error}, using delta handler fallback`);
      // Fallback to delta handler
      this.app.registerDeltaInputHandler((delta, next) => {
        if (this.isSubscriptionMessage(delta)) {
          this.handleSubscriptionRequest(delta);
        }
        next(delta);
      });
    }
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

      // For now, send some sample historical data
      this.streamSampleHistoricalData(subscriptionId, path);

    } catch (error) {
      this.app.error(`Error starting historical stream for ${path}`);
    }
  }

  private streamSampleHistoricalData(_subscriptionId: string, path: string) {
    this.app.debug(`Streaming sample historical data for ${path}`);

    // Generate sample historical data
    const sampleData = [
      { timestamp: new Date(Date.now() - 3600000), value: Math.random() * 100 },
      { timestamp: new Date(Date.now() - 1800000), value: Math.random() * 100 },
      { timestamp: new Date(Date.now() - 900000), value: Math.random() * 100 },
    ];

    // Send sample data as delta messages
    sampleData.forEach((data, index) => {
      // Create a delta message with historical data
      const delta = {
        context: 'vessels.self' as Context,
        updates: [{
          timestamp: data.timestamp.toISOString() as Timestamp,
          values: [{
            path: path as Path,
            value: data.value
          }]
        }]
      };

      // Inject the historical data into SignalK's stream
      setTimeout(() => {
        this.app.handleMessage('signalk-parquet-historical', delta);
      }, index * 1000); // 1 second between each data point
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
}