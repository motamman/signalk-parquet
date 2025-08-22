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

    // Debug: inspect what's available in the app object
    this.app.debug(`App object keys: ${Object.keys(this.app).join(', ')}`);
    
    // Check for different possible WebSocket access points
    const possiblePaths = [
      'server.interfaces.ws',
      'interfaces.ws', 
      'websocket',
      'ws',
      'streamprovider',
      'streamProvider'
    ];
    
    let wsFound = false;
    for (const path of possiblePaths) {
      const value = this.getNestedProperty(this.app, path);
      if (value) {
        this.app.debug(`Found potential WebSocket at: ${path}`);
        this.app.debug(`WebSocket object keys: ${Object.keys(value).join(', ')}`);
        wsFound = true;
      }
    }
    
    if (!wsFound) {
      this.app.debug('No WebSocket interfaces found, using delta handler approach');
    }

    // For now, use a simple approach - just provide historical data when requested
    // We'll trigger it manually rather than intercepting subscriptions
    this.app.debug('Historical streaming service ready for manual triggers');
  }
  
  private getNestedProperty(obj: any, path: string): any {
    return path.split('.').reduce((current, key) => current?.[key], obj);
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

  // Manual trigger for testing historical data streaming
  public triggerHistoricalStream(path: string) {
    this.app.debug(`Manually triggering historical stream for: ${path}`);
    this.startHistoricalStream('vessels.self', path, { path, period: 1000 });
  }
}