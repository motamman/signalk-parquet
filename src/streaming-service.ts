import { Server as HttpServer } from 'http';
import * as WebSocket from 'ws';
import { HistoryAPI } from './HistoryAPI';
import { ZonedDateTime } from '@js-joda/core';

export interface StreamingServiceOptions {
  historyAPI: HistoryAPI;
  selfId: string;
  debug?: boolean;
}

export interface StreamSubscription {
  id: string;
  path: string;
  timeWindow: string;
  aggregates: string[];
  refreshInterval: number;
  timer?: NodeJS.Timeout;
}

export class StreamingService {
  private wss: WebSocket.Server;
  private historyAPI: HistoryAPI;
  private selfId: string;
  private debug: boolean;
  private activeSubscriptions: Map<string, StreamSubscription> = new Map();
  private connectedClients: Set<WebSocket> = new Set();

  constructor(httpServer: HttpServer, options: StreamingServiceOptions) {
    this.historyAPI = options.historyAPI;
    this.selfId = options.selfId;
    this.debug = options.debug || false;

    try {
      // Create WebSocket server
      this.wss = new WebSocket.Server({ 
        server: httpServer,
        path: '/signalk-parquet-stream'
      });

      this.setupEventHandlers();
      this.log('Streaming service initialized with direct HistoryAPI calls');
    } catch (error) {
      this.log('Failed to initialize WebSocket server:', error);
      throw error;
    }
  }

  private setupEventHandlers(): void {
    this.wss.on('connection', (ws: WebSocket) => {
      this.log('Client connected');
      this.connectedClients.add(ws);

      ws.on('message', (data: WebSocket.RawData) => {
        try {
          const message = JSON.parse(data.toString());
          this.handleMessage(ws, message);
        } catch (error) {
          this.log('Error parsing message:', error);
          ws.send(JSON.stringify({ error: 'Invalid JSON' }));
        }
      });

      ws.on('close', () => {
        this.log('Client disconnected');
        this.connectedClients.delete(ws);
      });

      ws.on('error', (error) => {
        this.log('WebSocket error:', error);
        this.connectedClients.delete(ws);
      });

      // Send welcome message
      ws.send(JSON.stringify({ 
        type: 'welcome', 
        message: 'Connected to SignalK Parquet Streaming Service'
      }));
    });
  }

  private handleMessage(ws: WebSocket, message: any): void {
    switch (message.type) {
      case 'subscribe':
        this.handleSubscribe(ws, message);
        break;
      case 'unsubscribe':
        this.handleUnsubscribe(ws, message);
        break;
      default:
        ws.send(JSON.stringify({ error: `Unknown message type: ${message.type}` }));
    }
  }

  private handleSubscribe(ws: WebSocket, message: any): void {
    const { subscriptionId, path, timeWindow, aggregates, refreshInterval } = message;
    
    if (!subscriptionId || !path || !timeWindow) {
      ws.send(JSON.stringify({ 
        error: 'Missing required fields: subscriptionId, path, timeWindow' 
      }));
      return;
    }

    // Create subscription
    const subscription: StreamSubscription = {
      id: subscriptionId,
      path,
      timeWindow,
      aggregates: aggregates || ['current'],
      refreshInterval: refreshInterval || 1000
    };

    // Start streaming data
    this.startStreaming(subscription);
    this.activeSubscriptions.set(subscriptionId, subscription);

    ws.send(JSON.stringify({
      type: 'subscribed',
      subscriptionId,
      message: `Subscribed to ${path} with ${timeWindow} window`
    }));

    this.log(`Created subscription: ${subscriptionId} for path ${path}`);
  }

  private handleUnsubscribe(ws: WebSocket, message: any): void {
    const { subscriptionId } = message;
    
    if (this.activeSubscriptions.has(subscriptionId)) {
      this.stopStreaming(subscriptionId);
      this.activeSubscriptions.delete(subscriptionId);
      
      ws.send(JSON.stringify({
        type: 'unsubscribed',
        subscriptionId
      }));

      this.log(`Removed subscription: ${subscriptionId}`);
    } else {
      ws.send(JSON.stringify({ 
        error: `Subscription not found: ${subscriptionId}` 
      }));
    }
  }

  private startStreaming(subscription: StreamSubscription): void {
    const fetchData = async () => {
      try {
        // Calculate time window
        const { fromTime, toTime } = this.calculateTimeWindow(subscription.timeWindow);
        
        this.log(`Fetching data for ${subscription.path} from ${fromTime} to ${toTime}`);

        // Parse times to ZonedDateTime (same as HistoryAPI does)
        const from = ZonedDateTime.parse(fromTime + (fromTime.endsWith('Z') ? '' : 'Z'));
        const to = ZonedDateTime.parse(toTime + (toTime.endsWith('Z') ? '' : 'Z'));
        const context = `vessels.${this.selfId}`;

        // Create mock request/response to call getValues directly
        const mockReq = {
          query: {
            paths: subscription.path,
            // Let HistoryAPI calculate resolution automatically
          }
        } as any;

        let capturedResult: any = null;
        const mockRes = {
          json: (data: any) => { capturedResult = data; },
          status: () => mockRes
        } as any;

        // Call HistoryAPI.getValues directly (same as REST API)
        await this.historyAPI.getValues(
          context as any,
          from,
          to,
          false, // shouldRefresh
          (msg: string) => this.log(msg), // debug function
          mockReq,
          mockRes
        );

        if (capturedResult && capturedResult.data) {
          // Transform to streaming format
          const streamData = {
            type: 'data',
            subscriptionId: subscription.id,
            path: subscription.path,
            timeWindow: subscription.timeWindow,
            timestamp: new Date().toISOString(),
            data: capturedResult.data, // Full dataset with buckets
            meta: {
              range: capturedResult.range,
              dataPoints: capturedResult.data.length
            }
          };

          // Broadcast to all connected clients
          this.broadcast(streamData);
        } else {
          this.log(`No data returned for subscription ${subscription.id}`);
        }

      } catch (error) {
        this.log(`Error fetching data for subscription ${subscription.id}:`, error);
        
        // Send error to clients
        this.broadcast({
          type: 'error',
          subscriptionId: subscription.id,
          error: error instanceof Error ? error.message : 'Unknown error'
        });
      }
    };

    // Fetch initial data immediately
    fetchData();

    // Set up periodic refresh
    subscription.timer = setInterval(fetchData, subscription.refreshInterval);
  }

  private stopStreaming(subscriptionId: string): void {
    const subscription = this.activeSubscriptions.get(subscriptionId);
    if (subscription && subscription.timer) {
      clearInterval(subscription.timer);
      subscription.timer = undefined;
    }
  }

  private calculateTimeWindow(timeWindow: string): { fromTime: string, toTime: string } {
    const now = new Date();
    const toTime = now.toISOString();

    // Parse duration like "5m", "1h", "30s"
    const match = timeWindow.match(/^(\d+)([smhd])$/);
    if (!match) {
      throw new Error(`Invalid time window format: ${timeWindow}`);
    }

    const amount = parseInt(match[1]);
    const unit = match[2];
    const fromDate = new Date(now);

    switch (unit) {
      case 's':
        fromDate.setSeconds(fromDate.getSeconds() - amount);
        break;
      case 'm':
        fromDate.setMinutes(fromDate.getMinutes() - amount);
        break;
      case 'h':
        fromDate.setHours(fromDate.getHours() - amount);
        break;
      case 'd':
        fromDate.setDate(fromDate.getDate() - amount);
        break;
      default:
        throw new Error(`Unsupported time unit: ${unit}`);
    }

    const fromTime = fromDate.toISOString();

    return { fromTime, toTime };
  }

  broadcast(data: any): void {
    const message = JSON.stringify(data);
    this.connectedClients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });
  }

  getStats(): { connectedClients: number; activeSubscriptions: number } {
    return {
      connectedClients: this.connectedClients.size,
      activeSubscriptions: this.activeSubscriptions.size
    };
  }

  shutdown(): void {
    this.log('Shutting down streaming service');
    
    // Stop all subscriptions
    this.activeSubscriptions.forEach((subscription) => {
      this.stopStreaming(subscription.id);
    });
    this.activeSubscriptions.clear();

    // Close all WebSocket connections
    this.connectedClients.forEach(client => {
      client.close();
    });
    this.connectedClients.clear();

    // Close WebSocket server
    if (this.wss) {
      this.wss.close();
    }
  }

  private log(message: string, ...args: any[]): void {
    if (this.debug) {
      console.log(`[StreamingService] ${message}`, ...args);
    }
  }
}