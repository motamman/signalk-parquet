import { Server as HttpServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import { Observable, Subscription } from 'rxjs';
import { UniversalDataSource, DataSourceConfig, StreamResponse } from './universal-datasource';
import { HistoryAPI } from './HistoryAPI';

export interface StreamingServiceOptions {
  historyAPI: HistoryAPI;
  selfId: string;
  debug?: boolean;
}

export interface ClientSubscription {
  id: string;
  ws: WebSocket;
  dataSource: UniversalDataSource;
  subscription: Subscription;
  config: DataSourceConfig;
}

export class StreamingService {
  private wss: WebSocketServer;
  private historyAPI: HistoryAPI;
  private selfId: string;
  private debug: boolean;
  private clientSubscriptions: Map<string, ClientSubscription> = new Map();
  private subscriptionCounter = 0;

  constructor(httpServer: HttpServer, options: StreamingServiceOptions) {
    this.historyAPI = options.historyAPI;
    this.selfId = options.selfId;
    this.debug = options.debug || false;

    try {
      // Create native WebSocket server - much cleaner than Socket.IO
      this.wss = new WebSocketServer({ 
        server: httpServer,
        path: '/parquet-stream'
      });

      this.setupEventHandlers();
      this.log('Streaming service initialized successfully with native WebSocket');
    } catch (error) {
      this.log('Failed to initialize WebSocket server:', error);
      // Don't throw error - let plugin continue without streaming
      this.wss = null as any;
    }
  }

  private setupEventHandlers(): void {
    if (!this.wss) {
      this.log('Cannot setup event handlers - WebSocket server not initialized');
      return;
    }
    
    this.wss.on('connection', (ws: WebSocket) => {
      const clientId = `client_${Date.now()}_${Math.random()}`;
      this.log(`Client connected: ${clientId}`);

      // Send initial connection info
      this.sendMessage(ws, 'connected', {
        selfId: this.selfId,
        timestamp: new Date().toISOString()
      });

      // Handle incoming messages
      ws.on('message', (data: Buffer) => {
        try {
          const message = JSON.parse(data.toString());
          this.handleMessage(ws, message, clientId);
        } catch (error) {
          this.log('Error parsing message:', error);
          this.sendMessage(ws, 'error', { message: 'Invalid JSON' });
        }
      });

      // Handle disconnect
      ws.on('close', () => {
        this.handleDisconnect(ws, clientId);
        this.log(`Client disconnected: ${clientId}`);
      });

      ws.on('error', (error) => {
        this.log(`WebSocket error for ${clientId}:`, error);
      });
    });
  }

  private handleMessage(ws: WebSocket, message: any, clientId: string): void {
    const { type, data } = message;

    switch (type) {
      case 'subscribe':
        this.handleSubscribe(ws, data, clientId);
        break;
      case 'unsubscribe':
        this.handleUnsubscribe(data.subscriptionId);
        break;
      case 'updateConfig':
        this.handleUpdateConfig(data.subscriptionId, data.config);
        break;
      case 'query':
        this.handleQuery(ws, data);
        break;
      default:
        this.sendMessage(ws, 'error', { message: `Unknown message type: ${type}` });
    }
  }

  private handleSubscribe(ws: WebSocket, config: DataSourceConfig, clientId: string): void {
    try {
      const subscriptionId = `sub_${++this.subscriptionCounter}`;
      
      // Validate config
      if (!config.path) {
        this.sendMessage(ws, 'error', { message: 'Path is required for subscription' });
        return;
      }

      // Create data source
      const dataSource = new UniversalDataSource(config, this.historyAPI);
      
      // Subscribe to the stream
      const subscription = dataSource.stream().subscribe({
        next: (data: StreamResponse) => {
          this.sendMessage(ws, 'data', {
            subscriptionId,
            data,
            timestamp: new Date().toISOString()
          });
        },
        error: (error: any) => {
          this.log(`Stream error for ${subscriptionId}:`, error);
          this.sendMessage(ws, 'error', {
            subscriptionId,
            message: error.message || 'Stream error'
          });
        }
      });

      // Store subscription
      this.clientSubscriptions.set(subscriptionId, {
        id: subscriptionId,
        ws,
        dataSource,
        subscription,
        config
      });

      // Confirm subscription
      this.sendMessage(ws, 'subscribed', {
        subscriptionId,
        config,
        timestamp: new Date().toISOString()
      });

      this.log(`Created subscription ${subscriptionId} for path: ${config.path}`);

    } catch (error) {
      this.log('Subscribe error:', error);
      this.sendMessage(ws, 'error', { message: 'Failed to create subscription' });
    }
  }

  private handleUnsubscribe(subscriptionId: string): void {
    const clientSub = this.clientSubscriptions.get(subscriptionId);
    if (clientSub) {
      clientSub.subscription.unsubscribe();
      this.clientSubscriptions.delete(subscriptionId);
      
      this.sendMessage(clientSub.ws, 'unsubscribed', {
        subscriptionId,
        timestamp: new Date().toISOString()
      });

      this.log(`Unsubscribed: ${subscriptionId}`);
    }
  }

  private handleUpdateConfig(subscriptionId: string, newConfig: Partial<DataSourceConfig>): void {
    const clientSub = this.clientSubscriptions.get(subscriptionId);
    if (clientSub) {
      clientSub.dataSource.updateConfig(newConfig);
      clientSub.config = { ...clientSub.config, ...newConfig };
      
      this.sendMessage(clientSub.ws, 'configUpdated', {
        subscriptionId,
        config: clientSub.config,
        timestamp: new Date().toISOString()
      });

      this.log(`Updated config for ${subscriptionId}`);
    }
  }

  private async handleQuery(ws: WebSocket, data: { config: DataSourceConfig; from?: string; to?: string }): Promise<void> {
    try {
      const { config, from, to } = data;
      
      if (!config.path) {
        this.sendMessage(ws, 'queryError', { message: 'Path is required for query' });
        return;
      }

      const dataSource = new UniversalDataSource(config, this.historyAPI);
      const result = await dataSource.query(from, to);
      
      this.sendMessage(ws, 'queryResult', {
        config,
        data: result,
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      this.log('Query error:', error);
      this.sendMessage(ws, 'queryError', { message: 'Query failed' });
    }
  }

  private handleDisconnect(ws: WebSocket, clientId: string): void {
    // Clean up all subscriptions for this WebSocket
    const toDelete: string[] = [];
    
    this.clientSubscriptions.forEach((clientSub, subscriptionId) => {
      if (clientSub.ws === ws) {
        clientSub.subscription.unsubscribe();
        toDelete.push(subscriptionId);
      }
    });

    toDelete.forEach(id => this.clientSubscriptions.delete(id));
    
    if (toDelete.length > 0) {
      this.log(`Cleaned up ${toDelete.length} subscriptions for disconnected client`);
    }
  }

  private sendMessage(ws: WebSocket, type: string, data: any): void {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type, data }));
    }
  }

  /**
   * Get statistics about active connections and subscriptions
   */
  getStats(): {
    connectedClients: number;
    activeSubscriptions: number;
    subscriptionsByPath: Record<string, number>;
  } {
    const subscriptionsByPath: Record<string, number> = {};
    
    this.clientSubscriptions.forEach(sub => {
      const path = sub.config.path;
      subscriptionsByPath[path] = (subscriptionsByPath[path] || 0) + 1;
    });

    return {
      connectedClients: this.wss ? this.wss.clients.size : 0,
      activeSubscriptions: this.clientSubscriptions.size,
      subscriptionsByPath
    };
  }

  /**
   * Broadcast a message to all connected clients
   */
  broadcast(event: string, data: any): void {
    if (this.wss) {
      this.wss.clients.forEach(ws => {
        this.sendMessage(ws, event, data);
      });
    } else {
      this.log('Cannot broadcast - WebSocket server not initialized');
    }
  }

  /**
   * Shutdown the streaming service
   */
  shutdown(): void {
    // Unsubscribe all active subscriptions
    this.clientSubscriptions.forEach(sub => {
      sub.subscription.unsubscribe();
    });
    this.clientSubscriptions.clear();

    // Close WebSocket server properly
    if (this.wss) {
      try {
        // Close all client connections
        this.wss.clients.forEach(ws => {
          ws.close();
        });
        
        // Close the WebSocket server
        this.wss.close(() => {
          this.log('WebSocket streaming service shut down completely');
        });
      } catch (error) {
        this.log('Error during WebSocket shutdown:', error);
      }
    }
  }

  private log(...args: any[]): void {
    if (this.debug) {
      console.log('[StreamingService-WebSocket]', ...args);
    }
  }
}

// Client-side helper types and interfaces for TypeScript consumers
export interface StreamingClient {
  subscribe(config: DataSourceConfig): string;
  unsubscribe(subscriptionId: string): void;
  query(config: DataSourceConfig, from?: string, to?: string): Promise<StreamResponse>;
  on(event: 'data' | 'error' | 'subscribed' | 'unsubscribed', callback: Function): void;
}
