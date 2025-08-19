import { Server as HttpServer } from 'http';
import { Server as SocketIOServer, Socket } from 'socket.io';
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
  socket: Socket;
  dataSource: UniversalDataSource;
  subscription: Subscription;
  config: DataSourceConfig;
}

export class StreamingService {
  private io: SocketIOServer;
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
      // Create Socket.IO server with unique path and safer options to avoid conflicts
      const socketPath = '/signalk-parquet-stream';
      
      this.io = new SocketIOServer(httpServer, {
        path: socketPath,
        cors: {
          origin: "*",
          methods: ["GET", "POST"]
        },
        transports: ['websocket', 'polling'],
        // Safer options to prevent HTTP server conflicts
        allowEIO3: true,
        pingTimeout: 60000,  // Increased timeout
        pingInterval: 25000, // Increased interval
        upgradeTimeout: 10000,
        maxHttpBufferSize: 1e6,
        // Prevent Socket.IO from interfering with existing routes
        serveClient: false
      });

      this.setupEventHandlers();
      this.log('Streaming service initialized successfully');
    } catch (error) {
      this.log('Failed to initialize Socket.IO server:', error);
      // Don't throw error - let plugin continue without streaming
      this.io = null as any;
    }
  }

  private setupEventHandlers(): void {
    if (!this.io) {
      this.log('Cannot setup event handlers - Socket.IO server not initialized');
      return;
    }
    
    this.io.on('connection', (socket: Socket) => {
      this.log(`Client connected: ${socket.id}`);

      // Handle data source subscription
      socket.on('subscribe', (config: DataSourceConfig) => {
        this.handleSubscribe(socket, config);
      });

      // Handle unsubscribe
      socket.on('unsubscribe', (subscriptionId: string) => {
        this.handleUnsubscribe(subscriptionId);
      });

      // Handle configuration updates
      socket.on('updateConfig', (data: { subscriptionId: string; config: Partial<DataSourceConfig> }) => {
        this.handleUpdateConfig(data.subscriptionId, data.config);
      });

      // Handle one-time queries
      socket.on('query', async (data: { config: DataSourceConfig; from?: string; to?: string }) => {
        await this.handleQuery(socket, data);
      });

      // Handle disconnect
      socket.on('disconnect', () => {
        this.handleDisconnect(socket);
        this.log(`Client disconnected: ${socket.id}`);
      });

      // Send initial connection info
      socket.emit('connected', {
        selfId: this.selfId,
        timestamp: new Date().toISOString()
      });
    });
  }

  private handleSubscribe(socket: Socket, config: DataSourceConfig): void {
    try {
      const subscriptionId = `sub_${++this.subscriptionCounter}`;
      
      // Validate config
      if (!config.path) {
        socket.emit('error', { message: 'Path is required for subscription' });
        return;
      }

      // Create data source
      const dataSource = new UniversalDataSource(config, this.historyAPI);
      
      // Subscribe to the stream
      const subscription = dataSource.stream().subscribe({
        next: (data: StreamResponse) => {
          socket.emit('data', {
            subscriptionId,
            data,
            timestamp: new Date().toISOString()
          });
        },
        error: (error: any) => {
          this.log(`Stream error for ${subscriptionId}:`, error);
          socket.emit('error', {
            subscriptionId,
            message: error.message || 'Stream error'
          });
        }
      });

      // Store subscription
      this.clientSubscriptions.set(subscriptionId, {
        id: subscriptionId,
        socket,
        dataSource,
        subscription,
        config
      });

      // Confirm subscription
      socket.emit('subscribed', {
        subscriptionId,
        config,
        timestamp: new Date().toISOString()
      });

      this.log(`Created subscription ${subscriptionId} for path: ${config.path}`);

    } catch (error) {
      this.log('Subscribe error:', error);
      socket.emit('error', { message: 'Failed to create subscription' });
    }
  }

  private handleUnsubscribe(subscriptionId: string): void {
    const clientSub = this.clientSubscriptions.get(subscriptionId);
    if (clientSub) {
      clientSub.subscription.unsubscribe();
      this.clientSubscriptions.delete(subscriptionId);
      
      clientSub.socket.emit('unsubscribed', {
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
      
      clientSub.socket.emit('configUpdated', {
        subscriptionId,
        config: clientSub.config,
        timestamp: new Date().toISOString()
      });

      this.log(`Updated config for ${subscriptionId}`);
    }
  }

  private async handleQuery(socket: Socket, data: { config: DataSourceConfig; from?: string; to?: string }): Promise<void> {
    try {
      const { config, from, to } = data;
      
      if (!config.path) {
        socket.emit('queryError', { message: 'Path is required for query' });
        return;
      }

      const dataSource = new UniversalDataSource(config, this.historyAPI);
      const result = await dataSource.query(from, to);
      
      socket.emit('queryResult', {
        config,
        data: result,
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      this.log('Query error:', error);
      socket.emit('queryError', { message: 'Query failed' });
    }
  }

  private handleDisconnect(socket: Socket): void {
    // Clean up all subscriptions for this socket
    const toDelete: string[] = [];
    
    this.clientSubscriptions.forEach((clientSub, subscriptionId) => {
      if (clientSub.socket.id === socket.id) {
        clientSub.subscription.unsubscribe();
        toDelete.push(subscriptionId);
      }
    });

    toDelete.forEach(id => this.clientSubscriptions.delete(id));
    
    if (toDelete.length > 0) {
      this.log(`Cleaned up ${toDelete.length} subscriptions for disconnected client`);
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
      connectedClients: this.io ? this.io.sockets.sockets.size : 0,
      activeSubscriptions: this.clientSubscriptions.size,
      subscriptionsByPath
    };
  }

  /**
   * Broadcast a message to all connected clients
   */
  broadcast(event: string, data: any): void {
    if (this.io) {
      this.io.emit(event, data);
    } else {
      this.log('Cannot broadcast - Socket.IO server not initialized');
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

    // Close Socket.IO server properly to avoid HTTP server conflicts
    if (this.io) {
      try {
        // Disconnect all clients first
        this.io.disconnectSockets();
        
        // Close the Socket.IO server with callback to ensure clean shutdown
        this.io.close(() => {
          this.log('Streaming service shut down completely');
        });
      } catch (error) {
        this.log('Error during Socket.IO shutdown:', error);
        // Force close if there's an error
        try {
          this.io.close();
        } catch (forceError) {
          this.log('Force close also failed:', forceError);
        }
      }
    }
  }

  private log(...args: any[]): void {
    if (this.debug) {
      console.log('[StreamingService]', ...args);
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
