/**
 * SignalK Parquet Streaming Client
 * 
 * Provides a simple API for connecting to the streaming service and subscribing to data streams.
 * Uses Socket.IO for WebSocket communication with fallback to polling.
 */

class SignalKStreamingClient {
  constructor(options = {}) {
    this.baseUrl = options.baseUrl || '';
    this.debug = options.debug || false;
    this.socket = null;
    this.subscriptions = new Map();
    this.connectionState = 'disconnected'; // disconnected, connecting, connected, error
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
    this.reconnectDelay = 1000; // Start with 1 second
    this.eventHandlers = new Map();
    
    // Auto-connect if requested
    if (options.autoConnect !== false) {
      this.connect();
    }
  }

  /**
   * Connect to the streaming service
   */
  connect() {
    if (this.socket && this.socket.connected) {
      this.log('Already connected');
      return;
    }

    this.connectionState = 'connecting';
    this.log('Connecting to streaming service...');

    // Load Socket.IO client library if not already loaded
    if (typeof io === 'undefined') {
      console.error('Socket.IO client library not loaded. Please include socket.io client script.');
      this.connectionState = 'error';
      this.emit('error', { message: 'Socket.IO client library not loaded' });
      return;
    }

    try {
      this.socket = io(this.baseUrl, {
        path: '/signalk-parquet-stream',
        transports: ['websocket', 'polling'],
        upgrade: true,
        timeout: 20000,
      });

      this.setupEventHandlers();
    } catch (error) {
      this.log('Connection error:', error);
      this.connectionState = 'error';
      this.emit('error', error);
    }
  }

  /**
   * Set up Socket.IO event handlers
   */
  setupEventHandlers() {
    this.socket.on('connect', () => {
      this.log('Connected to streaming service');
      this.connectionState = 'connected';
      this.reconnectAttempts = 0;
      this.reconnectDelay = 1000;
      this.emit('connected');
    });

    this.socket.on('disconnect', (reason) => {
      this.log('Disconnected:', reason);
      this.connectionState = 'disconnected';
      this.emit('disconnected', { reason });
      
      // Attempt reconnection for client-side disconnects
      if (reason === 'io client disconnect') {
        this.scheduleReconnect();
      }
    });

    this.socket.on('connect_error', (error) => {
      this.log('Connection error:', error);
      this.connectionState = 'error';
      this.emit('error', error);
      this.scheduleReconnect();
    });

    this.socket.on('data', (message) => {
      this.handleDataMessage(message);
    });

    this.socket.on('subscribed', (message) => {
      this.log('Subscription confirmed:', message);
      this.emit('subscribed', message);
    });

    this.socket.on('unsubscribed', (message) => {
      this.log('Unsubscription confirmed:', message);
      this.subscriptions.delete(message.subscriptionId);
      this.emit('unsubscribed', message);
    });

    this.socket.on('error', (message) => {
      this.log('Server error:', message);
      this.emit('error', message);
    });

    this.socket.on('queryResult', (message) => {
      this.emit('queryResult', message);
    });

    this.socket.on('queryError', (message) => {
      this.log('Query error:', message);
      this.emit('queryError', message);
    });
  }

  /**
   * Handle incoming data messages
   */
  handleDataMessage(message) {
    const { subscriptionId, data, timestamp } = message;
    
    // Try to find the subscription by ID first
    let subscription = this.subscriptions.get(subscriptionId);
    
    // If not found, use the first subscription (for ID mismatch between server and client)
    if (!subscription && this.subscriptions.size > 0) {
      subscription = Array.from(this.subscriptions.values())[0];
      this.log('Using first subscription due to ID mismatch between server and client');
    }
    
    if (subscription) {
      // Call the subscription callback
      if (subscription.callback) {
        try {
          subscription.callback(data, { subscriptionId, timestamp });
        } catch (error) {
          this.log('Error in subscription callback:', error);
        }
      }
      
      // Emit generic data event
      this.emit('data', { subscriptionId, data, timestamp });
    } else {
      this.log('No matching subscription found for:', subscriptionId);
    }
  }

  /**
   * Schedule a reconnection attempt
   */
  scheduleReconnect() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      this.log('Max reconnection attempts reached');
      this.emit('maxReconnectAttemptsReached');
      return;
    }

    this.reconnectAttempts++;
    const delay = Math.min(this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1), 30000);
    
    this.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
    
    setTimeout(() => {
      if (this.connectionState !== 'connected') {
        this.connect();
      }
    }, delay);
  }

  /**
   * Subscribe to a data stream
   * 
   * @param {Object} config - Data source configuration
   * @param {Function} callback - Called when new data arrives
   * @returns {string} Subscription ID
   */
  subscribe(config, callback) {
    if (!this.socket || !this.socket.connected) {
      throw new Error('Not connected to streaming service');
    }

    const subscriptionId = `client_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    
    this.subscriptions.set(subscriptionId, {
      config,
      callback,
      createdAt: new Date().toISOString()
    });

    this.socket.emit('subscribe', config);
    this.log('Subscribing to:', config);

    return subscriptionId;
  }

  /**
   * Unsubscribe from a data stream
   * 
   * @param {string} subscriptionId - Subscription ID to unsubscribe
   */
  unsubscribe(subscriptionId) {
    if (!this.socket || !this.socket.connected) {
      throw new Error('Not connected to streaming service');
    }

    if (this.subscriptions.has(subscriptionId)) {
      this.socket.emit('unsubscribe', subscriptionId);
      this.subscriptions.delete(subscriptionId);
      this.log('Unsubscribing from:', subscriptionId);
    }
  }

  /**
   * Update subscription configuration
   * 
   * @param {string} subscriptionId - Subscription ID to update
   * @param {Object} newConfig - New configuration
   */
  updateSubscription(subscriptionId, newConfig) {
    if (!this.socket || !this.socket.connected) {
      throw new Error('Not connected to streaming service');
    }

    if (this.subscriptions.has(subscriptionId)) {
      const subscription = this.subscriptions.get(subscriptionId);
      subscription.config = { ...subscription.config, ...newConfig };
      
      this.socket.emit('updateConfig', { subscriptionId, config: newConfig });
      this.log('Updating subscription:', subscriptionId, newConfig);
    }
  }

  /**
   * Perform a one-time query
   * 
   * @param {Object} config - Data source configuration
   * @param {string} from - Start time (optional)
   * @param {string} to - End time (optional)
   * @returns {Promise} Promise that resolves with query result
   */
  query(config, from, to) {
    return new Promise((resolve, reject) => {
      if (!this.socket || !this.socket.connected) {
        reject(new Error('Not connected to streaming service'));
        return;
      }

      const queryId = `query_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      
      // Set up one-time listeners
      const resultHandler = (message) => {
        resolve(message);
        this.socket.off('queryResult', resultHandler);
        this.socket.off('queryError', errorHandler);
      };
      
      const errorHandler = (message) => {
        reject(new Error(message.message || 'Query failed'));
        this.socket.off('queryResult', resultHandler);
        this.socket.off('queryError', errorHandler);
      };

      this.socket.on('queryResult', resultHandler);
      this.socket.on('queryError', errorHandler);

      // Send query
      this.socket.emit('query', { config, from, to, queryId });
      this.log('Querying:', config, { from, to });
    });
  }

  /**
   * Get active subscriptions
   */
  getSubscriptions() {
    return Array.from(this.subscriptions.entries()).map(([id, sub]) => ({
      id,
      ...sub
    }));
  }

  /**
   * Get connection state
   */
  getConnectionState() {
    return {
      state: this.connectionState,
      connected: this.socket?.connected || false,
      subscriptionCount: this.subscriptions.size,
      reconnectAttempts: this.reconnectAttempts
    };
  }

  /**
   * Add event listener
   */
  on(event, handler) {
    if (!this.eventHandlers.has(event)) {
      this.eventHandlers.set(event, []);
    }
    this.eventHandlers.get(event).push(handler);
  }

  /**
   * Remove event listener
   */
  off(event, handler) {
    if (this.eventHandlers.has(event)) {
      const handlers = this.eventHandlers.get(event);
      const index = handlers.indexOf(handler);
      if (index > -1) {
        handlers.splice(index, 1);
      }
    }
  }

  /**
   * Emit event to listeners
   */
  emit(event, data) {
    if (this.eventHandlers.has(event)) {
      this.eventHandlers.get(event).forEach(handler => {
        try {
          handler(data);
        } catch (error) {
          this.log('Error in event handler:', error);
        }
      });
    }
  }

  /**
   * Disconnect from streaming service
   */
  disconnect() {
    if (this.socket) {
      this.socket.disconnect();
      this.subscriptions.clear();
      this.connectionState = 'disconnected';
      this.log('Disconnected from streaming service');
    }
  }

  /**
   * Internal logging
   */
  log(...args) {
    if (this.debug) {
      console.log('[SignalKStreamingClient]', ...args);
    }
  }
}

// Export for use in modules or as global
if (typeof module !== 'undefined' && module.exports) {
  module.exports = SignalKStreamingClient;
} else {
  window.SignalKStreamingClient = SignalKStreamingClient;
}