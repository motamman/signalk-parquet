import { StreamingService } from './streaming-service';
import { PluginState } from './types';
import { ServerAPI } from '@signalk/server-api';

/**
 * Restore saved stream subscriptions on plugin startup
 */
async function restoreStreamSubscriptions(state: PluginState, app: ServerAPI): Promise<void> {
  if (!state.streamingService) {
    return;
  }

  // Load streaming subscriptions from webapp config
  const { loadWebAppConfig } = require('./commands');
  const webAppConfig = loadWebAppConfig(app);
  const streamingSubscriptions = webAppConfig.streamingSubscriptions || [];
  const enabledStreams = streamingSubscriptions.filter((stream: any) => stream.enabled);
  
  if (enabledStreams.length === 0) {
    app.debug('No enabled stream subscriptions to restore');
    return;
  }

  app.debug(`Restoring ${enabledStreams.length} enabled stream subscriptions`);

  for (const streamConfig of enabledStreams) {
    try {
      // Import the UniversalDataSource and HistoryAPI here to avoid circular imports
      const { UniversalDataSource } = require('./universal-datasource');
      const { HistoryAPI } = require('./HistoryAPI');
      
      const historyAPI = new HistoryAPI(app.selfId, state.currentConfig!.outputDirectory);
      
      const dataSourceConfig = {
        path: streamConfig.path,
        timeWindow: streamConfig.timeWindow,
        aggregates: streamConfig.aggregates,
        refreshInterval: streamConfig.refreshInterval
      };

      // Create data source and subscribe to stream
      const dataSource = new UniversalDataSource(dataSourceConfig, historyAPI);
      const subscription = dataSource.stream().subscribe({
        next: (data: any) => {
          // Broadcast to all connected WebSocket clients
          state.streamingService.broadcast('data', {
            subscriptionId: streamConfig.id,
            data,
            timestamp: new Date().toISOString()
          });
        },
        error: (error: any) => {
          app.error(`Stream error for ${streamConfig.name} (${streamConfig.path}): ${error}`);
        }
      });

      // Store the subscription for cleanup on shutdown
      if (!state.restoredSubscriptions) {
        state.restoredSubscriptions = new Map();
      }
      state.restoredSubscriptions.set(streamConfig.id, {
        subscription,
        dataSource,
        config: streamConfig
      });

      app.debug(`Restored stream: ${streamConfig.name} (${streamConfig.path})`);
    } catch (error) {
      app.error(`Failed to restore stream ${streamConfig.name}: ${error}`);
    }
  }
}

/**
 * Initialize streaming service at runtime
 */
export async function initializeStreamingService(state: PluginState, app: ServerAPI): Promise<{success: boolean, error?: string}> {
  try {
    if (state.streamingService) {
      return { success: true }; // Already initialized
    }

    // Check if streaming is enabled in config
    if (!state.currentConfig?.enableStreaming) {
      return { 
        success: false, 
        error: 'Streaming is disabled in plugin configuration' 
      };
    }

    app.debug('Initializing streaming service at runtime...');
    
    // Create the HistoryAPI instance for streaming
    const { HistoryAPI } = require('./HistoryAPI');
    const historyAPI = new HistoryAPI(
      app.selfId,
      state.currentConfig!.outputDirectory
    );
    
    // Access the HTTP server from the SignalK app
    const httpServer = (app as any).router?.parent?.server || 
                     (app as any).httpServer || 
                     (app as any).server;
    
    if (!httpServer) {
      return { 
        success: false, 
        error: 'HTTP server not available for streaming service' 
      };
    }

    // Create streaming service
    state.streamingService = new StreamingService(httpServer, {
      historyAPI: historyAPI,
      selfId: app.selfId,
      debug: true
    });
    
    if (state.streamingService) {
      app.debug('Streaming service initialized successfully at runtime');
      
      // Restore enabled stream subscriptions
      try {
        await restoreStreamSubscriptions(state, app);
      } catch (restoreError) {
        app.error(`Failed to restore stream subscriptions: ${restoreError}`);
        // Continue - don't let restore failure break streaming
      }
      
      return { success: true };
    } else {
      return { 
        success: false, 
        error: 'Streaming service creation returned null/undefined' 
      };
    }
  } catch (error) {
    app.error(`Failed to initialize streaming service at runtime: ${error}`);
    return { 
      success: false, 
      error: error instanceof Error ? error.message : 'Unknown error' 
    };
  }
}

/**
 * Shutdown streaming service at runtime
 */
export function shutdownStreamingService(state: PluginState, app: ServerAPI): void {
  try {
    if (!state.streamingService) {
      app.debug('Streaming service already stopped');
      return;
    }

    app.debug('Shutting down streaming service at runtime...');

    // Clean up restored stream subscriptions
    if (state.restoredSubscriptions) {
      state.restoredSubscriptions.forEach((sub, id) => {
        try {
          sub.subscription.unsubscribe();
          app.debug(`Unsubscribed restored stream: ${id}`);
        } catch (error) {
          app.error(`Error unsubscribing restored stream ${id}: ${error}`);
        }
      });
      state.restoredSubscriptions.clear();
    }

    // Shutdown streaming service
    try {
      state.streamingService.shutdown();
      app.debug('Streaming service shut down successfully at runtime');
    } catch (error) {
      app.error(`Error shutting down streaming service: ${error}`);
    }
    
    state.streamingService = undefined;
    state.streamingEnabled = false;
  } catch (error) {
    app.error(`Failed to shutdown streaming service at runtime: ${error}`);
  }
}