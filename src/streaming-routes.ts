import { Router, Request, Response } from 'express';
import { PluginState, StreamingSubscriptionConfig } from './types';
import { UniversalDataSource, DataSourceConfig } from './universal-datasource';
import { HistoryAPI } from './HistoryAPI';
import { ServerAPI } from '@signalk/server-api';
import { loadWebAppConfig, saveWebAppConfig } from './commands';

export function registerStreamingRoutes(router: Router, state: PluginState, app: ServerAPI): void {
  // Create a HistoryAPI instance for streaming routes
  const historyAPI = new HistoryAPI(
    app.selfId,
    state.currentConfig?.outputDirectory || ''
  );

  /**
   * GET /api/stream/stats
   * Get streaming service statistics
   */
  router.get('/api/stream/stats', (req: Request, res: Response) => {
    try {
      if (!state.streamingService) {
        return res.status(503).json({
          error: 'Streaming service not available',
          connectedClients: 0,
          activeSubscriptions: 0,
          subscriptionsByPath: {}
        });
      }

      const stats = state.streamingService.getStats();
      res.json({
        ...stats,
        serverTime: new Date().toISOString(),
        streamingEnabled: true
      });
    } catch (error) {
      app.error(`Error getting streaming stats: ${error}`);
      res.status(500).json({ error: 'Failed to get streaming stats' });
    }
  });

  /**
   * POST /api/stream/query
   * One-time query without WebSocket subscription
   */
  router.post('/api/stream/query', async (req: Request, res: Response) => {
    try {
      const { config, from, to } = req.body as {
        config: DataSourceConfig;
        from?: string;
        to?: string;
      };

      if (!config || !config.path) {
        return res.status(400).json({ error: 'Config with path is required' });
      }

      // Validate time parameters
      if (from && to) {
        const fromDate = new Date(from);
        const toDate = new Date(to);
        if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) {
          return res.status(400).json({ error: 'Invalid date format' });
        }
        if (fromDate >= toDate) {
          return res.status(400).json({ error: 'from must be before to' });
        }
      }

      // @ts-ignore - TypeScript constructor signature issue
      const dataSource = new UniversalDataSource(config, historyAPI);
      const result = await dataSource.query(from, to);
      
      res.json({
        success: true,
        data: result,
        query: { config, from, to },
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      app.error(`Error in stream query: ${error}`);
      res.status(500).json({ 
        error: 'Query failed',
        message: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  });

  /**
   * POST /api/stream/validate
   * Validate streaming configuration
   */
  router.post('/api/stream/validate', async (req: Request, res: Response) => {
    try {
      const { config } = req.body as { config: DataSourceConfig };

      if (!config) {
        return res.status(400).json({ error: 'Config is required' });
      }

      const errors: string[] = [];
      
      // Validate path
      if (!config.path) {
        errors.push('path is required');
      }

      // Validate timeWindow format
      if (config.timeWindow) {
        if (Array.isArray(config.timeWindow)) {
          const [from, to] = config.timeWindow;
          const fromDate = new Date(from);
          const toDate = new Date(to);
          if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) {
            errors.push('Invalid date format in timeWindow array');
          }
        } else {
          const durationMatch = config.timeWindow.match(/^(\d+)([smhd])$/);
          if (!durationMatch) {
            errors.push('Invalid timeWindow duration format. Use format like "30s", "5m", "1h", "2d"');
          }
        }
      }

      // Validate aggregates
      if (config.aggregates) {
        const validAggregates = ['current', 'min', 'max', 'average', 'first', 'last', 'median'];
        const invalidAggregates = config.aggregates.filter(agg => !validAggregates.includes(agg));
        if (invalidAggregates.length > 0) {
          errors.push(`Invalid aggregates: ${invalidAggregates.join(', ')}`);
        }
      }

      // Validate resolution
      if (config.resolution) {
        const resolutionMatch = config.resolution.match(/^(\d+)([smhd])$/);
        if (!resolutionMatch) {
          errors.push('Invalid resolution format. Use format like "1s", "30s", "1m"');
        }
      }

      // Validate refreshInterval
      if (config.refreshInterval !== undefined) {
        if (typeof config.refreshInterval !== 'number' || config.refreshInterval < 100) {
          errors.push('refreshInterval must be a number >= 100 (milliseconds)');
        }
      }

      if (errors.length > 0) {
        return res.status(400).json({
          valid: false,
          errors
        });
      }

      // Try to create a data source to validate it works
      try {
        // @ts-ignore - TypeScript constructor signature issue
        const dataSource = new UniversalDataSource(config, historyAPI);
        // Test query to validate path exists and is accessible
        const testResult = await dataSource.query();
        
        res.json({
          valid: true,
          config,
          pathExists: testResult.values.length > 0,
          message: testResult.values.length > 0 
            ? 'Configuration is valid and path has data'
            : 'Configuration is valid but path has no data'
        });
      } catch (error) {
        res.json({
          valid: false,
          errors: [`DataSource creation failed: ${error instanceof Error ? error.message : 'Unknown error'}`]
        });
      }

    } catch (error) {
      app.error(`Error validating stream config: ${error}`);
      res.status(500).json({ 
        error: 'Validation failed',
        message: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  });


  /**
   * POST /api/stream/broadcast
   * Broadcast a message to all connected streaming clients (admin only)
   */
  router.post('/api/stream/broadcast', (req: Request, res: Response) => {
    try {
      if (!state.streamingService) {
        return res.status(503).json({ error: 'Streaming service not available' });
      }

      const { event, data } = req.body;
      
      if (!event) {
        return res.status(400).json({ error: 'event is required' });
      }

      state.streamingService.broadcast(event, {
        ...data,
        serverTimestamp: new Date().toISOString()
      });

      res.json({
        success: true,
        message: `Broadcasted ${event} to all connected clients`
      });

    } catch (error) {
      app.error(`Error broadcasting to streaming clients: ${error}`);
      res.status(500).json({ 
        error: 'Broadcast failed',
        message: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  });

  /**
   * GET /api/stream/subscriptions
   * Get saved stream subscriptions
   */
  router.get('/api/stream/subscriptions', (req: Request, res: Response) => {
    try {
      const webAppConfig = loadWebAppConfig(app);
      const subscriptions = webAppConfig.streamingSubscriptions || [];
      res.json({
        success: true,
        subscriptions,
        total: subscriptions.length
      });
    } catch (error) {
      app.error(`Error getting stream subscriptions: ${error}`);
      res.status(500).json({ 
        success: false,
        error: 'Failed to get stream subscriptions',
        message: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  });

  /**
   * POST /api/stream/subscriptions
   * Save a new stream subscription
   */
  router.post('/api/stream/subscriptions', (req: Request, res: Response) => {
    try {
      const { name, path, timeWindow, aggregates, refreshInterval } = req.body as {
        name: string;
        path: string;
        timeWindow: string;
        aggregates: string[];
        refreshInterval: number;
      };

      if (!name || !path) {
        return res.status(400).json({ 
          success: false,
          error: 'Name and path are required' 
        });
      }

      const newSubscription: StreamingSubscriptionConfig = {
        id: `stream_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
        name,
        enabled: true,
        path,
        timeWindow: timeWindow || '5m',
        aggregates: aggregates || ['current'],
        refreshInterval: refreshInterval || 1000,
        createdAt: new Date().toISOString()
      };

      // Load current webapp config and add new subscription
      const webAppConfig = loadWebAppConfig(app);
      const streamingSubscriptions = webAppConfig.streamingSubscriptions || [];
      streamingSubscriptions.push(newSubscription);

      // Save updated config
      saveWebAppConfig(webAppConfig.paths, webAppConfig.commands, app, streamingSubscriptions);
      app.debug(`Saved stream subscription: ${newSubscription.name} (${newSubscription.path})`);

      res.json({
        success: true,
        subscription: newSubscription,
        message: 'Stream subscription saved successfully'
      });

    } catch (error) {
      app.error(`Error saving stream subscription: ${error}`);
      res.status(500).json({ 
        success: false,
        error: 'Failed to save stream subscription',
        message: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  });

  /**
   * PUT /api/stream/subscriptions/:id
   * Update a stream subscription
   */
  router.put('/api/stream/subscriptions/:id', (req: Request, res: Response) => {
    try {
      const { id } = req.params;
      const updates = req.body;

      const webAppConfig = loadWebAppConfig(app);
      const streamingSubscriptions = webAppConfig.streamingSubscriptions || [];

      const subscriptionIndex = streamingSubscriptions.findIndex(sub => sub.id === id);
      if (subscriptionIndex === -1) {
        return res.status(404).json({ 
          success: false,
          error: 'Stream subscription not found' 
        });
      }

      // Update the subscription
      streamingSubscriptions[subscriptionIndex] = {
        ...streamingSubscriptions[subscriptionIndex],
        ...updates
      };

      const updatedSubscription = streamingSubscriptions[subscriptionIndex];

      // Save updated config
      saveWebAppConfig(webAppConfig.paths, webAppConfig.commands, app, streamingSubscriptions);
      app.debug(`Updated stream subscription: ${updatedSubscription.name}`);

      res.json({
        success: true,
        subscription: updatedSubscription,
        message: 'Stream subscription updated successfully'
      });

    } catch (error) {
      app.error(`Error updating stream subscription: ${error}`);
      res.status(500).json({ 
        success: false,
        error: 'Failed to update stream subscription',
        message: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  });

  /**
   * DELETE /api/stream/subscriptions/:id
   * Delete a stream subscription
   */
  router.delete('/api/stream/subscriptions/:id', (req: Request, res: Response) => {
    try {
      const { id } = req.params;

      const webAppConfig = loadWebAppConfig(app);
      const streamingSubscriptions = webAppConfig.streamingSubscriptions || [];

      const subscriptionIndex = streamingSubscriptions.findIndex(sub => sub.id === id);
      if (subscriptionIndex === -1) {
        return res.status(404).json({ 
          success: false,
          error: 'Stream subscription not found' 
        });
      }

      const deletedSubscription = streamingSubscriptions[subscriptionIndex];
      streamingSubscriptions.splice(subscriptionIndex, 1);

      // Save updated config
      saveWebAppConfig(webAppConfig.paths, webAppConfig.commands, app, streamingSubscriptions);
      app.debug(`Deleted stream subscription: ${deletedSubscription.name}`);

      res.json({
        success: true,
        message: 'Stream subscription deleted successfully'
      });

    } catch (error) {
      app.error(`Error deleting stream subscription: ${error}`);
      res.status(500).json({ 
        success: false,
        error: 'Failed to delete stream subscription',
        message: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  });

  app.debug('Streaming API routes registered');
}