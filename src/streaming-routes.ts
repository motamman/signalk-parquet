import { Router, Request, Response } from 'express';
import { PluginState } from './types';
import { UniversalDataSource, DataSourceConfig } from './universal-datasource';
import { HistoryAPI } from './HistoryAPI';
import { ServerAPI } from '@signalk/server-api';

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
      app.error('Error getting streaming stats:', error);
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
      app.error('Error in stream query:', error);
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
      app.error('Error validating stream config:', error);
      res.status(500).json({ 
        error: 'Validation failed',
        message: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  });

  /**
   * GET /api/stream/available-paths
   * Get list of available paths for streaming
   */
  router.get('/api/stream/available-paths', async (req: Request, res: Response) => {
    try {
      // Use the path discovery utility
      const { discoverPaths } = require('./utils/path-discovery');
      
      if (!state.currentConfig?.outputDirectory) {
        return res.status(500).json({ error: 'Output directory not configured' });
      }

      const paths = await discoverPaths(state.currentConfig.outputDirectory, app.debug);
      
      res.json({
        paths: paths.map((pathInfo: any) => ({
          path: pathInfo.path,
          contexts: pathInfo.contexts,
          lastSeen: pathInfo.lastSeen,
          fileCount: pathInfo.files.length
        })),
        totalPaths: paths.length,
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      app.error('Error getting available paths:', error);
      res.status(500).json({ 
        error: 'Failed to get available paths',
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
      app.error('Error broadcasting to streaming clients:', error);
      res.status(500).json({ 
        error: 'Broadcast failed',
        message: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  });

  app.debug('Streaming API routes registered');
}