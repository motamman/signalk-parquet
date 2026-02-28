import { Context, Path, ServerAPI } from '@signalk/server-api';
import { minimatch } from 'minimatch';
import { PathConfig, PluginConfig, PluginState } from '../types';
import { loadWebAppConfig, saveWebAppConfig } from '../commands';
import { updateDataSubscriptions } from '../data-handler';

export interface AutoDiscoveryResult {
  configured: boolean;
  path: string;
  reason?: string;
}

export class AutoDiscoveryService {
  private configuredCount: number = 0;
  private app: ServerAPI;
  private pluginConfig: PluginConfig;
  private state: PluginState;
  private currentPaths: PathConfig[];
  private configurationLock: Promise<void> = Promise.resolve();

  constructor(
    app: ServerAPI,
    pluginConfig: PluginConfig,
    state: PluginState,
    currentPaths: PathConfig[]
  ) {
    this.app = app;
    this.pluginConfig = pluginConfig;
    this.state = state;
    this.currentPaths = currentPaths;
  }

  /**
   * Set the initial count of auto-discovered paths from existing config.
   * Called during initialization to restore the counter.
   */
  setInitialCount(count: number): void {
    this.configuredCount = count;
    this.app.debug(
      `[AutoDiscovery] Initialized with ${count} existing auto-discovered paths`
    );
  }

  /**
   * Get the current count of auto-configured paths
   */
  getConfiguredCount(): number {
    return this.configuredCount;
  }

  /**
   * Update references when config/paths change
   */
  updateReferences(
    pluginConfig: PluginConfig,
    currentPaths: PathConfig[]
  ): void {
    this.pluginConfig = pluginConfig;
    this.currentPaths = currentPaths;
  }

  /**
   * Main entry point: attempt to auto-configure a path if conditions are met.
   * Called when a query returns no data for a path.
   * Uses a lock to prevent race conditions on concurrent requests.
   */
  async maybeAutoConfigurePath(
    path: Path,
    context: Context
  ): Promise<AutoDiscoveryResult> {
    // Serialize all auto-discovery operations to prevent race conditions
    return new Promise(resolve => {
      this.configurationLock = this.configurationLock
        .then(async () => {
          const result = await this.doAutoConfigurePath(path, context);
          resolve(result);
        })
        .catch(error => {
          // Ensure errors don't break the chain, but still resolve with failure
          this.app.error(
            `[AutoDiscovery] Lock chain error for ${path}: ${error}`
          );
          resolve({
            configured: false,
            path,
            reason: `Lock error: ${(error as Error).message}`,
          });
        });
    });
  }

  /**
   * Internal implementation of auto-configure logic.
   * Must only be called through the lock in maybeAutoConfigurePath.
   */
  private async doAutoConfigurePath(
    path: Path,
    context: Context
  ): Promise<AutoDiscoveryResult> {
    this.app.debug(
      `[AutoDiscovery] doAutoConfigurePath called for ${path} in context ${context}`
    );
    const config = this.pluginConfig.autoDiscovery;

    // Check if auto-discovery is enabled
    if (!config?.enabled) {
      this.app.debug(`[AutoDiscovery] Auto-discovery is disabled in config`);
      return {
        configured: false,
        path,
        reason: 'Auto-discovery is disabled',
      };
    }

    this.app.debug(
      `[AutoDiscovery] Config: enabled=${config.enabled}, requireLiveData=${config.requireLiveData}, maxPaths=${config.maxAutoConfiguredPaths}`
    );

    // Check if path is already configured
    const existingPath = this.currentPaths.find(p => p.path === path);
    if (existingPath) {
      return {
        configured: false,
        path,
        reason: 'Path is already configured',
      };
    }

    // Check max limit
    const maxPaths = config.maxAutoConfiguredPaths ?? 100;
    if (this.configuredCount >= maxPaths) {
      this.app.debug(
        `[AutoDiscovery] Limit reached (${this.configuredCount}/${maxPaths}), skipping ${path}`
      );
      return {
        configured: false,
        path,
        reason: `Maximum auto-configured paths limit reached (${maxPaths})`,
      };
    }

    // Check exclude patterns
    if (config.excludePatterns && config.excludePatterns.length > 0) {
      const isExcluded = this.matchesPattern(path, config.excludePatterns);
      if (isExcluded) {
        this.app.debug(`[AutoDiscovery] Path ${path} matches exclude pattern`);
        return {
          configured: false,
          path,
          reason: 'Path matches exclude pattern',
        };
      }
    }

    // Check include patterns (if specified, path must match at least one)
    if (config.includePatterns && config.includePatterns.length > 0) {
      const isIncluded = this.matchesPattern(path, config.includePatterns);
      if (!isIncluded) {
        this.app.debug(
          `[AutoDiscovery] Path ${path} does not match any include pattern`
        );
        return {
          configured: false,
          path,
          reason: 'Path does not match include patterns',
        };
      }
    }

    // Check if live data exists (if required)
    if (config.requireLiveData) {
      const hasLiveData = this.checkPathHasLiveData(path, context);
      if (!hasLiveData) {
        this.app.debug(
          `[AutoDiscovery] Path ${path} has no live data in SignalK`
        );
        return {
          configured: false,
          path,
          reason: 'Path has no live data in SignalK',
        };
      }
    }

    // All checks passed - configure the path
    return await this.configurePath(path, context);
  }

  /**
   * Check if a path has live data in SignalK
   */
  private checkPathHasLiveData(path: Path, context: Context): boolean {
    try {
      // For vessels.self context, use getSelfPath
      if (
        context === 'vessels.self' ||
        context === `vessels.${this.app.selfId}`
      ) {
        const value = this.app.getSelfPath(path);
        this.app.debug(
          `[AutoDiscovery] getSelfPath(${path}) returned: ${JSON.stringify(value)}`
        );
        return value !== undefined && value !== null;
      }

      // For other contexts, try to get the value from the full model
      // Note: This may not be available depending on SignalK version
      const fullPath = `${context}.${path}`;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const model = (this.app as any).getPath?.(fullPath);
      this.app.debug(
        `[AutoDiscovery] getPath(${fullPath}) returned: ${JSON.stringify(model)}`
      );
      return model !== undefined && model !== null;
    } catch (error) {
      this.app.debug(
        `[AutoDiscovery] Error checking live data for ${path}: ${error}`
      );
      return false;
    }
  }

  /**
   * Check if a path matches any of the given glob patterns
   */
  private matchesPattern(path: string, patterns: string[]): boolean {
    for (const pattern of patterns) {
      if (minimatch(path, pattern, { dot: true })) {
        return true;
      }
    }
    return false;
  }

  /**
   * Configure a path for recording and update subscriptions
   */
  private async configurePath(
    path: Path,
    context: Context
  ): Promise<AutoDiscoveryResult> {
    try {
      // Create the new path configuration
      const newPathConfig: PathConfig = {
        path,
        name: this.generatePathName(path),
        enabled: true,
        regimen: undefined, // Always enabled, no regimen dependency
        source: undefined, // Accept all sources
        context: context,
        autoDiscovered: true,
      };

      // Add to current paths (this.currentPaths is the source of truth)
      this.currentPaths.push(newPathConfig);
      this.configuredCount++;

      // Load current webapp config to get commands, then save with updated paths
      // Don't push to webAppConfig.paths - use this.currentPaths as the authoritative source
      const webAppConfig = loadWebAppConfig(this.app);
      saveWebAppConfig(this.currentPaths, webAppConfig.commands, this.app);

      // Update data subscriptions to include the new path
      updateDataSubscriptions(
        this.currentPaths,
        this.state,
        this.pluginConfig,
        this.app
      );

      this.app.debug(
        `[AutoDiscovery] Successfully auto-configured path: ${path}`
      );

      return {
        configured: true,
        path,
        reason: 'Path auto-configured successfully',
      };
    } catch (error) {
      this.app.error(
        `[AutoDiscovery] Failed to configure path ${path}: ${error}`
      );
      return {
        configured: false,
        path,
        reason: `Configuration failed: ${(error as Error).message}`,
      };
    }
  }

  /**
   * Generate a human-readable name from a SignalK path
   */
  private generatePathName(path: string): string {
    // Convert "navigation.speedOverGround" to "Navigation Speed Over Ground"
    const parts = path.split('.');
    const words = parts.map(part => {
      // Split camelCase
      const spaced = part.replace(/([a-z])([A-Z])/g, '$1 $2');
      // Capitalize first letter
      return spaced.charAt(0).toUpperCase() + spaced.slice(1);
    });
    return `[Auto] ${words.join(' ')}`;
  }
}
