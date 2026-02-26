# Auto-Regimen on Missing Data Plan

> **Status**: Planning
> **Created**: 2026-02-26
> **Goal**: Automatically create and activate a regimen for any SignalK path when data is requested but no recording configuration exists

---

## Problem Statement

### Current Behavior

When a client requests historical data for a path that isn't being recorded:
1. HistoryAPI queries parquet files
2. Returns empty results (no data exists)
3. User must manually:
   - Go to plugin UI
   - Add path configuration
   - Assign to a regimen (or enable directly)
   - Wait for data to accumulate

### Pain Points

| Issue | Impact |
|-------|--------|
| Manual setup required | Poor developer/integrator experience |
| No feedback on why data is missing | Confusion about whether path exists |
| Must know paths ahead of time | Can't easily discover new data sources |
| Multiple steps to enable recording | Friction for new users |

### User Story

> "As a SignalK user, when I request `navigation.courseOverGroundTrue` and no data exists, I want the system to automatically start recording that path so data is available on my next query."

---

## Proposed Solution

### Auto-Regimen Feature

When a HistoryAPI request targets a path with no data:
1. Check if the path exists in SignalK (live data available)
2. If path exists but isn't configured → auto-create configuration
3. Assign path to a special `auto-discovered` regimen
4. Activate the regimen immediately
5. Return response indicating data will be available soon

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ HistoryAPI      │────▶│ Check Path       │────▶│ Auto-Configure  │
│ Request         │     │ Configuration    │     │ If Missing      │
└─────────────────┘     └──────────────────┘     └─────────────────┘
        │                       │                        │
   path not in                path not               create config
   parquet files            configured               activate regimen
                                                     start recording
```

### Response Enhancement

Current response for missing data:
```json
{
  "context": "vessels.self",
  "data": [],
  "values": [...]
}
```

Enhanced response:
```json
{
  "context": "vessels.self",
  "data": [],
  "values": [...],
  "meta": {
    "autoConfigured": true,
    "paths": ["navigation.courseOverGroundTrue"],
    "regimen": "auto-discovered",
    "message": "Path has been auto-configured for recording. Data will be available shortly."
  }
}
```

---

## Configuration Options

### Plugin Config Schema

Add to `PluginConfig` in `src/types.ts`:

```typescript
export interface PluginConfig {
  // ... existing config ...

  // Auto-discovery settings
  autoDiscovery?: {
    enabled: boolean;                    // Master switch (default: false)
    regimenName: string;                 // Name for auto-created regimen (default: "auto-discovered")
    activateImmediately: boolean;        // Start recording right away (default: true)
    excludePatterns?: string[];          // Paths to never auto-configure (e.g., ["electrical.*"])
    includePatterns?: string[];          // Restrict to matching paths only
    maxAutoConfiguredPaths?: number;     // Prevent runaway configuration (default: 100)
    requireLiveData: boolean;            // Only configure if path has current SignalK data (default: true)
  };
}
```

### Default Configuration

```typescript
const DEFAULT_AUTO_DISCOVERY: AutoDiscoveryConfig = {
  enabled: false,  // Opt-in feature
  regimenName: 'auto-discovered',
  activateImmediately: true,
  excludePatterns: [
    'design.*',           // Static vessel data
    'communication.*',    // Comms (likely transient)
    'notifications.*',    // Notifications (managed separately)
  ],
  includePatterns: [],    // Empty = allow all (except excludes)
  maxAutoConfiguredPaths: 100,
  requireLiveData: true,
};
```

---

## Phase 1: Core Auto-Discovery Logic

### 1.1 Create Auto-Discovery Service

Create: `src/services/auto-discovery.ts`

```typescript
import { Path, ServerAPI, Context } from '@signalk/server-api';
import { PathConfig, PluginConfig, PluginState } from '../types';
import { loadWebAppConfig, saveWebAppConfig } from '../index';
import { updateDataSubscriptions } from '../data-handler';
import { minimatch } from 'minimatch';

export interface AutoDiscoveryResult {
  configured: boolean;
  path: Path;
  regimen: string;
  reason?: string;  // Why it was/wasn't configured
}

export class AutoDiscoveryService {
  private configuredCount = 0;
  private app: ServerAPI;
  private config: PluginConfig;
  private state: PluginState;

  constructor(app: ServerAPI, config: PluginConfig, state: PluginState) {
    this.app = app;
    this.config = config;
    this.state = state;
  }

  /**
   * Check if a path should be auto-configured and configure it if so
   */
  async maybeAutoConfigurePath(
    path: Path,
    context: Context
  ): Promise<AutoDiscoveryResult> {
    const autoConfig = this.config.autoDiscovery;

    // Feature disabled
    if (!autoConfig?.enabled) {
      return { configured: false, path, regimen: '', reason: 'Auto-discovery disabled' };
    }

    // Check if path is already configured
    const webAppConfig = loadWebAppConfig(this.app);
    const existingPath = webAppConfig.paths.find(
      p => p.path === path && (p.context === context || !p.context)
    );

    if (existingPath) {
      return { configured: false, path, regimen: '', reason: 'Path already configured' };
    }

    // Check max limit
    if (this.configuredCount >= (autoConfig.maxAutoConfiguredPaths || 100)) {
      return { configured: false, path, regimen: '', reason: 'Max auto-configured paths reached' };
    }

    // Check exclude patterns
    if (autoConfig.excludePatterns?.some(pattern => minimatch(path, pattern))) {
      return { configured: false, path, regimen: '', reason: 'Path matches exclude pattern' };
    }

    // Check include patterns (if specified, path must match at least one)
    if (autoConfig.includePatterns?.length > 0) {
      const matchesInclude = autoConfig.includePatterns.some(
        pattern => minimatch(path, pattern)
      );
      if (!matchesInclude) {
        return { configured: false, path, regimen: '', reason: 'Path does not match include patterns' };
      }
    }

    // Check if path has live data in SignalK
    if (autoConfig.requireLiveData) {
      const hasLiveData = this.checkPathHasLiveData(path, context);
      if (!hasLiveData) {
        return { configured: false, path, regimen: '', reason: 'No live data available for path' };
      }
    }

    // Auto-configure the path
    return this.configurePath(path, context, autoConfig.regimenName || 'auto-discovered');
  }

  /**
   * Check if a path currently has data in SignalK
   */
  private checkPathHasLiveData(path: Path, context: Context): boolean {
    try {
      // Use SignalK API to check if path exists
      const pathParts = path.split('.');
      let current = this.app.getSelfPath('');

      // For non-self contexts, get from full path
      if (context && !context.includes('self')) {
        current = this.app.getPath(context);
      }

      // Navigate to the path
      for (const part of pathParts) {
        if (!current || typeof current !== 'object') return false;
        current = current[part];
      }

      // Path exists if we found a value
      return current !== undefined;
    } catch {
      return false;
    }
  }

  /**
   * Create path configuration and activate regimen
   */
  private configurePath(
    path: Path,
    context: Context,
    regimenName: string
  ): AutoDiscoveryResult {
    const webAppConfig = loadWebAppConfig(this.app);
    const currentPaths = webAppConfig.paths;
    const currentCommands = webAppConfig.commands;

    // Create new path configuration
    const newPathConfig: PathConfig = {
      path,
      name: `Auto: ${path}`,
      enabled: false,  // Controlled by regimen
      regimen: regimenName,
      context: context,
    };

    // Add to configuration
    currentPaths.push(newPathConfig);

    // Save configuration
    saveWebAppConfig(currentPaths, currentCommands, this.app);

    // Activate the regimen if configured to do so
    if (this.config.autoDiscovery?.activateImmediately !== false) {
      this.state.activeRegimens.add(regimenName);

      // Update subscriptions to start recording
      updateDataSubscriptions(currentPaths, this.state, this.config, this.app);
    }

    this.configuredCount++;

    console.log(`[AutoDiscovery] Auto-configured path: ${path} (regimen: ${regimenName})`);

    return {
      configured: true,
      path,
      regimen: regimenName,
    };
  }

  /**
   * Get count of auto-configured paths
   */
  getConfiguredCount(): number {
    return this.configuredCount;
  }

  /**
   * Reset counter (call on plugin restart)
   */
  resetCounter(): void {
    this.configuredCount = 0;
  }
}
```

### 1.2 Integrate with HistoryAPI

Location: `src/HistoryAPI.ts`

Modify the values endpoint to check for auto-discovery:

```typescript
// In getNumericValues() or the /values endpoint handler

async function handleValuesRequest(
  req: FromToContextRequest,
  res: Response,
  autoDiscoveryService: AutoDiscoveryService,
  // ... other params
): Promise<void> {
  const { from, to, context } = getRequestParams(req, selfId);
  const pathSpecs = parsePathSpecs(req.query.paths);

  // Execute query as normal
  const result = await getNumericValues(context, from, to, ...);

  // Track which paths had no data and were auto-configured
  const autoConfiguredPaths: AutoDiscoveryResult[] = [];

  // For each path that returned no data, consider auto-configuration
  for (const pathSpec of pathSpecs) {
    const hasData = result.data.some(row => {
      const pathIndex = result.values.findIndex(v => v.path === pathSpec.path);
      return row[pathIndex + 1] !== null;
    });

    if (!hasData) {
      const autoResult = await autoDiscoveryService.maybeAutoConfigurePath(
        pathSpec.path,
        context
      );
      if (autoResult.configured) {
        autoConfiguredPaths.push(autoResult);
      }
    }
  }

  // Add metadata to response if paths were auto-configured
  if (autoConfiguredPaths.length > 0) {
    (result as any).meta = {
      autoConfigured: true,
      paths: autoConfiguredPaths.map(r => r.path),
      regimen: autoConfiguredPaths[0].regimen,
      message: `${autoConfiguredPaths.length} path(s) auto-configured for recording. Data will be available shortly.`,
    };
  }

  res.json(result);
}
```

### 1.3 Initialize Service on Plugin Start

Location: `src/index.ts`

```typescript
import { AutoDiscoveryService } from './services/auto-discovery';

// In plugin.start()
const autoDiscoveryService = new AutoDiscoveryService(app, config, state);
state.autoDiscoveryService = autoDiscoveryService;

// Pass to HistoryAPI initialization
initHistoryAPI(router, dataDir, app, selfId, config, autoDiscoveryService);
```

---

## Phase 2: API Endpoint for Manual Activation

### 2.1 Add Regimen Control Endpoint

Add to `src/api-routes.ts`:

```typescript
// GET /api/regimens - List all regimens and their status
router.get('/api/regimens', (req, res) => {
  const webAppConfig = loadWebAppConfig(app);

  // Extract unique regimens from path configs
  const regimens = new Map<string, { active: boolean; pathCount: number }>();

  for (const pathConfig of webAppConfig.paths) {
    if (pathConfig.regimen) {
      const existing = regimens.get(pathConfig.regimen) || { active: false, pathCount: 0 };
      existing.pathCount++;
      existing.active = state.activeRegimens.has(pathConfig.regimen);
      regimens.set(pathConfig.regimen, existing);
    }
  }

  res.json({
    regimens: Array.from(regimens.entries()).map(([name, info]) => ({
      name,
      active: info.active,
      pathCount: info.pathCount,
    })),
  });
});

// POST /api/regimens/:name/activate
router.post('/api/regimens/:name/activate', (req, res) => {
  const { name } = req.params;

  state.activeRegimens.add(name);

  // Update subscriptions
  const webAppConfig = loadWebAppConfig(app);
  updateDataSubscriptions(webAppConfig.paths, state, config, app);

  res.json({ success: true, regimen: name, active: true });
});

// POST /api/regimens/:name/deactivate
router.post('/api/regimens/:name/deactivate', (req, res) => {
  const { name } = req.params;

  state.activeRegimens.delete(name);

  // Update subscriptions
  const webAppConfig = loadWebAppConfig(app);
  updateDataSubscriptions(webAppConfig.paths, state, config, app);

  res.json({ success: true, regimen: name, active: false });
});
```

---

## Phase 3: UI Integration

### 3.1 Auto-Discovery Settings Panel

Add to `public/index.html`:

```html
<div id="auto-discovery" class="settings-panel">
  <h3>Auto-Discovery Settings</h3>

  <div class="form-group">
    <label>
      <input type="checkbox" id="autoDiscoveryEnabled" />
      Enable auto-discovery of requested paths
    </label>
    <p class="help-text">
      When enabled, paths requested via the History API that don't exist
      will be automatically configured for recording.
    </p>
  </div>

  <div class="form-group">
    <label for="autoRegimenName">Auto-discovery regimen name</label>
    <input type="text" id="autoRegimenName" value="auto-discovered" />
  </div>

  <div class="form-group">
    <label>
      <input type="checkbox" id="autoActivateImmediately" checked />
      Activate regimen immediately
    </label>
  </div>

  <div class="form-group">
    <label for="autoExcludePatterns">Exclude patterns (one per line)</label>
    <textarea id="autoExcludePatterns" rows="4">design.*
communication.*
notifications.*</textarea>
  </div>

  <div class="form-group">
    <label for="autoMaxPaths">Maximum auto-configured paths</label>
    <input type="number" id="autoMaxPaths" value="100" min="1" max="1000" />
  </div>

  <button onclick="saveAutoDiscoverySettings()">Save Settings</button>
</div>
```

### 3.2 Regimen Management Panel

```html
<div id="regimens" class="tab-panel">
  <h2>Regimen Management</h2>

  <div id="regimenList">
    <!-- Populated dynamically -->
  </div>

  <script>
    async function loadRegimens() {
      const resp = await fetch('/plugins/signalk-parquet/api/regimens');
      const { regimens } = await resp.json();

      const list = document.getElementById('regimenList');
      list.innerHTML = regimens.map(r => `
        <div class="regimen-item ${r.active ? 'active' : ''}">
          <span class="regimen-name">${r.name}</span>
          <span class="regimen-paths">${r.pathCount} paths</span>
          <button onclick="toggleRegimen('${r.name}', ${!r.active})">
            ${r.active ? 'Deactivate' : 'Activate'}
          </button>
        </div>
      `).join('');
    }

    async function toggleRegimen(name, activate) {
      const action = activate ? 'activate' : 'deactivate';
      await fetch(`/plugins/signalk-parquet/api/regimens/${name}/${action}`, {
        method: 'POST',
      });
      loadRegimens();
    }
  </script>
</div>
```

---

## Phase 4: Persistence and Recovery

### 4.1 Persist Active Regimens

Active regimens should survive plugin restarts.

Location: `src/index.ts` / `src/types.ts`

```typescript
// In PluginState
interface PluginState {
  // ... existing state ...
  activeRegimens: Set<string>;  // Already exists
}

// Save active regimens to webapp config
interface WebAppConfig {
  paths: PathConfig[];
  commands: CommandConfig[];
  activeRegimens?: string[];  // NEW: persist active regimens
}

// On plugin start, restore active regimens
const webAppConfig = loadWebAppConfig(app);
if (webAppConfig.activeRegimens) {
  for (const regimen of webAppConfig.activeRegimens) {
    state.activeRegimens.add(regimen);
  }
}

// When regimens change, persist them
function saveActiveRegimens(state: PluginState, app: ServerAPI): void {
  const webAppConfig = loadWebAppConfig(app);
  webAppConfig.activeRegimens = Array.from(state.activeRegimens);
  saveWebAppConfig(webAppConfig.paths, webAppConfig.commands, app);
}
```

### 4.2 Auto-Discovery State Recovery

```typescript
// On plugin start, count existing auto-discovered paths
function recoverAutoDiscoveryState(
  autoDiscoveryService: AutoDiscoveryService,
  app: ServerAPI
): void {
  const webAppConfig = loadWebAppConfig(app);
  const autoRegimenName = config.autoDiscovery?.regimenName || 'auto-discovered';

  const autoConfiguredCount = webAppConfig.paths.filter(
    p => p.regimen === autoRegimenName
  ).length;

  // Set the counter so we respect maxAutoConfiguredPaths
  for (let i = 0; i < autoConfiguredCount; i++) {
    autoDiscoveryService.incrementCounter();
  }

  console.log(`[AutoDiscovery] Recovered ${autoConfiguredCount} auto-configured paths`);
}
```

---

## Phase 5: Bulk Discovery Mode

### 5.1 Discover All Available Paths

Add an endpoint to discover and configure all SignalK paths at once.

```typescript
// POST /api/auto-discovery/discover-all
router.post('/api/auto-discovery/discover-all', async (req, res) => {
  const { regimenName = 'bulk-discovered' } = req.body;

  // Get all available paths from SignalK
  const allPaths = app.getAvailablePaths?.() || [];

  const results: AutoDiscoveryResult[] = [];

  for (const path of allPaths) {
    const result = await autoDiscoveryService.maybeAutoConfigurePath(
      path as Path,
      'vessels.self' as Context
    );
    results.push(result);
  }

  const configured = results.filter(r => r.configured);

  res.json({
    success: true,
    totalPaths: allPaths.length,
    configuredCount: configured.length,
    configured: configured.map(r => r.path),
    skipped: results.filter(r => !r.configured).map(r => ({
      path: r.path,
      reason: r.reason,
    })),
  });
});
```

---

## Implementation Checklist

### Phase 1 (Core Logic)
- [ ] Add `autoDiscovery` to `PluginConfig` type
- [ ] Add dependency: `npm install minimatch`
- [ ] Create `src/services/auto-discovery.ts`
- [ ] Add `AutoDiscoveryService` initialization to plugin start
- [ ] Integrate auto-discovery into HistoryAPI values endpoint
- [ ] Add `meta` field to `DataResult` type for auto-discovery info

### Phase 2 (API Endpoints)
- [ ] Add `GET /api/regimens` endpoint
- [ ] Add `POST /api/regimens/:name/activate` endpoint
- [ ] Add `POST /api/regimens/:name/deactivate` endpoint
- [ ] Add `POST /api/auto-discovery/discover-all` endpoint

### Phase 3 (UI)
- [ ] Add auto-discovery settings panel to plugin UI
- [ ] Add regimen management tab
- [ ] Add "Discover All" button for bulk configuration

### Phase 4 (Persistence)
- [ ] Add `activeRegimens` to `WebAppConfig`
- [ ] Persist active regimens on change
- [ ] Restore active regimens on plugin start
- [ ] Recover auto-discovery counter on restart

### Phase 5 (Testing)
- [ ] Test: Request missing path → auto-configured
- [ ] Test: Request excluded path → not configured
- [ ] Test: Reach max limit → stops configuring
- [ ] Test: Regimen activation persists across restart
- [ ] Test: Bulk discovery configures expected paths

---

## Security Considerations

| Risk | Mitigation |
|------|------------|
| Runaway path creation | `maxAutoConfiguredPaths` limit |
| Sensitive path exposure | `excludePatterns` for credentials, tokens |
| Disk space exhaustion | Combine with retention policies |
| DoS via crafted requests | `requireLiveData` ensures only real paths |

---

## Configuration Examples

### Minimal (Default Off)
```json
{
  "autoDiscovery": {
    "enabled": false
  }
}
```

### Navigation Only
```json
{
  "autoDiscovery": {
    "enabled": true,
    "regimenName": "nav-data",
    "includePatterns": ["navigation.*"],
    "maxAutoConfiguredPaths": 20
  }
}
```

### Record Everything
```json
{
  "autoDiscovery": {
    "enabled": true,
    "regimenName": "all-data",
    "excludePatterns": [],
    "maxAutoConfiguredPaths": 500
  }
}
```

---

## References

- [SignalK Path Specification](https://signalk.org/specification/1.7.0/doc/)
- [Minimatch Glob Patterns](https://github.com/isaacs/minimatch)
- Existing implementation: `src/data-handler.ts:shouldSubscribeToPath()`
