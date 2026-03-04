# SignalK App Store Platform Compatibility Filtering

> **Status**: Proposal
> **Created**: 2026-03-03
> **Target**: SignalK Server
> **Repository**: https://github.com/SignalK/signalk-server

---

## Problem Statement

Plugins with native dependencies (like DuckDB) fail to install on incompatible platforms. Users on Venus OS (32-bit ARM) see confusing npm errors when trying to install signalk-parquet:

```
Error loading duckdb native binding: unsupported arch 'arm' for platform 'linux'
```

Currently:
- Users discover incompatibility AFTER attempting install
- Error messages are cryptic (native binding failures)
- No way to know beforehand if a plugin will work

---

## Proposed Solution

Add platform compatibility filtering to the SignalK App Store:

1. **Detect system capabilities** at server startup
2. **Read plugin requirements** from package.json `os`/`cpu` fields
3. **Filter or warn** in the App Store UI before install

---

## Implementation Plan

### Phase 1: System Detection Module

**File**: `packages/server-api/src/platform.ts` (new file)

```typescript
export interface PlatformInfo {
  arch: 'x64' | 'arm64' | 'arm' | string;
  platform: 'linux' | 'darwin' | 'win32' | string;
  isVenusOS: boolean;
  nodeVersion: string;
  bits: 32 | 64;
}

export function detectPlatform(): PlatformInfo {
  const arch = process.arch;
  const platform = process.platform;

  // Detect 32-bit vs 64-bit userspace
  let bits: 32 | 64 = 64;
  if (arch === 'arm') {
    bits = 32;
  } else if (arch === 'arm64' && platform === 'linux') {
    // Check if running 32-bit userspace on 64-bit kernel (Venus OS)
    try {
      const { execSync } = require('child_process');
      const result = execSync('getconf LONG_BIT', { encoding: 'utf8' }).trim();
      bits = result === '32' ? 32 : 64;
    } catch {
      // Assume 64-bit if we can't determine
    }
  }

  // Detect Venus OS
  const isVenusOS = detectVenusOS();

  return {
    arch,
    platform,
    isVenusOS,
    nodeVersion: process.version,
    bits
  };
}

function detectVenusOS(): boolean {
  try {
    const fs = require('fs');
    // Venus OS has specific markers
    return fs.existsSync('/opt/victronenergy') ||
           fs.existsSync('/etc/venus');
  } catch {
    return false;
  }
}
```

### Phase 2: Expose Platform Info via API

**File**: `packages/server-admin-ui/src/api/index.ts`

Add endpoint to expose platform info:

```typescript
router.get('/api/platform', (req, res) => {
  res.json(detectPlatform());
});
```

This allows the Admin UI to know the current platform.

### Phase 3: Plugin Compatibility Checking

**File**: `packages/server-admin-ui/src/api/appstore.ts`

Modify the app store plugin fetching to include compatibility:

```typescript
interface PluginInfo {
  name: string;
  version: string;
  description: string;
  // ... existing fields

  // New fields
  compatibility: {
    os?: string[];      // From package.json "os" field
    cpu?: string[];     // From package.json "cpu" field
    engines?: {         // From package.json "engines" field
      node?: string;
    };
  };
  isCompatible: boolean;
  incompatibilityReason?: string;
}

function checkCompatibility(
  plugin: PluginInfo,
  platform: PlatformInfo
): { compatible: boolean; reason?: string } {

  // Check CPU architecture
  if (plugin.compatibility.cpu) {
    const cpuList = plugin.compatibility.cpu;

    // Check for exclusions (e.g., "!arm")
    for (const cpu of cpuList) {
      if (cpu.startsWith('!') && cpu.slice(1) === platform.arch) {
        return {
          compatible: false,
          reason: `Not compatible with ${platform.arch} architecture`
        };
      }
    }

    // Check for inclusions
    const allowedCpus = cpuList.filter(c => !c.startsWith('!'));
    if (allowedCpus.length > 0 && !allowedCpus.includes(platform.arch)) {
      // Special case: arm64 plugin on 32-bit userspace
      if (allowedCpus.includes('arm64') && platform.arch === 'arm') {
        return {
          compatible: false,
          reason: 'Requires 64-bit OS (32-bit ARM not supported)'
        };
      }
      return {
        compatible: false,
        reason: `Requires ${allowedCpus.join(' or ')} architecture`
      };
    }
  }

  // Check OS
  if (plugin.compatibility.os) {
    const osList = plugin.compatibility.os;
    if (!osList.includes(platform.platform)) {
      return {
        compatible: false,
        reason: `Not compatible with ${platform.platform}`
      };
    }
  }

  // Check Node.js version
  if (plugin.compatibility.engines?.node) {
    const semver = require('semver');
    if (!semver.satisfies(process.version, plugin.compatibility.engines.node)) {
      return {
        compatible: false,
        reason: `Requires Node.js ${plugin.compatibility.engines.node}`
      };
    }
  }

  return { compatible: true };
}
```

### Phase 4: NPM Registry Integration

The SignalK App Store fetches plugin info from npm. We need to ensure `os`, `cpu`, and `engines` fields are retrieved.

**Modify npm fetch to include these fields**:

```typescript
async function fetchPluginInfo(packageName: string): Promise<PluginInfo> {
  const response = await fetch(`https://registry.npmjs.org/${packageName}/latest`);
  const data = await response.json();

  return {
    name: data.name,
    version: data.version,
    description: data.description,
    // ... existing mappings

    // Add compatibility fields
    compatibility: {
      os: data.os,
      cpu: data.cpu,
      engines: data.engines
    }
  };
}
```

### Phase 5: Admin UI Changes

**File**: `packages/server-admin-ui/src/views/Appstore.vue` (or equivalent)

#### 5.1 Add Filter Toggle

```vue
<template>
  <div class="appstore">
    <div class="filters">
      <label>
        <input type="checkbox" v-model="hideIncompatible" />
        Hide incompatible plugins
      </label>
      <span class="platform-info">
        Platform: {{ platform.arch }} / {{ platform.platform }}
        <span v-if="platform.isVenusOS">(Venus OS)</span>
      </span>
    </div>

    <div class="plugin-list">
      <plugin-card
        v-for="plugin in filteredPlugins"
        :key="plugin.name"
        :plugin="plugin"
        :compatible="plugin.isCompatible"
      />
    </div>
  </div>
</template>
```

#### 5.2 Plugin Card Styling

```vue
<template>
  <div class="plugin-card" :class="{ incompatible: !compatible }">
    <h3>{{ plugin.name }}</h3>
    <p>{{ plugin.description }}</p>

    <div v-if="!compatible" class="compatibility-warning">
      <icon name="warning" />
      {{ plugin.incompatibilityReason }}
    </div>

    <button
      :disabled="!compatible"
      @click="install"
    >
      {{ compatible ? 'Install' : 'Not Compatible' }}
    </button>
  </div>
</template>

<style>
.plugin-card.incompatible {
  opacity: 0.6;
  background: #fff3cd;
}

.compatibility-warning {
  color: #856404;
  background: #fff3cd;
  padding: 8px;
  border-radius: 4px;
  margin: 10px 0;
}
</style>
```

### Phase 6: Venus OS Special Handling

Since Venus OS is a common case, add special detection and messaging:

```typescript
function getVenusOSWarning(plugin: PluginInfo): string | null {
  if (!platform.isVenusOS) return null;

  // Check if plugin has native dependencies that won't work
  const knownIncompatible = [
    '@duckdb/node-api',
    'duckdb',
    'better-sqlite3', // Actually works, but example
  ];

  const deps = {
    ...plugin.dependencies,
    ...plugin.optionalDependencies
  };

  for (const dep of knownIncompatible) {
    if (deps[dep]) {
      return `This plugin uses ${dep} which is not compatible with Venus OS (32-bit ARM)`;
    }
  }

  return null;
}
```

---

## UI Mockup

```
┌─────────────────────────────────────────────────────────────┐
│  SignalK App Store                                          │
├─────────────────────────────────────────────────────────────┤
│  Platform: arm / linux (Venus OS)                           │
│  [✓] Hide incompatible plugins                              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ signalk-autopilot                          [Install] │   │
│  │ Autopilot integration for SignalK                    │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ signalk-parquet                    [Not Compatible] │   │
│  │ ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ │   │
│  │ ⚠️ Requires 64-bit OS (32-bit ARM not supported)     │   │
│  │ This plugin uses @duckdb/node-api which is not       │   │
│  │ compatible with Venus OS.                            │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ signalk-raspberry-pi-monitoring            [Install] │   │
│  │ Monitor Raspberry Pi system stats                    │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Plugin Author Requirements

For this system to work, plugin authors need to add compatibility fields to their `package.json`:

```json
{
  "name": "signalk-parquet",
  "version": "0.7.5",
  "os": ["darwin", "linux", "win32"],
  "cpu": ["x64", "arm64"],
  "engines": {
    "node": ">=18.0.0"
  }
}
```

**Documentation needed**: Update SignalK plugin development docs to recommend including these fields.

---

## Files to Modify in SignalK Server

| File | Change |
|------|--------|
| `packages/server-api/src/platform.ts` | New - platform detection |
| `packages/server-api/src/index.ts` | Export platform module |
| `src/api/admin.ts` | Add `/api/platform` endpoint |
| `src/api/appstore.ts` | Add compatibility checking |
| `packages/server-admin-ui/src/views/Appstore.vue` | UI filtering and warnings |
| `packages/server-admin-ui/src/components/PluginCard.vue` | Compatibility styling |

---

## Rollout Plan

1. **Phase 1**: Add platform detection (no UI changes)
2. **Phase 2**: Add `/api/platform` endpoint
3. **Phase 3**: Fetch `os`/`cpu` from npm registry
4. **Phase 4**: Add compatibility checking logic
5. **Phase 5**: Update Admin UI with filtering
6. **Phase 6**: Document for plugin authors

---

## Alternative: Simpler Approach

If full UI integration is too complex, a simpler MVP:

1. Just add a "System Info" section to the App Store page showing:
   ```
   Your system: linux/arm (32-bit)
   Some plugins may not be compatible with this architecture.
   ```

2. Let npm's built-in `os`/`cpu` checking handle the actual blocking.

3. Plugins add `"cpu": ["x64", "arm64"]` to their package.json.

This provides user awareness without requiring complex server changes.

---

## Summary

This proposal adds platform-aware filtering to the SignalK App Store, preventing users from attempting to install incompatible plugins. The key benefits:

1. **Better UX**: Users see compatibility before attempting install
2. **Clear messaging**: "Not compatible with Venus OS" instead of cryptic npm errors
3. **Leverages existing npm fields**: `os`, `cpu`, `engines` are standard
4. **Minimal plugin author burden**: Just add 2 lines to package.json

The feature would particularly help Venus OS users who currently face confusing failures with native-dependency plugins.
