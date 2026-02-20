# Hive-Style Partitioning, S3 Storage & Aggregation Tiers Plan

> **Status**: Planning
> **Created**: 2026-02-19
> **Goal**: Restructure parquet storage with Hive partitioning, add S3 support, and implement aggregation tiers for optimal query performance

---

## Current State

```
data/
  vessels/
    urn_mrn_imo_mmsi_368396230/
      navigation/
        position/
          signalk_data_2025-08-20T1116.parquet
```

**Problems**: Not Hive-style, no partition pruning, no aggregation tiers, local only.

---

## Target Structure

```
data/
  tier=raw/
    context=vessels.urn_mrn_imo_mmsi_368396230/
      path=navigation.position/
        year=2025/
          day=232/
            *.parquet
  tier=5s/
    ...
  tier=60s/
    ...
  tier=1h/
    ...
```

### Aggregation Tiers

| Tier | Resolution | Points/Day | Retention | Use Case |
|------|------------|------------|-----------|----------|
| `raw` | 1s | 86,400 | 7 days | Debugging, precise replay |
| `5s` | 5s | 17,280 | 30 days | Recent track visualization |
| `60s` | 1min | 1,440 | 2 years | Monthly charts |
| `1h` | 1hr | 24 | Forever | Yearly overview |

---

## Phase 1-4: See Previous Sections

(Hive paths, aggregation pipeline, S3 integration, retention policies)

---

## Phase 5: Migration Tool (Integrated into Plugin UI)

Migration is built into the plugin's web interface, following the same pattern as validation/repair jobs.

### 5.1 Backend: Migration Service

Create `src/services/migration-service.ts`:

```typescript
export interface MigrationProgress {
  jobId: string;
  status: 'scanning' | 'running' | 'completed' | 'cancelled' | 'error';
  phase: 'scan' | 'migrate' | 'verify' | 'cleanup';
  processed: number;
  total: number;
  percent: number;
  bytesTransferred: number;
  currentFile?: string;
  cancelRequested?: boolean;
  error?: string;
}

export class MigrationService {
  async scanSource(source: StorageLocation): Promise<ScanResult>;
  async migrate(source, target, tier, progress, onProgress): Promise<void>;
}
```

### 5.2 Backend: API Routes

Add to `src/api-routes.ts` (same pattern as `validationJobs`):

```typescript
const migrationJobs = new Map<string, MigrationProgress>();

// POST /api/migrate/scan - Scan source and return stats
router.post('/api/migrate/scan', async (req, res) => {
  const result = await migrationService.scanSource(req.body);
  res.json(result);
});

// POST /api/migrate - Start migration job
router.post('/api/migrate', async (req, res) => {
  const jobId = `mig_${Date.now()}`;
  migrationJobs.set(jobId, { jobId, status: 'scanning', ... });
  runMigrationAsync(jobId, req.body);
  res.json({ success: true, jobId });
});

// GET /api/migrate/progress/:jobId
router.get('/api/migrate/progress/:jobId', (req, res) => {
  res.json(migrationJobs.get(req.params.jobId));
});

// POST /api/migrate/cancel/:jobId
router.post('/api/migrate/cancel/:jobId', (req, res) => {
  const job = migrationJobs.get(req.params.jobId);
  job.cancelRequested = true;
  res.json({ success: true });
});
```

### 5.3 Frontend: Migration Tab

Add to `public/index.html`:

```html
<div id="migration" class="tab-panel">
  <h2>Migrate to Hive Structure</h2>

  <div class="form-group">
    <label>Source</label>
    <select id="migrationSourceType">
      <option value="local">Local</option>
      <option value="s3">S3</option>
    </select>
    <input id="migrationSourcePath" placeholder="./data" />
  </div>

  <div class="form-group">
    <label>Target</label>
    <select id="migrationTargetType">...</select>
    <input id="migrationTargetPath" placeholder="./data-hive" />
  </div>

  <div class="form-group">
    <label>Tier</label>
    <select id="migrationTier">
      <option value="raw">Raw</option>
      <option value="5s">5s</option>
      <option value="60s">60s</option>
      <option value="1h">1h</option>
    </select>
  </div>

  <button onclick="scanMigrationSource()">Scan</button>
  <button onclick="startMigration()">Migrate</button>
  <button onclick="cancelMigration()">Cancel</button>

  <div id="migrationProgress">
    <div class="progress-bar">...</div>
    <p id="migrationStatus"></p>
  </div>
</div>
```

### 5.4 Frontend: JavaScript

Create `public/js/migration.js`:

```javascript
let currentJobId = null;

export async function scanMigrationSource() {
  const resp = await fetch('/plugins/signalk-parquet/api/migrate/scan', {
    method: 'POST',
    body: JSON.stringify({ sourceType, sourcePath }),
  });
  const result = await resp.json();
  showScanResults(result);
}

export async function startMigration() {
  const resp = await fetch('/plugins/signalk-parquet/api/migrate', {
    method: 'POST',
    body: JSON.stringify({ source, target, tier }),
  });
  currentJobId = (await resp.json()).jobId;
  pollProgress();
}

function pollProgress() {
  setInterval(async () => {
    const resp = await fetch(`/api/migrate/progress/${currentJobId}`);
    const progress = await resp.json();
    updateProgressUI(progress);
    if (progress.status === 'completed') clearInterval();
  }, 1000);
}

export async function cancelMigration() {
  await fetch(`/api/migrate/cancel/${currentJobId}`, { method: 'POST' });
}
```

---

## Implementation Checklist

### Phase 5: Migration (Plugin UI)
- [ ] Create `src/services/migration-service.ts`
- [ ] Add migration API routes to `api-routes.ts`
- [ ] Add migration job tracking (same pattern as validation)
- [ ] Create migration tab in `public/index.html`
- [ ] Create `public/js/migration.js`
- [ ] Register migration functions in `main.js`
- [ ] Test local → local migration
- [ ] Test local → S3 migration
- [ ] Test S3 → S3 migration

---

## Usage Flow (User Perspective)

1. Open SignalK plugin web UI
2. Go to "Migration" tab
3. Select source (current data directory)
4. Select target (new Hive directory or S3)
5. Click "Scan" to see file count/size
6. Click "Migrate" to start
7. Watch progress bar
8. Update plugin config to use new directory
9. Optionally delete old data
