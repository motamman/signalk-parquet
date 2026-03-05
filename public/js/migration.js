/**
 * Migration UI JavaScript
 *
 * Handles SQLite buffer status and data migration to Hive partitioning
 */

let currentMigrationJobId = null;
let migrationPollInterval = null;

/**
 * Refresh buffer status display
 */
async function refreshBufferStatus() {
  const container = document.getElementById('bufferStatus');
  container.innerHTML =
    '<p><span class="loading">Loading buffer status...</span></p>';

  try {
    const response = await fetch('/plugins/signalk-parquet/api/buffer/stats');
    const data = await response.json();

    if (!data.success) {
      container.innerHTML = `<p style="color: red;">Error: ${data.error}</p>`;
      return;
    }

    if (!data.enabled) {
      container.innerHTML = `
        <p style="color: #666;">
          <strong>SQLite Buffer:</strong> Not enabled<br>
          <small>The plugin is using the in-memory buffer. Enable SQLite buffer in plugin settings for crash-safe data ingestion.</small>
        </p>
      `;
      return;
    }

    const stats = data.stats;
    const exportStatus = data.exportService;

    const formatBytes = bytes => {
      if (bytes < 1024) return `${bytes} B`;
      if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
      return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
    };

    const formatTime = isoString => {
      if (!isoString) return 'Never';
      const d = new Date(isoString);
      const utcTime = d.toLocaleString([], { timeZone: 'UTC' });
      return `${d.toLocaleString()} (${utcTime} UTC)`;
    };

    const formatUtcHourAsLocal = utcHour => {
      const d = new Date();
      d.setUTCHours(utcHour, 0, 0, 0);
      const localTime = d.toLocaleTimeString([], {
        hour: 'numeric',
        minute: '2-digit',
      });
      return localTime;
    };

    container.innerHTML = `
      <p style="color: #555; margin: 0 0 15px 0; font-size: 0.95em;">
        Incoming SignalK data is buffered in SQLite, then exported to Parquet files on a daily schedule (or on restart). Exported records are retained for 48 hours as a safety net, then purged.</p>
      <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px;">
        <div style="background: white; padding: 15px; border-radius: 5px; border: 1px solid #ddd;">
          <strong style="color: #FF9800;">Pending Export</strong><br>
          <span style="font-size: 1.5em;">${stats.pendingRecords.toLocaleString()}</span>
          <small style="display: block; color: #999; margin-top: 4px;">Waiting to be written to Parquet</small>
        </div>
        <div style="background: white; padding: 15px; border-radius: 5px; border: 1px solid #ddd;">
          <strong style="color: #4CAF50;">Exported</strong><br>
          <span style="font-size: 1.5em;">${stats.exportedRecords.toLocaleString()}</span>
          <small style="display: block; color: #999; margin-top: 4px;">Written to Parquet, purged after 48h</small>
        </div>
        <div style="background: white; padding: 15px; border-radius: 5px; border: 1px solid #ddd;">
          <strong style="color: #1565C0;">Total Records</strong><br>
          <span style="font-size: 1.5em;">${stats.totalRecords.toLocaleString()}</span>
          <small style="display: block; color: #999; margin-top: 4px;">Pending + exported still in buffer</small>
        </div>
        <div style="background: white; padding: 15px; border-radius: 5px; border: 1px solid #ddd;">
          <strong style="color: #666;">Database Size</strong><br>
          <span style="font-size: 1.5em;">${formatBytes(stats.dbSizeBytes + stats.walSizeBytes)}</span>
          <small style="display: block; color: #999;">DB: ${formatBytes(stats.dbSizeBytes)}, WAL: ${formatBytes(stats.walSizeBytes)}</small>
          <small style="display: block; color: #999; margin-top: 4px;"></small>
        </div>
      </div>

      ${
        exportStatus
          ? `
      <div style="margin-top: 15px; padding: 10px; background: white; border-radius: 5px; border: 1px solid #ddd;">
        <strong>Export Service:</strong>
        <span style="color: #4CAF50;">Daily Mode</span>
        ${exportStatus.isExporting ? ' (exporting...)' : ''}
        | <strong>Last Process:</strong> ${exportStatus.lastExportTrigger ? exportStatus.lastExportTrigger.charAt(0).toUpperCase() + exportStatus.lastExportTrigger.slice(1) : 'None'}
        | <strong>Last Export:</strong> ${formatTime(exportStatus.lastExportTime)}
        <br>
        <strong>Last Batch:</strong> ${(exportStatus.lastBatchExported || 0).toLocaleString()} records
        | <strong>Schedule:</strong> Daily at ${formatUtcHourAsLocal(exportStatus.dailyExportHour)} (${exportStatus.dailyExportHour}:00 UTC)
      </div>
      `
          : ''
      }

      ${
        stats.oldestPendingTimestamp
          ? `
      <div style="margin-top: 10px; font-size: 0.9em; color: #666;">
        <strong>Oldest pending:</strong> ${formatTime(stats.oldestPendingTimestamp)}
        | <strong>Newest record:</strong> ${formatTime(stats.newestRecordTimestamp)}
      </div>
      `
          : ''
      }
    `;
  } catch (error) {
    container.innerHTML = `<p style="color: red;">Failed to load buffer status: ${error.message}</p>`;
  }
}

/**
 * Force export of pending records
 */
async function forceBufferExport() {
  const button = event.target;
  button.disabled = true;
  button.textContent = '⏳ Exporting...';

  try {
    const response = await fetch('/plugins/signalk-parquet/api/buffer/export', {
      method: 'POST',
    });
    const data = await response.json();

    if (data.success) {
      alert(
        `Export complete!\n\nRecords exported: ${data.recordsExported}\nFiles created: ${data.filesCreated.length}\nDuration: ${data.duration}ms`
      );
      refreshBufferStatus();
    } else {
      alert(`Export failed: ${data.error}`);
    }
  } catch (error) {
    alert(`Export failed: ${error.message}`);
  } finally {
    button.disabled = false;
    button.textContent = '📤 Force Export';
  }
}

/**
 * Scan for files to migrate
 */
async function scanForMigration() {
  const resultsDiv = document.getElementById('migrationScanResults');
  const contentDiv = document.getElementById('migrationScanContent');
  const startBtn = document.getElementById('startMigrationBtn');

  resultsDiv.style.display = 'block';
  contentDiv.innerHTML =
    '<p><span class="loading">Scanning files...</span></p>';
  startBtn.disabled = true;

  try {
    const response = await fetch('/plugins/signalk-parquet/api/migrate/scan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    const data = await response.json();

    if (!data.success) {
      contentDiv.innerHTML = `<p style="color: red;">Scan failed: ${data.error}</p>`;
      return;
    }

    if (data.totalFiles === 0) {
      contentDiv.innerHTML = `
        <p style="color: #666;">No flat-structure files found to migrate.</p>
        <p>Your data may already be in Hive format, or the data directory is empty.</p>
      `;
      return;
    }

    // Build path summary table
    const pathRows = data.byPath
      .map(
        p => `
      <tr>
        <td style="font-family: monospace; font-size: 0.9em;">${p.path}</td>
        <td style="text-align: right;">${p.count.toLocaleString()}</td>
        <td style="text-align: right;">${(p.size / 1024 / 1024).toFixed(2)} MB</td>
      </tr>
    `
      )
      .join('');

    contentDiv.innerHTML = `
      <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-bottom: 15px;">
        <div style="background: #e8f5e9; padding: 10px; border-radius: 5px; text-align: center;">
          <strong>Total Files</strong><br>
          <span style="font-size: 1.3em;">${data.totalFiles.toLocaleString()}</span>
        </div>
        <div style="background: #e3f2fd; padding: 10px; border-radius: 5px; text-align: center;">
          <strong>Total Size</strong><br>
          <span style="font-size: 1.3em;">${data.totalSizeMB} MB</span>
        </div>
        <div style="background: #fff3e0; padding: 10px; border-radius: 5px; text-align: center;">
          <strong>Est. Time</strong><br>
          <span style="font-size: 1.3em;">${formatDuration(data.estimatedTimeSeconds)}</span>
        </div>
      </div>

      <details style="margin-top: 10px;">
        <summary style="cursor: pointer; color: #1565C0; font-weight: 500;">View by path (${data.byPath.length} paths)</summary>
        <table style="width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 0.9em;">
          <thead>
            <tr style="background: #f5f5f5;">
              <th style="text-align: left; padding: 8px; border-bottom: 2px solid #ddd;">Path</th>
              <th style="text-align: right; padding: 8px; border-bottom: 2px solid #ddd;">Files</th>
              <th style="text-align: right; padding: 8px; border-bottom: 2px solid #ddd;">Size</th>
            </tr>
          </thead>
          <tbody>
            ${pathRows}
          </tbody>
        </table>
      </details>

      <p style="margin-top: 15px; color: #666;">
        Source style: <strong>${data.sourceStyle}</strong>
      </p>
    `;

    startBtn.disabled = false;
  } catch (error) {
    contentDiv.innerHTML = `<p style="color: red;">Scan failed: ${error.message}</p>`;
  }
}

/**
 * Format duration in human-readable format
 */
function formatDuration(seconds) {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

/**
 * Start migration
 */
async function startMigration() {
  const tier = document.getElementById('migrationTier').value;
  const deleteSource = document.getElementById('migrationDeleteSource').checked;
  const startBtn = document.getElementById('startMigrationBtn');
  const cancelBtn = document.getElementById('cancelMigrationBtn');
  const progressDiv = document.getElementById('migrationProgress');

  startBtn.disabled = true;
  cancelBtn.disabled = false;
  progressDiv.style.display = 'block';

  try {
    const response = await fetch('/plugins/signalk-parquet/api/migrate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        targetTier: tier,
        deleteSource: deleteSource,
      }),
    });
    const data = await response.json();

    if (!data.success) {
      alert(`Failed to start migration: ${data.error}`);
      startBtn.disabled = false;
      cancelBtn.disabled = true;
      return;
    }

    currentMigrationJobId = data.jobId;

    // Start polling for progress
    migrationPollInterval = setInterval(pollMigrationProgress, 1000);
  } catch (error) {
    alert(`Failed to start migration: ${error.message}`);
    startBtn.disabled = false;
    cancelBtn.disabled = true;
  }
}

/**
 * Poll migration progress
 */
async function pollMigrationProgress() {
  if (!currentMigrationJobId) return;

  try {
    const response = await fetch(
      `/plugins/signalk-parquet/api/migrate/progress/${currentMigrationJobId}`
    );
    const data = await response.json();

    if (!data.success) {
      console.error('Failed to get progress:', data.error);
      return;
    }

    updateMigrationProgress(data);

    // Check if completed
    if (
      data.status === 'completed' ||
      data.status === 'cancelled' ||
      data.status === 'error'
    ) {
      clearInterval(migrationPollInterval);
      migrationPollInterval = null;
      document.getElementById('startMigrationBtn').disabled = false;
      document.getElementById('cancelMigrationBtn').disabled = true;

      if (data.status === 'completed') {
        alert(
          `Migration complete!\n\nFiles migrated: ${data.filesMigrated}\nFiles skipped: ${data.filesSkipped}\nData processed: ${data.bytesProcessedMB} MB`
        );
      } else if (data.status === 'cancelled') {
        alert('Migration cancelled.');
      } else if (data.status === 'error') {
        alert(`Migration failed: ${data.error}`);
      }
    }
  } catch (error) {
    console.error('Error polling progress:', error);
  }
}

/**
 * Update migration progress UI
 */
function updateMigrationProgress(data) {
  const progressBar = document.getElementById('migrationProgressBar');
  const progressText = document.getElementById('migrationProgressText');
  const currentFile = document.getElementById('migrationCurrentFile');

  progressBar.style.width = `${data.percent}%`;
  progressText.textContent = `${data.percent}% complete (${data.processed}/${data.total} files)`;

  if (data.currentFile) {
    currentFile.textContent = `Current: ${data.currentFile}`;
  } else {
    currentFile.textContent = '';
  }

  // Update status color
  if (data.status === 'running') {
    progressBar.style.background = 'linear-gradient(90deg, #4CAF50, #8BC34A)';
  } else if (data.status === 'completed') {
    progressBar.style.background = '#4CAF50';
  } else if (data.status === 'cancelled') {
    progressBar.style.background = '#FF9800';
  } else if (data.status === 'error') {
    progressBar.style.background = '#f44336';
  }
}

/**
 * Cancel migration
 */
async function cancelMigration() {
  if (!currentMigrationJobId) return;

  try {
    const response = await fetch(
      `/plugins/signalk-parquet/api/migrate/cancel/${currentMigrationJobId}`,
      {
        method: 'POST',
      }
    );
    const data = await response.json();

    if (data.success) {
      document.getElementById('migrationProgressText').textContent =
        'Cancelling...';
    } else {
      alert(`Failed to cancel: ${data.error}`);
    }
  } catch (error) {
    alert(`Failed to cancel: ${error.message}`);
  }
}

/**
 * Refresh Parquet store stats display
 */
async function refreshStoreStats() {
  const container = document.getElementById('storeStats');
  container.innerHTML =
    '<p><span class="loading">Loading store stats...</span></p>';

  try {
    const response = await fetch('/plugins/signalk-parquet/api/store/stats');
    const data = await response.json();

    if (!data.success) {
      container.innerHTML = `<p style="color: red;">Error: ${data.error}</p>`;
      return;
    }

    const stats = data.stats;

    if (stats.totalFiles === 0) {
      container.innerHTML = `
        <p style="color: #666;">No Parquet files found in the data store.</p>
        <p><small>Data will appear here once the plugin starts collecting and exporting data.</small></p>
      `;
      return;
    }

    // Build context rows
    const contextRows = stats.contexts
      .map(
        ctx => `
      <tr>
        <td style="font-family: monospace; font-size: 0.9em;">${ctx.name}</td>
        <td style="text-align: right;">${ctx.pathCount.toLocaleString()}</td>
        <td style="text-align: right;">${ctx.fileCount.toLocaleString()}</td>
      </tr>
    `
      )
      .join('');

    // Build tier rows
    const tierRows = stats.tiers
      .map(
        t => `
      <tr>
        <td style="font-family: monospace;">${t.tier}</td>
        <td style="text-align: right;">${t.fileCount.toLocaleString()}</td>
      </tr>
    `
      )
      .join('');

    const formatDate = iso => {
      if (!iso) return 'N/A';
      return new Date(iso).toLocaleDateString();
    };

    container.innerHTML = `
      <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 15px; margin-bottom: 15px;">
        <div style="background: white; padding: 15px; border-radius: 5px; border: 1px solid #ddd;">
          <strong style="color: #2e7d32;">Vessels</strong><br>
          <span style="font-size: 1.5em;">${stats.totalContexts}</span>
        </div>
        <div style="background: white; padding: 15px; border-radius: 5px; border: 1px solid #ddd;">
          <strong style="color: #1565C0;">Total Paths</strong><br>
          <span style="font-size: 1.5em;">${stats.totalPaths.toLocaleString()}</span>
        </div>
        <div style="background: white; padding: 15px; border-radius: 5px; border: 1px solid #ddd;">
          <strong style="color: #FF9800;">Total Files</strong><br>
          <span style="font-size: 1.5em;">${stats.totalFiles.toLocaleString()}</span>
        </div>
        <div style="background: white; padding: 15px; border-radius: 5px; border: 1px solid #ddd;">
          <strong style="color: #666;">Data Range</strong><br>
          <span style="font-size: 1.1em;">${formatDate(stats.earliestDate)} &ndash; ${formatDate(stats.latestDate)}</span>
        </div>
      </div>

      <details style="margin-top: 10px;">
        <summary style="cursor: pointer; color: #2e7d32; font-weight: 500;">Vessels/Contexts (${stats.totalContexts})</summary>
        <table style="width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 0.9em;">
          <thead>
            <tr style="background: #f5f5f5;">
              <th style="text-align: left; padding: 8px; border-bottom: 2px solid #ddd;">Context</th>
              <th style="text-align: right; padding: 8px; border-bottom: 2px solid #ddd;">Paths</th>
              <th style="text-align: right; padding: 8px; border-bottom: 2px solid #ddd;">Files</th>
            </tr>
          </thead>
          <tbody>${contextRows}</tbody>
        </table>
      </details>

      <details style="margin-top: 10px;">
        <summary style="cursor: pointer; color: #2e7d32; font-weight: 500;">Files by Tier</summary>
        <table style="width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 0.9em;">
          <thead>
            <tr style="background: #f5f5f5;">
              <th style="text-align: left; padding: 8px; border-bottom: 2px solid #ddd;">Tier</th>
              <th style="text-align: right; padding: 8px; border-bottom: 2px solid #ddd;">Files</th>
            </tr>
          </thead>
          <tbody>${tierRows}</tbody>
        </table>
      </details>
    `;
  } catch (error) {
    container.innerHTML = `<p style="color: red;">Failed to load store stats: ${error.message}</p>`;
  }
}

// Export functions for global access
window.refreshBufferStatus = refreshBufferStatus;
window.forceBufferExport = forceBufferExport;
window.scanForMigration = scanForMigration;
window.startMigration = startMigration;
window.cancelMigration = cancelMigration;
window.refreshStoreStats = refreshStoreStats;

// Initialize on tab show
document.addEventListener('DOMContentLoaded', () => {
  // Check if we're on the migration tab
  const migrationTab = document.getElementById('migration');
  if (migrationTab && migrationTab.classList.contains('active')) {
    refreshStoreStats();
    refreshBufferStatus();
  }
});
