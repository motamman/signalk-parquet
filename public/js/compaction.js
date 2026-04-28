/**
 * Compaction UI
 *
 * Drives the /api/compact/* endpoints from the Status tab. Mirrors the
 * scan-then-start-then-poll flow used by the migration and import UIs.
 */

let currentCompactionJobId = localStorage.getItem('compactionJobId') || null;
let compactionPollInterval = null;
// The exact config that produced the most recent successful scan. Start
// posts this — not the live form values — so a user can't scan one
// (tier, beforeYear, pathFilter) and then accidentally compact a
// different set after editing the inputs. Cleared by invalidateScan()
// on any form change.
let lastCompactionScanConfig = null;

const COMPACT_PHASE_LABEL = {
  scan: 'Scanning',
  compact: 'Compacting',
};

function explainCompactHttpError(response) {
  if (response.status === 401) {
    return 'Not logged in. Open the Signal K admin UI, log in, then try again from this tab.';
  }
  return `HTTP ${response.status} ${response.statusText || ''}`.trim();
}

function readCompactConfig() {
  const tier = document.getElementById('compactTier').value;
  const beforeYearRaw = document
    .getElementById('compactBeforeYear')
    .value.trim();
  const pathFilter = document.getElementById('compactPathFilter').value.trim();
  const body = { tier };
  if (beforeYearRaw) {
    const n = Number(beforeYearRaw);
    if (Number.isFinite(n)) body.beforeYear = n;
  }
  if (pathFilter) body.pathFilter = pathFilter;
  return body;
}

function invalidateCompactionScan() {
  if (!lastCompactionScanConfig) return;
  lastCompactionScanConfig = null;
  const resultsDiv = document.getElementById('compactScanResults');
  const contentDiv = document.getElementById('compactScanContent');
  if (resultsDiv) resultsDiv.style.display = 'none';
  if (contentDiv) contentDiv.innerHTML = '';
  // Don't touch Start while a job is in flight; pollCompactionProgress
  // owns the disabled state until terminal status. Otherwise reflect
  // "no valid scan" by disabling Start.
  if (!currentCompactionJobId) {
    const startBtn = document.getElementById('startCompactionBtn');
    if (startBtn) startBtn.disabled = true;
  }
}

async function scanCompaction() {
  const resultsDiv = document.getElementById('compactScanResults');
  const contentDiv = document.getElementById('compactScanContent');
  const startBtn = document.getElementById('startCompactionBtn');

  resultsDiv.style.display = 'block';
  contentDiv.innerHTML = '<p><span class="loading">Scanning…</span></p>';
  startBtn.disabled = true;
  // Snapshot the exact config we're scanning. Persisted on success so
  // Start can reuse it; cleared on failure (or any form edit).
  const scanConfig = readCompactConfig();
  lastCompactionScanConfig = null;

  try {
    const response = await fetch('/plugins/signalk-parquet/api/compact/scan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(scanConfig),
    });

    if (!response.ok) {
      contentDiv.textContent = `Scan failed: ${explainCompactHttpError(response)}`;
      return;
    }
    const data = await response.json();
    if (!data.success) {
      contentDiv.textContent = `Scan failed: ${data.error}`;
      return;
    }

    if (data.totalGroups === 0) {
      contentDiv.innerHTML = `
        <p style="color: #666;">
          No (path, year) groups need compacting in tier <code>${data.tier}</code>
          (years before ${data.beforeYear}). Either there's nothing yet, or the
          previous compaction has already merged everything.
        </p>
      `;
      return;
    }

    contentDiv.innerHTML = `
      <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-bottom: 15px;">
        <div style="background: #e8eaf6; padding: 10px; border-radius: 5px; text-align: center;">
          <strong>Groups</strong><br>
          <span style="font-size: 1.3em;">${data.totalGroups.toLocaleString()}</span>
        </div>
        <div style="background: #c5cae9; padding: 10px; border-radius: 5px; text-align: center;">
          <strong>Source files</strong><br>
          <span style="font-size: 1.3em;">${data.totalSourceFiles.toLocaleString()}</span>
        </div>
        <div style="background: #9fa8da; padding: 10px; border-radius: 5px; text-align: center;">
          <strong>Source size</strong><br>
          <span style="font-size: 1.3em;">${data.totalSourceMB} MB</span>
        </div>
      </div>
      <p style="color: #666;">
        Will produce <strong>${data.totalGroups.toLocaleString()}</strong>
        new files (one per group) and remove the
        <strong>${data.totalSourceFiles.toLocaleString()}</strong> source files.
      </p>
    `;

    const tableWrap = document.createElement('details');
    tableWrap.innerHTML =
      '<summary style="cursor:pointer;color:#283593;font-weight:500;">Group breakdown</summary>';
    const table = document.createElement('table');
    table.style.cssText =
      'width:100%;border-collapse:collapse;margin-top:10px;font-size:0.9em;';
    table.innerHTML = `
      <thead>
        <tr style="background:#f5f5f5;">
          <th style="text-align:left;padding:8px;border-bottom:2px solid #ddd;">Year</th>
          <th style="text-align:left;padding:8px;border-bottom:2px solid #ddd;">Path</th>
          <th style="text-align:left;padding:8px;border-bottom:2px solid #ddd;">Context</th>
          <th style="text-align:right;padding:8px;border-bottom:2px solid #ddd;">Files</th>
          <th style="text-align:right;padding:8px;border-bottom:2px solid #ddd;">Size</th>
        </tr>
      </thead>
    `;
    const tbody = document.createElement('tbody');
    for (const g of data.groups.slice(0, 200)) {
      const tr = document.createElement('tr');
      const cells = [
        String(g.year),
        g.path,
        g.context,
        g.sourceFiles.toLocaleString(),
        `${g.sourceMB} MB`,
      ];
      cells.forEach((text, idx) => {
        const td = document.createElement('td');
        td.style.padding = '6px 8px';
        td.style.borderBottom = '1px solid #eee';
        if (idx >= 3) td.style.textAlign = 'right';
        td.style.fontFamily = idx === 1 || idx === 2 ? 'monospace' : '';
        td.textContent = text;
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    tableWrap.appendChild(table);
    if (data.groups.length > 200) {
      const overflow = document.createElement('p');
      overflow.style.color = '#666';
      overflow.style.marginTop = '10px';
      overflow.textContent = `Showing 200 of ${data.groups.length} groups`;
      tableWrap.appendChild(overflow);
    }
    contentDiv.appendChild(tableWrap);

    lastCompactionScanConfig = scanConfig;
    startBtn.disabled = false;
  } catch (error) {
    contentDiv.textContent = `Scan failed: ${error.message}`;
  }
}

async function startCompaction() {
  const startBtn = document.getElementById('startCompactionBtn');
  const cancelBtn = document.getElementById('cancelCompactionBtn');
  const progressDiv = document.getElementById('compactionProgress');

  // Refuse to start without a fresh scan. Without this, an edit after
  // scan would compact a different set than the preview showed.
  if (!lastCompactionScanConfig) {
    alert('Run a scan first, or rescan after editing the inputs.');
    return;
  }

  startBtn.disabled = true;
  cancelBtn.disabled = false;
  progressDiv.style.display = 'block';
  document.getElementById('compactionProgressBar').style.width = '0%';
  document.getElementById('compactionProgressBar').style.background =
    'linear-gradient(90deg, #3949ab, #7986cb)';
  document.getElementById('compactionProgressText').textContent = 'Starting…';
  document.getElementById('compactionCurrentGroup').textContent = '';
  document.getElementById('compactionStats').textContent = '';

  try {
    const response = await fetch('/plugins/signalk-parquet/api/compact', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(lastCompactionScanConfig),
    });
    if (!response.ok) {
      alert(`Failed to start compaction: ${explainCompactHttpError(response)}`);
      progressDiv.style.display = 'none';
      cancelBtn.disabled = true;
      startBtn.disabled = false;
      return;
    }
    const data = await response.json();
    if (!data.success) {
      alert(`Failed to start compaction: ${data.error}`);
      progressDiv.style.display = 'none';
      cancelBtn.disabled = true;
      startBtn.disabled = false;
      return;
    }
    currentCompactionJobId = data.jobId;
    localStorage.setItem('compactionJobId', data.jobId);
    compactionPollInterval = setInterval(pollCompactionProgress, 1000);
  } catch (error) {
    alert(`Failed to start compaction: ${error.message}`);
    progressDiv.style.display = 'none';
    cancelBtn.disabled = true;
    startBtn.disabled = false;
  }
}

async function pollCompactionProgress() {
  if (!currentCompactionJobId) return;

  try {
    const response = await fetch(
      `/plugins/signalk-parquet/api/compact/progress/${currentCompactionJobId}`
    );
    if (response.status === 404) {
      clearInterval(compactionPollInterval);
      compactionPollInterval = null;
      currentCompactionJobId = null;
      localStorage.removeItem('compactionJobId');
      document.getElementById('compactionProgress').style.display = 'none';
      document.getElementById('cancelCompactionBtn').disabled = true;
      document.getElementById('startCompactionBtn').disabled = false;
      return;
    }
    if (!response.ok) return; // transient — keep polling

    const data = await response.json();
    updateCompactionProgress(data);

    if (
      data.status === 'completed' ||
      data.status === 'cancelled' ||
      data.status === 'error'
    ) {
      clearInterval(compactionPollInterval);
      compactionPollInterval = null;
      currentCompactionJobId = null;
      localStorage.removeItem('compactionJobId');
      document.getElementById('cancelCompactionBtn').disabled = true;
      document.getElementById('startCompactionBtn').disabled = false;

      if (data.status === 'completed') {
        const before = Number(data.bytesBefore || 0);
        const after = Number(data.bytesAfter || 0);
        const ratio = before > 0 ? (after / before) * 100 : 0;
        alert(
          `Compaction complete.\n\n` +
            `Groups compacted: ${data.groupsCompacted}\n` +
            `Groups skipped:   ${data.groupsSkipped}\n` +
            `Source files removed: ${data.filesRemoved}\n` +
            `Size before: ${data.bytesBeforeMB} MB\n` +
            `Size after:  ${data.bytesAfterMB} MB\n` +
            `Saved:       ${data.savingsMB} MB (output is ${ratio.toFixed(1)}% of input)`
        );
      } else if (data.status === 'cancelled') {
        alert('Compaction cancelled.');
      } else if (data.status === 'error') {
        alert(`Compaction failed: ${data.error}`);
      }
    }
  } catch (error) {
    console.error('Error polling compaction progress:', error);
  }
}

function updateCompactionProgress(data) {
  const bar = document.getElementById('compactionProgressBar');
  const text = document.getElementById('compactionProgressText');
  const current = document.getElementById('compactionCurrentGroup');
  const stats = document.getElementById('compactionStats');

  const phase = COMPACT_PHASE_LABEL[data.phase] || data.phase || '';
  bar.style.width = `${data.percent}%`;
  text.textContent =
    `${data.percent}% complete (${data.processed}/${data.total} groups` +
    (phase ? `, ${phase.toLowerCase()}` : '') +
    ')';
  current.textContent = data.currentGroup
    ? `Current: ${data.currentGroup}`
    : '';
  stats.textContent =
    `Compacted: ${data.groupsCompacted} ` +
    `· Skipped: ${data.groupsSkipped} ` +
    `· Source files removed: ${data.filesRemoved}`;

  if (data.status === 'running' || data.status === 'scanning') {
    bar.style.background = 'linear-gradient(90deg, #3949ab, #7986cb)';
  } else if (data.status === 'completed') {
    bar.style.background = '#4caf50';
  } else if (data.status === 'cancelled') {
    bar.style.background = '#ff9800';
  } else if (data.status === 'error') {
    bar.style.background = '#f44336';
  }
}

async function cancelCompaction() {
  if (!currentCompactionJobId) return;
  const cancelBtn = document.getElementById('cancelCompactionBtn');
  cancelBtn.disabled = true;
  try {
    const response = await fetch(
      `/plugins/signalk-parquet/api/compact/cancel/${currentCompactionJobId}`,
      { method: 'POST' }
    );
    if (response.ok) {
      document.getElementById('compactionProgressText').textContent =
        'Cancelling…';
    }
  } catch (error) {
    console.error('Cancel request failed:', error);
  }
}

window.scanCompaction = scanCompaction;
window.startCompaction = startCompaction;
window.cancelCompaction = cancelCompaction;

document.addEventListener('DOMContentLoaded', () => {
  // Any edit to the form inputs invalidates the previous scan: Start
  // becomes disabled until a fresh scan against the new config runs.
  for (const id of ['compactTier', 'compactBeforeYear', 'compactPathFilter']) {
    const el = document.getElementById(id);
    if (!el) continue;
    el.addEventListener('input', invalidateCompactionScan);
    el.addEventListener('change', invalidateCompactionScan);
  }

  if (currentCompactionJobId) {
    const progressDiv = document.getElementById('compactionProgress');
    if (progressDiv) {
      progressDiv.style.display = 'block';
      document.getElementById('cancelCompactionBtn').disabled = false;
      // Disable Start while a previous job is still being polled.
      // Pressing Start here would orphan the original job from the
      // page (currentCompactionJobId gets overwritten and the existing
      // poll interval is replaced) — the server would also refuse the
      // second start as a conflict, but the UI shouldn't even let it
      // be attempted. Re-enabled by pollCompactionProgress when the
      // job reaches a terminal state.
      document.getElementById('startCompactionBtn').disabled = true;
      compactionPollInterval = setInterval(pollCompactionProgress, 1000);
    }
  }
});
