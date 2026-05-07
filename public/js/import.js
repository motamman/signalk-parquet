/**
 * GPX Import UI
 *
 * Primary flow: user picks (or drops) local .gpx files and clicks Start Import.
 * Files are posted multipart to /api/import/gpx/upload; the backend stages
 * them and kicks off the existing import job pipeline.
 *
 * Advanced flow: user enters a server-side directory path. This is useful
 * for bulk imports from mounted USB drives or existing track archives on
 * the Signal K host.
 */

let currentGpxImportJobId = localStorage.getItem('gpxImportJobId') || null;
let gpxImportPollInterval = null;
let gpxSelectedFiles = [];
let gpxUploadInFlight = false;

// Human labels for the backend "phase" field — the service uses internal
// short names, but users shouldn't see "parse" or "write" bare.
const GPX_PHASE_LABEL = {
  scan: 'Finding files',
  parse: 'Reading files',
  write: 'Writing parquet',
  aggregate: 'Building tiers',
};

function getSelectedGpxPaths() {
  return Array.from(document.querySelectorAll('.gpxPath'))
    .filter(cb => cb.checked)
    .map(cb => cb.value);
}

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
}

function renderSelectedFiles() {
  const el = document.getElementById('gpxSelectedFiles');
  if (!el) return;
  // Use textContent/DOM API instead of innerHTML so filenames from the user's
  // disk can't smuggle HTML into the page.
  el.textContent = '';
  if (gpxSelectedFiles.length === 0) {
    el.textContent = 'No files selected';
    return;
  }
  const total = gpxSelectedFiles.reduce((sum, f) => sum + f.size, 0);
  const header = document.createElement('strong');
  header.textContent = `${gpxSelectedFiles.length} file(s), ${formatBytes(total)}`;
  el.appendChild(header);
  const shown = Math.min(10, gpxSelectedFiles.length);
  for (let i = 0; i < shown; i++) {
    const f = gpxSelectedFiles[i];
    const row = document.createElement('div');
    row.textContent = `• ${f.name} (${formatBytes(f.size)})`;
    el.appendChild(row);
  }
  if (gpxSelectedFiles.length > shown) {
    const more = document.createElement('div');
    more.style.color = '#999';
    more.style.marginTop = '4px';
    more.textContent = `…and ${gpxSelectedFiles.length - shown} more`;
    el.appendChild(more);
  }
}

function refreshStartButtonState() {
  const btn = document.getElementById('startGpxImportBtn');
  if (!btn) return;
  btn.disabled =
    gpxUploadInFlight ||
    currentGpxImportJobId !== null ||
    (gpxSelectedFiles.length === 0 && !serverDirValue());
}

function setGpxFiles(fileList) {
  const all = Array.from(fileList);
  const accepted = all.filter(f => f.name.toLowerCase().endsWith('.gpx'));
  const rejected = all.length - accepted.length;
  gpxSelectedFiles = accepted;

  // Surface a clear note when the user dropped non-.gpx files (or a folder,
  // which appears as zero files in dataTransfer.files).
  const note = document.getElementById('gpxRejectedNote');
  if (note) {
    if (all.length === 0) {
      note.textContent =
        'Nothing to import — folders can’t be dragged in, please pick .gpx files directly.';
      note.style.display = 'block';
    } else if (rejected > 0) {
      note.textContent = `Ignored ${rejected} non-.gpx file(s).`;
      note.style.display = 'block';
    } else {
      note.style.display = 'none';
    }
  }

  renderSelectedFiles();
  refreshStartButtonState();
}

function serverDirValue() {
  const el = document.getElementById('gpxSourceDirectory');
  return el ? el.value.trim() : '';
}

function initGpxDropZone() {
  const zone = document.getElementById('gpxDropZone');
  const input = document.getElementById('gpxFileInput');
  if (!zone || !input) return;

  const openPicker = () => input.click();
  zone.addEventListener('click', openPicker);
  // Keyboard activation (Enter / Space) for screen-reader and keyboard users;
  // the zone carries role="button" + tabindex="0" in the HTML for this.
  zone.addEventListener('keydown', e => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      openPicker();
    }
  });
  input.addEventListener('change', () => setGpxFiles(input.files));

  zone.addEventListener('dragover', e => {
    e.preventDefault();
    zone.style.background = '#f3e5f5';
    zone.style.borderColor = '#6a1b9a';
  });
  zone.addEventListener('dragleave', () => {
    zone.style.background = '#fafafa';
    zone.style.borderColor = '#8e24aa';
  });
  zone.addEventListener('drop', e => {
    e.preventDefault();
    zone.style.background = '#fafafa';
    zone.style.borderColor = '#8e24aa';
    if (e.dataTransfer && e.dataTransfer.files) {
      setGpxFiles(e.dataTransfer.files);
    }
  });

  // Also enable Start Import when the user types a server dir instead.
  const dirInput = document.getElementById('gpxSourceDirectory');
  if (dirInput) {
    dirInput.addEventListener('input', refreshStartButtonState);
  }
}

// Warn the user if they try to close the tab during an upload — the browser
// will abort the multipart request and leave temp files behind on the server.
window.addEventListener('beforeunload', e => {
  if (gpxUploadInFlight) {
    e.preventDefault();
    e.returnValue = '';
  }
});

// Map a failed fetch response into a user-friendly error string. 401 from
// SignalK means the browser has no session cookie — surface a clear hint
// instead of the generic "Failed to start import" with a cryptic message.
async function explainHttpError(response) {
  if (response.status === 401) {
    return 'Not logged in. Open the Signal K admin UI, log in, then try again from this tab.';
  }
  if (response.status === 413) {
    return 'Upload too large (over 200 MB per file or 500 files total).';
  }
  try {
    const d = await response.json();
    if (d && d.error) return d.error;
  } catch {
    // not JSON (e.g. HTML login page)
  }
  return `HTTP ${response.status} ${response.statusText || ''}`.trim();
}

async function scanGpxImport() {
  // Only meaningful for the server-directory fallback.
  const sourceDir = serverDirValue();
  const resultsDiv = document.getElementById('gpxScanResults');
  const contentDiv = document.getElementById('gpxScanContent');

  if (!sourceDir) {
    alert(
      'Enter a server directory first (Advanced). For uploads, just click Start Import.'
    );
    return;
  }

  resultsDiv.style.display = 'block';
  contentDiv.innerHTML = '<p><span class="loading">Scanning files...</span></p>';

  try {
    const response = await fetch(
      '/plugins/signalk-parquet/api/import/gpx/scan',
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sourceDirectory: sourceDir }),
      }
    );
    if (!response.ok) {
      contentDiv.textContent = `Scan failed: ${await explainHttpError(response)}`;
      return;
    }
    const data = await response.json();

    if (!data.success) {
      contentDiv.textContent = `Scan failed: ${data.error}`;
      return;
    }

    if (data.totalFiles === 0) {
      contentDiv.innerHTML =
        '<p style="color: #666;">No .gpx files found under that directory.</p>';
      return;
    }

    // Render file list as DOM (not innerHTML) so user-supplied filenames
    // can't inject markup.
    const tbody = document.createElement('tbody');
    for (const f of data.files.slice(0, 200)) {
      const tr = document.createElement('tr');
      const tdName = document.createElement('td');
      tdName.style.fontFamily = 'monospace';
      tdName.style.fontSize = '0.9em';
      tdName.textContent = f.name;
      const tdSize = document.createElement('td');
      tdSize.style.textAlign = 'right';
      tdSize.textContent = `${f.sizeMB} MB`;
      tr.appendChild(tdName);
      tr.appendChild(tdSize);
      tbody.appendChild(tr);
    }

    contentDiv.innerHTML = `
      <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; margin-bottom: 15px;">
        <div style="background: #f3e5f5; padding: 10px; border-radius: 5px; text-align: center;">
          <strong>GPX Files Found</strong><br>
          <span style="font-size: 1.3em;">${data.totalFiles.toLocaleString()}</span>
        </div>
        <div style="background: #e1bee7; padding: 10px; border-radius: 5px; text-align: center;">
          <strong>Total Size</strong><br>
          <span style="font-size: 1.3em;">${data.totalSizeMB} MB</span>
        </div>
      </div>
      <details>
        <summary style="cursor: pointer; color: #6a1b9a; font-weight: 500;">File list</summary>
        <table style="width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 0.9em;">
          <thead>
            <tr style="background: #f5f5f5;">
              <th style="text-align: left; padding: 8px; border-bottom: 2px solid #ddd;">File</th>
              <th style="text-align: right; padding: 8px; border-bottom: 2px solid #ddd;">Size</th>
            </tr>
          </thead>
        </table>
      </details>
    `;
    // Insert the safely-built tbody into the table we just rendered.
    const table = contentDiv.querySelector('table');
    if (table) table.appendChild(tbody);
    if (data.files.length > 200) {
      const overflow = document.createElement('p');
      overflow.style.color = '#666';
      overflow.style.marginTop = '10px';
      overflow.textContent = `Showing 200 of ${data.files.length} files`;
      contentDiv.appendChild(overflow);
    }
  } catch (error) {
    contentDiv.textContent = `Scan failed: ${error.message}`;
  }
}

async function startGpxImport() {
  const startBtn = document.getElementById('startGpxImportBtn');
  const cancelBtn = document.getElementById('cancelGpxImportBtn');
  const progressDiv = document.getElementById('gpxImportProgress');
  const progressText = document.getElementById('gpxImportProgressText');
  const progressBar = document.getElementById('gpxImportProgressBar');
  const currentFileEl = document.getElementById('gpxImportCurrentFile');

  // Disable the button immediately so a second click can't race us before
  // validation alerts fire and before we flip gpxUploadInFlight.
  startBtn.disabled = true;

  const sourceDir = serverDirValue();
  const contextInput = document.getElementById('gpxContext').value.trim();
  const paths = getSelectedGpxPaths();
  const deleteSource = !!document.getElementById('gpxDeleteSource')?.checked;

  if (gpxSelectedFiles.length === 0 && !sourceDir) {
    alert(
      'Pick at least one .gpx file, or set a server directory under Advanced.'
    );
    refreshStartButtonState();
    return;
  }
  if (paths.length === 0) {
    alert('Please select at least one Signal K path to emit.');
    refreshStartButtonState();
    return;
  }

  // Kick off the UI transition before the request so the user gets immediate
  // feedback even when the multipart body takes a while to send.
  progressDiv.style.display = 'block';
  progressBar.style.width = '0%';
  progressBar.style.background = 'linear-gradient(90deg, #8e24aa, #ba68c8)';
  if (gpxSelectedFiles.length > 0) {
    const total = gpxSelectedFiles.reduce((s, f) => s + f.size, 0);
    progressText.textContent = `Uploading ${gpxSelectedFiles.length} file(s), ${formatBytes(total)}…`;
  } else {
    progressText.textContent = 'Starting…';
  }
  if (currentFileEl) currentFileEl.textContent = '';
  cancelBtn.disabled = false;

  try {
    let response;
    gpxUploadInFlight = true;
    if (gpxSelectedFiles.length > 0) {
      const form = new FormData();
      for (const f of gpxSelectedFiles) {
        form.append('files', f, f.name);
      }
      if (contextInput) form.append('context', contextInput);
      form.append('paths', paths.join(','));
      response = await fetch(
        '/plugins/signalk-parquet/api/import/gpx/upload',
        { method: 'POST', body: form }
      );
    } else {
      response = await fetch('/plugins/signalk-parquet/api/import/gpx', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sourceDirectory: sourceDir,
          context: contextInput || undefined,
          paths,
          deleteSource,
        }),
      });
    }
    gpxUploadInFlight = false;

    if (!response.ok) {
      alert(`Failed to start import: ${await explainHttpError(response)}`);
      progressDiv.style.display = 'none';
      cancelBtn.disabled = true;
      refreshStartButtonState();
      return;
    }

    const data = await response.json();
    if (!data.success) {
      alert(`Failed to start import: ${data.error}`);
      progressDiv.style.display = 'none';
      cancelBtn.disabled = true;
      refreshStartButtonState();
      return;
    }

    currentGpxImportJobId = data.jobId;
    localStorage.setItem('gpxImportJobId', data.jobId);
    gpxImportPollInterval = setInterval(pollGpxImportProgress, 1000);
  } catch (error) {
    gpxUploadInFlight = false;
    alert(`Failed to start import: ${error.message}`);
    progressDiv.style.display = 'none';
    cancelBtn.disabled = true;
    refreshStartButtonState();
  }
}

async function pollGpxImportProgress() {
  if (!currentGpxImportJobId) return;

  try {
    const response = await fetch(
      `/plugins/signalk-parquet/api/import/gpx/progress/${currentGpxImportJobId}`
    );

    // 404 = job TTL-evicted. Stop polling silently.
    if (response.status === 404) {
      clearInterval(gpxImportPollInterval);
      gpxImportPollInterval = null;
      currentGpxImportJobId = null;
      localStorage.removeItem('gpxImportJobId');
      document.getElementById('gpxImportProgress').style.display = 'none';
      document.getElementById('cancelGpxImportBtn').disabled = true;
      refreshStartButtonState();
      return;
    }
    if (!response.ok) {
      // transient error — don't kill the poll loop over one bad response
      return;
    }

    const data = await response.json();
    updateGpxImportProgress(data);

    if (
      data.status === 'completed' ||
      data.status === 'cancelled' ||
      data.status === 'error'
    ) {
      clearInterval(gpxImportPollInterval);
      gpxImportPollInterval = null;
      currentGpxImportJobId = null;
      localStorage.removeItem('gpxImportJobId');
      document.getElementById('cancelGpxImportBtn').disabled = true;

      if (data.status === 'completed') {
        const parquetCount = (data.filesCreated || []).length;
        const hint = samplePartitionPath(data.filesCreated);
        alert(
          `Import complete.\n\n` +
            `Files imported:   ${data.filesImported}\n` +
            `Files skipped:    ${data.filesSkipped}\n` +
            `Points parsed:    ${Number(data.pointsParsed).toLocaleString()}\n` +
            `Records written:  ${Number(data.recordsWritten).toLocaleString()}\n` +
            `Parquet files:    ${parquetCount}\n` +
            (hint ? `\nFiles landed under:\n  ${hint}` : '')
        );
        gpxSelectedFiles = [];
        renderSelectedFiles();
        const note = document.getElementById('gpxRejectedNote');
        if (note) note.style.display = 'none';
      } else if (data.status === 'cancelled') {
        alert('Import cancelled.');
      } else if (data.status === 'error') {
        alert(`Import failed: ${data.error}`);
      }
      refreshStartButtonState();
    }
  } catch (error) {
    console.error('Error polling GPX import progress:', error);
  }
}

// Extract the common "tier=raw/context=.../path=.../" prefix from the list
// of parquet paths produced, so the completion dialog can tell the user
// roughly where the data went without dumping 16 full paths.
function samplePartitionPath(files) {
  if (!Array.isArray(files) || files.length === 0) return '';
  // Take the first file's path up through the last "year=" segment's parent.
  const parts = files[0].split(/[\\/]/);
  const yearIdx = parts.findIndex(p => p.startsWith('year='));
  if (yearIdx <= 0) return files[0];
  return parts.slice(0, yearIdx).join('/') + '/…';
}

function updateGpxImportProgress(data) {
  const progressBar = document.getElementById('gpxImportProgressBar');
  const progressText = document.getElementById('gpxImportProgressText');
  const currentFile = document.getElementById('gpxImportCurrentFile');
  const stats = document.getElementById('gpxImportStats');

  const phase = GPX_PHASE_LABEL[data.phase] || data.phase || '';

  if (data.phase === 'aggregate') {
    // Switch the bar to date-based progress; the file-percent has already
    // hit 100% by this phase and would otherwise sit frozen.
    const aggDone = data.aggregationDatesProcessed || 0;
    const aggTotal = data.aggregationDatesTotal || 0;
    const aggPercent =
      aggTotal > 0 ? Math.round((aggDone / aggTotal) * 100) : 0;
    progressBar.style.width = `${aggPercent}%`;
    progressText.textContent = `Building tiers: ${aggDone}/${aggTotal} dates (${aggPercent}%)`;
    currentFile.textContent = data.aggregationCurrentDate
      ? `Current date: ${data.aggregationCurrentDate}`
      : '';
  } else {
    progressBar.style.width = `${data.percent}%`;
    progressText.textContent =
      `${data.percent}% complete (${data.processed}/${data.total} files` +
      (phase ? `, ${phase.toLowerCase()}` : '') +
      ')';
    currentFile.textContent = data.currentFile
      ? `Current: ${data.currentFile}`
      : '';
  }
  stats.textContent =
    `Points parsed: ${Number(data.pointsParsed).toLocaleString()} ` +
    `· Records written: ${Number(data.recordsWritten).toLocaleString()} ` +
    `· Output files: ${(data.filesCreated || []).length}`;

  if (data.status === 'running' || data.status === 'scanning') {
    progressBar.style.background = 'linear-gradient(90deg, #8e24aa, #ba68c8)';
  } else if (data.status === 'completed') {
    progressBar.style.background = '#4caf50';
  } else if (data.status === 'cancelled') {
    progressBar.style.background = '#ff9800';
  } else if (data.status === 'error') {
    progressBar.style.background = '#f44336';
  }
}

async function cancelGpxImport() {
  if (!currentGpxImportJobId) return;
  const cancelBtn = document.getElementById('cancelGpxImportBtn');
  cancelBtn.disabled = true; // spam-proof
  try {
    const response = await fetch(
      `/plugins/signalk-parquet/api/import/gpx/cancel/${currentGpxImportJobId}`,
      { method: 'POST' }
    );
    // 400 here means the job is already done — race with the completion
    // poll. Not worth alerting the user about.
    if (response.ok) {
      document.getElementById('gpxImportProgressText').textContent =
        'Cancelling…';
    }
  } catch (error) {
    // non-fatal; poll will pick up the final status anyway
    console.error('Cancel request failed:', error);
  }
}

window.scanGpxImport = scanGpxImport;
window.startGpxImport = startGpxImport;
window.cancelGpxImport = cancelGpxImport;

document.addEventListener('DOMContentLoaded', () => {
  initGpxDropZone();
  refreshStartButtonState();

  if (currentGpxImportJobId) {
    const progressDiv = document.getElementById('gpxImportProgress');
    if (progressDiv) {
      progressDiv.style.display = 'block';
      document.getElementById('cancelGpxImportBtn').disabled = false;
      gpxImportPollInterval = setInterval(pollGpxImportProgress, 1000);
    }
  }
});
