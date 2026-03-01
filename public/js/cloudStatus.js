import { getPluginPath } from './utils.js';

export async function testS3Connection() {
  const resultDiv = document.getElementById('s3TestResult');
  const button = document.querySelector('button[onclick="testS3Connection()"]');

  button.disabled = true;
  button.textContent = '🔄 Testing...';
  resultDiv.innerHTML = '<div class="loading">Testing S3 connection...</div>';

  try {
    const response = await fetch(`${getPluginPath()}/api/test-s3`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    const result = await response.json();

    if (result.success) {
      resultDiv.innerHTML = `
                <div class="success">
                    ✅ ${result.message}<br>
                    <strong>Bucket:</strong> ${result.bucket}<br>
                    <strong>Region:</strong> ${result.region}<br>
                    <strong>Key Prefix:</strong> ${result.keyPrefix}
                </div>
            `;
    } else {
      resultDiv.innerHTML = `<div class="error">❌ ${result.error}</div>`;
    }
  } catch (error) {
    resultDiv.innerHTML = `<div class="error">❌ Network error: ${error.message}</div>`;
  } finally {
    button.disabled = false;
    button.textContent = '🔗 Test S3 Connection';
  }
}

// Store compare results for sync
let pendingSync = [];
let compareJobId = null;

export async function compareS3Files() {
  const resultDiv = document.getElementById('s3CompareResult');
  const syncBtn = document.getElementById('syncToS3Btn');
  const compareBtn = document.querySelector('button[onclick="compareS3Files()"]');

  compareBtn.disabled = true;
  syncBtn.disabled = true;
  pendingSync = [];

  resultDiv.innerHTML = `
    <div style="background: #fff; padding: 15px; border-radius: 5px; margin-top: 10px;">
      <div id="s3CompareProgress">
        <div class="loading">Starting comparison...</div>
        <div style="margin-top: 10px;">
          <div style="background: #e0e0e0; height: 20px; border-radius: 10px; overflow: hidden;">
            <div id="s3ProgressBar" style="background: #2196F3; height: 100%; width: 0%; transition: width 0.3s;"></div>
          </div>
          <p id="s3ProgressText" style="margin-top: 5px; color: #666;">Initializing...</p>
        </div>
      </div>
    </div>
  `;

  try {
    // Start the compare job
    const startResponse = await fetch(`${getPluginPath()}/api/s3/compare`, {
      method: 'POST',
    });
    const startResult = await startResponse.json();

    if (!startResult.success) {
      resultDiv.innerHTML = `<div class="error">❌ ${startResult.error}</div>`;
      compareBtn.disabled = false;
      return;
    }

    compareJobId = startResult.jobId;

    // Poll for progress
    const pollInterval = setInterval(async () => {
      try {
        const statusResponse = await fetch(`${getPluginPath()}/api/s3/compare/${compareJobId}`);
        const status = await statusResponse.json();

        if (!status.success) {
          clearInterval(pollInterval);
          resultDiv.innerHTML = `<div class="error">❌ ${status.error}</div>`;
          compareBtn.disabled = false;
          return;
        }

        // Update progress
        document.getElementById('s3ProgressBar').style.width = `${status.progress}%`;
        document.getElementById('s3ProgressText').textContent = status.phase;

        if (status.status === 'completed') {
          clearInterval(pollInterval);
          showCompareResults(status.result);
          compareBtn.disabled = false;
        } else if (status.status === 'error') {
          clearInterval(pollInterval);
          resultDiv.innerHTML = `<div class="error">❌ ${status.error}</div>`;
          compareBtn.disabled = false;
        }
      } catch (err) {
        clearInterval(pollInterval);
        resultDiv.innerHTML = `<div class="error">❌ Polling error: ${err.message}</div>`;
        compareBtn.disabled = false;
      }
    }, 500); // Poll every 500ms

  } catch (error) {
    resultDiv.innerHTML = `<div class="error">❌ Network error: ${error.message}</div>`;
    compareBtn.disabled = false;
  }
}

function showCompareResults(result) {
  const resultDiv = document.getElementById('s3CompareResult');
  const syncBtn = document.getElementById('syncToS3Btn');
  const summary = result.summary;
  pendingSync = result.localOnly || [];

  let html = `
    <div style="background: #fff; padding: 15px; border-radius: 5px; margin-top: 10px;">
      <h4 style="margin-top: 0;">📊 Comparison Summary</h4>
      <table style="width: 100%; border-collapse: collapse;">
        <tr><td style="padding: 5px;"><strong>Local Files:</strong></td><td>${summary.localTotal.toLocaleString()}</td></tr>
        <tr><td style="padding: 5px;"><strong>S3 Files:</strong></td><td>${summary.s3Total.toLocaleString()}</td></tr>
        <tr style="background: #e8f5e9;"><td style="padding: 5px;"><strong>✅ Synced:</strong></td><td>${summary.synced.toLocaleString()}</td></tr>
        <tr style="background: #fff3e0;"><td style="padding: 5px;"><strong>⬆️ Local Only (need upload):</strong></td><td>${summary.localOnly.toLocaleString()} (${summary.localOnlySizeMB} MB)</td></tr>
        <tr style="background: #fce4ec;"><td style="padding: 5px;"><strong>☁️ S3 Only (orphaned):</strong></td><td>${summary.s3Only.toLocaleString()}</td></tr>
      </table>
  `;

  if (summary.localOnly > 0) {
    syncBtn.disabled = false;
    html += `<p style="margin-top: 15px; color: #E65100;">⚠️ ${summary.localOnly} files need to be uploaded to S3.</p>`;
  } else {
    html += `<p style="margin-top: 15px; color: #2E7D32;">✅ All local files are synced to S3!</p>`;
  }

  html += '</div>';
  resultDiv.innerHTML = html;
}

export async function syncToS3() {
  const resultDiv = document.getElementById('s3CompareResult');
  const syncBtn = document.getElementById('syncToS3Btn');
  const compareBtn = document.querySelector('button[onclick="compareS3Files()"]');

  syncBtn.disabled = true;
  compareBtn.disabled = true;

  // Add progress indicator
  resultDiv.innerHTML += `
    <div id="syncProgress" style="margin-top: 15px; padding: 15px; background: #e3f2fd; border-radius: 5px;">
      <div style="margin-bottom: 10px;">
        <div style="background: #e0e0e0; height: 20px; border-radius: 10px; overflow: hidden;">
          <div id="s3SyncProgressBar" style="background: #4CAF50; height: 100%; width: 0%; transition: width 0.3s;"></div>
        </div>
      </div>
      <p id="s3SyncProgressText" style="margin: 5px 0; color: #666;">Starting sync...</p>
      <p id="s3SyncStats" style="margin: 5px 0; font-size: 12px; color: #888;"></p>
    </div>
  `;

  try {
    // Start the sync job
    const startResponse = await fetch(`${getPluginPath()}/api/s3/sync`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({}),
    });

    const startResult = await startResponse.json();

    if (!startResult.success) {
      document.getElementById('syncProgress').innerHTML = `<div class="error">❌ ${startResult.error}</div>`;
      syncBtn.disabled = false;
      compareBtn.disabled = false;
      return;
    }

    const syncJobId = startResult.jobId;

    // Poll for progress
    const pollInterval = setInterval(async () => {
      try {
        const statusResponse = await fetch(`${getPluginPath()}/api/s3/sync/${syncJobId}`);
        const status = await statusResponse.json();

        if (!status.success) {
          clearInterval(pollInterval);
          document.getElementById('syncProgress').innerHTML = `<div class="error">❌ ${status.error}</div>`;
          syncBtn.disabled = false;
          compareBtn.disabled = false;
          return;
        }

        // Update progress
        document.getElementById('s3SyncProgressBar').style.width = `${status.progress}%`;
        document.getElementById('s3SyncProgressText').textContent = status.phase;
        document.getElementById('s3SyncStats').textContent =
          `Uploaded: ${status.filesUploaded} | Failed: ${status.filesFailed} | Total: ${status.filesTotal}`;

        if (status.status === 'completed') {
          clearInterval(pollInterval);
          document.getElementById('syncProgress').innerHTML = `
            <div class="success">
              ✅ Sync complete!<br>
              <strong>Uploaded:</strong> ${status.filesUploaded} files<br>
              <strong>Failed:</strong> ${status.filesFailed} files
              ${status.errors?.length > 0 ? `<br><br><strong>Errors:</strong><br>${status.errors.slice(0, 5).join('<br>')}${status.errors.length > 5 ? `<br>... and ${status.errors.length - 5} more` : ''}` : ''}
            </div>
          `;
          pendingSync = [];
          syncBtn.textContent = '📤 Sync Missing to S3';
          syncBtn.disabled = true;
          compareBtn.disabled = false;
        } else if (status.status === 'error') {
          clearInterval(pollInterval);
          document.getElementById('syncProgress').innerHTML = `<div class="error">❌ ${status.phase}</div>`;
          syncBtn.disabled = false;
          compareBtn.disabled = false;
        }
      } catch (err) {
        clearInterval(pollInterval);
        document.getElementById('syncProgress').innerHTML = `<div class="error">❌ Polling error: ${err.message}</div>`;
        syncBtn.disabled = false;
        compareBtn.disabled = false;
      }
    }, 500);

  } catch (error) {
    document.getElementById('syncProgress').innerHTML = `<div class="error">❌ Network error: ${error.message}</div>`;
    syncBtn.disabled = false;
    compareBtn.disabled = false;
  }
}
