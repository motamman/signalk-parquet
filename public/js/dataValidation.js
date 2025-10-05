import { getPluginPath } from './utils.js';

// Global variables for validation cancellation
let currentValidationController = null;
let currentRepairController = null;
let currentValidationJobId = null;
let validationCancelRequested = false;
let currentRepairJobId = null;
let repairCancelRequested = false;

// Cancel current validation
export function cancelValidation() {
  if (validationCancelRequested) {
    return;
  }

  const cancelBtn = document.getElementById('cancelValidationBtn');
  const validateBtn = document.getElementById('validateBtn');

  if (currentValidationJobId) {
    validationCancelRequested = true;
    if (cancelBtn) {
      cancelBtn.disabled = true;
      cancelBtn.textContent = 'Cancelling...';
    }
    if (validateBtn) {
      validateBtn.textContent = '⏳ Cancelling validation...';
      validateBtn.disabled = true;
    }

    fetch(
      `${getPluginPath()}/api/validate-schemas/cancel/${currentValidationJobId}`,
      {
        method: 'POST',
      }
    )
      .then(response => {
        if (!response.ok) {
          console.error('Cancel validation request failed:', response.status);
        }
      })
      .catch(error => {
        console.error('Cancel validation request error:', error);
      });
  } else if (currentValidationController) {
    validationCancelRequested = true;
    currentValidationController.abort();
    currentValidationController = null;
    if (cancelBtn) {
      cancelBtn.disabled = true;
      cancelBtn.textContent = 'Cancelling...';
    }
    if (validateBtn) {
      validateBtn.textContent = '⏳ Cancelling validation...';
      validateBtn.disabled = true;
    }
  }
}

// Cancel current repair
export function cancelRepair() {
  if (repairCancelRequested) {
    return;
  }

  repairCancelRequested = true;

  const repairBtn = document.getElementById('repairBtn');
  if (repairBtn) {
    repairBtn.textContent = '⏳ Cancelling repair...';
    repairBtn.disabled = true;
  }

  if (currentRepairJobId) {
    fetch(
      `${getPluginPath()}/api/repair-schemas/cancel/${currentRepairJobId}`,
      {
        method: 'POST',
      }
    ).catch(error => {
      console.error('Cancel repair request error:', error);
    });
  }
}

// Poll for validation progress
async function pollValidationProgress(
  jobId,
  timerElement,
  startTime,
  abortSignal
) {
  let polling = true;
  let lastProgress = null;

  while (polling) {
    try {
      if (abortSignal?.aborted) {
        throw new DOMException('Validation polling aborted', 'AbortError');
      }

      const response = await fetch(
        `${getPluginPath()}/api/validate-schemas/progress/${jobId}`,
        {
          signal: abortSignal,
          cache: 'no-store',
        }
      );

      if (!response.ok) {
        console.error('Failed to get progress:', response.status);
        break;
      }

      const progress = await response.json();
      lastProgress = progress;

      const processed = progress.processed ?? 0;
      const total = progress.total ?? 0;
      const percent =
        progress.percent ??
        (total > 0 ? Math.round((processed / total) * 100) : 0);
      const elapsed = (new Date() - startTime) / 1000;

      const vessel = progress.currentVessel || 'Detecting vessel...';
      const contextPath =
        progress.currentRelativePath || progress.currentFile || 'Processing...';
      const isCancelling =
        progress.status === 'cancelling' || progress.cancelRequested;
      const statusText =
        progress.status === 'cancelled'
          ? 'Status: Cancelled'
          : isCancelling
            ? 'Status: Cancelling...'
            : 'Status: Running';
      const statusColor =
        progress.status === 'cancelled'
          ? '#d32f2f'
          : isCancelling
            ? '#b36b00'
            : '#666';

      if (isCancelling) {
        validationCancelRequested = true;
      }

      timerElement.innerHTML = `
                <strong>⏱️ Validation Progress</strong><br>
                Started: ${startTime.toLocaleTimeString()}<br>
                Elapsed: ${elapsed.toFixed(1)}s<br>
                <strong>Files: ${processed.toLocaleString()} / ${total.toLocaleString()} (${percent}%)</strong><br>
                <span style="font-size: 0.9em; color: #666;">Vessel: ${vessel}</span><br>
                <span style="font-size: 0.9em; color: #666;">Current: ${contextPath}</span><br>
                <span style="font-size: 0.9em; color: ${statusColor};">${statusText}</span>
            `;

      if (
        progress.status === 'completed' ||
        progress.status === 'cancelled' ||
        progress.status === 'error'
      ) {
        polling = false;

        const endTime = new Date();
        const totalTime = (endTime - startTime) / 1000;

        if (progress.status === 'completed') {
          timerElement.innerHTML = `
                        <strong>⏱️ Validation Complete</strong><br>
                        Started: ${startTime.toLocaleTimeString()}<br>
                        Completed: ${endTime.toLocaleTimeString()}<br>
                        <strong>Total Time: ${totalTime.toFixed(1)}s</strong><br>
                        <strong>📁 Files: ${total.toLocaleString()} (100%)</strong>
                    `;
          timerElement.style.background = '#e8f5e8';
          timerElement.style.borderColor = '#4caf50';
        } else if (progress.status === 'cancelled') {
          timerElement.innerHTML = `
                        <strong>❌ Validation Cancelled</strong><br>
                        Started: ${startTime.toLocaleTimeString()}<br>
                        Cancelled: ${endTime.toLocaleTimeString()}<br>
                        <strong>Time: ${totalTime.toFixed(1)}s</strong>
                    `;
          timerElement.style.background = '#fff3cd';
          timerElement.style.borderColor = '#ffc107';
        } else if (progress.status === 'error') {
          timerElement.innerHTML = `
                        <strong>⏱️ Validation Failed</strong><br>
                        Started: ${startTime.toLocaleTimeString()}<br>
                        Failed: ${endTime.toLocaleTimeString()}<br>
                        <strong>Total Time: ${totalTime.toFixed(1)}s</strong>
                    `;
          timerElement.style.background = '#ffeaea';
          timerElement.style.borderColor = '#f44336';
        }
      }

      await new Promise(resolve => setTimeout(resolve, 500));
    } catch (error) {
      if (error.name === 'AbortError') {
        return lastProgress;
      }
      console.error('Progress polling error:', error);
      break;
    }
  }

  return lastProgress;
}

// Poll for repair progress
async function pollRepairProgress(jobId, timerElement, startTime, abortSignal) {
  let polling = true;
  let lastProgress = null;

  while (polling) {
    try {
      if (abortSignal?.aborted) {
        throw new DOMException('Repair polling aborted', 'AbortError');
      }

      const response = await fetch(
        `${getPluginPath()}/api/repair-schemas/progress/${jobId}`,
        {
          signal: abortSignal,
          cache: 'no-store',
        }
      );

      if (!response.ok) {
        console.error('Failed to get repair progress:', response.status);
        break;
      }

      const progress = await response.json();
      lastProgress = progress;

      const processed = progress.processed ?? 0;
      const total = progress.total ?? 0;
      const percent =
        progress.percent ??
        (total > 0 ? Math.round((processed / total) * 100) : 0);
      const elapsed = (new Date() - startTime) / 1000;
      const statusText = progress.message || 'Repair in progress';
      const statusColor =
        progress.status === 'cancelled'
          ? '#d32f2f'
          : progress.status === 'cancelling'
            ? '#b36b00'
            : '#666';

      timerElement.innerHTML = [
        '<strong>🔧 Repair Progress</strong><br>',
        `Started: ${startTime.toLocaleTimeString()}<br>`,
        `Elapsed: ${elapsed.toFixed(1)}s<br>`,
        `<strong>Files: ${processed.toLocaleString()} / ${total.toLocaleString()} (${percent}%)</strong><br>`,
        progress.currentFile
          ? `<span style="font-size: 0.9em; color: #666;">Current: ${progress.currentFile}</span><br>`
          : '',
        `<span style="font-size: 0.9em; color: ${statusColor};">${statusText}</span>`,
      ]
        .filter(Boolean)
        .join('');

      if (
        progress.status === 'completed' ||
        progress.status === 'cancelled' ||
        progress.status === 'error'
      ) {
        polling = false;
        const endTime = new Date();
        const totalTime = (endTime - startTime) / 1000;
        lastProgress.completedAt = endTime;

        if (progress.status === 'completed') {
          timerElement.innerHTML = [
            '<strong>✅ Repair Completed</strong><br>',
            `Started: ${startTime.toLocaleTimeString()}<br>`,
            `Ended: ${endTime.toLocaleTimeString()}<br>`,
            `<strong>Total Time: ${totalTime.toFixed(1)}s</strong>`,
          ].join('');
          timerElement.style.background = '#e8f5e8';
          timerElement.style.borderColor = '#4caf50';
        } else if (progress.status === 'cancelled') {
          timerElement.innerHTML = [
            '<strong>❌ Repair Cancelled</strong><br>',
            `Started: ${startTime.toLocaleTimeString()}<br>`,
            `Cancelled: ${endTime.toLocaleTimeString()}<br>`,
            `<strong>Total Time: ${totalTime.toFixed(1)}s</strong>`,
          ].join('');
          timerElement.style.background = '#fff3cd';
          timerElement.style.borderColor = '#ffc107';
        } else if (progress.status === 'error') {
          timerElement.innerHTML = [
            '<strong>⛔ Repair Failed</strong><br>',
            `Started: ${startTime.toLocaleTimeString()}<br>`,
            `Ended: ${endTime.toLocaleTimeString()}<br>`,
            `<strong>Total Time: ${totalTime.toFixed(1)}s</strong>`,
          ].join('');
          timerElement.style.background = '#ffeaea';
          timerElement.style.borderColor = '#f44336';
        }
      }

      await new Promise(resolve => setTimeout(resolve, 500));
    } catch (error) {
      if (error.name === 'AbortError') {
        return lastProgress;
      }
      console.error('Repair progress polling error:', error);
      break;
    }
  }

  return lastProgress;
}

export async function runDataValidation() {
  const statusDiv = document.getElementById('validationStatus');
  const resultsDiv = document.getElementById('validationResults');
  const summaryDiv = document.getElementById('validationSummary');
  const detailsDiv = document.getElementById('validationDetails');
  const summaryHeader = document.getElementById('validationSummaryHeader');
  const detailsHeader = document.getElementById('validationDetailsHeader');
  const button = document.getElementById('validateBtn');
  const repairBtn = document.getElementById('repairBtn');

  validationCancelRequested = false;
  currentValidationJobId = null;

  // Show loading state with cancel button and timer
  button.textContent = '⏸️ Running Validation (click to cancel)';
  button.style.background = '#ffc107';
  button.onclick = cancelValidation;
  if (repairBtn) {
    repairBtn.style.display = 'inline-block';
    repairBtn.disabled = true;
  }

  // Start timer
  const startTime = new Date();
  const timerElement = document.createElement('div');
  timerElement.id = 'validationTimer';
  timerElement.style.cssText =
    'background: #e3f2fd; border: 1px solid #2196f3; border-radius: 5px; padding: 15px; margin-bottom: 15px; text-align: center; font-family: monospace; box-shadow: 0 2px 4px rgba(0,0,0,0.1);';

  statusDiv.innerHTML = '';
  statusDiv.appendChild(timerElement);
  resultsDiv.style.display = 'none';
  summaryDiv.innerHTML = '';
  detailsDiv.innerHTML = '';
  summaryDiv.style.display = 'none';
  if (summaryHeader) summaryHeader.style.display = 'none';
  if (detailsHeader) detailsHeader.style.display = 'none';

  // Update timer every 100ms - NO FAKE PROGRESS
  const timerInterval = setInterval(() => {
    const elapsed = (new Date() - startTime) / 1000;

    timerElement.innerHTML = `
            <strong>⏱️ Validation Timer</strong><br>
            Started: ${startTime.toLocaleTimeString()}<br>
            Elapsed: ${elapsed.toFixed(1)}s<br>
            <span id="validationProgress" style="font-size: 0.9em; color: #666;">Running validation...</span>
        `;
  }, 100);

  const controller = new AbortController();
  currentValidationController = controller;

  try {
    // Set flag to track running validation
    currentValidationController = true;

    const response = await fetch(`${getPluginPath()}/api/validate-schemas`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      signal: controller.signal,
    });
    console.log('✅ Fetch completed, response status:', response.status);

    const result = await response.json();

    if (!response.ok) {
      throw new Error(result.message || 'Validation failed');
    }

    let finalProgress = null;

    // Start polling for progress if we got a jobId
    if (result.jobId) {
      console.log('📋 Starting progress polling for job:', result.jobId);
      currentValidationJobId = result.jobId;
      clearInterval(timerInterval); // Stop the basic timer since we're switching to progress polling
      finalProgress = await pollValidationProgress(
        result.jobId,
        timerElement,
        startTime,
        controller.signal
      );
    } else {
      // Old style response without polling
      console.log('⚠️ No jobId in response, using old style completion');
    }

    // Stop timer and show final time
    clearInterval(timerInterval);
    const endTime = new Date();
    const totalTime = (endTime - startTime) / 1000;

    const summaryData = finalProgress?.result || result;
    const overallTotal = finalProgress?.total ?? summaryData?.totalFiles ?? 0;

    if (summaryData) {
      const totalFiles = summaryData.totalFiles ?? 0;
      const totalVessels = summaryData.totalVessels ?? 0;
      const correctSchemas = summaryData.correctSchemas ?? 0;
      const violations = summaryData.violations ?? 0;
      const isCancelled = Boolean(
        summaryData.cancelled ||
          summaryData.error === 'Validation cancelled by user'
      );
      const isSuccess = Boolean(summaryData.success && !isCancelled);
      const errorMessage = summaryData.error || summaryData.message || '';
      const header = isCancelled
        ? '❌ Validation Cancelled'
        : isSuccess
          ? '⏱️ Validation Complete'
          : '⛔ Validation Failed';
      const background = isCancelled
        ? '#fff3cd'
        : isSuccess
          ? '#e8f5e8'
          : '#ffeaea';
      const border = isCancelled
        ? '#ffc107'
        : isSuccess
          ? '#4caf50'
          : '#f44336';
      const successRate =
        totalFiles > 0
          ? ((correctSchemas / totalFiles) * 100).toFixed(1)
          : '0.0';

      const filesLabel = isCancelled ? 'Processed files' : 'Total files';
      const vesselsLabel = isCancelled
        ? 'Vessels encountered'
        : 'Total vessels';
      const correctLabel = isCancelled
        ? 'Correct schemas (processed)'
        : 'Correct schemas';
      const violationsLabel = isCancelled
        ? 'Detected schema issues'
        : 'Schema violations';

      const statsLines = [
        `<div style="display: flex; justify-content: space-between; margin-bottom: 6px;"><span>📁 <strong>${filesLabel}:</strong></span> <span style="font-weight: bold;">${totalFiles.toLocaleString()}${isCancelled && overallTotal ? ` / ${overallTotal.toLocaleString()}` : isSuccess ? ' (100%)' : ''}</span></div>`,
        `<div style="display: flex; justify-content: space-between; margin-bottom: 6px;"><span>🚢 <strong>${vesselsLabel}:</strong></span> <span style="font-weight: bold;">${totalVessels.toLocaleString()}</span></div>`,
        `<div style="display: flex; justify-content: space-between; margin-bottom: 6px;"><span>✅ <strong>${correctLabel}:</strong></span> <span style="font-weight: bold; color: #4caf50;">${correctSchemas.toLocaleString()}</span></div>`,
        `<div style="display: flex; justify-content: space-between; margin-bottom: 6px;"><span>❌ <strong>${violationsLabel}:</strong></span> <span style="font-weight: bold; color: ${violations > 0 ? '#f44336' : '#4caf50'};">${violations.toLocaleString()}</span></div>`,
        `<div style="display: flex; justify-content: space-between;"><span>📊 <strong>${isCancelled ? 'Processed success rate' : 'Success rate'}:</strong></span> <span style="font-weight: bold; color: ${parseFloat(successRate) === 100 ? '#4caf50' : '#ff9800'};">${successRate}%</span></div>`,
      ].join('');

      const statusNote =
        isCancelled && overallTotal
          ? '<div style="margin-top: 8px; color: #b36b00;">Cancellation requested during processing. Only partial results are available.</div>'
          : !isSuccess && !isCancelled && errorMessage
            ? `<div style="margin-top: 8px; color: #d32f2f;">${errorMessage}</div>`
            : '';

      timerElement.innerHTML = [
        `<strong>${header}</strong><br>`,
        `Started: ${startTime.toLocaleTimeString()}<br>`,
        `Ended: ${endTime.toLocaleTimeString()}<br>`,
        `<strong>Total Time: ${totalTime.toFixed(1)}s</strong>`,
        `<div style="margin-top: 15px; padding: 12px; background: rgba(255,255,255,0.3); border-radius: 4px; text-align: left; line-height: 1.8; font-size: 14px;">${statsLines}</div>`,
        statusNote,
      ]
        .filter(Boolean)
        .join('');
      timerElement.style.background = background;
      timerElement.style.borderColor = border;

      if (isSuccess || isCancelled) {
        const hasViolations =
          Array.isArray(summaryData.violationDetails) &&
          summaryData.violationDetails.length > 0;

        if (hasViolations) {
          if (summaryHeader) {
            summaryHeader.style.display = 'none';
          }
          summaryDiv.style.display = 'none';
          summaryDiv.innerHTML = '';

          if (detailsHeader) {
            detailsHeader.style.display = 'block';
            detailsHeader.textContent = `Schema Violations (${violations.toLocaleString()})`;
          }
          detailsDiv.innerHTML = summaryData.violationDetails.join('\n');
          resultsDiv.style.display = 'block';
        } else {
          if (summaryHeader) summaryHeader.style.display = 'none';
          if (detailsHeader) detailsHeader.style.display = 'none';
          summaryDiv.style.display = 'none';
          summaryDiv.innerHTML = '';
          detailsDiv.innerHTML = '';
          resultsDiv.style.display = 'none';
        }

        if (repairBtn) {
          repairBtn.style.display = 'inline-block';
          repairBtn.disabled = violations === 0;
        }
      } else {
        if (summaryHeader) {
          summaryHeader.style.display = 'block';
          summaryHeader.textContent = 'Validation Summary';
        }
        if (detailsHeader) detailsHeader.style.display = 'none';
        summaryDiv.innerHTML = errorMessage
          ? `❌ ${errorMessage}`
          : 'Validation failed.';
        summaryDiv.style.display = summaryDiv.innerHTML ? 'block' : 'none';
        detailsDiv.innerHTML = '';
        resultsDiv.style.display = summaryDiv.innerHTML ? 'block' : 'none';

        if (repairBtn) {
          repairBtn.style.display = 'inline-block';
          repairBtn.disabled = true;
        }
      }
    }
  } catch (error) {
    // Stop timer on error
    clearInterval(timerInterval);
    const endTime = new Date();
    const totalTime = (endTime - startTime) / 1000;

    // Check if it was cancelled
    if (error.name === 'AbortError') {
      // Try to get progress info from the server response if available
      let progressInfo = '';
      let violationCount = 0;
      try {
        if (currentValidationJobId) {
          const progressResponse = await fetch(
            `${getPluginPath()}/api/validate-schemas/progress/${currentValidationJobId}`,
            {
              cache: 'no-store',
            }
          );
          if (progressResponse.ok) {
            const cancelData = await progressResponse.json();
            if (
              cancelData.result &&
              Array.isArray(cancelData.result.violationFiles)
            ) {
              violationCount = cancelData.result.violationFiles.length;
            }
            if (
              cancelData.processed !== undefined &&
              cancelData.total !== undefined
            ) {
              const percentage =
                cancelData.total > 0
                  ? Math.round((cancelData.processed / cancelData.total) * 100)
                  : 0;
              progressInfo = `<br><strong>Progress: ${cancelData.processed.toLocaleString()}/${cancelData.total.toLocaleString()} files (${percentage}%)</strong>`;
            }
          }
        }
      } catch (parseError) {
        // Ignore parse errors, just show basic cancellation
      }

      timerElement.innerHTML = `
                <strong>❌ Validation Cancelled</strong><br>
                Started: ${startTime.toLocaleTimeString()}<br>
                Cancelled: ${endTime.toLocaleTimeString()}<br>
                <strong>Time: ${totalTime.toFixed(1)}s</strong>${progressInfo}
            `;
      timerElement.style.background = '#fff3cd';
      timerElement.style.borderColor = '#ffc107';

      if (repairBtn) {
        repairBtn.style.display = 'inline-block';
        repairBtn.disabled = violationCount === 0;
      }
    } else {
      timerElement.innerHTML = `
                <strong>⏱️ Validation Failed</strong><br>
                Started: ${startTime.toLocaleTimeString()}<br>
                Failed: ${endTime.toLocaleTimeString()}<br>
                <strong>Total Time: ${totalTime.toFixed(1)}s</strong>
            `;
      timerElement.style.background = '#ffeaea';
      timerElement.style.borderColor = '#f44336';
      console.error('Validation error:', error);
    }

    if (repairBtn) {
      repairBtn.style.display = 'inline-block';
      repairBtn.disabled = true;
    }
  } finally {
    currentValidationJobId = null;
    validationCancelRequested = false;
    currentValidationController = null;
    button.onclick = runDataValidation;
    button.disabled = false;
    button.textContent = '🔍 Run Schema Validation';
    button.style.background = '';
  }
}

export async function repairSchemas() {
  const statusDiv = document.getElementById('validationStatus');
  const repairBtn = document.getElementById('repairBtn');

  // If a job is already running, treat the click as a cancel request
  if (currentRepairJobId || currentRepairController) {
    cancelRepair();
    return;
  }

  repairCancelRequested = false;
  currentRepairJobId = null;

  repairBtn.textContent = '⏸️ Repairing Schemas (click to cancel)';
  repairBtn.style.background = '#ffc107';
  repairBtn.onclick = cancelRepair;
  repairBtn.disabled = false;

  const startTime = new Date();
  const timerElement = document.createElement('div');
  timerElement.id = 'repairTimer';
  timerElement.style.cssText =
    'background: #e3f2fd; border: 1px solid #2196f3; border-radius: 5px; padding: 10px; margin-bottom: 15px; text-align: center; font-family: monospace;';
  timerElement.innerHTML = `
        <strong>🔧 Repair Timer</strong><br>
        Started: ${startTime.toLocaleTimeString()}<br>
        Elapsed: 0.0s<br>
        <span style="font-size: 0.9em; color: #666;">Preparing repair job...</span>
    `;

  statusDiv.innerHTML = '';
  statusDiv.appendChild(timerElement);

  const timerInterval = setInterval(() => {
    const elapsed = (new Date() - startTime) / 1000;
    timerElement.innerHTML = `
            <strong>🔧 Repair Timer</strong><br>
            Started: ${startTime.toLocaleTimeString()}<br>
            Elapsed: ${elapsed.toFixed(1)}s<br>
            <span style="font-size: 0.9em; color: #666;">Preparing repair job...</span>
        `;
  }, 100);

  let finalProgress = null;

  try {
    // Set flag to track running repair
    currentRepairController = true;

    const response = await fetch(`${getPluginPath()}/api/repair-schemas`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    const result = await response.json();

    if (!response.ok) {
      throw new Error(result.message || 'Repair failed');
    }

    if (!result.jobId) {
      timerElement.innerHTML =
        '<strong>❌ Repair response missing job ID</strong>';
      timerElement.style.background = '#ffeaea';
      timerElement.style.borderColor = '#f44336';
      return;
    }

    currentRepairJobId = result.jobId;
    currentRepairController = new AbortController();

    clearInterval(timerInterval);
    finalProgress = await pollRepairProgress(
      result.jobId,
      timerElement,
      startTime,
      currentRepairController.signal
    );

    if (finalProgress && finalProgress.result) {
      const {
        repairedFiles,
        backedUpFiles,
        skippedFiles,
        quarantinedFiles,
        errors,
        message,
      } = finalProgress.result;
      const summaryLines = [
        `<strong>${message || 'Repair summary'}</strong>`,
        `✅ Repaired: ${repairedFiles.toLocaleString()}`,
        `📦 Backups created: ${backedUpFiles.toLocaleString()}`,
        `⏭️ Skipped (already clean): ${skippedFiles.length.toLocaleString()}`,
        `🚫 Quarantined: ${quarantinedFiles.length.toLocaleString()}`,
        `⚠️ Errors: ${errors.length.toLocaleString()}`,
      ];

      timerElement.innerHTML = summaryLines.join('<br>');

      if (errors.length > 0) {
        const errorList = document.createElement('div');
        errorList.style.cssText =
          'margin-top: 10px; background: #fff3f3; border: 1px solid #f44336; padding: 10px; font-size: 12px; max-height: 150px; overflow-y: auto;';
        errorList.innerHTML = errors.map(err => `❌ ${err}`).join('<br>');
        timerElement.appendChild(errorList);
      }

      if (finalProgress.status === 'completed') {
        timerElement.style.background = '#e8f5e8';
        timerElement.style.borderColor = '#4caf50';
      } else if (finalProgress.status === 'cancelled') {
        timerElement.style.background = '#fff3cd';
        timerElement.style.borderColor = '#ffc107';
      } else if (finalProgress.status === 'error') {
        timerElement.style.background = '#ffeaea';
        timerElement.style.borderColor = '#f44336';
      }
    }
  } catch (error) {
    clearInterval(timerInterval);
    timerElement.innerHTML = `<strong>❌ Repair Error</strong><br>${error.message}`;
    timerElement.style.background = '#ffeaea';
    timerElement.style.borderColor = '#f44336';
    console.error('Repair error:', error);
  } finally {
    clearInterval(timerInterval);
    currentRepairJobId = null;
    repairCancelRequested = false;

    if (currentRepairController) {
      currentRepairController.abort();
      currentRepairController = null;
    }

    repairBtn.textContent = '🔧 Repair Schema Violations';
    repairBtn.style.background = '';
    repairBtn.onclick = repairSchemas;
    repairBtn.disabled = false;
  }
}
