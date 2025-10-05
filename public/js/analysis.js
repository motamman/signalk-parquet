import { getPluginPath } from './utils.js';

let currentAnalysisId = null;

export async function testClaudeConnection() {
  const btn = document.getElementById('testConnectionBtn');
  const result = document.getElementById('claudeConnectionResult');

  btn.disabled = true;
  btn.innerHTML = '🔄 Testing...';
  result.innerHTML = '';

  try {
    const response = await fetch(
      `${getPluginPath()}/api/analyze/test-connection`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      }
    );

    const data = await response.json();

    if (data.success) {
      result.innerHTML = `<div style="color: green; padding: 10px; background: #d4edda; border-radius: 5px;">
        ✅ Claude API connection successful!<br>
        Model: ${data.model}<br>
        Response time: ${data.responseTime}ms
    </div>`;
    } else {
      result.innerHTML = `<div style="color: red; padding: 10px; background: #f8d7da; border-radius: 5px;">
        ❌ Connection failed: ${data.error}
    </div>`;
    }
  } catch (error) {
    result.innerHTML = `<div style="color: red; padding: 10px; background: #f8d7da; border-radius: 5px;">
    ❌ Error: ${error.message}
</div>`;
  } finally {
    btn.disabled = false;
    btn.innerHTML = '🔗 Test Claude Connection';
  }
}

// ===========================================
// VESSEL CONTEXT FUNCTIONS
// ===========================================

// Load vessel context when AI tab is initialized
export async function loadVesselContext() {
  try {
    console.log('Loading vessel context...');
    const response = await fetch(`${getPluginPath()}/api/vessel-context`);
    console.log('Vessel context response status:', response.status);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const data = await response.json();
    console.log('Vessel context data:', data);

    if (data.success && data.data) {
      const context = data.data;

      // Update auto-extracted vessel info display
      displayVesselInfo(context.vesselInfo);

      // Update custom context textarea
      const customContextTextarea = document.getElementById(
        'customVesselContext'
      );
      if (customContextTextarea) {
        customContextTextarea.value = context.customContext || '';
      }

      // Show last updated info
      updateVesselContextStatus(
        `Last updated: ${new Date(context.lastUpdated).toLocaleString()}`
      );
    } else {
      console.log('No vessel context data found, showing empty state');
      displayVesselInfo({});
      updateVesselContextStatus(
        'No vessel context found - click Refresh to extract from SignalK'
      );
    }
  } catch (error) {
    console.error('Error loading vessel context:', error);
    const autoInfoDiv = document.getElementById('autoVesselInfo');
    if (autoInfoDiv) {
      autoInfoDiv.innerHTML = `<div style="color: red;">Error loading vessel context: ${error.message}</div>`;
    }
    updateVesselContextStatus(
      'Error loading vessel context: ' + error.message,
      true
    );
  }
}

// Display vessel information in the UI
function displayVesselInfo(vesselInfo) {
  const autoInfoDiv = document.getElementById('autoVesselInfo');
  if (!autoInfoDiv) return;

  if (!vesselInfo || Object.keys(vesselInfo).length === 0) {
    autoInfoDiv.innerHTML = `
    <div style="color: #666; font-style: italic; text-align: center; padding: 20px;">
        No vessel information found.<br>
        <small>Click "Refresh from SignalK" to extract vessel data automatically.</small>
    </div>`;
    return;
  }

  const sections = [];

  // Basic identification
  if (vesselInfo.name || vesselInfo.callsign || vesselInfo.mmsi) {
    const items = [];
    if (vesselInfo.name)
      items.push(`<strong>Name:</strong> ${vesselInfo.name}`);
    if (vesselInfo.callsign)
      items.push(`<strong>Call Sign:</strong> ${vesselInfo.callsign}`);
    if (vesselInfo.mmsi)
      items.push(`<strong>MMSI:</strong> ${vesselInfo.mmsi}`);
    if (vesselInfo.flag)
      items.push(`<strong>Flag:</strong> ${vesselInfo.flag}`);
    sections.push(
      `<div><strong>🆔 Identification:</strong> ${items.join(', ')}</div>`
    );
  }

  // Physical characteristics
  const physical = [];

  // Handle length - could be number or object with overall property
  if (vesselInfo.length) {
    const lengthValue =
      typeof vesselInfo.length === 'object' && vesselInfo.length.overall
        ? vesselInfo.length.overall
        : vesselInfo.length;
    if (lengthValue) physical.push(`${lengthValue}m LOA`);
  }

  if (vesselInfo.beam) physical.push(`${vesselInfo.beam}m beam`);

  // Handle draft - could be number or object with maximum property
  if (vesselInfo.draft) {
    const draftValue =
      typeof vesselInfo.draft === 'object' && vesselInfo.draft.maximum
        ? vesselInfo.draft.maximum
        : vesselInfo.draft;
    if (draftValue) physical.push(`${draftValue}m draft`);
  }

  if (vesselInfo.height) physical.push(`${vesselInfo.height}m height`);
  if (vesselInfo.displacement)
    physical.push(`${vesselInfo.displacement}t displacement`);
  if (physical.length > 0) {
    sections.push(
      `<div><strong>📏 Physical:</strong> ${physical.join(', ')}</div>`
    );
  }

  // Vessel type
  if (vesselInfo.vesselType) {
    sections.push(
      `<div><strong>🚢 Type:</strong> ${vesselInfo.vesselType}</div>`
    );
  }

  // Technical specs
  const technical = [];
  if (vesselInfo.grossTonnage) technical.push(`${vesselInfo.grossTonnage} GT`);
  if (vesselInfo.netTonnage) technical.push(`${vesselInfo.netTonnage} NT`);
  if (vesselInfo.deadWeight) technical.push(`${vesselInfo.deadWeight}t DWT`);
  if (technical.length > 0) {
    sections.push(
      `<div><strong>⚖️ Tonnage:</strong> ${technical.join(', ')}</div>`
    );
  }

  // Build info
  const build = [];
  if (vesselInfo.builder) build.push(`Built by ${vesselInfo.builder}`);
  if (vesselInfo.buildYear) build.push(`in ${vesselInfo.buildYear}`);
  if (vesselInfo.hullNumber) build.push(`(Hull: ${vesselInfo.hullNumber})`);
  if (build.length > 0) {
    sections.push(`<div><strong>🔨 Build:</strong> ${build.join(' ')}</div>`);
  }

  if (sections.length === 0) {
    autoInfoDiv.innerHTML = `<div style="color: #666; font-style: italic;">No vessel details available - try refreshing from SignalK</div>`;
  } else {
    autoInfoDiv.innerHTML = sections.join('<br style="margin: 8px 0;">');
  }
}

// Refresh vessel information from SignalK
export async function refreshVesselInfo() {
  const btn = event.target;
  btn.disabled = true;
  btn.innerHTML = '🔄 Refreshing...';

  try {
    console.log('Refreshing vessel info from SignalK...');
    const response = await fetch(
      `${getPluginPath()}/api/vessel-context/refresh`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      }
    );
    console.log('Refresh response status:', response.status);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const data = await response.json();
    console.log('Refresh data:', data);

    if (data.success && data.data) {
      displayVesselInfo(data.data.vesselInfo);
      updateVesselContextStatus(
        'Vessel information refreshed from SignalK data'
      );
    } else {
      console.error('Refresh failed:', data);
      updateVesselContextStatus(
        'Failed to refresh: ' + (data.error || 'Unknown error'),
        true
      );
    }
  } catch (error) {
    console.error('Error refreshing vessel info:', error);
    updateVesselContextStatus(
      'Error refreshing vessel information: ' + error.message,
      true
    );
  } finally {
    btn.disabled = false;
    btn.innerHTML = '🔄 Refresh from SignalK';
  }
}

// Save vessel context (both auto-extracted and custom)
export async function saveVesselContext() {
  const btn = event.target;
  btn.disabled = true;
  btn.innerHTML = '💾 Saving...';

  try {
    const customContext = document.getElementById('customVesselContext').value;

    const response = await fetch(`${getPluginPath()}/api/vessel-context`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        customContext: customContext,
      }),
    });

    const data = await response.json();

    if (data.success) {
      updateVesselContextStatus('Context saved successfully ✅');
    } else {
      updateVesselContextStatus(
        'Failed to save: ' + (data.error || 'Unknown error'),
        true
      );
    }
  } catch (error) {
    console.error('Error saving vessel context:', error);
    updateVesselContextStatus('Error saving context: ' + error.message, true);
  } finally {
    btn.disabled = false;
    btn.innerHTML = '💾 Save Context';
  }
}

// Preview Claude context
export async function previewClaudeContext() {
  try {
    console.log('Generating Claude context preview...');
    const response = await fetch(
      `${getPluginPath()}/api/vessel-context/claude-preview`
    );
    console.log('Preview response status:', response.status);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const data = await response.json();
    console.log('Preview data:', data);

    if (data.success && data.data) {
      const modal = document.getElementById('contextPreviewModal');
      const content = document.getElementById('contextPreviewContent');

      content.textContent = data.data.contextText;
      modal.style.display = 'block';
    } else {
      console.error('Preview failed:', data);
      updateVesselContextStatus(
        'Failed to generate preview: ' + (data.error || 'Unknown error'),
        true
      );
    }
  } catch (error) {
    console.error('Error previewing context:', error);
    updateVesselContextStatus(
      'Error generating preview: ' + error.message,
      true
    );
  }
}

// Close context preview modal
export function closeContextPreview() {
  const modal = document.getElementById('contextPreviewModal');
  modal.style.display = 'none';
}

// Close modals when clicking outside
window.onclick = function (event) {
  const contextModal = document.getElementById('contextPreviewModal');
  const historyModal = document.getElementById('analysisHistoryModal');
  const viewModal = document.getElementById('analysisViewModal');

  if (event.target === contextModal) {
    contextModal.style.display = 'none';
  } else if (event.target === historyModal) {
    historyModal.style.display = 'none';
  } else if (event.target === viewModal) {
    viewModal.style.display = 'none';
  }
};

// Update vessel context status message
function updateVesselContextStatus(message, isError = false) {
  const statusDiv = document.getElementById('vesselContextStatus');
  if (statusDiv) {
    statusDiv.innerHTML = `<span style="color: ${isError ? 'red' : 'green'};">${message}</span>`;

    // Clear status after 5 seconds
    setTimeout(() => {
      statusDiv.innerHTML = '';
    }, 5000);
  }
}

// Toggle vessel context section
export function toggleVesselContext() {
  const content = document.getElementById('vesselContextContent');
  const icon = document.getElementById('vesselContextToggleIcon');

  if (content.style.display === 'none') {
    content.style.display = 'block';
    icon.textContent = '▼';
  } else {
    content.style.display = 'none';
    icon.textContent = '▶';
  }
}

// ===========================================
// END VESSEL CONTEXT FUNCTIONS
// ===========================================

// Load analysis templates and populate UI
export async function loadAnalysisTemplates() {
  try {
    const response = await fetch(`${getPluginPath()}/api/analyze/templates`);
    const data = await response.json();

    if (data.success && data.templates) {
      // Flatten template categories into a single array
      const allTemplates = [];
      data.templates.forEach(category => {
        if (category.templates) {
          allTemplates.push(...category.templates);
        }
      });
      populateTemplateCards(allTemplates);
      populateTemplateDropdown(allTemplates);
    }
  } catch (error) {
    console.error('Error loading analysis templates:', error);
  }
}

// Populate template dropdown
function populateTemplateDropdown(templates) {
  const select = document.getElementById('analysisTemplate');
  if (!select) {
    console.warn(
      'analysisTemplate select element not found; skipping dropdown population.'
    );
    return;
  }

  const templateLookup = new Map();

  select.innerHTML = '<option value="">Select a template...</option>';

  templates.forEach(template => {
    const option = document.createElement('option');
    option.value = template.id;
    option.textContent = `${template.icon} ${template.name}`;
    templateLookup.set(template.id, template);
    select.appendChild(option);
  });

  select.onchange = event => {
    const templateId = event.target.value;
    if (!templateId) {
      return;
    }

    const selected = templateLookup.get(templateId);
    if (!selected) {
      return;
    }

    const defaultPath = selected.defaultPath || selected.path || '';
    const promptField = document.getElementById('customPrompt');
    if (promptField && selected.prompt) {
      promptField.value = selected.prompt;
    }

    if (defaultPath) {
      runQuickAnalysis(defaultPath);
    } else {
      alert(
        'This template does not define a default data path. Select paths manually, adjust the prompt if needed, then run a custom analysis.'
      );
    }
  };
}

function populateTemplateCards(templates) {
  const container = document.getElementById('analysisTemplateCards');
  if (!container) {
    return;
  }

  if (!Array.isArray(templates) || templates.length === 0) {
    container.innerHTML =
      '<div style="color: #666; font-style: italic;">No templates available yet.</div>';
    return;
  }

  container.innerHTML = templates
    .map(template => {
      const defaultPath = template.defaultPath || template.path || '';
      const buttonHtml = defaultPath
        ? `
            <button type="button" onclick="runQuickAnalysis('${defaultPath}')" style="background: #2196F3; color: white; border: none; padding: 8px 12px; border-radius: 4px; cursor: pointer; font-size: 0.9em;">
                🚀 Run Template
            </button>
        `
        : '';

      return `
        <div style="background: #f8f9fa; border: 1px solid #e0e0e0; border-radius: 8px; padding: 15px; margin-bottom: 15px;">
            <div style="display: flex; align-items: center; margin-bottom: 10px;">
                <span style="font-size: 1.5em; margin-right: 10px;">${template.icon || '📊'}</span>
                <h4 style="margin: 0; font-size: 1.1em;">${template.name}</h4>
            </div>
            <p style="margin: 0 0 10px 0; color: #555; font-size: 0.95em;">${template.description || 'Run this analysis to explore your data.'}</p>
            ${buttonHtml}
        </div>
        `;
    })
    .join('');
}

// Run quick analysis WITHOUT templates - just analyze data directly
export async function runQuickAnalysis(dataPath) {
  const result = document.getElementById('analysisResults');
  const content = document.getElementById('analysisContent');

  result.style.display = 'block';
  content.innerHTML =
    '<div style="text-align: center; padding: 20px;">🔄 Running direct analysis...</div>';

  try {
    // Skip templates entirely - use custom analysis with generic prompt
    const analysisRequest = {
      dataPath,
      analysisType: 'custom',
      customPrompt: `Analyze this maritime sensor data and provide insights. Focus on:
1. Data patterns and trends over time
2. Any anomalies or unusual readings  
3. Statistical summary of the data
4. Practical insights for maritime operations
5. Data quality assessment

Provide actionable insights based on what you observe in the data.`,
      timeRange: {
        start: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString(), // Last 7 days
        end: new Date().toISOString(),
      },
    };

    console.log(`🚀 Running template-free analysis for: ${dataPath}`);

    const response = await fetch(`${getPluginPath()}/api/analyze`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(analysisRequest),
    });

    const data = await response.json();

    if (data.success && data.data) {
      displayAnalysisResult(data.data);
    } else {
      content.innerHTML = `<div style="color: red;">❌ Analysis failed: ${data.error}</div>`;
    }
  } catch (error) {
    content.innerHTML = `<div style="color: red;">❌ Error: ${error.message}</div>`;
  }
}

// Get selected data paths
export function getSelectedDataPaths() {
  const checkboxes = document.querySelectorAll(
    '#analysisDataPathContainer input[type="checkbox"]:checked'
  );
  return Array.from(checkboxes).map(checkbox => checkbox.value);
}

// Update selected path count
export function updateSelectedPathCount() {
  const selectedPaths = getSelectedDataPaths();
  document.getElementById('selectedPathCount').textContent =
    selectedPaths.length;
}

// Handle path checkbox change
export function handlePathCheckboxChange() {
  updateSelectedPathCount();
}

// Select all paths
export function selectAllPaths() {
  const checkboxes = document.querySelectorAll(
    '#analysisDataPathContainer input[type="checkbox"]'
  );
  checkboxes.forEach(checkbox => (checkbox.checked = true));
  updateSelectedPathCount();
}

// Clear all paths
export function clearAllPaths() {
  const checkboxes = document.querySelectorAll(
    '#analysisDataPathContainer input[type="checkbox"]'
  );
  checkboxes.forEach(checkbox => (checkbox.checked = false));
  updateSelectedPathCount();
}

// Get path icon based on path name
function getPathIcon(path) {
  let icon = '📊';
  if (path.includes('wind')) icon = '💨';
  else if (path.includes('navigation')) icon = '🧭';
  else if (path.includes('position')) icon = '📍';
  else if (path.includes('temperature')) icon = '🌡️';
  else if (path.includes('battery') || path.includes('electrical')) icon = '🔋';
  else if (path.includes('command')) icon = '⚙️';
  return icon;
}

// Populate analysis path checkboxes
export function populateAnalysisPathCheckboxes(paths) {
  const container = document.getElementById('analysisDataPathContainer');

  if (!paths || paths.length === 0) {
    container.innerHTML =
      '<div style="color: #666; font-style: italic;">No data paths available</div>';
    return;
  }

  container.innerHTML = ''; // Clear loading message

  paths.forEach((pathInfo, _index) => {
    const checkboxDiv = document.createElement('div');
    checkboxDiv.className = 'path-checkbox-row';

    const pathInfoDiv = document.createElement('div');
    pathInfoDiv.className = 'path-info';

    const iconSpan = document.createElement('span');
    iconSpan.className = 'path-icon';
    iconSpan.textContent = getPathIcon(pathInfo.path);

    const label = document.createElement('label');
    label.htmlFor = `path_${pathInfo.path.replace(/[^a-zA-Z0-9]/g, '_')}`;
    label.textContent = `${pathInfo.path} (${pathInfo.fileCount} files)`;
    label.style.cssText = 'cursor: pointer; margin: 0;';

    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.value = pathInfo.path;
    checkbox.id = `path_${pathInfo.path.replace(/[^a-zA-Z0-9]/g, '_')}`;
    checkbox.onchange = handlePathCheckboxChange;
    checkbox.className = 'path-checkbox';

    pathInfoDiv.appendChild(iconSpan);
    pathInfoDiv.appendChild(label);
    checkboxDiv.appendChild(pathInfoDiv);
    checkboxDiv.appendChild(checkbox);
    container.appendChild(checkboxDiv);
  });

  updateSelectedPathCount();
}

// Global variable to track current analysis request
let currentAnalysisController = null;

// Cancel current analysis
export function cancelAnalysis() {
  if (currentAnalysisController) {
    currentAnalysisController.abort();
    currentAnalysisController = null;
  }
}

// Run custom analysis
export async function runCustomAnalysis() {
  const runButton =
    document.getElementById('runAnalysisBtn') ||
    document.querySelector('button[onclick="runCustomAnalysis()"]');

  // If already running, cancel the current analysis
  if (currentAnalysisController) {
    cancelAnalysis();
    return;
  }
  const selectedPaths = getSelectedDataPaths();
  const customPrompt = document.getElementById('customPrompt').value;
  const startDate = document.getElementById('analysisStartDate').value;
  const endDate = document.getElementById('analysisEndDate').value;
  const aggregationMethod = document.getElementById('aggregationMethod').value;
  const resolution = document.getElementById('resolution').value;
  const claudeModel = document.getElementById('claudeModelMain').value;
  const enableDatabaseAccess = document.getElementById(
    'enableDatabaseAccess'
  ).checked;

  // Skip path validation in database access mode - Claude can access all data
  if (!enableDatabaseAccess && (!selectedPaths || selectedPaths.length === 0)) {
    alert('Please select at least one data path');
    return;
  }

  const result = document.getElementById('analysisResults');
  const content = document.getElementById('analysisContent');

  result.style.display = 'block';

  // Start timer
  const startTime = new Date();
  const timerElement = document.createElement('div');
  timerElement.id = 'analysisTimer';
  timerElement.style.cssText =
    'background: #e3f2fd; border: 1px solid #2196f3; border-radius: 5px; padding: 10px; margin-bottom: 15px; text-align: center; font-family: monospace;';

  content.innerHTML = '';
  content.appendChild(timerElement);

  const loadingElement = document.createElement('div');
  loadingElement.style.cssText = 'text-align: center; padding: 20px;';
  loadingElement.innerHTML = ``;
  content.appendChild(loadingElement);

  // Update timer every 100ms
  const timerInterval = setInterval(() => {
    const elapsed = (new Date() - startTime) / 1000;
    timerElement.innerHTML = `
    <strong>⏱️ Analysis Timer</strong><br>
    Prompt sent: ${startTime.toLocaleTimeString()}<br>
    Elapsed: ${elapsed.toFixed(1)}s
`;
  }, 100);

  // Update button to show running state
  const originalButtonText = runButton.textContent;
  runButton.textContent = '⏸️ Running Analysis (click to cancel)';
  runButton.style.background = '#ffc107';

  try {
    // Create abort controller for cancellation
    currentAnalysisController = new AbortController();

    const analysisRequest = {
      dataPath: enableDatabaseAccess
        ? 'database_access_mode'
        : selectedPaths.join(','), // REST API supports comma-separated paths
      analysisType: 'custom',
      customPrompt:
        customPrompt ||
        `Analyze this maritime sensor data and provide insights. Focus on:
1. Data patterns and trends over time
2. Any anomalies or unusual readings
3. Statistical summary of the data
4. Practical insights for maritime operations
5. Data quality assessment

Provide actionable insights based on what you observe in the data.`,
      timeRange: {
        start: startDate
          ? new Date(startDate).toISOString()
          : new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
        end: endDate
          ? new Date(endDate).toISOString()
          : new Date().toISOString(),
      },
      aggregationMethod: aggregationMethod || 'average',
      resolution: resolution || '', // Empty string = Auto
      claudeModel: claudeModel || 'claude-sonnet-4-20250514',
      useDatabaseAccess: enableDatabaseAccess,
    };

    console.log(
      `🚀 Running custom analysis for paths: ${selectedPaths.join(', ')}`
    );

    const response = await fetch(`${getPluginPath()}/api/analyze`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(analysisRequest),
      signal: currentAnalysisController.signal,
    });

    const data = await response.json();

    // Stop timer and show final time
    clearInterval(timerInterval);
    const endTime = new Date();
    const totalTime = (endTime - startTime) / 1000;
    // Debug what we're actually getting
    console.log('Full response data:', JSON.stringify(data, null, 2));

    // Get token usage if available
    let tokenInfo = '';
    if (data.usage) {
      const usage = data.usage;
      tokenInfo = `<br><strong>Tokens: ${usage.input_tokens || 0} in + ${usage.output_tokens || 0} out = ${(usage.input_tokens || 0) + (usage.output_tokens || 0)} total</strong>`;
    } else {
      tokenInfo = `<br><small style="color: #666;">No token usage found in response</small>`;
    }

    timerElement.innerHTML = `
    <strong>⏱️ Analysis Complete</strong><br>
    Started: ${startTime.toLocaleTimeString()}<br>
    Completed: ${endTime.toLocaleTimeString()}<br>
    <strong>Total Time: ${totalTime.toFixed(1)}s</strong>${tokenInfo}
`;
    timerElement.style.background = data.success ? '#e8f5e8' : '#ffeaea';
    timerElement.style.borderColor = data.success ? '#4caf50' : '#f44336';

    // Remove loading message and clear controller
    loadingElement.remove();
    currentAnalysisController = null;

    // Restore button
    runButton.textContent = originalButtonText;
    runButton.style.background = '';

    if (data.success && data.data) {
      displayAnalysisResult(data.data);
    } else {
      const errorElement = document.createElement('div');
      errorElement.style.color = 'red';
      errorElement.innerHTML = `❌ Analysis failed: ${data.error}`;
      content.appendChild(errorElement);
    }
  } catch (error) {
    // Stop timer on error
    clearInterval(timerInterval);
    const endTime = new Date();
    const totalTime = (endTime - startTime) / 1000;

    // Check if it was cancelled
    if (error.name === 'AbortError') {
      timerElement.innerHTML = `
        <strong>❌ Analysis Cancelled</strong><br>
        Started: ${startTime.toLocaleTimeString()}<br>
        Cancelled: ${endTime.toLocaleTimeString()}<br>
        <strong>Time: ${totalTime.toFixed(1)}s</strong>
    `;
      timerElement.style.background = '#fff3cd';
      timerElement.style.borderColor = '#ffc107';
    } else {
      timerElement.innerHTML = `
        <strong>⏱️ Analysis Failed</strong><br>
        Started: ${startTime.toLocaleTimeString()}<br>
        Failed: ${endTime.toLocaleTimeString()}<br>
        <strong>Total Time: ${totalTime.toFixed(1)}s</strong>
    `;
      timerElement.style.background = '#ffeaea';
      timerElement.style.borderColor = '#f44336';
    }

    loadingElement.remove();
    currentAnalysisController = null;

    // Restore button
    runButton.textContent = originalButtonText;
    runButton.style.background = '';

    const errorElement = document.createElement('div');
    errorElement.style.color = 'red';
    errorElement.innerHTML = `❌ Error: ${error.message}`;
    content.appendChild(errorElement);
  }
}

// Convert MMSI numbers to MarineTraffic links
function convertMMSIToLinks(container) {
  const mmsiRegex = /(MMSI:?\s?)(\d{8,9})/gi;

  // Function to recursively process text nodes
  function processTextNodes(node) {
    if (node.nodeType === Node.TEXT_NODE) {
      const text = node.textContent;
      if (mmsiRegex.test(text)) {
        const newText = text.replace(mmsiRegex, (match, prefix, mmsi) => {
          return `${prefix}<a href="https://www.marinetraffic.com/en/ais/details/ships/mmsi:${mmsi}" target="_blank" style="color: #1976d2; text-decoration: underline;" title="View ${mmsi} on MarineTraffic">${mmsi}</a>`;
        });

        // Replace text node with HTML
        const tempDiv = document.createElement('div');
        tempDiv.innerHTML = newText;

        // Replace the text node with new nodes
        const fragment = document.createDocumentFragment();
        while (tempDiv.firstChild) {
          fragment.appendChild(tempDiv.firstChild);
        }
        node.parentNode.replaceChild(fragment, node);
      }
    } else if (node.nodeType === Node.ELEMENT_NODE) {
      // Process child nodes (but skip existing links)
      if (node.tagName.toLowerCase() !== 'a') {
        const children = Array.from(node.childNodes);
        children.forEach(child => processTextNodes(child));
      }
    }
  }

  processTextNodes(container);
}

// Process and embed charts from Claude responses
function processChartRequests(container) {
  console.log('Processing chart requests in container:', container);
  // Look for JSON chart specifications in the text - improved pattern to handle nested objects
  const chartRegex = /```json\s*([\s\S]*?"type":\s*"chart"[\s\S]*?)\s*```/gi;

  // First try processing the entire innerHTML
  const fullText = container.innerHTML || container.textContent || '';
  console.log('Full container text:', fullText.substring(0, 500));
  const fullMatches = [...fullText.matchAll(chartRegex)];
  console.log('Matches in full text:', fullMatches.length);

  // Process all matches found in full text
  let processedCharts = false;
  if (fullMatches.length > 0) {
    let newHTML = fullText;
    fullMatches.forEach((match, index) => {
      let validJsonString = ''; // Declare in outer scope for error handling
      try {
        let jsonString = match[1].trim();
        jsonString = jsonString
          .replace(/&quot;/g, '"')
          .replace(/&amp;/g, '&')
          .replace(/&lt;/g, '<')
          .replace(/&gt;/g, '>');

        // Extract only valid JSON by finding the first complete JSON object
        validJsonString = jsonString;
        try {
          // Find the end of the first complete JSON object
          let braceCount = 0;
          let inString = false;
          let escaped = false;
          let jsonEndIndex = -1;

          for (let i = 0; i < jsonString.length; i++) {
            const char = jsonString[i];

            if (escaped) {
              escaped = false;
              continue;
            }

            if (char === '\\' && inString) {
              escaped = true;
              continue;
            }

            if (char === '"' && !escaped) {
              inString = !inString;
              continue;
            }

            if (!inString) {
              if (char === '{') {
                braceCount++;
              } else if (char === '}') {
                braceCount--;
                if (braceCount === 0) {
                  jsonEndIndex = i + 1;
                  break;
                }
              }
            }
          }

          if (jsonEndIndex > 0) {
            validJsonString = jsonString.substring(0, jsonEndIndex);
            console.log(
              '🔧 FRONTEND - Extracted valid JSON, length:',
              validJsonString.length,
              'vs original:',
              jsonString.length
            );
          }
        } catch (extractError) {
          console.warn(
            '🔧 JSON extraction failed, using original:',
            extractError
          );
        }

        console.log(
          '🔍 FRONTEND - Processing JSON length:',
          validJsonString.length
        );

        const chartSpec = JSON.parse(validJsonString);
        console.log('Parsed chart spec:', chartSpec);

        if (chartSpec.type === 'chart') {
          const chartId = `chart-${Date.now()}-${Math.random().toString(36).substr(2, 9)}-${index}`;
          const chartHTML = `
                <div style="margin: 20px 0; padding: 15px; background: #f8f9fa; border-radius: 8px; border-left: 4px solid #2196F3;">
                    <h4 style="margin: 0 0 15px 0; color: #333;">📊 ${chartSpec.title || 'Chart'}</h4>
                    <div id="${chartId}" style="width: 100%; height: 400px;"></div>
                </div>
            `;
          newHTML = newHTML.replace(match[0], chartHTML);
          processedCharts = true;

          // Queue chart rendering for after DOM update
          setTimeout(() => renderChart(chartId, chartSpec), 300);
        }
      } catch (e) {
        console.error('Failed to parse chart specification:', e);
        console.error('Raw JSON that failed to parse:', validJsonString);

        // Show the problematic area around the error position
        if (e.message.includes('position')) {
          const match = e.message.match(/position (\d+)/);
          if (match) {
            const errorPos = parseInt(match[1]);
            const start = Math.max(0, errorPos - 100);
            const end = Math.min(validJsonString.length, errorPos + 100);
            console.error(
              '🚨 PROBLEM AREA around position',
              errorPos,
              ':',
              validJsonString.substring(start, end)
            );
            console.error(
              '🚨 CHARACTER AT ERROR:',
              validJsonString[errorPos],
              'ASCII:',
              validJsonString.charCodeAt(errorPos)
            );
          }
        }
      }
    });

    if (processedCharts) {
      container.innerHTML = newHTML;
    }
  }

  // Processing complete - legacy text node processing disabled
  // Charts are now processed using direct HTML replacement above
}

// Render chart using Plotly.js (native format)
function renderChart(chartId, chartSpec) {
  console.log('Attempting to render Plotly chart:', chartId, chartSpec);

  const chartDiv = document.getElementById(chartId);
  if (!chartDiv) {
    console.error('Chart div not found:', chartId);
    return;
  }

  console.log('Chart div found:', chartDiv);

  // Handle both new native Plotly format and legacy Chart.js format
  let plotlyData, plotlyLayout, plotlyConfig;

  if (chartSpec.data && chartSpec.layout) {
    // Native Plotly format - use directly
    console.log('Using native Plotly format');
    plotlyData = chartSpec.data;
    plotlyLayout = chartSpec.layout;
    plotlyConfig = chartSpec.config || {};
  } else if (chartSpec.datasets) {
    // Legacy Chart.js format - convert to Plotly
    console.log('Converting Chart.js format to Plotly');

    plotlyData = chartSpec.datasets.map(dataset => ({
      x: chartSpec.labels || [],
      y: dataset.data || [],
      name: dataset.label || '',
      type: 'scatter',
      mode: dataset.borderDash ? 'lines' : 'lines+markers',
      line: {
        color: dataset.borderColor || dataset.backgroundColor || '#2196F3',
        width: dataset.borderWidth || 2,
        dash: dataset.borderDash ? 'dash' : 'solid',
      },
      marker: {
        size: 4,
        color: dataset.borderColor || dataset.backgroundColor || '#2196F3',
      },
      fill: dataset.fill ? 'tonexty' : 'none',
      fillcolor: dataset.backgroundColor || 'rgba(33, 150, 243, 0.1)',
    }));

    plotlyLayout = {
      title: {
        text: chartSpec.title || '',
        font: { size: 16 },
      },
      xaxis: {
        title: chartSpec.xAxisLabel || '',
        showgrid: true,
        zeroline: false,
      },
      yaxis: {
        title: chartSpec.yAxisLabel || '',
        showgrid: true,
        zeroline: false,
      },
      showlegend: true,
      hovermode: 'x unified',
      margin: { l: 60, r: 30, t: 50, b: 50 },
      plot_bgcolor: 'rgba(0,0,0,0)',
      paper_bgcolor: 'rgba(0,0,0,0)',
    };
  } else {
    console.error(
      'Invalid chart specification - no data or datasets found:',
      chartSpec
    );
    return;
  }

  // Default Plotly config
  const defaultConfig = {
    responsive: true,
    displayModeBar: true,
    modeBarButtonsToRemove: ['pan2d', 'select2d', 'lasso2d', 'resetScale2d'],
    displaylogo: false,
  };

  plotlyConfig = { ...defaultConfig, ...plotlyConfig };

  console.log(
    'Creating Plotly chart with data:',
    plotlyData,
    'layout:',
    plotlyLayout
  );

  try {
    Plotly.newPlot(chartId, plotlyData, plotlyLayout, plotlyConfig);
    console.log('Plotly chart created successfully:', chartId);

    return true;
  } catch (error) {
    console.error('Failed to create Plotly chart:', chartId, error);
  }
}

// Display analysis results
function displayAnalysisResult(analysisResult) {
  const content = document.getElementById('analysisContent');

  let html = `
<div style="border-left: 4px solid #667eea; padding-left: 15px; margin-bottom: 20px;">
    <h4 style="color: #667eea; margin: 0 0 10px 0;">📊 Analysis Summary</h4>
    <div style="white-space: pre-wrap; line-height: 1.6;">${analysisResult.analysis}</div>
</div>
    `;

  if (analysisResult.insights && analysisResult.insights.length > 0) {
    html += `
    <div style="margin-bottom: 20px;">
        <h4 style="color: #28a745; margin: 0 0 10px 0;">💡 Key Insights</h4>
        <ul style="padding-left: 20px;">
            ${analysisResult.insights.map(insight => `<li style="margin-bottom: 5px;">${insight}</li>`).join('')}
        </ul>
    </div>
`;
  }

  if (
    analysisResult.recommendations &&
    analysisResult.recommendations.length > 0
  ) {
    html += `
    <div style="margin-bottom: 20px;">
        <h4 style="color: #ffc107; margin: 0 0 10px 0;">🎯 Recommendations</h4>
        <ul style="padding-left: 20px;">
            ${analysisResult.recommendations.map(rec => `<li style="margin-bottom: 5px;">${rec}</li>`).join('')}
        </ul>
    </div>
`;
  }

  if (analysisResult.anomalies && analysisResult.anomalies.length > 0) {
    html += `
    <div style="margin-bottom: 20px;">
        <h4 style="color: #dc3545; margin: 0 0 10px 0;">⚠️ Anomalies Detected</h4>
        <div style="background: #f8f9fa; padding: 10px; border-radius: 5px;">
            ${analysisResult.anomalies
              .map(
                anomaly => `
                <div style="margin-bottom: 8px; padding: 8px; background: white; border-radius: 4px;">
                    <strong>${anomaly.severity.toUpperCase()}</strong> - ${anomaly.description}
                    <br><small style="color: #666;">Timestamp: ${new Date(anomaly.timestamp).toLocaleString()}</small>
                </div>
            `
              )
              .join('')}
        </div>
    </div>
`;
  }

  html += `
<div style="margin-top: 20px; padding-top: 15px; border-top: 1px solid #dee2e6; font-size: 12px; color: #6c757d;">
    <strong>Analysis Metadata:</strong><br>
    Data Path: ${analysisResult.metadata.dataPath}<br>
    Records Analyzed: ${analysisResult.metadata.recordCount}<br>
    Confidence: ${(analysisResult.confidence * 100).toFixed(1)}%<br>
    Data Quality: ${analysisResult.dataQuality}<br>
    Timestamp: ${new Date(analysisResult.timestamp).toLocaleString()}
</div>
    `;

  // Always preserve existing charts and timers, never use innerHTML = which destroys charts
  const resultsDiv = document.createElement('div');
  resultsDiv.innerHTML = html;

  // Clear content but preserve timer and any existing charts
  const children = Array.from(content.children);
  children.forEach(child => {
    // Preserve timer and any Plotly chart containers
    if (
      child.id === 'analysisTimer' ||
      child.querySelector('div[id^="chart-"]') ||
      (child.id && child.id.startsWith('chart-')) ||
      child.querySelector('.plotly-graph-div')
    ) {
      // Keep this element
    } else {
      child.remove();
    }
  });
  content.appendChild(resultsDiv);

  // Process chart requests FIRST (before MMSI link conversion which would break JSON)
  processChartRequests(resultsDiv);
  // Convert MMSI numbers to MarineTraffic links in the new results (after charts are processed)
  convertMMSIToLinks(resultsDiv);

  // Show follow-up section for database access mode and store analysis ID
  if (
    analysisResult.metadata &&
    analysisResult.metadata.useDatabaseAccess &&
    analysisResult.id
  ) {
    currentAnalysisId = analysisResult.id;
    const followUpSection = document.getElementById('followUpSection');
    if (followUpSection) {
      followUpSection.style.display = 'block';
    }
  } else {
    currentAnalysisId = null;
    const followUpSection = document.getElementById('followUpSection');
    if (followUpSection) {
      followUpSection.style.display = 'none';
    }
  }
}

// Open analysis history modal
export async function openAnalysisHistoryModal() {
  const modal = document.getElementById('analysisHistoryModal');
  const content = document.getElementById('analysisHistoryModalContent');

  // Show modal immediately
  modal.style.display = 'block';
  content.innerHTML =
    '<div style="text-align: center; padding: 20px;"><div>🔄 Loading analysis history...</div></div>';

  try {
    const response = await fetch(
      `${getPluginPath()}/api/analyze/history?limit=20`
    );
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const data = await response.json();

    if (data.success && data.data && data.data.length > 0) {
      let html = '<div>';

      data.data.forEach((analysis, _index) => {
        const date = new Date(analysis.timestamp);
        const isRecent = Date.now() - date.getTime() < 24 * 60 * 60 * 1000; // Less than 24 hours

        html += `
            <div style="background: ${isRecent ? '#f0f8ff' : 'white'}; border: 1px solid #ddd; border-radius: 8px; padding: 20px; margin-bottom: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 15px;">
                    <div>
                        <h4 style="margin: 0 0 5px 0; color: #333; font-size: 1.1em;">${analysis.metadata.analysisType || 'Custom Analysis'}</h4>
                        <div style="font-size: 0.9em; color: #666; margin-bottom: 5px;">
                            <strong>Path:</strong> ${analysis.metadata.dataPath}
                        </div>
                        <div style="font-size: 0.85em; color: #888;">
                            ${date.toLocaleString()} ${isRecent ? '(Recent)' : ''}
                        </div>
                    </div>
                    <div style="display: flex; gap: 8px;">
                        <button onclick="viewAnalysis('${analysis.id}')" style="background: #2196F3; color: white; border: none; padding: 8px 12px; border-radius: 4px; cursor: pointer; font-size: 0.9em;">
                            👁️ View
                        </button>
                        <button onclick="deleteAnalysis('${analysis.id}')" style="background: #f44336; color: white; border: none; padding: 8px 12px; border-radius: 4px; cursor: pointer; font-size: 0.9em;">
                            🗑️ Delete
                        </button>
                    </div>
                </div>
                <div style="font-size: 0.9em; color: #555; line-height: 1.4; max-height: 80px; overflow: hidden; position: relative;">
                    ${analysis.analysis.substring(0, 300)}${analysis.analysis.length > 300 ? '...' : ''}
                </div>
                ${
                  analysis.insights && analysis.insights.length > 0
                    ? `
                    <div style="margin-top: 10px; font-size: 0.85em; color: #666;">
                        <strong>Key Insights:</strong> ${analysis.insights.slice(0, 2).join(', ')}${analysis.insights.length > 2 ? '...' : ''}
                    </div>
                `
                    : ''
                }
            </div>
        `;
      });

      html += '</div>';
      content.innerHTML = html;
    } else {
      content.innerHTML = `
        <div style="text-align: center; padding: 40px; color: #666;">
            <div style="font-size: 3em; margin-bottom: 20px;">📈</div>
            <h3>No Analysis History Found</h3>
            <p>Your Claude AI analyses will appear here once you start running them.</p>
        </div>
    `;
    }
  } catch (error) {
    content.innerHTML = `
    <div style="text-align: center; padding: 40px; color: #d32f2f;">
        <div style="font-size: 3em; margin-bottom: 20px;">⚠️</div>
        <h3>Error Loading History</h3>
        <p>${error.message}</p>
    </div>
`;
  }
}

// Close analysis history modal
export function closeAnalysisHistoryModal() {
  document.getElementById('analysisHistoryModal').style.display = 'none';
}

// View individual analysis
export async function viewAnalysis(analysisId) {
  const modal = document.getElementById('analysisViewModal');
  const content = document.getElementById('analysisViewContent');

  // Show modal
  modal.style.display = 'block';
  content.innerHTML =
    '<div style="text-align: center; padding: 20px;">🔄 Loading analysis...</div>';

  try {
    // Find the analysis in the current data (could also fetch individually)
    const historyResponse = await fetch(
      `${getPluginPath()}/api/analyze/history`
    );
    const historyData = await historyResponse.json();

    if (historyData.success && historyData.data) {
      const analysis = historyData.data.find(a => a.id === analysisId);
      if (analysis) {
        displayFullAnalysis(analysis);
      } else {
        content.innerHTML =
          '<div style="text-align: center; padding: 20px; color: #d32f2f;">Analysis not found.</div>';
      }
    }
  } catch (error) {
    content.innerHTML = `<div style="text-align: center; padding: 20px; color: #d32f2f;">Error loading analysis: ${error.message}</div>`;
  }
}

// Display full analysis in modal
function displayFullAnalysis(analysis) {
  const content = document.getElementById('analysisViewContent');
  const title = document.getElementById('analysisViewTitle');

  title.textContent = `🧠 ${analysis.metadata.analysisType || 'Analysis'} - ${new Date(analysis.timestamp).toLocaleDateString()}`;

  let html = `
<div style="margin-bottom: 20px; padding: 15px; background: #f8f9fa; border-radius: 8px; border-left: 4px solid #2196F3;">
    <h4 style="margin: 0 0 10px 0; color: #333;">Analysis Metadata</h4>
    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; font-size: 0.9em;">
        <div><strong>Data Path:</strong> ${analysis.metadata.dataPath}</div>
                <div><strong>Analysis Type:</strong> ${analysis.metadata.analysisType || 'Custom'}</div>
                <div><strong>Date:</strong> ${new Date(analysis.timestamp).toLocaleString()}</div>
                <div><strong>Record Count:</strong> ${analysis.metadata.recordCount || 'N/A'}</div>
                <div><strong>Confidence:</strong> ${Math.round((analysis.confidence || 0) * 100)}%</div>
                <div><strong>Data Quality:</strong> ${analysis.dataQuality || 'N/A'}</div>
            </div>
        </div>

        <div style="margin-bottom: 20px;">
            <h4 style="color: #333; border-bottom: 2px solid #2196F3; padding-bottom: 5px;">🧠 Main Analysis</h4>
            <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; line-height: 1.6; white-space: pre-wrap;">${analysis.analysis}</div>
        </div>
    `;

  if (analysis.insights && analysis.insights.length > 0) {
    html += `
            <div style="margin-bottom: 20px;">
                <h4 style="color: #333; border-bottom: 2px solid #4CAF50; padding-bottom: 5px;">💡 Key Insights</h4>
                <ul style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; line-height: 1.6;">
        `;
    analysis.insights.forEach(insight => {
      html += `<li style="margin-bottom: 8px;">${insight}</li>`;
    });
    html += `</ul></div>`;
  }

  if (analysis.recommendations && analysis.recommendations.length > 0) {
    html += `
            <div style="margin-bottom: 20px;">
                <h4 style="color: #333; border-bottom: 2px solid #FF9800; padding-bottom: 5px;">🎯 Recommendations</h4>
                <ul style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; line-height: 1.6;">
        `;
    analysis.recommendations.forEach(rec => {
      html += `<li style="margin-bottom: 8px;">${rec}</li>`;
    });
    html += `</ul></div>`;
  }

  if (analysis.anomalies && analysis.anomalies.length > 0) {
    html += `
            <div style="margin-bottom: 20px;">
                <h4 style="color: #333; border-bottom: 2px solid #f44336; padding-bottom: 5px;">⚠️ Anomalies Detected</h4>
                <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
        `;
    analysis.anomalies.forEach(anomaly => {
      html += `
                <div style="margin-bottom: 15px; padding: 10px; background: #fff3e0; border-left: 4px solid #ff9800; border-radius: 4px;">
                    <div style="font-weight: bold; margin-bottom: 5px;">${new Date(anomaly.timestamp).toLocaleString()}</div>
                    <div style="margin-bottom: 5px;"><strong>Value:</strong> ${anomaly.value} (Expected: ${anomaly.expectedRange.min} - ${anomaly.expectedRange.max})</div>
                    <div style="margin-bottom: 5px;"><strong>Severity:</strong> ${anomaly.severity} (Confidence: ${Math.round(anomaly.confidence * 100)}%)</div>
                    <div>${anomaly.description}</div>
                </div>
            `;
    });
    html += `</div></div>`;
  }

  // Add copy button
  html += `
        <div style="text-align: center; margin-top: 20px; padding-top: 20px; border-top: 1px solid #ddd;">
            <button onclick="deleteAnalysis('${analysis.id}', true)" style="background: #f44336; color: white; border: none; padding: 12px 24px; border-radius: 6px; cursor: pointer;">
                🗑️ Delete Analysis
            </button>
        </div>
    `;

  content.innerHTML = html;

  // Process chart requests FIRST (before MMSI link conversion which would break JSON)
  processChartRequests(content);
  // Convert MMSI numbers to MarineTraffic links in the modal content (after charts are processed)
  convertMMSIToLinks(content);
}

// Close analysis view modal
export function closeAnalysisViewModal() {
  document.getElementById('analysisViewModal').style.display = 'none';
}

// Delete analysis
export async function deleteAnalysis(analysisId, isFullView = false) {
  try {
    if (
      !confirm(
        'Are you sure you want to delete this analysis? This action cannot be undone.'
      )
    ) {
      return;
    }

    const response = await fetch(
      `${getPluginPath()}/api/analyze/history/${analysisId}`,
      {
        method: 'DELETE',
      }
    );
    const result = await response.json();

    if (result.success) {
      // Close modal if in full view
      if (isFullView) {
        closeAnalysisViewModal();
      }

      // Refresh the history list
      openAnalysisHistoryModal();
    } else {
      alert(`Failed to delete analysis: ${result.error}`);
    }
  } catch (error) {
    console.error('Failed to delete analysis:', error);
    alert('Failed to delete analysis');
  }
}

// Analyze data from Data Paths tab
export async function analyzeDataPath(signalkPath, _directory) {
  // Switch to AI Analysis tab
  showTab('aiAnalysis');

  // First select the path checkbox (before enabling database mode)
  const checkboxId = `path_${signalkPath.replace(/[^a-zA-Z0-9]/g, '_')}`;
  const checkbox = document.getElementById(checkboxId);
  if (checkbox) {
    // Clear all other selections and select only this path
    const allCheckboxes = document.querySelectorAll(
      '#analysisDataPathContainer input[type="checkbox"]'
    );
    allCheckboxes.forEach(cb => (cb.checked = false));
    checkbox.checked = true;
    updateSelectedPathCount();
  }

  // Enable database access mode for path-specific analysis
  const databaseCheckbox = document.getElementById('enableDatabaseAccess');
  if (databaseCheckbox && !databaseCheckbox.checked) {
    databaseCheckbox.checked = true;
    toggleAnalysisMode(); // Update UI to show database mode
  }

  // Set path-specific analysis prompt
  const promptField = document.getElementById('customPrompt');
  if (promptField) {
    promptField.value = `Analyze the data for path: ${signalkPath}

Please provide insights about:
1. Recent data patterns and trends
2. Any anomalies or unusual readings
3. Data quality and consistency
4. Key statistics and ranges
5. Notable changes over time

Focus specifically on the ${signalkPath} sensor data and provide actionable insights.`;
  }

  // Set recent time range for quick analysis
  const startDateField = document.getElementById('analysisStartDate');
  const endDateField = document.getElementById('analysisEndDate');
  if (startDateField && endDateField) {
    const now = new Date();
    const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    endDateField.value = now.toISOString().slice(0, 16);
    startDateField.value = yesterday.toISOString().slice(0, 16);
  }

  // Auto-trigger the analysis immediately
  runCustomAnalysis();
}

// Toggle between analysis modes
export function toggleAnalysisMode() {
  const checkbox = document.getElementById('enableDatabaseAccess');
  const legacyDesc = document.getElementById('legacyModeDesc');
  const databaseDesc = document.getElementById('databaseModeDesc');
  const selectionSection = document.getElementById('selectionOptionsSection');
  const selectionSubtext = document.getElementById('selectionToggleSubtext');

  if (checkbox.checked) {
    // Show database mode description
    legacyDesc.style.display = 'none';
    databaseDesc.style.display = 'block';

    // Grey out selection options for database mode
    selectionSection.style.opacity = '0.5';
    selectionSection.style.pointerEvents = 'none';
    selectionSubtext.textContent = '(Not needed for database access mode)';

    console.log('Switched to Direct Database Access mode');
  } else {
    // Show legacy mode description
    legacyDesc.style.display = 'block';
    databaseDesc.style.display = 'none';

    // Enable selection options for legacy mode
    selectionSection.style.opacity = '1';
    selectionSection.style.pointerEvents = 'auto';
    selectionSubtext.textContent = '(Advanced options for legacy mode)';

    console.log('Switched to Legacy Data Sampling mode');
  }
}

// Toggle selection options visibility
export function toggleSelectionOptions() {
  const container = document.getElementById('selectionOptionsContainer');
  const icon = document.getElementById('selectionToggleIcon');

  if (container.style.display === 'none' || !container.style.display) {
    // Show options
    container.style.display = 'block';
    icon.textContent = '▼';
    console.log('Selection options expanded');
  } else {
    // Hide options
    container.style.display = 'none';
    icon.textContent = '▶';
    console.log('Selection options collapsed');
  }
}

// Initialize AI Analysis tab when it becomes active
export function initializeAIAnalysisTab() {
  // Load vessel context
  loadVesselContext();

  // Load available data paths for both cards and dropdown
  loadAvailableDataPaths();
  loadAnalysisTemplates();

  // Set default date range (last 24 hours)
  const now = new Date();
  const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);

  document.getElementById('analysisEndDate').value = now
    .toISOString()
    .slice(0, 16);
  document.getElementById('analysisStartDate').value = yesterday
    .toISOString()
    .slice(0, 16);

  // Initialize UI state based on default checkbox state
  toggleAnalysisMode();
}

// Load available data paths for analysis
export async function loadAvailableDataPaths() {
  try {
    const response = await fetch(`${getPluginPath()}/api/paths`);
    const data = await response.json();

    if (data.success && data.paths) {
      // Populate custom analysis checkboxes
      populateAnalysisPathCheckboxes(data.paths);
    } else {
      console.error('Failed to load data paths:', data.error);
    }
  } catch (error) {
    console.error('Error loading available data paths:', error);
  }
}

// Load data paths for analysis dropdown
export async function loadDataPathsForAnalysis() {
  try {
    const response = await fetch(`${getPluginPath()}/api/paths`);
    const data = await response.json();

    if (data.success && data.paths) {
      const container = document.getElementById('analysisDataPathContainer');

      if (data.paths.length === 0) {
        container.innerHTML =
          '<div style="color: #666; font-style: italic;">No data paths available</div>';
        return;
      }

      container.innerHTML = ''; // Clear loading message

      data.paths.forEach((pathInfo, _index) => {
        const checkboxDiv = document.createElement('div');
        checkboxDiv.className = 'path-checkbox-row';

        const pathInfoDiv = document.createElement('div');
        pathInfoDiv.className = 'path-info';

        const iconSpan = document.createElement('span');
        iconSpan.className = 'path-icon';
        iconSpan.textContent = getPathIcon(pathInfo.path);

        const label = document.createElement('label');
        label.htmlFor = `path_${pathInfo.path.replace(/[^a-zA-Z0-9]/g, '_')}`;
        label.textContent = `${pathInfo.path} (${pathInfo.fileCount} files)`;
        label.style.cssText = 'cursor: pointer; margin: 0;';

        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.value = pathInfo.path;
        checkbox.id = `path_${pathInfo.path.replace(/[^a-zA-Z0-9]/g, '_')}`;
        checkbox.onchange = handlePathCheckboxChange;
        checkbox.className = 'path-checkbox';

        pathInfoDiv.appendChild(iconSpan);
        pathInfoDiv.appendChild(label);
        checkboxDiv.appendChild(pathInfoDiv);
        checkboxDiv.appendChild(checkbox);
        container.appendChild(checkboxDiv);
      });

      updateSelectedPathCount();
    }
  } catch (error) {
    console.error('Error loading data paths for analysis:', error);
    const container = document.getElementById('analysisDataPathContainer');
    container.innerHTML =
      '<div style="color: #d32f2f;">Error loading paths</div>';
  }
}

// Ask follow-up question to continue conversation
export async function askFollowUpQuestion() {
  const questionTextarea = document.getElementById('followUpQuestion');
  const askButton = document.getElementById('askFollowUpBtn');
  const question = questionTextarea.value.trim();

  if (!question) {
    alert('Please enter a question.');
    return;
  }

  if (!currentAnalysisId) {
    alert('No active conversation. Please run a database analysis first.');
    return;
  }

  // Disable UI during request
  askButton.disabled = true;
  askButton.textContent = '💬 Asking...';
  questionTextarea.disabled = true;

  // Start timer for follow-up
  const startTime = new Date();
  const content = document.getElementById('analysisContent');
  const followUpTimerElement = document.createElement('div');
  followUpTimerElement.id = 'followUpTimer';
  followUpTimerElement.style.cssText =
    'background: #fff3e0; border: 1px solid #ff9800; border-radius: 5px; padding: 10px; margin-bottom: 15px; text-align: center; font-family: monospace;';
  content.appendChild(followUpTimerElement);

  // Update timer every 100ms
  const timerInterval = setInterval(() => {
    const elapsed = (new Date() - startTime) / 1000;
    followUpTimerElement.innerHTML = `
            <strong>⏱️ Follow-up Timer</strong><br>
            Question sent: ${startTime.toLocaleTimeString()}<br>
            Elapsed: ${elapsed.toFixed(1)}s
        `;
  }, 100);

  try {
    const response = await fetch(`${getPluginPath()}/api/analyze/followup`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        conversationId: currentAnalysisId,
        question: question,
      }),
    });

    const result = await response.json();

    // Stop timer and show final time
    clearInterval(timerInterval);
    const endTime = new Date();
    const totalTime = (endTime - startTime) / 1000;

    // Get token usage if available
    let tokenInfo = '';
    if (result.usage) {
      const usage = result.usage;
      tokenInfo = `<br><strong>Tokens: ${usage.input_tokens || 0} in + ${usage.output_tokens || 0} out = ${(usage.input_tokens || 0) + (usage.output_tokens || 0)} total</strong>`;
    }

    followUpTimerElement.innerHTML = `
            <strong>⏱️ Follow-up Complete</strong><br>
            Started: ${startTime.toLocaleTimeString()}<br>
            Completed: ${endTime.toLocaleTimeString()}<br>
            <strong>Total Time: ${totalTime.toFixed(1)}s</strong>${tokenInfo}
        `;
    followUpTimerElement.style.background = result.success
      ? '#e8f5e8'
      : '#ffeaea';
    followUpTimerElement.style.borderColor = result.success
      ? '#4caf50'
      : '#f44336';

    if (result.success) {
      // Display the follow-up response in the analysis content area
      const followUpHtml = `
                <div style="border-left: 4px solid #1976d2; padding-left: 15px; margin-bottom: 20px; background: #f8fdff;">
                    <h4 style="color: #1976d2; margin: 0 0 10px 0;">💬 Follow-up Question</h4>
                    <div style="font-style: italic; color: #666; margin-bottom: 10px;">${question}</div>
                    <div style="white-space: pre-wrap; line-height: 1.6;">${result.data.analysis}</div>
                </div>
            `;

      // Append to existing content
      content.innerHTML += followUpHtml;

      // Create a temporary container with just the new follow-up content
      const tempDiv = document.createElement('div');
      tempDiv.innerHTML = followUpHtml;

      // Process only the new follow-up content
      // Process chart requests FIRST (before MMSI link conversion which would break JSON)
      processChartRequests(tempDiv);
      convertMMSIToLinks(tempDiv);

      // If tempDiv was modified, replace the last follow-up section
      if (tempDiv.innerHTML !== followUpHtml) {
        const followUpSections = content.querySelectorAll(
          'div[style*="border-left: 4px solid #1976d2"]'
        );
        if (followUpSections.length > 0) {
          followUpSections[followUpSections.length - 1].outerHTML =
            tempDiv.innerHTML;
        }
      }

      // Clear the question input
      questionTextarea.value = '';

      // Scroll to show the new response
      content.scrollIntoView({ behavior: 'smooth', block: 'end' });
    } else {
      const errorElement = document.createElement('div');
      errorElement.style.color = 'red';
      errorElement.innerHTML = `❌ Follow-up failed: ${result.error}`;
      content.appendChild(errorElement);
    }
  } catch (error) {
    // Stop timer on error
    clearInterval(timerInterval);
    const endTime = new Date();
    const totalTime = (endTime - startTime) / 1000;

    // Try to get token usage even on error (in case it's a partial failure)
    let tokenInfo = '';
    try {
      if (error.response && error.response.usage) {
        const usage = error.response.usage;
        tokenInfo = `<br><strong>Tokens: ${usage.input_tokens || 0} in + ${usage.output_tokens || 0} out = ${(usage.input_tokens || 0) + (usage.output_tokens || 0)} total</strong>`;
      }
    } catch (e) {
      // Ignore token parsing errors
    }

    followUpTimerElement.innerHTML = `
            <strong>⏱️ Follow-up Failed</strong><br>
            Started: ${startTime.toLocaleTimeString()}<br>
            Failed: ${endTime.toLocaleTimeString()}<br>
            <strong>Total Time: ${totalTime.toFixed(1)}s</strong>${tokenInfo}
        `;
    followUpTimerElement.style.background = '#ffeaea';
    followUpTimerElement.style.borderColor = '#f44336';

    console.error('Follow-up question error:', error);
    alert('Failed to ask follow-up question. Please try again.');
  } finally {
    // Re-enable UI
    askButton.disabled = false;
    askButton.textContent = '💬 Ask';
    questionTextarea.disabled = false;
    questionTextarea.focus();
  }
}
