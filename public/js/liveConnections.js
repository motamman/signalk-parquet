import { getPluginPath } from './utils.js';

let activeStreams = [];

export async function loadStreams() {
    // DISABLED - Streaming functionality has been disabled
    // try {
    //     const response = await fetch(`${getPluginPath()}/api/streams`);
    //     const result = await response.json();
        
    //     if (result.success) {
    //         activeStreams = result.streams || [];
    //         displayStreams(activeStreams);
    //         loadStreamStats();
    //     } else {
    //         document.getElementById('streamsContainer').innerHTML = `<div class="error">Error loading streams: ${result.error}</div>`;
    //     }
    // } catch (error) {
    //     document.getElementById('streamsContainer').innerHTML = `<div class="error">Network error: ${error.message}</div>`;
    // }
    return;
}

function displayStreams(streams) {
    const container = document.getElementById('streamsContainer');
    
    if (!streams || streams.length === 0) {
        container.innerHTML = `
            <div style="text-align: center; padding: 40px; background: #f8f9fa; border-radius: 5px; margin: 20px 0;">
                <h3 style="color: #666; margin-bottom: 10px;">No Active Streams</h3>
                <p style="color: #666; margin-bottom: 20px;">Create your first historical data stream to get started.</p>
                <button onclick="showAddStreamForm()">➕ Create Your First Stream</button>
            </div>
        `;
        return;
    }

    let html = '<div class="table-container"><table><thead><tr>';
    html += '<th>Name</th><th>Path</th><th>Status</th><th>Time Range</th><th>Resolution</th><th>Rate</th><th>Statistics</th><th>Progress</th><th>Stream URL</th><th>Actions</th>';
    html += '</tr></thead><tbody>';

    streams.forEach((stream, index) => {
        const isRunning = stream.status === 'running';
        const isPaused = stream.status === 'paused';
        const isStopped = stream.status === 'stopped' || stream.status === 'created';
        
        const status = isRunning ? '🟢 Running' : isPaused ? '⏸️ Paused' : '🔴 Stopped';
        const progress = stream.dataPointsStreamed ? `${stream.dataPointsStreamed} points` : '-';
        const timeRange = `${stream.timeRange || '1h'} (${stream.startTime || 'N/A'} - ${stream.endTime || 'N/A'})`;
        const statistics = stream.aggregateMethod || 'average';
        const streamMode = stream.isIncremental ? '📈' : '📊';
        const lastTimestamp = stream.lastTimestamp ? new Date(stream.lastTimestamp).toLocaleTimeString() : 'Never';
        
        // Generate stream URLs
        const streamName = stream.name.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
        const aggregateMethod = stream.aggregateMethod || 'average';
        const streamPath = `streaming.${streamName}.${aggregateMethod}`;
        const wsUrl = `ws://${window.location.host}/signalk/v1/stream`;
        const restUrl = `http://${window.location.host}/signalk/v1/api/vessels/self/${streamPath}`;
        
        html += `<tr>
            <td><strong>${stream.name}</strong></td>
            <td><code>${stream.path}</code></td>
            <td>${status}</td>
            <td>${timeRange}</td>
            <td>${stream.resolution || 30000}ms</td>
            <td>${stream.rate || 5000}ms</td>
            <td><small>${statistics} ${streamMode}<br/>Last: ${lastTimestamp}</small></td>
            <td>${progress}</td>
            <td>
                ${isRunning ? 
                    `<div style="font-size: 11px;">
                        <strong>Path:</strong> <code style="font-size: 10px;">${streamPath}</code><br/>
                        <strong>REST:</strong> <a href="${restUrl}" target="_blank" style="font-size: 10px; color: #007bff;">API Link</a><br/>
                        <strong>WS:</strong> <code style="font-size: 10px;">${wsUrl}</code>
                    </div>` : 
                    '<small style="color: #666;">Stream not running</small>'
                }
            </td>
            <td>
                ${isStopped ? 
                    `<button onclick="startStream('${stream.id}')" style="padding: 5px 10px; font-size: 12px; background: #28a745;">▶️ Start</button>` : 
                    `<button onclick="pauseStream('${stream.id}')" style="padding: 5px 10px; font-size: 12px; background: #ffc107;">${isPaused ? '▶️ Resume' : '⏸️ Pause'}</button>
                     <button onclick="stopStream('${stream.id}')" style="padding: 5px 10px; font-size: 12px; background: #dc3545;">⏹️ Stop</button>`
                }
                <button onclick="editStream('${stream.id}')" style="padding: 5px 10px; font-size: 12px;">✏️ Edit</button>
                <button onclick="deleteStream('${stream.id}')" style="padding: 5px 10px; font-size: 12px; background: #dc3545;">🗑️ Delete</button>
            </td>
        </tr>`;
    });

    html += '</tbody></table></div>';
    container.innerHTML = html;
    
    // Update live connection icon based on active streams
    updateLiveConnectionIcon(streams);
}

function updateLiveConnectionIcon(streams) {
    const hasActiveStreams = streams && streams.some(stream => stream.status === 'running');
    const icon = hasActiveStreams ? '🟢' : '🔴';
    
    // Update tab button
    const tabButton = document.querySelector('.tab-button[onclick="showTab(\'liveConnections\')"]');
    if (tabButton) {
        tabButton.innerHTML = `${icon} Live Connections`;
    }
    
    // Update section header
    const sectionHeader = document.querySelector('#liveConnections h2');
    if (sectionHeader) {
        sectionHeader.innerHTML = `${icon} Live Connections`;
    }
}

async function loadStreamStats() {
    // DISABLED - Streaming functionality has been disabled
    // try {
    //     const response = await fetch(`${getPluginPath()}/api/streams/stats`);
    //     const result = await response.json();
        
    //     if (result.success) {
    //         displayStreamStats(result.stats);
    //     } else {
    //         document.getElementById('streamStats').innerHTML = `<div class="error">Error loading stats: ${result.error}</div>`;
    //     }
    // } catch (error) {
    //     document.getElementById('streamStats').innerHTML = `<div class="error">Network error: ${error.message}</div>`;
    // }
    return;
}

function displayStreamStats(stats) {
    const container = document.getElementById('streamStats');
    
    const html = `
        <div class="stats">
            <div class="stat-item">
                <div class="stat-value">${stats.totalStreams || 0}</div>
                <div class="stat-label">Total Streams</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">${stats.runningStreams || 0}</div>
                <div class="stat-label">Active</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">${stats.totalDataPointsStreamed || 0}</div>
                <div class="stat-label">Data Points Streamed</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">${stats.connectedClients || 0}</div>
                <div class="stat-label">Connected Clients</div>
            </div>
        </div>
    `;
    
    container.innerHTML = html;
}

export function showAddStreamForm() {
    document.getElementById('addStreamForm').style.display = 'block';
    
    // Show/hide custom time range based on selection
    const timeRangeSelect = document.getElementById('streamTimeRange');
    timeRangeSelect.addEventListener('change', function() {
        const customDiv = document.getElementById('customTimeRange');
        customDiv.style.display = this.value === 'custom' ? 'block' : 'none';
    });
}

export function hideAddStreamForm() {
    document.getElementById('addStreamForm').style.display = 'none';
    clearAddStreamForm();
}

function clearAddStreamForm() {
    document.getElementById('streamName').value = '';
    
    // Reset path field to dropdown if it was converted to manual input
    const pathField = document.getElementById('streamPath');
    if (pathField.tagName === 'INPUT') {
        // Recreate the select element
        const pathContainer = pathField.parentNode;
        const pathSelect = document.createElement('select');
        pathSelect.id = 'streamPath';
        pathSelect.style.cssText = 'flex: 1;';
        pathSelect.innerHTML = '<option value="">Select a SignalK path...</option>';
        
        pathContainer.replaceChild(pathSelect, pathField);
        
        // Reload paths
        loadSignalKPaths();
    } else {
        pathField.value = '';
    }
    
    document.getElementById('streamTimeRange').value = '1h';
    document.getElementById('streamResolution').value = '30000';
    document.getElementById('streamRate').value = '500';
    document.getElementById('streamAutoStart').checked = true;
    document.getElementById('customTimeRange').style.display = 'none';
    document.getElementById('streamStartTime').value = '';
    document.getElementById('streamEndTime').value = '';
}

async function loadSignalKPaths() {
    const pathSelect = document.getElementById('streamPath');
    const originalHTML = pathSelect.innerHTML;
    
    try {
        pathSelect.innerHTML = '<option value="">Loading paths...</option>';
        
        // Use the same endpoint as the Available Paths tab
        const response = await fetch(`${getPluginPath()}/api/paths`);
        const result = await response.json();
        
        if (result.success && Array.isArray(result.paths) && result.paths.length > 0) {
            // Clear loading message and add paths
            pathSelect.innerHTML = '<option value="">Select a SignalK path...</option>';
            
            // Sort paths alphabetically for better UX
            const sortedPaths = result.paths.sort((a, b) => a.path.localeCompare(b.path));
            
            sortedPaths.forEach(pathInfo => {
                const option = document.createElement('option');
                option.value = pathInfo.path;
                option.textContent = `${pathInfo.path} (${pathInfo.fileCount} files)`;
                pathSelect.appendChild(option);
            });
            
        } else {
            // No paths found - provide manual entry option
            pathSelect.innerHTML = '<option value="">No historical paths found</option>';
            const manualOption = document.createElement('option');
            manualOption.value = 'manual';
            manualOption.textContent = '✏️ Enter path manually';
            pathSelect.appendChild(manualOption);
        }
    } catch (error) {
        console.error('Error loading SignalK paths:', error);
        
        // Provide manual entry option as fallback
        pathSelect.innerHTML = '<option value="">Select or enter a SignalK path...</option>';
        const manualOption = document.createElement('option');
        manualOption.value = 'manual';
        manualOption.textContent = '✏️ Enter path manually';
        pathSelect.appendChild(manualOption);
    }
}

async function loadVersionInfo() {
    try {
        const response = await fetch(`${getPluginPath()}/api/version`);
        if (response.ok) {
            const versionData = await response.json();
            const headerElement = document.querySelector('.header h1');
            if (headerElement) {
                const versionSpan = document.createElement('span');
                versionSpan.style.fontSize = '0.4em';
                versionSpan.style.color = 'rgb(200, 200, 200)';
                versionSpan.style.fontWeight = 'normal';
                versionSpan.style.marginLeft = '10px';
                versionSpan.textContent = `v${versionData.version}`;
                headerElement.appendChild(versionSpan);
            }
        }
    } catch (error) {
        console.log('Could not load version information:', error);
    }
}

export async function refreshSignalKPaths() {
    await loadSignalKPaths();
}

let liveConnectionsInitialized = false;

export async function initLiveConnections() {
    if (liveConnectionsInitialized) {
        return;
    }

    liveConnectionsInitialized = true;

    await loadVersionInfo();
    await loadSignalKPaths();

    const pathSelect = document.getElementById('streamPath');
    let manualInput = null;

    if (pathSelect) {
        pathSelect.addEventListener('change', function() {
            if (this.value === 'manual') {
                manualInput = document.createElement('input');
                manualInput.type = 'text';
                manualInput.id = 'streamPath';
                manualInput.placeholder = 'Enter SignalK path manually (e.g., navigation.position)';
                manualInput.style.cssText = this.style.cssText;

                this.parentNode.replaceChild(manualInput, this);
                manualInput.focus();
            }
        });
    }
}

export async function createStream() {
    const streamConfig = {
        name: document.getElementById('streamName').value.trim(),
        path: document.getElementById('streamPath').value.trim(),
        timeRange: document.getElementById('streamTimeRange').value,
        resolution: parseInt(document.getElementById('streamResolution').value),
        rate: parseInt(document.getElementById('streamRate').value),
        aggregateMethod: document.getElementById('streamAggregateMethod').value,
        windowSize: parseInt(document.getElementById('streamWindowSize').value),
        autoStart: document.getElementById('streamAutoStart').checked
    };

    // Handle custom time range
    if (streamConfig.timeRange === 'custom') {
        streamConfig.startTime = document.getElementById('streamStartTime').value;
        streamConfig.endTime = document.getElementById('streamEndTime').value;
        
        if (!streamConfig.startTime || !streamConfig.endTime) {
            alert('Custom time range requires both start and end times');
            return;
        }
    }

    if (!streamConfig.name || !streamConfig.path) {
        alert('Stream name and path are required');
        return;
    }

    // Streaming functionality has been disabled
    alert('Streaming functionality has been disabled');
}

export async function startStream(streamId) {
    await streamAction(streamId, 'start');
}

export async function pauseStream(streamId) {
    await streamAction(streamId, 'pause');
}

export async function stopStream(streamId) {
    await streamAction(streamId, 'stop');
}

export async function deleteStream(streamId) {
    if (!confirm('Are you sure you want to delete this stream? This action cannot be undone.')) {
        return;
    }
    await streamAction(streamId, 'delete');
}

async function streamAction(streamId, action) {
    try {
        const method = action === 'delete' ? 'DELETE' : 'PUT';
        const baseEndpoint = `${getPluginPath()}/api/streams/${streamId}`;
        const url = action === 'delete' ? baseEndpoint : `${baseEndpoint}/${action}`;
        
        const response = await fetch(url, { method });
        const result = await response.json();

        if (result.success) {
            // Streaming disabled - no need to refresh
            const actionMsg = action === 'delete' ? 'deleted' : `${action}ed`;
            // Don't show alert for successful actions, just refresh the display
        } else {
            alert(`Error ${action}ing stream: ${result.error}`);
        }
    } catch (error) {
        alert(`Network error: ${error.message}`);
    }
}

export async function editStream(streamId) {
    try {
        // Get the current stream configuration
        // Streaming functionality has been disabled
        alert('Streaming functionality has been disabled');
        return;
        
        if (!result.success) {
            alert('Error loading stream data');
            return;
        }
        
        const stream = result.streams.find(s => s.id === streamId);
        if (!stream) {
            alert('Stream not found');
            return;
        }
        
        // Populate the form with current values
        document.getElementById('streamName').value = stream.name || '';
        
        // Handle path selection - check if it exists in dropdown
        const pathSelect = document.getElementById('streamPath');
        const streamPath = stream.path || '';
        let pathFound = false;
        
        for (let option of pathSelect.options) {
            if (option.value === streamPath) {
                pathSelect.value = streamPath;
                pathFound = true;
                break;
            }
        }
        
        // If path not found in dropdown, convert to manual input
        if (!pathFound && streamPath) {
            const manualInput = document.createElement('input');
            manualInput.type = 'text';
            manualInput.id = 'streamPath';
            manualInput.value = streamPath;
            manualInput.placeholder = 'Enter SignalK path manually (e.g., navigation.position)';
            manualInput.style.cssText = pathSelect.style.cssText;
            
            pathSelect.parentNode.replaceChild(manualInput, pathSelect);
        }
        
        document.getElementById('streamTimeRange').value = stream.timeRange || '1h';
        document.getElementById('streamResolution').value = stream.resolution || 30000;
        document.getElementById('streamRate').value = stream.rate || 1000;
        document.getElementById('streamAggregateMethod').value = stream.aggregateMethod || 'average';
        document.getElementById('streamWindowSize').value = stream.windowSize || 50;
        document.getElementById('streamAutoStart').checked = false; // Don't auto-start on edit
        
        // Handle custom time range
        if (stream.timeRange === 'custom') {
            document.getElementById('customTimeRange').style.display = 'block';
            document.getElementById('streamStartTime').value = stream.startTime || '';
            document.getElementById('streamEndTime').value = stream.endTime || '';
        } else {
            document.getElementById('customTimeRange').style.display = 'none';
        }
        
        // Show the form and change the create button to update
        document.getElementById('addStreamForm').style.display = 'block';
        document.getElementById('addStreamButton').style.display = 'none';
        
        // Change form title and button
        const formTitle = document.querySelector('#addStreamForm h4');
        formTitle.textContent = `Edit Stream: ${stream.name}`;
        
        // Replace create button with update/cancel buttons
        const buttonContainer = document.querySelector('#addStreamForm .form-group:last-child > div');
        buttonContainer.innerHTML = `
            <button onclick="updateStream('${streamId}')">✅ Update Stream</button>
            <button class="btn-secondary" onclick="cancelEditStream()">❌ Cancel</button>
        `;
        
    } catch (error) {
        alert(`Error loading stream for editing: ${error.message}`);
    }
}

export async function updateStream(streamId) {
    const streamConfig = {
        name: document.getElementById('streamName').value.trim(),
        path: document.getElementById('streamPath').value.trim(),
        timeRange: document.getElementById('streamTimeRange').value,
        resolution: parseInt(document.getElementById('streamResolution').value),
        rate: parseInt(document.getElementById('streamRate').value),
        aggregateMethod: document.getElementById('streamAggregateMethod').value,
        windowSize: parseInt(document.getElementById('streamWindowSize').value),
        autoRestart: true // Restart if it was running
    };

    // Handle custom time range
    if (streamConfig.timeRange === 'custom') {
        streamConfig.startTime = document.getElementById('streamStartTime').value;
        streamConfig.endTime = document.getElementById('streamEndTime').value;
        
        if (!streamConfig.startTime || !streamConfig.endTime) {
            alert('Custom time range requires both start and end times');
            return;
        }
    }

    if (!streamConfig.name || !streamConfig.path) {
        alert('Stream name and path are required');
        return;
    }

    // Streaming functionality has been disabled
    alert('Streaming functionality has been disabled');
}

export function cancelEditStream() {
    // Hide the form and reset it
    hideAddStreamForm();
    
    // Reset form title and buttons
    const formTitle = document.querySelector('#addStreamForm h4');
    formTitle.textContent = 'Add New Stream Configuration';
    
    const buttonContainer = document.querySelector('#addStreamForm .form-group:last-child > div');
    buttonContainer.innerHTML = `
        <button onclick="createStream()">✅ Create Stream</button>
        <button class="btn-secondary" onclick="hideAddStreamForm()">❌ Cancel</button>
    `;
}

// Live Data Display Functions
let liveDataEntries = [];
let liveDataPaused = false;
const MAX_LIVE_ENTRIES = 100;

function addLiveDataEntry(streamId, streamName, path, value, timestamp, ema = null, sma = null) {
    if (liveDataPaused) return;

    // Check for duplicates using streamId (which now includes timestamp)
    const existingEntry = liveDataEntries.find(entry => entry.streamId === streamId);
    if (existingEntry) {
        return; // Skip duplicate entry
    }

    const entry = {
        timestamp: timestamp || new Date().toISOString(),
        streamId: streamId,
        streamName: streamName,
        path: path,
        value: value,
        ema: ema,
        sma: sma
    };

    // Add entry to array
    liveDataEntries.push(entry);
    
    // Sort by timestamp (newest first)
    liveDataEntries.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());

    // Keep only max entries
    if (liveDataEntries.length > MAX_LIVE_ENTRIES) {
        liveDataEntries = liveDataEntries.slice(0, MAX_LIVE_ENTRIES);
    }

    updateLiveDataDisplay();
}

function updateLiveDataDisplay() {
    const tbody = document.getElementById('liveDataBody');
    const countElement = document.getElementById('liveDataCount');

    countElement.textContent = liveDataEntries.length;

    if (liveDataEntries.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="6" style="padding: 20px; text-align: center; color: #666;">
                    No data streaming yet. Start a stream to see live data.
                </td>
            </tr>
        `;
        return;
    }

    let html = '';
    liveDataEntries.forEach((entry, index) => {
        // Create date object and handle UTC timestamps properly
        const date = new Date(entry.timestamp);
        const now = new Date();
        
        // Check if timestamp is from today
        const isToday = date.toDateString() === now.toDateString();
        
        let timeStr;
        if (isToday) {
            // Today: just show time
            timeStr = date.toLocaleTimeString();
        } else {
            // Not today: show "Aug 21, 6:25 PM" format
            const options = { 
                month: 'short', 
                day: 'numeric', 
                hour: 'numeric', 
                minute: '2-digit',
                hour12: true 
            };
            timeStr = date.toLocaleString(undefined, options);
        }
        const valueStr = formatValue(entry.value);
        const emaStr = entry.ema !== null ? entry.ema.toFixed(3) : '-';
        const smaStr = entry.sma !== null ? entry.sma.toFixed(3) : '-';
        const rowClass = index % 2 === 0 ? 'background: #f9f9f9;' : '';
        

        html += `
            <tr style="${rowClass}">
                <td style="padding: 6px 8px; border-bottom: 1px solid #eee; font-family: monospace;">${timeStr}</td>
                <td style="padding: 6px 8px; border-bottom: 1px solid #eee;"><strong>${entry.streamName}</strong></td>
                <td style="padding: 6px 8px; border-bottom: 1px solid #eee;"><code style="font-size: 11px;">${entry.path}</code></td>
                <td style="padding: 6px 8px; border-bottom: 1px solid #eee; font-family: monospace;">${valueStr}</td>
                <td style="padding: 6px 8px; border-bottom: 1px solid #eee; font-family: monospace; color: #007bff;">${emaStr}</td>
                <td style="padding: 6px 8px; border-bottom: 1px solid #eee; font-family: monospace; color: #28a745;">${smaStr}</td>
            </tr>
        `;
    });

    tbody.innerHTML = html;
}

function formatValue(value) {
    if (value === null || value === undefined) {
        return '<em>null</em>';
    }
    
    if (typeof value === 'object') {
        if (value.latitude !== undefined && value.longitude !== undefined) {
            // Position object
            return `${value.latitude.toFixed(6)}, ${value.longitude.toFixed(6)}`;
        }
        // Other objects - show as JSON
        return JSON.stringify(value);
    }
    
    if (typeof value === 'number') {
        return value.toFixed(3);
    }
    
    return String(value);
}

export function clearLiveData() {
    if (confirm('Clear all live data entries?')) {
        liveDataEntries = [];
        updateLiveDataDisplay();
    }
}

export function toggleLiveDataPause() {
    liveDataPaused = !liveDataPaused;
    const btn = document.getElementById('pauseDataBtn');
    btn.textContent = liveDataPaused ? '▶️ Resume' : '⏸️ Pause';
    btn.style.background = liveDataPaused ? '#28a745' : '#ffc107';
}

export function showDataSummary() {
    const panel = document.getElementById('dataSummaryPanel');
    const content = document.getElementById('dataSummaryContent');
    
    if (panel.style.display === 'none') {
        // Analyze the data
        const pathStats = {};
        const streamTypes = {initial: 0, incremental: 0};
        let totalBuckets = 0;
        
        liveDataEntries.forEach(entry => {
            // Extract path and value
            const path = entry.path;
            const value = parseFloat(entry.value);
            const streamName = entry.streamName;
            
            if (!isNaN(value)) {
                if (!pathStats[path]) {
                    pathStats[path] = {
                        values: [],
                        methods: new Set(),
                        bucketCounts: []
                    };
                }
                pathStats[path].values.push(value);
                
                // Extract method and bucket count from stream name
                const methodMatch = streamName.match(/(INITIAL|INCREMENTAL): (\w+)/);
                if (methodMatch) {
                    const [, type, method] = methodMatch;
                    pathStats[path].methods.add(method);
                    streamTypes[type.toLowerCase()]++;
                }
                
                // Extract bucket count
                const bucketMatch = streamName.match(/\((\d+) buckets\)/);
                if (bucketMatch) {
                    const bucketCount = parseInt(bucketMatch[1]);
                    pathStats[path].bucketCounts.push(bucketCount);
                    totalBuckets += bucketCount;
                }
            }
        });
        
        let summaryHtml = '<div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px;">';
        
        // Stream Statistics
        summaryHtml += '<div><strong>📈 Stream Activity:</strong><br/>';
        summaryHtml += `Initial Loads: ${streamTypes.initial}<br/>`;
        summaryHtml += `Incremental Updates: ${streamTypes.incremental}<br/>`;
        summaryHtml += `Total Time Buckets: ${totalBuckets}</div>`;
        
        // Path Analysis
        summaryHtml += '<div><strong>🎯 Path Analysis:</strong><br/>';
        Object.entries(pathStats).forEach(([path, stats]) => {
            const values = stats.values;
            const min = Math.min(...values).toFixed(3);
            const max = Math.max(...values).toFixed(3);
            const avg = (values.reduce((a, b) => a + b, 0) / values.length).toFixed(3);
            const latest = values[0]?.toFixed(3) || 'N/A';
            const methods = Array.from(stats.methods).join(', ');
            const avgBuckets = stats.bucketCounts.length > 0 ? 
                (stats.bucketCounts.reduce((a, b) => a + b, 0) / stats.bucketCounts.length).toFixed(0) : 'N/A';
            
            summaryHtml += `<strong>${path}:</strong><br/>`;
            summaryHtml += `• Methods: ${methods}<br/>`;
            summaryHtml += `• Range: ${min} - ${max}<br/>`;
            summaryHtml += `• Average: ${avg}<br/>`;
            summaryHtml += `• Latest: ${latest}<br/>`;
            summaryHtml += `• Avg Buckets: ${avgBuckets}<br/><br/>`;
        });
        summaryHtml += '</div></div>';
        
        // Data Trends
        summaryHtml += '<div style="margin-top: 15px;"><strong>📊 Data Interpretation:</strong><br/>';
        summaryHtml += 'This shows your time-bucketed statistical streaming in action!<br/>';
        summaryHtml += '• Each row represents a statistical calculation (MAX, AVG, etc.) for a time bucket<br/>';
        summaryHtml += '• INITIAL = First load with full time window<br/>';
        summaryHtml += '• INCREMENTAL = New buckets from sliding window updates<br/>';
        summaryHtml += '• Values change as new data arrives and statistics are recalculated</div>';
        
        content.innerHTML = summaryHtml;
        panel.style.display = 'block';
    } else {
        panel.style.display = 'none';
    }
}

// WebSocket connection for real-time streaming data
let ws = null;
let wsReconnectAttempts = 0;
const maxReconnectAttempts = 5;

function connectWebSocket() {
    // For now, use polling mode since we need a dedicated WebSocket server
    startLiveDataPolling();
}

function handleWebSocketMessage(message) {
    // Handle time-series data from our streaming service
    if (message.type === 'timeSeriesData') {
        handleTimeSeriesData(message);
    }
    // Handle regular SignalK delta messages
    else if (message.updates && Array.isArray(message.updates)) {
        message.updates.forEach(update => {
            if (update.values && Array.isArray(update.values)) {
                update.values.forEach(valueUpdate => {
                    addLiveDataEntry(
                        'signalk',
                        'SignalK Stream',
                        valueUpdate.path,
                        valueUpdate.value,
                        update.timestamp || new Date().toISOString()
                    );
                });
            }
        });
    }
}

function handleTimeSeriesData(message) {
    const { streamId, metadata, data } = message;
    
    
    // Add each time-bucketed data point to the live display
    data.forEach((point, index) => {
        const displayValue = typeof point.value === 'object' ? 
            JSON.stringify(point.value) : 
            (typeof point.value === 'number' ? point.value.toFixed(3) : point.value);
        
        const displayName = `${metadata.aggregateMethod.toUpperCase()}: ${metadata.path}`;
        const bucketInfo = `[${point.bucketIndex}/${metadata.totalPoints}]`;
        
        // Extract EMA and SMA if available for numeric values
        const ema = (typeof point.ema === 'number') ? point.ema : null;
        const sma = (typeof point.sma === 'number') ? point.sma : null;
        
        addLiveDataEntry(
            streamId,
            `${displayName} ${bucketInfo}`,
            metadata.path,
            displayValue,
            point.timestamp,
            ema,
            sma
        );
    });
}

// Enhanced polling function that shows real time-bucketed streaming data points
function startLiveDataPolling() {
    
    let pollCount = 0;
    
    setInterval(async () => {
        if (liveDataPaused) return;

        pollCount++;

        // Streaming functionality has been disabled - no running streams
        return;
    }, 3000); // Check every 3 seconds
}

// Initialize WebSocket connection when page loads  
setTimeout(() => {
    connectWebSocket();
}, 1000);
