import { getPluginPath } from './utils.js';

let editingThresholds = [];
let addingThresholds = [];

export async function loadCommands() {
    try {
        const response = await fetch(`${getPluginPath()}/api/commands`);
        const result = await response.json();
        
        if (result.success) {
            displayCommands(result.commands || []);
        } else {
            document.getElementById('commandContainer').innerHTML = `<div class="error">Error loading commands: ${result.error}</div>`;
        }
    } catch (error) {
        document.getElementById('commandContainer').innerHTML = `<div class="error">Error loading commands: ${error.message}</div>`;
    }
}

function displayCommands(commands) {
    const container = document.getElementById('commandContainer');

    if (!commands || commands.length === 0) {
        container.innerHTML = '<div class="info">No commands registered yet.</div>';
        return;
    }

    let html = '<div class="table-container"><table><thead><tr>';
    html += '<th>Command</th><th>Path</th><th>Description</th><th>Automation</th><th>Status</th><th>Actions</th>';
    html += '</tr></thead><tbody>';

    commands.forEach(command => {
        const status = command.active ? '🟢 Active' : '🔴 Inactive';
        const registeredDate = new Date(command.registered).toLocaleString();
        const keywords = command.keywords ? command.keywords.join(', ') : '';

        // Automation info
        let automationInfo = '<div style="font-size: 0.9em;">';

        // Manual override status
        if (command.manualOverride) {
            const expiry = command.manualOverrideUntil ?
                ` (expires ${new Date(command.manualOverrideUntil).toLocaleString()})` : ' (permanent)';
            automationInfo += `<div style="color: #ff9800; margin-bottom: 5px;">🔒 Manual Override${expiry}</div>`;
        }

        // Thresholds configuration (multiple thresholds supported)
        if (command.thresholds && command.thresholds.length > 0) {
            command.thresholds.forEach(threshold => {
                if (threshold.enabled) {
                    const operator = {
                        'gt': '>', 'lt': '<', 'eq': '=', 'ne': '≠', 'true': 'is true', 'false': 'is false'
                    }[threshold.operator] || threshold.operator;

                    const value = threshold.value !== undefined ? ` ${threshold.value}` : '';
                    const action = threshold.activateOnMatch ? 'ON' : 'OFF';

                    automationInfo += `<div style="color: #2196f3; margin-bottom: 3px;">
                        🎯 <strong>${threshold.watchPath}</strong> ${operator}${value} → ${action}
                    </div>`;

                    if (threshold.hysteresis) {
                        automationInfo += `<div style="color: #666; font-size: 0.8em;">⏱️ ${threshold.hysteresis}s hysteresis</div>`;
                    }
                }
            });
        } else if (command.defaultState !== undefined) {
            automationInfo += `<div style="color: #4caf50;">⚙️ Default: ${command.defaultState ? 'ON' : 'OFF'}</div>`;
        } else {
            automationInfo += '<div style="color: #999;">Manual control only</div>';
        }

        if (keywords) {
            automationInfo += `<div style="color: #666; font-size: 0.8em; margin-top: 3px;">🏷️ ${keywords}</div>`;
        }

        automationInfo += '</div>';

        html += `<tr>
            <td>
                <div><strong>${command.command}</strong></div>
                <div style="font-size: 0.8em; color: #666; margin-top: 2px;">${command.description || 'No description'}</div>
                <div style="font-size: 0.7em; color: #999; margin-top: 2px;">Registered: ${registeredDate}</div>
            </td>
            <td><code style="font-size: 0.8em;">vessels.self.commands.${command.command}</code></td>
            <td>${automationInfo}</td>
            <td style="text-align: center;">
                <div style="margin-bottom: 5px;">${status}</div>
                ${command.manualOverride ?
                    `<button onclick="setManualOverride('${command.command}', false)"
                            style="background: #ff9800; color: white; border: none; padding: 2px 6px; border-radius: 3px; font-size: 0.8em;">
                        🔓 Clear Override
                    </button>` :
                    `<button onclick="promptManualOverride('${command.command}')"
                            style="background: #666; color: white; border: none; padding: 2px 6px; border-radius: 3px; font-size: 0.8em;">
                        🔒 Override
                    </button>`
                }
            </td>
            <td>
                <div style="display: flex; flex-direction: column; gap: 3px;">
                    <div>
                        <button onclick="executeCommand('${command.command}', true)" style="margin-right: 3px; padding: 4px 8px; font-size: 0.8em;">▶️ Start</button>
                        <button onclick="executeCommand('${command.command}', false)" style="padding: 4px 8px; font-size: 0.8em;">⏹️ Stop</button>
                    </div>
                    <div>
                        <button onclick="showEditCommandForm('${command.command}')" class="btn-secondary" style="margin-right: 3px; padding: 3px 6px; font-size: 0.8em;">✏️ Edit</button>
                        <button onclick="unregisterCommand('${command.command}')" class="btn-danger" style="padding: 3px 6px; font-size: 0.8em;">❌ Remove</button>
                    </div>
                </div>
            </td>
        </tr>`;
    });

    html += '</tbody></table></div>';
    container.innerHTML = html;
}

export function showAddCommandForm() {
    document.getElementById('addCommandForm').style.display = 'block';
}

export function hideAddCommandForm() {
    document.getElementById('addCommandForm').style.display = 'none';
    clearAddCommandForm();
}

function clearAddCommandForm() {
    document.getElementById('commandName').value = '';
    document.getElementById('commandDescription').value = '';
    document.getElementById('commandKeywords').value = '';
    document.getElementById('addCommandDefaultState').value = '';

    // Clear thresholds
    addingThresholds = [];
    displayAddCommandThresholdsList();

    // Hide threshold form if visible
    document.getElementById('addCommandThresholdForm').style.display = 'none';
}

// Edit command functions
export async function showEditCommandForm(commandName) {
    try {
        // Find the command in the current commands list
    const response = await fetch(`${getPluginPath()}/api/commands`).then(r => r.json());
        const command = response.commands.find(cmd => cmd.command === commandName);
        
        if (!command) {
            alert('Command not found');
            return;
        }
        
        // Populate the form
        document.getElementById('editCommandName').value = command.command;
        document.getElementById('editCommandDescription').value = command.description || '';
        document.getElementById('editCommandKeywords').value = command.keywords ? command.keywords.join(', ') : '';

        // Populate default state
        const defaultStateSelect = document.getElementById('editCommandDefaultState');
        if (command.defaultState === true) {
            defaultStateSelect.value = 'true';
        } else if (command.defaultState === false) {
            defaultStateSelect.value = 'false';
        } else {
            defaultStateSelect.value = '';
        }

        // Populate thresholds configuration (multiple thresholds supported)
        editingThresholds = command.thresholds ? [...command.thresholds] : [];
        displayThresholdsList();

        // Show the form
        document.getElementById('editCommandForm').style.display = 'block';
        
    } catch (error) {
        alert('Failed to load command details: ' + error.message);
    }
}

export function hideEditCommandForm() {
    document.getElementById('editCommandForm').style.display = 'none';
}

export async function updateCommand() {
    try {
        const command = document.getElementById('editCommandName').value.trim();
        const description = document.getElementById('editCommandDescription').value.trim();
        const keywordsInput = document.getElementById('editCommandKeywords').value.trim();

        // Parse keywords from comma-separated string
        const keywords = keywordsInput ? keywordsInput.split(',').map(k => k.trim()).filter(k => k.length > 0) : undefined;

        // Get default state
        const defaultStateValue = document.getElementById('editCommandDefaultState').value;
        const defaultState = defaultStateValue === '' ? undefined : (defaultStateValue === 'true');

        // Get thresholds configuration (multiple thresholds supported)
        const thresholds = editingThresholds.length > 0 ? editingThresholds : undefined;

        const response = await fetch(`${getPluginPath()}/api/commands/${command}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                description: description || undefined,
                keywords: keywords,
                defaultState: defaultState,
                thresholds: thresholds
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            hideEditCommandForm();
            await loadCommands();
            alert('Command updated successfully!');
        } else {
            alert('Failed to update command: ' + result.error);
        }
        
    } catch (error) {
        alert('Error updating command: ' + error.message);
    }
}

export async function registerCommand() {
    try {
        const command = document.getElementById('commandName').value.trim();
        const description = document.getElementById('commandDescription').value.trim();
        const keywordsInput = document.getElementById('commandKeywords').value.trim();
        
        if (!command) {
            alert('Command name is required');
            return;
        }
        
        // Parse keywords from comma-separated string
        const keywords = keywordsInput ? keywordsInput.split(',').map(k => k.trim()).filter(k => k.length > 0) : undefined;

        // Get default state
        const defaultStateValue = document.getElementById('addCommandDefaultState').value;
        const defaultState = defaultStateValue === '' ? undefined : (defaultStateValue === 'true');

        // Get thresholds configuration
        const thresholds = addingThresholds.length > 0 ? addingThresholds : undefined;

        const response = await fetch(`${getPluginPath()}/api/commands`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                command: command,
                description: description || undefined,
                keywords: keywords,
                defaultState: defaultState,
                thresholds: thresholds
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            hideAddCommandForm();
            await loadCommands();
            await loadPathConfigurations(); // Refresh paths to show the auto-created path
            alert(`Command '${command}' registered successfully!\n\nA path configuration has been automatically created and enabled for this command.`);
        } else {
            alert(`Error registering command: ${result.error}`);
        }
    } catch (error) {
        alert(`Network error: ${error.message}`);
    }
}

export async function executeCommand(commandName, value) {
    try {
        const response = await fetch(`${getPluginPath()}/api/commands/${commandName}/execute`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                value: value
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            await loadCommands();
            await loadCommandHistory();
            alert(`Command '${commandName}' ${value ? 'started' : 'stopped'} successfully`);
        } else {
            alert(`Error executing command: ${result.error}`);
        }
    } catch (error) {
        alert(`Network error: ${error.message}`);
    }
}

export async function unregisterCommand(commandName) {
    if (!confirm(`Are you sure you want to unregister command '${commandName}'?`)) {
        return;
    }
    
    try {
        const response = await fetch(`${getPluginPath()}/api/commands/${commandName}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (result.success) {
            await loadCommands();
            await loadCommandHistory();
            await loadPathConfigurations(); // Refresh paths to show the removed path
            alert(`Command '${commandName}' unregistered successfully!\n\nThe associated path configuration has been automatically removed.`);
        } else {
            alert(`Error unregistering command: ${result.error}`);
        }
    } catch (error) {
        alert(`Network error: ${error.message}`);
    }
}

// Threshold UI functions


export function updateThresholdPathFilter() {
    populateThresholdPaths();
}

async function populateThresholdPaths() {
    const select = document.getElementById('newThresholdPath');
    const filterType = document.querySelector('input[name="thresholdPathFilter"]:checked')?.value || 'self';

    // Clear existing options except the default ones
    select.innerHTML = '<option value="">-- Select SignalK Path --</option><option value="custom">🖊️ Enter Custom Path</option>';

    try {
        // Fetch live SignalK data
        const response = await fetch('/signalk/v1/api/');
        const data = await response.json();

        // Extract paths from SignalK API with filter
        const allPaths = extractPathsFromSignalK(data, filterType);

        // Add available paths
        allPaths.forEach(path => {
            const option = document.createElement('option');
            option.value = path;
            option.textContent = path;
            select.appendChild(option);
        });

    } catch (error) {
        console.log('Could not load real-time SignalK paths for thresholds:', error);
    }

    // Handle custom path selection
    select.onchange = function() {
        const customInput = document.getElementById('newThresholdPathCustom');
        if (this.value === 'custom') {
            customInput.style.display = 'block';
            customInput.focus();
        } else {
            customInput.style.display = 'none';
        }
    };
}

export function toggleNewThresholdValueField() {
    const operator = document.getElementById('newThresholdOperator').value;
    const valueGroup = document.getElementById('newThresholdValueGroup');

    // Hide value field for true/false operators
    if (operator === 'true' || operator === 'false') {
        valueGroup.style.display = 'none';
    } else {
        valueGroup.style.display = 'block';
    }
}

export function addNewThreshold() {
    const form = document.getElementById('addThresholdForm');
    form.style.display = 'block';

    // Populate the path dropdown
    populateThresholdPaths();

    // Clear form
    document.getElementById('newThresholdPath').value = '';
    document.getElementById('newThresholdPathCustom').value = '';
    document.getElementById('newThresholdPathCustom').style.display = 'none';
    document.getElementById('newThresholdOperator').value = 'gt';
    document.getElementById('newThresholdValue').value = '';
    document.getElementById('newThresholdAction').value = 'true';
    document.getElementById('newThresholdHysteresis').value = '';

    toggleNewThresholdValueField();
}

export function cancelNewThreshold() {
    document.getElementById('addThresholdForm').style.display = 'none';
}

export function saveNewThreshold() {
    const pathSelect = document.getElementById('newThresholdPath');
    const pathCustom = document.getElementById('newThresholdPathCustom');
    const operator = document.getElementById('newThresholdOperator').value;
    const value = document.getElementById('newThresholdValue').value.trim();
    const action = document.getElementById('newThresholdAction').value === 'true';
    const hysteresis = document.getElementById('newThresholdHysteresis').value.trim();

    // Get the path
    let path = pathSelect.value;
    if (path === 'custom') {
        path = pathCustom.value.trim();
    }

    if (!path) {
        alert('Please select or enter a SignalK path');
        return;
    }

    // Validate value for non-boolean operators
    if (operator !== 'true' && operator !== 'false' && !value) {
        alert('Please enter a threshold value');
        return;
    }

    // Create new threshold
    const threshold = {
        enabled: true,
        watchPath: path,
        operator: operator,
        activateOnMatch: action
    };

    // Add value if needed
    if (operator !== 'true' && operator !== 'false' && value) {
        const numValue = parseFloat(value);
        threshold.value = isNaN(numValue) ? value : numValue;
    }

    // Add hysteresis if specified
    if (hysteresis) {
        const hysteresisValue = parseFloat(hysteresis);
        if (!isNaN(hysteresisValue)) {
            threshold.hysteresis = hysteresisValue;
        }
    }

    // Add to thresholds array
    editingThresholds.push(threshold);

    // Refresh the thresholds display
    displayThresholdsList();

    // Hide the form
    cancelNewThreshold();
}

function displayThresholdsList() {
    const container = document.getElementById('thresholdsList');

    if (!editingThresholds || editingThresholds.length === 0) {
        container.innerHTML = '<div style="color: #999; font-style: italic; padding: 10px;">No thresholds configured</div>';
        return;
    }

    let html = '';
    editingThresholds.forEach((threshold, index) => {
        const operator = {
            'gt': '>', 'lt': '<', 'eq': '=', 'ne': '≠', 'true': 'is true', 'false': 'is false'
        }[threshold.operator] || threshold.operator;

        const value = threshold.value !== undefined ? ` ${threshold.value}` : '';
        const action = threshold.activateOnMatch ? 'ON' : 'OFF';
        const hysteresis = threshold.hysteresis ? ` (${threshold.hysteresis}s hysteresis)` : '';

        html += `
            <div style="background: white; border: 1px solid #ddd; border-radius: 4px; padding: 10px; margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <strong>${threshold.watchPath}</strong> ${operator}${value} → ${action}${hysteresis}
                </div>
                <button onclick="removeThreshold(${index})" style="background: #f44336; color: white; border: none; padding: 4px 8px; border-radius: 3px; font-size: 0.8em;">
                    ❌ Remove
                </button>
            </div>
        `;
    });

    container.innerHTML = html;
}

export function removeThreshold(index) {
    editingThresholds.splice(index, 1);
    displayThresholdsList();
}

// Add command threshold functions
export function updateAddCmdThresholdPathFilter() {
    populateAddCmdThresholdPaths();
}

async function populateAddCmdThresholdPaths() {
    const select = document.getElementById('addCmdThresholdPath');
    const filterType = document.querySelector('input[name="addCmdPathFilter"]:checked')?.value || 'self';

    // Clear existing options except the default ones
    select.innerHTML = '<option value="">-- Select SignalK Path --</option><option value="custom">🖊️ Enter Custom Path</option>';

    try {
        // Fetch live SignalK data
        const response = await fetch('/signalk/v1/api/');
        const data = await response.json();

        // Extract paths from SignalK API with filter
        const allPaths = extractPathsFromSignalK(data, filterType);

        // Add available paths
        allPaths.forEach(path => {
            const option = document.createElement('option');
            option.value = path;
            option.textContent = path;
            select.appendChild(option);
        });

    } catch (error) {
        console.log('Could not load real-time SignalK paths for add command thresholds:', error);
    }

    // Handle custom path selection
    select.onchange = function() {
        const customInput = document.getElementById('addCmdThresholdPathCustom');
        if (this.value === 'custom') {
            customInput.style.display = 'block';
            customInput.focus();
        } else {
            customInput.style.display = 'none';
        }
    };
}

export function toggleAddCmdThresholdValueField() {
    const operator = document.getElementById('addCmdThresholdOperator').value;
    const valueGroup = document.getElementById('addCmdThresholdValueGroup');

    // Hide value field for true/false operators
    if (operator === 'true' || operator === 'false') {
        valueGroup.style.display = 'none';
    } else {
        valueGroup.style.display = 'block';
    }
}

export function addNewCommandThreshold() {
    const form = document.getElementById('addCommandThresholdForm');
    form.style.display = 'block';

    // Populate the path dropdown
    populateAddCmdThresholdPaths();

    // Clear form
    document.getElementById('addCmdThresholdPath').value = '';
    document.getElementById('addCmdThresholdPathCustom').value = '';
    document.getElementById('addCmdThresholdPathCustom').style.display = 'none';
    document.getElementById('addCmdThresholdOperator').value = 'gt';
    document.getElementById('addCmdThresholdValue').value = '';
    document.getElementById('addCmdThresholdAction').value = 'true';
    document.getElementById('addCmdThresholdHysteresis').value = '';

    toggleAddCmdThresholdValueField();
}

export function cancelAddCmdThreshold() {
    document.getElementById('addCommandThresholdForm').style.display = 'none';
}

export function saveAddCmdThreshold() {
    const pathSelect = document.getElementById('addCmdThresholdPath');
    const pathCustom = document.getElementById('addCmdThresholdPathCustom');
    const operator = document.getElementById('addCmdThresholdOperator').value;
    const value = document.getElementById('addCmdThresholdValue').value.trim();
    const action = document.getElementById('addCmdThresholdAction').value === 'true';
    const hysteresis = document.getElementById('addCmdThresholdHysteresis').value.trim();

    // Get the path
    let path = pathSelect.value;
    if (path === 'custom') {
        path = pathCustom.value.trim();
    }

    if (!path) {
        alert('Please select or enter a SignalK path');
        return;
    }

    // Validate value for non-boolean operators
    if (operator !== 'true' && operator !== 'false' && !value) {
        alert('Please enter a threshold value');
        return;
    }

    // Create new threshold
    const threshold = {
        enabled: true,
        watchPath: path,
        operator: operator,
        activateOnMatch: action
    };

    // Add value if needed
    if (operator !== 'true' && operator !== 'false' && value) {
        const numValue = parseFloat(value);
        threshold.value = isNaN(numValue) ? value : numValue;
    }

    // Add hysteresis if specified
    if (hysteresis) {
        const hysteresisValue = parseFloat(hysteresis);
        if (!isNaN(hysteresisValue)) {
            threshold.hysteresis = hysteresisValue;
        }
    }

    // Add to thresholds array
    addingThresholds.push(threshold);

    // Refresh the thresholds display
    displayAddCommandThresholdsList();

    // Hide the form
    cancelAddCmdThreshold();
}

function displayAddCommandThresholdsList() {
    const container = document.getElementById('addCommandThresholdsList');

    if (!addingThresholds || addingThresholds.length === 0) {
        container.innerHTML = '<div style="color: #999; font-style: italic; padding: 10px;">No thresholds configured</div>';
        return;
    }

    let html = '';
    addingThresholds.forEach((threshold, index) => {
        const operator = {
            'gt': '>', 'lt': '<', 'eq': '=', 'ne': '≠', 'true': 'is true', 'false': 'is false'
        }[threshold.operator] || threshold.operator;

        const value = threshold.value !== undefined ? ` ${threshold.value}` : '';
        const action = threshold.activateOnMatch ? 'ON' : 'OFF';
        const hysteresis = threshold.hysteresis ? ` (${threshold.hysteresis}s hysteresis)` : '';

        html += `
            <div style="background: white; border: 1px solid #ddd; border-radius: 4px; padding: 10px; margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <strong>${threshold.watchPath}</strong> ${operator}${value} → ${action}${hysteresis}
                </div>
                <button onclick="removeAddCommandThreshold(${index})" style="background: #f44336; color: white; border: none; padding: 4px 8px; border-radius: 3px; font-size: 0.8em;">
                    ❌ Remove
                </button>
            </div>
        `;
    });

    container.innerHTML = html;
}

export function removeAddCommandThreshold(index) {
    addingThresholds.splice(index, 1);
    displayAddCommandThresholdsList();
}

// Manual override functions
export async function setManualOverride(commandName, override, expiryMinutes = null) {
    try {
        const body = { override: override };
        if (expiryMinutes) {
            body.expiryMinutes = expiryMinutes;
        }

        const response = await fetch(`${getPluginPath()}/api/commands/${commandName}/override`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(body)
        });

        const result = await response.json();

        if (result.success) {
            await loadCommands();
            alert(`Manual override ${override ? 'enabled' : 'disabled'} for ${commandName}`);
        } else {
            alert(`Failed to set manual override: ${result.error}`);
        }
    } catch (error) {
        alert(`Network error: ${error.message}`);
    }
}

export async function promptManualOverride(commandName) {
    const override = confirm(`Enable manual override for command '${commandName}'?\n\nThis will ignore threshold conditions until manually cleared.`);

    if (override) {
        const expiry = prompt(`Override duration in minutes (leave empty for permanent):`);
        const expiryMinutes = expiry && !isNaN(expiry) ? parseInt(expiry) : null;

        await setManualOverride(commandName, true, expiryMinutes);
    }
}

export async function loadCommandHistory() {
    try {
        const response = await fetch(`${getPluginPath()}/api/commands/history`);
        const result = await response.json();
        
        if (result.success) {
            displayCommandHistory(result.data || []);
        } else {
            document.getElementById('commandHistoryContainer').innerHTML = `<div class="error">Error loading command history: ${result.error}</div>`;
        }
    } catch (error) {
        document.getElementById('commandHistoryContainer').innerHTML = `<div class="error">Error loading command history: ${error.message}</div>`;
    }
}

function displayCommandHistory(history) {
    const container = document.getElementById('commandHistoryContainer');

    if (!history || history.length === 0) {
        container.innerHTML = '<div class="info">No command history available.</div>';
        return;
    }

    let html = '<div class="table-container"><table><thead><tr>';
    html += '<th>Command</th><th>Action</th><th>Value</th><th>Status</th><th>Time</th><th>Error</th>';
    html += '</tr></thead><tbody>';

    history.forEach(entry => {
        const status = entry.success ? '✅ Success' : '❌ Failed';
        const timestamp = new Date(entry.timestamp).toLocaleString();
        const value = entry.value !== undefined ? (entry.value ? 'true' : 'false') : '-';
        html += `<tr>
            <td><strong>${entry.command}</strong></td>
            <td>${entry.action}</td>
            <td>${value}</td>
            <td>${status}</td>
            <td>${timestamp}</td>
            <td>${entry.error || '-'}</td>
        </tr>`;
    });

    html += '</tbody></table></div>';
    container.innerHTML = html;
}
