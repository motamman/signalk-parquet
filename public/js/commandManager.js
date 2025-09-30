import { getPluginPath } from './utils.js';

let editingThresholds = [];
let addingThresholds = [];
let currentEditingCommand = null;
let currentEditingRow = null;
let editFormDirty = false;
let editFormPlaceholder = null;
let editFormRowElement = null;

const editCommandFormElement = document.getElementById('editCommandForm');
let editFormOriginalParent = editCommandFormElement?.parentNode || null;

if (editCommandFormElement && editFormOriginalParent) {
    editFormPlaceholder = document.createElement('div');
    editFormPlaceholder.id = 'editCommandFormPlaceholder';
    editFormPlaceholder.style.display = 'none';
    editFormOriginalParent.insertBefore(editFormPlaceholder, editCommandFormElement.nextSibling);
}

function getEditCommandForm() {
    return editCommandFormElement;
}

function getEditCommandStatusElement() {
    return document.getElementById('editCommandStatus');
}

function getUpdateCommandButton() {
    return document.getElementById('updateCommandButton');
}

function extractPathsFromSignalK(obj, filterType = 'self') {
    const selfPaths = new Set();
    const nonSelfPaths = new Set();

    function extractRecursive(node, prefix = '') {
        if (!node || typeof node !== 'object') return;

        for (const key in node) {
            if (key === 'meta' || key === 'timestamp' || key === 'source' || key === '$source') continue;

            const currentPath = prefix ? `${prefix}.${key}` : key;

            if (node[key] && typeof node[key] === 'object') {
                if (node[key].value !== undefined) {
                    selfPaths.add(currentPath);
                } else {
                    extractRecursive(node[key], currentPath);
                }
            }
        }
    }

    const selfVesselId = obj?.self;
    const actualSelfId = selfVesselId && selfVesselId.startsWith('vessels.')
        ? selfVesselId.replace('vessels.', '')
        : selfVesselId;

    if (obj?.vessels) {
        if (actualSelfId && obj.vessels[actualSelfId]) {
            extractRecursive(obj.vessels[actualSelfId], '');
        }

        for (const vesselId in obj.vessels) {
            if (vesselId !== actualSelfId) {
                const tempPaths = new Set();
                function extractOtherVessel(node, prefix = '') {
                    if (!node || typeof node !== 'object') return;
                    for (const key in node) {
                        if (key === 'meta' || key === 'timestamp' || key === 'source' || key === '$source') continue;
                        const currentPath = prefix ? `${prefix}.${key}` : key;
                        if (node[key] && typeof node[key] === 'object') {
                            if (node[key].value !== undefined) {
                                tempPaths.add(currentPath);
                            } else {
                                extractOtherVessel(node[key], currentPath);
                            }
                        }
                    }
                }
                extractOtherVessel(obj.vessels[vesselId], '');
                tempPaths.forEach(path => nonSelfPaths.add(path));
            }
        }
    }

    for (const key in obj || {}) {
        if (!['vessels', 'self', 'version', 'sources', 'meta', 'timestamp'].includes(key)) {
            extractRecursive(obj[key], key);
        }
    }

    const targetPaths = filterType === 'self' ? selfPaths : nonSelfPaths;
    return Array.from(targetPaths).sort();
}

function updateEditCommandStatus(message = '', type = 'info') {
    const statusEl = getEditCommandStatusElement();
    if (!statusEl) return;
    if (!message) {
        statusEl.textContent = '';
        statusEl.style.display = 'none';
        return;
    }
    statusEl.textContent = message;
    statusEl.style.display = 'block';
    statusEl.style.color = type === 'error' ? '#d32f2f' : '#0066cc';
}

function setEditSaveButtonState() {
    const button = getUpdateCommandButton();
    if (!button) return;
    button.disabled = !editFormDirty;
    button.textContent = '✅ Update Command';
    button.style.opacity = editFormDirty ? '1' : '0.7';
}

function markEditFormDirty() {
    editFormDirty = true;
    setEditSaveButtonState();
    updateEditCommandStatus('Unsaved changes', 'info');
}

function clearEditFormDirtyState(message = '') {
    editFormDirty = false;
    setEditSaveButtonState();
    updateEditCommandStatus(message, 'info');
}

function setupEditFormFieldListeners() {
    const form = getEditCommandForm();
    if (!form || form.dataset.listenersAttached === 'true') {
        return;
    }

    const inputs = [
        'editCommandDescription',
        'editCommandKeywords'
    ];
    inputs.forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            el.addEventListener('input', () => markEditFormDirty());
        }
    });

    const defaultStateSelect = document.getElementById('editCommandDefaultState');
    if (defaultStateSelect) {
        defaultStateSelect.addEventListener('change', () => markEditFormDirty());
    }

    form.dataset.listenersAttached = 'true';
}

function ensureEditFormInDom() {
    const form = getEditCommandForm();
    if (!form) return null;

    if (!form.isConnected) {
        if (editFormPlaceholder && editFormPlaceholder.parentNode) {
            editFormPlaceholder.parentNode.insertBefore(form, editFormPlaceholder);
        } else {
            const parent = editFormOriginalParent || document.getElementById('commandManager') || document.body;
            parent.appendChild(form);
        }
    }

    return form;
}

function attachEditFormToRow(commandName, { scroll = true } = {}) {
    const form = ensureEditFormInDom();
    if (!form) return;

    const targetRow = document.querySelector(`tr[data-command-row="${commandName}"]`);
    if (!targetRow || !targetRow.parentNode) {
        return;
    }

    const tbody = targetRow.parentNode;
    if (!tbody) {
        return;
    }

    if (currentEditingRow && currentEditingRow !== targetRow) {
        currentEditingRow.classList.remove('editing-command-row');
        currentEditingRow.style.outline = '';
        currentEditingRow.style.outlineOffset = '';
    }

    targetRow.classList.add('editing-command-row');
    targetRow.style.outline = '2px solid #ffc107';
    targetRow.style.outlineOffset = '4px';
    currentEditingRow = targetRow;

    const columnCount = targetRow.children.length || targetRow.childElementCount || 1;

    if (!editFormRowElement) {
        editFormRowElement = document.createElement('tr');
        editFormRowElement.id = 'editCommandFormRow';
        editFormRowElement.classList.add('edit-command-form-row');
        const cell = document.createElement('td');
        cell.colSpan = columnCount;
        editFormRowElement.appendChild(cell);
    }

    const cell = editFormRowElement.firstElementChild || editFormRowElement.appendChild(document.createElement('td'));
    if (cell.colSpan !== columnCount) {
        cell.colSpan = columnCount;
    }
    cell.style.padding = '0';

    if (form.parentNode !== cell) {
        while (cell.firstChild) {
            cell.removeChild(cell.firstChild);
        }
        cell.appendChild(form);
    }

    tbody.insertBefore(editFormRowElement, targetRow.nextSibling);

    form.style.display = 'block';
    form.classList.add('active');
    form.style.width = '100%';
    form.style.maxWidth = '100%';
    form.style.margin = '20px 0';
    form.style.boxSizing = 'border-box';
    if (scroll) {
        targetRow.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }
}

function detachEditForm() {
    const form = getEditCommandForm();
    if (!form) {
        return;
    }

    if (currentEditingRow) {
        currentEditingRow.classList.remove('editing-command-row');
        currentEditingRow.style.outline = '';
        currentEditingRow.style.outlineOffset = '';
        currentEditingRow = null;
    }

    if (editFormRowElement && editFormRowElement.parentNode) {
        editFormRowElement.parentNode.removeChild(editFormRowElement);
    }

    if (editFormPlaceholder && editFormPlaceholder.parentNode) {
        editFormPlaceholder.parentNode.insertBefore(form, editFormPlaceholder);
    } else if (editFormOriginalParent) {
        editFormOriginalParent.appendChild(form);
    }

    cancelNewThreshold();

    form.style.display = 'none';
    form.classList.remove('active');
}

async function persistCommandChanges({ silent = false, closeOnSuccess = false, reason = '' } = {}) {
    try {
        const form = ensureEditFormInDom();
        if (!form) {
            if (!silent) {
                alert('Edit form is not available. Please reload the page.');
            }
            return false;
        }

        const command = document.getElementById('editCommandName').value.trim();
        const description = document.getElementById('editCommandDescription').value.trim();
        const keywordsInput = document.getElementById('editCommandKeywords').value.trim();
        const keywords = keywordsInput ? keywordsInput.split(',').map(k => k.trim()).filter(k => k.length > 0) : undefined;
        const defaultStateValue = document.getElementById('editCommandDefaultState').value;
        const defaultState = defaultStateValue === '' ? undefined : (defaultStateValue === 'true');
        const thresholds = editingThresholds.length > 0 ? editingThresholds : [];

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
            clearEditFormDirtyState(reason || (silent ? '' : 'Command updated successfully!'));
            await loadCommands();
            if (!closeOnSuccess) {
                attachEditFormToRow(command, { scroll: false });
            }
            if (closeOnSuccess) {
                hideEditCommandForm(true);
            }
            return true;
        }

        if (silent) {
            updateEditCommandStatus(result.error || 'Failed to update command', 'error');
        } else {
            alert('Failed to update command: ' + result.error);
        }
        return false;
    } catch (error) {
        if (silent) {
            updateEditCommandStatus(error.message || 'Error updating command', 'error');
        } else {
            alert('Error updating command: ' + error.message);
        }
        return false;
    }
}

export async function loadCommands() {
    try {
        const response = await fetch(`${getPluginPath()}/api/commands`);
        const result = await response.json();

        if (result.success) {
            displayCommands(result.commands || []);
            // Update automation states after displaying commands
            setTimeout(() => updateAllAutomationStates(), 100);

            // Subscribe to real-time command state updates
            setTimeout(() => subscribeToCommandStates(result.commands || []), 200);
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
    html += '<th>Command</th><th>Path</th><th>Description</th><th>Status</th><th>Actions</th>';
    html += '</tr></thead><tbody>';

    commands.forEach(command => {
        // Real-time status will be populated by SignalK subscription
        const status = `<span id="command-state-${command.command}" class="command-state">⏳ Loading...</span>`;
        const registeredDate = new Date(command.registered).toLocaleString();
        const keywords = command.keywords ? command.keywords.join(', ') : '';

        // Automation info
        let automationInfo = '<div style="font-size: 0.9em;">';

        // Automation status placeholder - will be populated by updateAutomationUI
        const hasThresholds = command.thresholds && command.thresholds.length > 0;
        if (hasThresholds) {
            automationInfo += `<div id="automation-status-${command.command}" style="margin-bottom: 5px;">
                <!-- Automation status will be updated dynamically -->
            </div>`;
        }

        // Manual override status (only for commands without thresholds)
        if (command.manualOverride && !hasThresholds) {
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
            automationInfo += '<div style="color: #999;">📱 Manual control only</div>';
        }

        if (keywords) {
            automationInfo += `<div style="color: #666; font-size: 0.8em; margin-top: 3px;">🏷️ ${keywords}</div>`;
        }

        automationInfo += '</div>';

        html += `<tr data-command-row="${command.command}">
            <td>
                <div><strong>${command.command}</strong></div>
                <div style="font-size: 0.8em; color: #666; margin-top: 2px;">${command.description || 'No description'}</div>
                <div style="font-size: 0.7em; color: #999; margin-top: 2px;">Registered: ${registeredDate}</div>
            </td>
            <td><code style="font-size: 0.8em;">vessels.self.commands.${command.command}</code></td>
            <td>${automationInfo}</td>
            <td style="text-align: center;">
                ${status}
            </td>
            <td style="text-align: center; min-width: 200px;">
                <div style="display: flex; flex-direction: column; gap: 3px; align-items: center;">
                    <div style="display: flex; gap: 3px;">
                        <button id="toggle-btn-${command.command}" onclick="toggleCommand('${command.command}')"
                                style="padding: 4px 8px; font-size: 0.8em;">🔴 Turn ON</button>
                    </div>
                    ${hasThresholds ?
                        `<div>
                            <button id="auto-toggle-${command.command}" onclick="toggleAutomation('${command.command}')"
                                    style="background: #ff9800; color: white; border: none; padding: 2px 6px; border-radius: 3px; font-size: 0.8em;">
                                👤 Disable Automation
                            </button>
                        </div>` :
                        // No override buttons for commands without thresholds - just use Start/Stop
                        ''
                    }
                    <div style="display: flex; gap: 3px;">
                        <button onclick="showEditCommandForm('${command.command}')" class="btn-secondary" style="padding: 3px 6px; font-size: 0.8em;">✏️ Edit</button>
                        <button onclick="unregisterCommand('${command.command}')" class="btn-danger" style="padding: 3px 6px; font-size: 0.8em;">❌ Remove</button>
                    </div>
                </div>
            </td>
        </tr>`;
    });

    html += '</tbody></table></div>';
    container.innerHTML = html;

    const form = getEditCommandForm();
    if (currentEditingCommand && form && form.style.display !== 'none') {
        attachEditFormToRow(currentEditingCommand, { scroll: false });
    }
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
    cancelAddCmdThreshold();
}

// Edit command functions
export async function showEditCommandForm(commandName) {
    try {
        if (currentEditingCommand && currentEditingCommand !== commandName && editFormDirty) {
            const proceed = confirm(`You have unsaved changes for ${currentEditingCommand}. Discard and edit ${commandName}?`);
            if (!proceed) {
                return;
            }
        }

        // Find the command in the current commands list
        const response = await fetch(`${getPluginPath()}/api/commands`).then(r => r.json());
        const command = response.commands.find(cmd => cmd.command === commandName);
        
        if (!command) {
            alert('Command not found');
            return;
        }

        currentEditingCommand = command.command;

        const form = ensureEditFormInDom();
        if (!form) {
            alert('Unable to locate the edit form in the document.');
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

        setupEditFormFieldListeners();
        attachEditFormToRow(command.command);
        clearEditFormDirtyState('');

    } catch (error) {
        alert('Failed to load command details: ' + error.message);
    }
}

export function hideEditCommandForm(force = false) {
    if (!force && editFormDirty) {
        const confirmClose = confirm('You have unsaved changes. Close without saving?');
        if (!confirmClose) {
            return;
        }
    }

    detachEditForm();
    currentEditingCommand = null;
    clearEditFormDirtyState('');
}

export async function updateCommand() {
    const success = await persistCommandChanges({ silent: false, closeOnSuccess: true });
    if (success) {
        alert('Command updated successfully!');
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
            await loadCommandHistory();
        } else {
            alert(`Error executing command: ${result.error}`);
        }
    } catch (error) {
        alert(`Network error: ${error.message}`);
    }
}

export async function toggleCommand(commandName) {
    // Get current state from the status display
    const stateElement = document.getElementById(`command-state-${commandName}`);
    if (!stateElement) return;

    const currentText = stateElement.textContent;
    const isCurrentlyOn = currentText.includes('🟢 ON');

    // Toggle to opposite state
    await executeCommand(commandName, !isCurrentlyOn);
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

// Path type detection
async function detectPathType(path) {
    try {
        const response = await fetch(`${getPluginPath()}/api/paths/${encodeURIComponent(path)}/type`);
        const result = await response.json();
        if (result.success) {
            return {
                dataType: result.dataType || 'unknown',
                unit: result.unit,
                enumValues: result.enumValues,
                description: result.description
            };
        }
    } catch (error) {
        console.log('Could not detect path type:', error);
    }
    return { dataType: 'unknown' };
}

// Update operator dropdown based on detected path type
function updateOperatorDropdown(operatorSelectId, dataType) {
    const operatorSelect = document.getElementById(operatorSelectId);
    if (!operatorSelect) return;

    // Clear existing options
    operatorSelect.innerHTML = '';

    let operators = [];

    switch (dataType) {
        case 'numeric':
        case 'angular':
            operators = [
                { value: 'gt', label: '> Greater Than' },
                { value: 'lt', label: '< Less Than' },
                { value: 'eq', label: '= Equal To' },
                { value: 'ne', label: '≠ Not Equal To' },
                { value: 'range', label: '⇄ Within Range' }
            ];
            break;
        case 'boolean':
            operators = [
                { value: 'true', label: 'Is True' },
                { value: 'false', label: 'Is False' }
            ];
            break;
        case 'string':
        case 'enum':
            operators = [
                { value: 'stringEquals', label: 'Equals' },
                { value: 'contains', label: 'Contains' },
                { value: 'startsWith', label: 'Starts With' },
                { value: 'endsWith', label: 'Ends With' }
            ];
            break;
        case 'position':
            operators = [
                { value: 'withinRadius', label: '📍 Within Radius' },
                { value: 'outsideRadius', label: '📍 Outside Radius' },
                { value: 'inBoundingBox', label: '🗺️ Inside Bounding Box' },
                { value: 'outsideBoundingBox', label: '🗺️ Outside Bounding Box' }
            ];
            break;
        default:
            // Unknown type - show all operators
            operators = [
                { value: 'gt', label: '> Greater Than' },
                { value: 'lt', label: '< Less Than' },
                { value: 'eq', label: '= Equal To' },
                { value: 'ne', label: '≠ Not Equal To' },
                { value: 'range', label: '⇄ Within Range' },
                { value: 'true', label: 'Is True' },
                { value: 'false', label: 'Is False' },
                { value: 'stringEquals', label: 'Equals (String)' },
                { value: 'contains', label: 'Contains' }
            ];
            break;
    }

    operators.forEach(op => {
        const option = document.createElement('option');
        option.value = op.value;
        option.textContent = op.label;
        operatorSelect.appendChild(option);
    });
}

// Update value fields based on operator and data type
function updateValueFields(operatorSelectId, valueContainerId, dataType) {
    const operator = document.getElementById(operatorSelectId)?.value;
    const container = document.getElementById(valueContainerId);
    if (!container) return;

    // Clear existing content
    container.innerHTML = '';

    if (!operator) return;

    // Boolean operators don't need value fields
    if (operator === 'true' || operator === 'false') {
        container.style.display = 'none';
        return;
    }

    container.style.display = 'block';

    // Range operator needs min/max fields
    if (operator === 'range') {
        container.innerHTML = `
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
                <div>
                    <label>Min Value:</label>
                    <input type="number" id="${valueContainerId}_min" step="any" style="width: 100%;">
                </div>
                <div>
                    <label>Max Value:</label>
                    <input type="number" id="${valueContainerId}_max" step="any" style="width: 100%;">
                </div>
            </div>
            ${dataType === 'angular' ? '<div style="font-size: 0.85em; color: #666; margin-top: 5px;">💡 Enter values in degrees (will be converted to radians)</div>' : ''}
        `;
        return;
    }

    // Geographic operators
    if (['withinRadius', 'outsideRadius'].includes(operator)) {
        container.innerHTML = `
            <div style="margin-bottom: 10px;">
                <label>
                    <input type="checkbox" id="${valueContainerId}_useHomePort">
                    Use Home Port as center
                </label>
            </div>
            <div id="${valueContainerId}_customLocation" style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 10px;">
                <div>
                    <label>Latitude:</label>
                    <input type="number" id="${valueContainerId}_lat" step="0.000001" placeholder="e.g., 40.712800" style="width: 100%;">
                </div>
                <div>
                    <label>Longitude:</label>
                    <input type="number" id="${valueContainerId}_lon" step="0.000001" placeholder="e.g., -74.006000" style="width: 100%;">
                </div>
            </div>
            <div>
                <label>Radius (meters):</label>
                <input type="number" id="${valueContainerId}_radius" step="1" min="0" placeholder="e.g., 1000" style="width: 100%;">
            </div>
        `;

        // Add event listener to toggle custom location fields
        const checkbox = document.getElementById(`${valueContainerId}_useHomePort`);
        const customLocation = document.getElementById(`${valueContainerId}_customLocation`);
        checkbox.addEventListener('change', function() {
            customLocation.style.display = this.checked ? 'none' : 'grid';
        });

        return;
    }

    if (['inBoundingBox', 'outsideBoundingBox'].includes(operator)) {
        container.innerHTML = `
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
                <div>
                    <label>North (lat):</label>
                    <input type="number" id="${valueContainerId}_north" step="0.000001" style="width: 100%;">
                </div>
                <div>
                    <label>South (lat):</label>
                    <input type="number" id="${valueContainerId}_south" step="0.000001" style="width: 100%;">
                </div>
                <div>
                    <label>East (lon):</label>
                    <input type="number" id="${valueContainerId}_east" step="0.000001" style="width: 100%;">
                </div>
                <div>
                    <label>West (lon):</label>
                    <input type="number" id="${valueContainerId}_west" step="0.000001" style="width: 100%;">
                </div>
            </div>
            <div style="font-size: 0.85em; color: #666; margin-top: 5px;">💡 Box can cross 180° meridian</div>
        `;
        return;
    }

    // String operators
    if (['contains', 'startsWith', 'endsWith', 'stringEquals'].includes(operator)) {
        container.innerHTML = `
            <label>Value:</label>
            <input type="text" id="${valueContainerId}_value" placeholder="Enter text" style="width: 100%;">
        `;
        return;
    }

    // Numeric operators (gt, lt, eq, ne)
    const isAngular = dataType === 'angular';
    container.innerHTML = `
        <label>Value:</label>
        <input type="number" id="${valueContainerId}_value" step="any" placeholder="Enter value" style="width: 100%;">
        ${isAngular ? '<div style="font-size: 0.85em; color: #666; margin-top: 5px;">💡 Enter value in degrees (will be converted to radians)</div>' : ''}
    `;
}

export function updateThresholdPathFilter() {
    populateThresholdPaths();
}

async function populateThresholdPaths() {
    ensureEditFormInDom();
    const select = document.getElementById('newThresholdPath');
    const filterType = document.querySelector('input[name="thresholdPathFilter"]:checked')?.value || 'self';

    // Clear existing options except the default ones
    select.innerHTML = '<option value="">-- Select SignalK Path --</option><option value="custom">🖊️ Enter Custom Path</option>';

    try {
        let allPaths = [];
        try {
            const response = await fetch('/signalk/v1/api/');
            if (response.ok) {
                const data = await response.json();
                allPaths = extractPathsFromSignalK(data, filterType);
            }
        } catch (error) {
            console.log('Could not load SignalK API data, falling back to plugin paths:', error);
        }

        if (!allPaths.length) {
            try {
                const pluginResponse = await fetch(`${getPluginPath()}/api/paths`);
                if (pluginResponse.ok) {
                    const pluginData = await pluginResponse.json();
                    if (pluginData.success && Array.isArray(pluginData.paths)) {
                        allPaths = pluginData.paths
                            .map(pathInfo => pathInfo.path)
                            .filter(Boolean);
                    }
                }
            } catch (pluginError) {
                console.log('Failed to load plugin paths for thresholds:', pluginError);
            }
        }

        const uniquePaths = Array.from(new Set(allPaths)).sort();

        uniquePaths.forEach(path => {
            const option = document.createElement('option');
            option.value = path;
            option.textContent = path;
            select.appendChild(option);
        });

    } catch (error) {
        console.log('Could not load real-time SignalK paths for thresholds:', error);
    }

    // Handle custom path selection and path type detection
    select.onchange = async function() {
        const customInput = document.getElementById('newThresholdPathCustom');
        if (this.value === 'custom') {
            customInput.style.display = 'block';
            customInput.focus();
        } else {
            customInput.style.display = 'none';

            // Detect path type and update operators
            if (this.value) {
                const typeInfo = await detectPathType(this.value);
                updateOperatorDropdown('newThresholdOperator', typeInfo.dataType);
                updateValueFields('newThresholdOperator', 'newThresholdValueGroup', typeInfo.dataType);

                // Store data type for later use
                document.getElementById('newThresholdOperator').dataset.pathDataType = typeInfo.dataType;
            }
        }
    };

    // Handle custom path input
    const customInput = document.getElementById('newThresholdPathCustom');
    if (customInput) {
        customInput.onblur = async function() {
            if (this.value) {
                const typeInfo = await detectPathType(this.value);
                updateOperatorDropdown('newThresholdOperator', typeInfo.dataType);
                updateValueFields('newThresholdOperator', 'newThresholdValueGroup', typeInfo.dataType);

                // Store data type for later use
                document.getElementById('newThresholdOperator').dataset.pathDataType = typeInfo.dataType;
            }
        };
    }
}

export function toggleNewThresholdValueField() {
    const operatorSelect = document.getElementById('newThresholdOperator');
    const dataType = operatorSelect?.dataset.pathDataType || 'unknown';
    updateValueFields('newThresholdOperator', 'newThresholdValueGroup', dataType);
}

export function addNewThreshold() {
    const form = document.getElementById('addThresholdForm');
    form.style.display = 'block';

    const trigger = document.getElementById('editCommandAddThresholdButton');
    if (trigger) {
        trigger.style.display = 'none';
    }

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
    const form = document.getElementById('addThresholdForm');
    if (form) {
        form.style.display = 'none';
    }

    const trigger = document.getElementById('editCommandAddThresholdButton');
    if (trigger) {
        trigger.style.display = 'inline-block';
    }
}

export function saveNewThreshold() {
    const pathSelect = document.getElementById('newThresholdPath');
    const pathCustom = document.getElementById('newThresholdPathCustom');
    const operator = document.getElementById('newThresholdOperator').value;
    const action = document.getElementById('newThresholdAction').value === 'true';
    const hysteresis = document.getElementById('newThresholdHysteresis').value.trim();
    const dataType = document.getElementById('newThresholdOperator')?.dataset.pathDataType || 'unknown';

    // Get the path
    let path = pathSelect.value;
    if (path === 'custom') {
        path = pathCustom.value.trim();
    }

    if (!path) {
        alert('Please select or enter a SignalK path');
        return;
    }

    // Create new threshold
    const threshold = {
        enabled: true,
        watchPath: path,
        operator: operator,
        activateOnMatch: action
    };

    // Handle different operator types
    if (operator === 'range') {
        const min = document.getElementById('newThresholdValueGroup_min')?.value;
        const max = document.getElementById('newThresholdValueGroup_max')?.value;
        if (!min || !max) {
            alert('Please enter both min and max values for range');
            return;
        }
        threshold.valueMin = parseFloat(min);
        threshold.valueMax = parseFloat(max);

        // Convert degrees to radians for angular values
        if (dataType === 'angular') {
            threshold.valueMin = threshold.valueMin * (Math.PI / 180);
            threshold.valueMax = threshold.valueMax * (Math.PI / 180);
        }
    } else if (operator === 'withinRadius' || operator === 'outsideRadius') {
        const useHomePort = document.getElementById('newThresholdValueGroup_useHomePort')?.checked;
        const radius = document.getElementById('newThresholdValueGroup_radius')?.value;

        if (!radius) {
            alert('Please enter a radius value');
            return;
        }

        threshold.useHomePort = useHomePort;
        threshold.radius = parseFloat(radius);

        if (!useHomePort) {
            const lat = document.getElementById('newThresholdValueGroup_lat')?.value;
            const lon = document.getElementById('newThresholdValueGroup_lon')?.value;
            if (!lat || !lon) {
                alert('Please enter latitude and longitude');
                return;
            }
            threshold.latitude = parseFloat(lat);
            threshold.longitude = parseFloat(lon);
        }
    } else if (operator === 'inBoundingBox' || operator === 'outsideBoundingBox') {
        const north = document.getElementById('newThresholdValueGroup_north')?.value;
        const south = document.getElementById('newThresholdValueGroup_south')?.value;
        const east = document.getElementById('newThresholdValueGroup_east')?.value;
        const west = document.getElementById('newThresholdValueGroup_west')?.value;

        if (!north || !south || !east || !west) {
            alert('Please enter all bounding box coordinates');
            return;
        }

        threshold.boundingBox = {
            north: parseFloat(north),
            south: parseFloat(south),
            east: parseFloat(east),
            west: parseFloat(west)
        };
    } else if (operator !== 'true' && operator !== 'false') {
        // String or numeric value
        const valueInput = document.getElementById('newThresholdValueGroup_value');
        if (!valueInput || !valueInput.value.trim()) {
            alert('Please enter a threshold value');
            return;
        }

        const value = valueInput.value.trim();

        // String operators
        if (['contains', 'startsWith', 'endsWith', 'stringEquals'].includes(operator)) {
            threshold.value = value;
        } else {
            // Numeric operators
            const numValue = parseFloat(value);
            if (isNaN(numValue)) {
                alert('Please enter a valid numeric value');
                return;
            }
            threshold.value = numValue;

            // Convert degrees to radians for angular values
            if (dataType === 'angular') {
                threshold.value = threshold.value * (Math.PI / 180);
            }
        }
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
    displayThresholdsList();
    cancelNewThreshold();
    markEditFormDirty();
}

function displayThresholdsList() {
    const container = document.getElementById('thresholdsList');

    if (!editingThresholds || editingThresholds.length === 0) {
        container.innerHTML = '<div style="color: #999; font-style: italic; padding: 10px;">No thresholds configured</div>';
        return;
    }

    let html = '';
    editingThresholds.forEach((threshold, index) => {
        let description = '';

        // Format description based on operator type
        if (threshold.operator === 'range') {
            description = `${threshold.valueMin} to ${threshold.valueMax}`;
        } else if (threshold.operator === 'withinRadius' || threshold.operator === 'outsideRadius') {
            const location = threshold.useHomePort ? 'home port' : `${threshold.latitude}, ${threshold.longitude}`;
            const op = threshold.operator === 'withinRadius' ? 'within' : 'outside';
            description = `${op} ${threshold.radius}m of ${location}`;
        } else if (threshold.operator === 'inBoundingBox' || threshold.operator === 'outsideBoundingBox') {
            const op = threshold.operator === 'inBoundingBox' ? 'inside' : 'outside';
            description = `${op} box [${threshold.boundingBox.north}°N, ${threshold.boundingBox.south}°S, ${threshold.boundingBox.east}°E, ${threshold.boundingBox.west}°W]`;
        } else if (threshold.operator === 'true' || threshold.operator === 'false') {
            description = threshold.operator === 'true' ? 'is true' : 'is false';
        } else {
            const operatorSymbol = {
                'gt': '>', 'lt': '<', 'eq': '=', 'ne': '≠',
                'contains': 'contains', 'startsWith': 'starts with', 'endsWith': 'ends with', 'stringEquals': 'equals'
            }[threshold.operator] || threshold.operator;
            description = `${operatorSymbol} ${threshold.value !== undefined ? threshold.value : ''}`;
        }

        const action = threshold.activateOnMatch ? 'ON' : 'OFF';
        const hysteresis = threshold.hysteresis ? ` (${threshold.hysteresis}s hysteresis)` : '';

        html += `
            <div style="background: white; border: 1px solid #ddd; border-radius: 4px; padding: 10px; margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <strong>${threshold.watchPath}</strong> ${description} → ${action}${hysteresis}
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
    markEditFormDirty();
}

// Add command threshold functions
export function updateAddCmdThresholdPathFilter() {
    populateAddCmdThresholdPaths();
}

async function populateAddCmdThresholdPaths() {
    ensureEditFormInDom();
    const select = document.getElementById('addCmdThresholdPath');
    const filterType = document.querySelector('input[name="addCmdPathFilter"]:checked')?.value || 'self';

    // Clear existing options except the default ones
    select.innerHTML = '<option value="">-- Select SignalK Path --</option><option value="custom">🖊️ Enter Custom Path</option>';

    try {
        let allPaths = [];
        try {
            const response = await fetch('/signalk/v1/api/');
            if (response.ok) {
                const data = await response.json();
                allPaths = extractPathsFromSignalK(data, filterType);
            }
        } catch (error) {
            console.log('Could not load SignalK API data for add command thresholds:', error);
        }

        if (!allPaths.length) {
            try {
                const pluginResponse = await fetch(`${getPluginPath()}/api/paths`);
                if (pluginResponse.ok) {
                    const pluginData = await pluginResponse.json();
                    if (pluginData.success && Array.isArray(pluginData.paths)) {
                        allPaths = pluginData.paths
                            .map(pathInfo => pathInfo.path)
                            .filter(Boolean);
                    }
                }
            } catch (pluginError) {
                console.log('Failed to load plugin paths for add command thresholds:', pluginError);
            }
        }

        const uniquePaths = Array.from(new Set(allPaths)).sort();

        uniquePaths.forEach(path => {
            const option = document.createElement('option');
            option.value = path;
            option.textContent = path;
            select.appendChild(option);
        });

    } catch (error) {
        console.log('Could not load real-time SignalK paths for add command thresholds:', error);
    }

    // Handle custom path selection and path type detection
    select.onchange = async function() {
        const customInput = document.getElementById('addCmdThresholdPathCustom');
        if (this.value === 'custom') {
            customInput.style.display = 'block';
            customInput.focus();
        } else {
            customInput.style.display = 'none';

            // Detect path type and update operators
            if (this.value) {
                const typeInfo = await detectPathType(this.value);
                updateOperatorDropdown('addCmdThresholdOperator', typeInfo.dataType);
                updateValueFields('addCmdThresholdOperator', 'addCmdThresholdValueGroup', typeInfo.dataType);

                // Store data type for later use
                document.getElementById('addCmdThresholdOperator').dataset.pathDataType = typeInfo.dataType;
            }
        }
    };

    // Handle custom path input
    const customInput = document.getElementById('addCmdThresholdPathCustom');
    if (customInput) {
        customInput.onblur = async function() {
            if (this.value) {
                const typeInfo = await detectPathType(this.value);
                updateOperatorDropdown('addCmdThresholdOperator', typeInfo.dataType);
                updateValueFields('addCmdThresholdOperator', 'addCmdThresholdValueGroup', typeInfo.dataType);

                // Store data type for later use
                document.getElementById('addCmdThresholdOperator').dataset.pathDataType = typeInfo.dataType;
            }
        };
    }
}

export function toggleAddCmdThresholdValueField() {
    const operatorSelect = document.getElementById('addCmdThresholdOperator');
    const dataType = operatorSelect?.dataset.pathDataType || 'unknown';
    updateValueFields('addCmdThresholdOperator', 'addCmdThresholdValueGroup', dataType);
}

export function addNewCommandThreshold() {
    const form = document.getElementById('addCommandThresholdForm');
    form.style.display = 'block';

    const trigger = document.getElementById('addCommandThresholdButton');
    if (trigger) {
        trigger.style.display = 'none';
    }

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
    const form = document.getElementById('addCommandThresholdForm');
    if (form) {
        form.style.display = 'none';
    }

    const trigger = document.getElementById('addCommandThresholdButton');
    if (trigger) {
        trigger.style.display = 'inline-block';
    }
}

export function saveAddCmdThreshold() {
    const pathSelect = document.getElementById('addCmdThresholdPath');
    const pathCustom = document.getElementById('addCmdThresholdPathCustom');
    const operator = document.getElementById('addCmdThresholdOperator').value;
    const action = document.getElementById('addCmdThresholdAction').value === 'true';
    const hysteresis = document.getElementById('addCmdThresholdHysteresis').value.trim();
    const dataType = document.getElementById('addCmdThresholdOperator')?.dataset.pathDataType || 'unknown';

    // Get the path
    let path = pathSelect.value;
    if (path === 'custom') {
        path = pathCustom.value.trim();
    }

    if (!path) {
        alert('Please select or enter a SignalK path');
        return;
    }

    // Create new threshold
    const threshold = {
        enabled: true,
        watchPath: path,
        operator: operator,
        activateOnMatch: action
    };

    // Handle different operator types
    if (operator === 'range') {
        const min = document.getElementById('addCmdThresholdValueGroup_min')?.value;
        const max = document.getElementById('addCmdThresholdValueGroup_max')?.value;
        if (!min || !max) {
            alert('Please enter both min and max values for range');
            return;
        }
        threshold.valueMin = parseFloat(min);
        threshold.valueMax = parseFloat(max);

        // Convert degrees to radians for angular values
        if (dataType === 'angular') {
            threshold.valueMin = threshold.valueMin * (Math.PI / 180);
            threshold.valueMax = threshold.valueMax * (Math.PI / 180);
        }
    } else if (operator === 'withinRadius' || operator === 'outsideRadius') {
        const useHomePort = document.getElementById('addCmdThresholdValueGroup_useHomePort')?.checked;
        const radius = document.getElementById('addCmdThresholdValueGroup_radius')?.value;

        if (!radius) {
            alert('Please enter a radius value');
            return;
        }

        threshold.useHomePort = useHomePort;
        threshold.radius = parseFloat(radius);

        if (!useHomePort) {
            const lat = document.getElementById('addCmdThresholdValueGroup_lat')?.value;
            const lon = document.getElementById('addCmdThresholdValueGroup_lon')?.value;
            if (!lat || !lon) {
                alert('Please enter latitude and longitude');
                return;
            }
            threshold.latitude = parseFloat(lat);
            threshold.longitude = parseFloat(lon);
        }
    } else if (operator === 'inBoundingBox' || operator === 'outsideBoundingBox') {
        const north = document.getElementById('addCmdThresholdValueGroup_north')?.value;
        const south = document.getElementById('addCmdThresholdValueGroup_south')?.value;
        const east = document.getElementById('addCmdThresholdValueGroup_east')?.value;
        const west = document.getElementById('addCmdThresholdValueGroup_west')?.value;

        if (!north || !south || !east || !west) {
            alert('Please enter all bounding box coordinates');
            return;
        }

        threshold.boundingBox = {
            north: parseFloat(north),
            south: parseFloat(south),
            east: parseFloat(east),
            west: parseFloat(west)
        };
    } else if (operator !== 'true' && operator !== 'false') {
        // String or numeric value
        const valueInput = document.getElementById('addCmdThresholdValueGroup_value');
        if (!valueInput || !valueInput.value.trim()) {
            alert('Please enter a threshold value');
            return;
        }

        const value = valueInput.value.trim();

        // String operators
        if (['contains', 'startsWith', 'endsWith', 'stringEquals'].includes(operator)) {
            threshold.value = value;
        } else {
            // Numeric operators
            const numValue = parseFloat(value);
            if (isNaN(numValue)) {
                alert('Please enter a valid numeric value');
                return;
            }
            threshold.value = numValue;

            // Convert degrees to radians for angular values
            if (dataType === 'angular') {
                threshold.value = threshold.value * (Math.PI / 180);
            }
        }
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
        let description = '';

        // Format description based on operator type
        if (threshold.operator === 'range') {
            description = `${threshold.valueMin} to ${threshold.valueMax}`;
        } else if (threshold.operator === 'withinRadius' || threshold.operator === 'outsideRadius') {
            const location = threshold.useHomePort ? 'home port' : `${threshold.latitude}, ${threshold.longitude}`;
            const op = threshold.operator === 'withinRadius' ? 'within' : 'outside';
            description = `${op} ${threshold.radius}m of ${location}`;
        } else if (threshold.operator === 'inBoundingBox' || threshold.operator === 'outsideBoundingBox') {
            const op = threshold.operator === 'inBoundingBox' ? 'inside' : 'outside';
            description = `${op} box [${threshold.boundingBox.north}°N, ${threshold.boundingBox.south}°S, ${threshold.boundingBox.east}°E, ${threshold.boundingBox.west}°W]`;
        } else if (threshold.operator === 'true' || threshold.operator === 'false') {
            description = threshold.operator === 'true' ? 'is true' : 'is false';
        } else {
            const operatorSymbol = {
                'gt': '>', 'lt': '<', 'eq': '=', 'ne': '≠',
                'contains': 'contains', 'startsWith': 'starts with', 'endsWith': 'ends with', 'stringEquals': 'equals'
            }[threshold.operator] || threshold.operator;
            description = `${operatorSymbol} ${threshold.value !== undefined ? threshold.value : ''}`;
        }

        const action = threshold.activateOnMatch ? 'ON' : 'OFF';
        const hysteresis = threshold.hysteresis ? ` (${threshold.hysteresis}s hysteresis)` : '';

        html += `
            <div style="background: white; border: 1px solid #ddd; border-radius: 4px; padding: 10px; margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <strong>${threshold.watchPath}</strong> ${description} → ${action}${hysteresis}
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

// Automation control functions
export async function toggleAutomation(commandName) {
    const button = document.getElementById(`auto-toggle-${commandName}`);
    const originalText = button?.textContent;

    try {
        // Show loading state
        if (button) {
            button.textContent = '⏳ Updating...';
            button.disabled = true;
        }

        // Get current automation state
        const currentState = await getAutomationState(commandName);
        const newState = !currentState;

        console.log(`🔄 Toggling automation for ${commandName}: ${currentState} → ${newState}`);

        // Update automation state
        const response = await fetch(`/signalk/v1/api/vessels/self/commands/${commandName}/auto`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                value: newState
            })
        });

        if (response.ok) {
            // Wait a moment for SignalK to process
            await new Promise(resolve => setTimeout(resolve, 500));

            // Verify the change took effect
            const verifyState = await getAutomationState(commandName);
            if (verifyState === newState) {
                updateAutomationUI(commandName, newState);
                console.log(`✅ Automation ${newState ? 'enabled' : 'disabled'} for ${commandName}`);

                // Show success feedback
                if (button) {
                    button.style.background = newState ? '#ff9800' : '#4caf50';
                    button.textContent = newState ? '👤 Disable Automation' : '🤖 Enable Automation';
                }
            } else {
                throw new Error(`State verification failed: expected ${newState}, got ${verifyState}`);
            }
        } else {
            const errorText = await response.text();
            throw new Error(`HTTP ${response.status}: ${errorText}`);
        }
    } catch (error) {
        console.error(`❌ Automation toggle failed for ${commandName}:`, error);
        alert(`Failed to update automation: ${error.message}`);

        // Restore button state
        if (button && originalText) {
            button.textContent = originalText;
        }
    } finally {
        // Re-enable button
        if (button) {
            button.disabled = false;
        }
    }
}

async function getAutomationState(commandName) {
    try {
        const response = await fetch(`/signalk/v1/api/vessels/self/commands/${commandName}/auto`);
        if (response.ok) {
            const data = await response.json();
            console.log(`🔍 getAutomationState(${commandName}):`, data);
            return data.value || false;
        } else {
            console.log(`❌ getAutomationState(${commandName}) HTTP ${response.status}`);
        }
    } catch (error) {
        console.log(`❌ getAutomationState(${commandName}) error:`, error);
    }
    return false; // Default to false if unable to fetch
}

function updateAutomationUI(commandName, autoEnabled) {
    // Update automation status display with clear, non-contradictory states
    const statusContainer = document.getElementById(`automation-status-${commandName}`);
    if (statusContainer) {
        if (autoEnabled) {
            // Automation is ON - show that thresholds are controlling the command
            statusContainer.innerHTML = `
                <div style="color: #4caf50; font-weight: bold; display: flex; align-items: center; gap: 8px;">
                    <span>🤖 Automated Control</span>
                    <span style="font-size: 0.8em; color: #666; font-weight: normal;">Controlled by thresholds</span>
                </div>
            `;
        } else {
            // Automation is OFF - show manual control with option to enable automation
            statusContainer.innerHTML = `
                <div style="color: #ff9800; font-weight: bold; display: flex; align-items: center; gap: 8px;">
                    <span>👤 Manual Control</span>
                    <span style="font-size: 0.8em; color: #666; font-weight: normal;">Thresholds inactive</span>
                </div>
            `;
        }
    }

    // Update automation toggle button with clear labels
    const autoToggleButton = document.getElementById(`auto-toggle-${commandName}`);
    if (autoToggleButton) {
        if (autoEnabled) {
            autoToggleButton.textContent = '👤 Disable Automation';
            autoToggleButton.style.background = '#ff9800';
            autoToggleButton.title = 'Disable automation and switch to manual control';
        } else {
            autoToggleButton.textContent = '🤖 Enable Automation';
            autoToggleButton.style.background = '#4caf50';
            autoToggleButton.title = 'Enable threshold-based automation';
        }
    }

    // Disable/enable command toggle button based on automation status
    const commandToggleButton = document.getElementById(`toggle-btn-${commandName}`);

    if (commandToggleButton) {
        if (autoEnabled) {
            // Automation is ON - disable manual button
            commandToggleButton.disabled = true;
            commandToggleButton.style.opacity = '0.5';
            commandToggleButton.style.cursor = 'not-allowed';
            commandToggleButton.title = 'Command is under automatic control';
        } else {
            // Automation is OFF - enable manual button and update text based on current state
            commandToggleButton.disabled = false;
            commandToggleButton.style.opacity = '1';
            commandToggleButton.style.cursor = 'pointer';

            // Update button text based on current command state
            const stateElement = document.getElementById(`command-state-${commandName}`);
            if (stateElement) {
                const isCurrentlyOn = stateElement.textContent.includes('🟢 ON');
                commandToggleButton.textContent = isCurrentlyOn ? '🔴 Turn OFF' : '🟢 Turn ON';
                commandToggleButton.title = isCurrentlyOn ? 'Manually stop the command' : 'Manually start the command';
            }
        }
    }
}

// Store WebSocket connection for real-time updates
let signalKWebSocket = null;

// Subscribe to real-time command state updates via SignalK WebSocket
function subscribeToCommandStates(commands) {
    // Close existing connection
    if (signalKWebSocket) {
        signalKWebSocket.close();
    }

    // Get SignalK WebSocket URL
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/signalk/v1/stream?subscribe=none`;

    try {
        signalKWebSocket = new WebSocket(wsUrl);

        signalKWebSocket.onopen = () => {
            console.log('📡 Connected to SignalK stream for command states');

            // Subscribe to each command path
            commands.forEach(command => {
                const subscription = {
                    context: 'vessels.self',
                    subscribe: [{
                        path: `commands.${command.command}`,
                        period: 1000,
                        format: 'delta',
                        policy: 'instant',
                        minPeriod: 200
                    }]
                };

                signalKWebSocket.send(JSON.stringify(subscription));
                console.log(`📡 Subscribed to commands.${command.command}`);
            });
        };

        signalKWebSocket.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);

                if (data.updates) {
                    data.updates.forEach(update => {
                        if (update.values) {
                            update.values.forEach(value => {
                                if (value.path && value.path.startsWith('commands.')) {
                                    const commandName = value.path.replace('commands.', '');

                                    // Handle SignalK value extraction properly
                                    let actualValue = value.value;
                                    if (actualValue && typeof actualValue === 'object' && 'value' in actualValue) {
                                        actualValue = actualValue.value;
                                    }

                                    // Only update if we have a valid value (not null/undefined)
                                    if (actualValue !== null && actualValue !== undefined) {
                                        const commandState = Boolean(actualValue);
                                        updateCommandStateDisplay(commandName, commandState);
                                    }
                                }
                            });
                        }
                    });
                }
            } catch (error) {
                console.warn('Failed to parse SignalK message:', error);
            }
        };

        signalKWebSocket.onerror = (error) => {
            console.error('SignalK WebSocket error:', error);
        };

        signalKWebSocket.onclose = () => {
            console.log('📡 SignalK stream disconnected');
        };

    } catch (error) {
        console.error('Failed to connect to SignalK stream:', error);
    }
}

// Update command state display in real-time
function updateCommandStateDisplay(commandName, isOn) {
    const stateElement = document.getElementById(`command-state-${commandName}`);
    if (stateElement) {
        if (isOn) {
            stateElement.innerHTML = '<span style="color: #4caf50; font-weight: bold;">🟢 ON</span>';
        } else {
            stateElement.innerHTML = '<span style="color: #f44336; font-weight: bold;">🔴 OFF</span>';
        }
    }

    // Also update the toggle button text
    const commandToggleButton = document.getElementById(`toggle-btn-${commandName}`);
    if (commandToggleButton && !commandToggleButton.disabled) {
        commandToggleButton.textContent = isOn ? '🔴 Turn OFF' : '🟢 Turn ON';
        commandToggleButton.title = isOn ? 'Manually stop the command' : 'Manually start the command';
    }
}

// Load and update automation states for all commands
export async function updateAllAutomationStates() {
    try {
        const response = await fetch(`${getPluginPath()}/api/commands`);
        const result = await response.json();

        if (result.success && result.commands) {
            for (const command of result.commands) {
                if (command.thresholds && command.thresholds.length > 0) {
                    const autoState = await getAutomationState(command.command);
                    updateAutomationUI(command.command, autoState);
                }
            }
        }
    } catch (error) {
        console.log('Error updating automation states:', error);
    }
}
