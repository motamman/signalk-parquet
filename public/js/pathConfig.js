import { getPluginPath } from './utils.js';

let showCommandPaths = false;

// Path Configuration Management Functions
export async function loadPathConfigurations() {
    try {
        const response = await fetch(`${getPluginPath()}/api/config/paths`);
        const result = await response.json();
        
        if (result.success) {
            displayPathConfigurations(result.paths);
        } else {
            document.getElementById('pathConfigContainer').innerHTML = 
                `<div class="error">Error loading path configurations: ${result.error}</div>`;
        }
    } catch (error) {
        document.getElementById('pathConfigContainer').innerHTML = 
            `<div class="error">Network error: ${error.message}</div>`;
    }
}

function displayPathConfigurations(paths) {
    const container = document.getElementById('pathConfigContainer');
    
    if (!paths || paths.length === 0) {
        container.innerHTML = `
            <div style="text-align: center; padding: 40px; background: #f8f9fa; border-radius: 5px; margin: 20px 0;">
                <h3 style="color: #666; margin-bottom: 10px;">No Path Configurations Found</h3>
                <p style="color: #666; margin-bottom: 20px;">You need to configure SignalK paths to start collecting data.</p>
                <button onclick="showAddPathForm()">➕ Add Your First Path</button>
            </div>
        `;
        return;
    }

    // Filter paths based on showCommandPaths setting
    const filteredPaths = showCommandPaths ? paths : paths.filter(path => !path.path || !path.path.startsWith('commands.'));
    
    if (filteredPaths.length === 0) {
        const commandCount = paths.filter(path => path.path && path.path.startsWith('commands.')).length;
        container.innerHTML = `
            <div style="text-align: center; padding: 40px; background: #f8f9fa; border-radius: 5px; margin: 20px 0;">
                <h3 style="color: #666; margin-bottom: 10px;">No Data Paths Found</h3>
                <p style="color: #666; margin-bottom: 20px;">
                    ${commandCount > 0 ? `${commandCount} command path${commandCount > 1 ? 's' : ''} hidden. ` : ''}
                    Add data paths to start collecting SignalK data.
                </p>
                <button onclick="showAddPathForm()">➕ Add Your First Data Path</button>
            </div>
        `;
        return;
    }

    let html = '<div class="table-container"><table><thead><tr>';
    html += '<th>Path</th><th>Always Enabled</th><th>Regimen</th><th>Source</th><th>Context</th><th>Exclude MMSI</th><th>Actions</th>'
    html += '</tr></thead><tbody>';

    filteredPaths.forEach((path, filteredIndex) => {
        // Find the original index in the full paths array
        const originalIndex = paths.findIndex(p => p === path);
        const excludeMMSI = path.excludeMMSI && path.excludeMMSI.length > 0 ? path.excludeMMSI.join(', ') : '';
        const isCommand = path.path && path.path.startsWith('commands.');
        const rowClass = isCommand ? 'style="background-color: #fff3cd;"' : '';
        
        html += `<tr data-index="${originalIndex}" ${rowClass}>
            <td><code>${path.path || ''}</code>${isCommand ? ' <span style="color: #856404; font-size: 11px;">(Command)</span>' : ''}</td>
            <td>${path.enabled ? '✅' : '❌'}</td>
            <td>${path.regimen || ''}</td>
            <td><code>${path.source || ''}</code></td>
            <td>${path.context || 'vessels.self'}</td>
            <td>${excludeMMSI}</td>
            <td>
                <button onclick="editPathConfiguration(${originalIndex})" style="padding: 5px 10px; font-size: 12px;">✏️ Edit</button>
                <button onclick="removePathConfiguration(${originalIndex})" style="padding: 5px 10px; font-size: 12px; background: #dc3545;">🗑️ Remove</button>
            </td>
        </tr>`;
    });

    html += '</tbody></table></div>';
    
    // Add summary info
    const totalPaths = paths.length;
    const commandPaths = paths.filter(path => path.path && path.path.startsWith('commands.')).length;
    const dataPaths = totalPaths - commandPaths;
    
    const summaryHtml = `
        <div style="margin-top: 10px; padding: 10px; background: #f8f9fa; border-radius: 5px; font-size: 14px; color: #666;">
            Showing ${filteredPaths.length} of ${totalPaths} paths 
            (${dataPaths} data path${dataPaths !== 1 ? 's' : ''}, ${commandPaths} command path${commandPaths !== 1 ? 's' : ''})
        </div>
    `;
    
    container.innerHTML = html + summaryHtml;
}

export function toggleCommandPaths() {
    showCommandPaths = !showCommandPaths;
    const button = document.getElementById('toggleCommandsBtn');
    button.textContent = showCommandPaths ? '🙈 Hide Commands' : '👁️ Show Commands';
    
    // Re-display the paths with the new filter
    loadPathConfigurations();
}

export async function showAddPathForm() {
    document.getElementById('addPathForm').style.display = 'block';
    populateSignalKPaths();
    await populateAvailableRegimens();
}

export function hideAddPathForm() {
    document.getElementById('addPathForm').style.display = 'none';
    clearAddPathForm();
}

function clearAddPathForm() {
    document.getElementById('pathSignalK').value = '';
    document.getElementById('pathSignalKCustom').value = '';
    document.getElementById('pathSignalKCustom').style.display = 'none';
    document.getElementById('pathEnabled').checked = false;
    document.getElementById('pathSource').value = '';
    document.getElementById('pathContext').value = 'vessels.self';
    document.getElementById('pathExcludeMMSI').value = '';
    document.getElementById('customRegimen').value = '';

    // Clear regimen checkboxes
    const checkboxes = document.querySelectorAll('#regimenCheckboxes input[type="checkbox"]');
    checkboxes.forEach(checkbox => checkbox.checked = false);
}

// Populate SignalK paths dropdown
async function populateSignalKPaths() {
    const dropdown = document.getElementById('pathSignalK');
    const filterType = document.querySelector('input[name="pathFilter"]:checked')?.value || 'self';

    try {
        const response = await fetch('/signalk/v1/api/');
        const data = await response.json();

        // Extract paths from SignalK API with filter
        const allPaths = extractPathsFromSignalK(data, filterType);

        // Get defined commands to exclude
        const commandsResponse = await fetch(`${getPluginPath()}/api/commands`);
        const commandsData = await commandsResponse.json();

        const definedCommands = new Set();
        if (commandsData.success && commandsData.commands) {
            commandsData.commands.forEach(cmd => {
                definedCommands.add(`commands.${cmd.command}`);
            });
        }

        // Filter out defined command paths
        const availablePaths = allPaths.filter(path => !definedCommands.has(path));

        // Clear existing options (except default ones)
        while (dropdown.children.length > 2) {
            dropdown.removeChild(dropdown.lastChild);
        }

        // Add available paths
        availablePaths.forEach(path => {
            const option = document.createElement('option');
            option.value = path;
            option.textContent = path;
            dropdown.appendChild(option);
        });

    } catch (error) {
        console.log('Could not load real-time SignalK paths:', error);
    }
}

// Update path filter
export function updatePathFilter() {
    populateSignalKPaths();
}

// Extract distinct paths from SignalK data, separating self vs non-self
function extractPathsFromSignalK(obj, filterType = 'self') {
    const selfPaths = new Set();
    const nonSelfPaths = new Set();

    function extractRecursive(obj, prefix = '') {
        if (!obj || typeof obj !== 'object') return;

        for (const key in obj) {
            if (key === 'meta' || key === 'timestamp' || key === 'source') continue;

            const currentPath = prefix ? `${prefix}.${key}` : key;

            if (obj[key] && typeof obj[key] === 'object') {
                if (obj[key].value !== undefined) {
                    // This is a data path with a value
                    selfPaths.add(currentPath);
                } else {
                    extractRecursive(obj[key], currentPath);
                }
            }
        }
    }

    // Get the self vessel ID to distinguish self from other vessels
    const selfVesselId = obj.self;
    // Remove 'vessels.' prefix if present to get the actual vessel ID
    const actualSelfId = selfVesselId && selfVesselId.startsWith('vessels.') ?
        selfVesselId.replace('vessels.', '') : selfVesselId;

    // Process vessels if they exist
    if (obj.vessels) {
        // Process self vessel
        if (actualSelfId && obj.vessels[actualSelfId]) {
            extractRecursive(obj.vessels[actualSelfId], '');
        }

        // Process other vessels and extract generic paths
        for (const vesselId in obj.vessels) {
            if (vesselId !== actualSelfId) {
                const tempPaths = new Set();
                function extractOtherVessel(obj, prefix = '') {
                    if (!obj || typeof obj !== 'object') return;
                    for (const key in obj) {
                        if (key === 'meta' || key === 'timestamp' || key === 'source') continue;
                        const currentPath = prefix ? `${prefix}.${key}` : key;
                        if (obj[key] && typeof obj[key] === 'object') {
                            if (obj[key].value !== undefined) {
                                tempPaths.add(currentPath);
                            } else {
                                extractOtherVessel(obj[key], currentPath);
                            }
                        }
                    }
                }
                extractOtherVessel(obj.vessels[vesselId], '');
                tempPaths.forEach(path => nonSelfPaths.add(path));
            }
        }
    }

    // Process top-level paths (non-vessel specific data like environment)
    for (const key in obj) {
        if (key !== 'vessels' && key !== 'self' && key !== 'version' && key !== 'sources' && key !== 'meta' && key !== 'timestamp') {
            extractRecursive(obj[key], key);
        }
    }


    // Return the appropriate set based on filter
    const targetPaths = filterType === 'self' ? selfPaths : nonSelfPaths;
    return Array.from(targetPaths).sort();
}

// Populate available regimens
async function populateAvailableRegimens() {
    const container = document.getElementById('regimenCheckboxes');

    try {
        // Get defined commands to use as regimens
        const commandsResponse = await fetch(`${getPluginPath()}/api/commands`);
        const commandsData = await commandsResponse.json();

        const availableRegimens = [];
        if (commandsData.success && commandsData.commands) {
            commandsData.commands.forEach(cmd => {
                availableRegimens.push(cmd.command);
            });
        }

        container.innerHTML = '';

        availableRegimens.forEach(regimen => {
            const label = document.createElement('label');
            label.style.display = 'flex';
            label.style.alignItems = 'center';
            label.style.marginBottom = '5px';
            label.style.cursor = 'pointer';
            label.style.fontSize = '0.9em';

            const span = document.createElement('span');
            span.textContent = regimen;
            span.style.flex = '1';
            span.style.marginRight = '8px';

            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.id = `regimen_${regimen}`;
            checkbox.value = regimen;
            checkbox.style.width = '16px';
            checkbox.style.height = '16px';

            label.appendChild(span);
            label.appendChild(checkbox);
            container.appendChild(label);
        });
    } catch (error) {
        console.log('Could not load regimens:', error);
    }
}

let listenersInitialized = false;

export function initPathConfigListeners() {
    if (listenersInitialized) {
        return;
    }

    document.addEventListener('change', function(e) {
        if (e.target.id === 'pathSignalK') {
            const customInput = document.getElementById('pathSignalKCustom');
            if (e.target.value === 'custom') {
                customInput.style.display = 'block';
                customInput.focus();
            } else {
                customInput.style.display = 'none';
            }
        }
    });

    listenersInitialized = true;
}

// Add custom regimen
export function addCustomRegimen() {
    const customInput = document.getElementById('customRegimen');
    const regimenName = customInput.value.trim();

    if (!regimenName) {
        alert('Please enter a regimen name');
        return;
    }

    // Check if already exists
    if (document.getElementById(`regimen_${regimenName}`)) {
        alert('This regimen already exists');
        return;
    }

    const container = document.getElementById('regimenCheckboxes');
    const div = document.createElement('div');
    div.style.marginBottom = '5px';

    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.id = `regimen_${regimenName}`;
    checkbox.value = regimenName;
    checkbox.checked = true; // Auto-select custom regimens
    checkbox.style.marginRight = '8px';

    const label = document.createElement('label');
    label.htmlFor = `regimen_${regimenName}`;
    label.textContent = `${regimenName} (custom)`;
    label.style.fontSize = '0.9em';
    label.style.fontStyle = 'italic';

    div.appendChild(checkbox);
    div.appendChild(label);
    container.appendChild(div);

    customInput.value = '';
}

export async function addPathConfiguration() {
    const excludeMMSIInput = document.getElementById('pathExcludeMMSI').value.trim();
    const excludeMMSI = excludeMMSIInput ? excludeMMSIInput.split(',').map(mmsi => mmsi.trim()).filter(mmsi => mmsi) : [];

    // Get path value (either from dropdown or custom input)
    const pathDropdown = document.getElementById('pathSignalK');
    const pathCustom = document.getElementById('pathSignalKCustom');
    const selectedPath = pathDropdown.value === 'custom' ? pathCustom.value.trim() : pathDropdown.value.trim();

    // Get selected regimens
    const selectedRegimens = [];
    const checkboxes = document.querySelectorAll('#regimenCheckboxes input[type="checkbox"]:checked');
    checkboxes.forEach(checkbox => {
        selectedRegimens.push(checkbox.value);
    });

    const pathConfig = {
        path: selectedPath,
        enabled: document.getElementById('pathEnabled').checked,
        regimen: selectedRegimens.join(','), // Join multiple regimens with comma
        source: document.getElementById('pathSource').value.trim() || undefined,
        context: document.getElementById('pathContext').value.trim() || 'vessels.self',
        excludeMMSI: excludeMMSI.length > 0 ? excludeMMSI : undefined
    };

    if (!pathConfig.path) {
        alert('SignalK path is required');
        return;
    }

    try {
        const response = await fetch(`${getPluginPath()}/api/config/paths`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(pathConfig)
        });

        const result = await response.json();

        if (result.success) {
            hideAddPathForm();
            await loadPathConfigurations();
            alert('Path configuration added successfully');
        } else {
            alert(`Error adding path configuration: ${result.error}`);
        }
    } catch (error) {
        alert(`Network error: ${error.message}`);
    }
}

export async function removePathConfiguration(index) {
    if (!confirm('Are you sure you want to remove this path configuration?')) {
        return;
    }

    try {
        const response = await fetch(`${getPluginPath()}/api/config/paths/${index}`, {
            method: 'DELETE'
        });

        const result = await response.json();

        if (result.success) {
            await loadPathConfigurations();
            alert('Path configuration removed successfully');
        } else {
            alert(`Error removing path configuration: ${result.error}`);
        }
    } catch (error) {
        alert(`Network error: ${error.message}`);
    }
}

let editingIndex = -1;

export async function editPathConfiguration(index) {
    // Cancel any existing edit
    if (editingIndex !== -1) {
        cancelEdit();
    }
    
    editingIndex = index;
    
    // Get the current path configuration
    const response = await fetch(`${getPluginPath()}/api/config/paths`);
    const result = await response.json();
    
    if (!result.success || !result.paths[index]) {
        alert('Error loading path configuration');
        return;
    }
    
    const path = result.paths[index];
    
    // Replace the row with edit form
    const row = document.querySelector(`tr[data-index="${index}"]`);
    if (row) {
        const excludeMMSIValue = path.excludeMMSI && path.excludeMMSI.length > 0 ? path.excludeMMSI.join(', ') : '';
        const currentRegimens = path.regimen ? path.regimen.split(',').map(r => r.trim()) : [];

        row.innerHTML = `
            <td>
                <div style="margin-bottom: 4px; font-size: 0.8em; display: flex; align-items: center; gap: 10px;">
                    <label style="display: flex; align-items: center; font-weight: normal; cursor: pointer; white-space: nowrap;">
                        <span style="margin-right: 3px;">🏠 Self</span>
                        <input type="radio" id="editPathFilterSelf${index}" name="editPathFilter${index}" value="self" checked onchange="updateEditPathFilter(${index})">
                    </label>
                    <label style="display: flex; align-items: center; font-weight: normal; cursor: pointer; white-space: nowrap;">
                        <span style="margin-right: 3px;">🚢 Others</span>
                        <input type="radio" id="editPathFilterOthers${index}" name="editPathFilter${index}" value="others" onchange="updateEditPathFilter(${index})">
                    </label>
                </div>
                <select id="editPath${index}" style="width: 100%; padding: 4px;">
                    <option value="">-- Select SignalK Path --</option>
                    <option value="custom">🖊️ Enter Custom Path</option>
                </select>
                <input type="text" id="editPathCustom${index}" placeholder="Enter custom SignalK path" style="width: 100%; padding: 4px; margin-top: 2px; display: none;">
            </td>
            <td><input type="checkbox" id="editEnabled${index}" ${path.enabled ? 'checked' : ''}></td>
            <td>
                <div style="max-height: 120px; overflow-y: auto; border: 1px solid #ddd; padding: 5px; background: white;">
                    <div id="editRegimenCheckboxes${index}">
                        <!-- Regimen checkboxes will be populated here -->
                    </div>
                </div>
            </td>
            <td><input type="text" id="editSource${index}" value="${path.source || ''}" style="width: 100%;" placeholder="e.g., mqtt-weatherflow-udp"></td>
            <td><input type="text" id="editContext${index}" value="${path.context || 'vessels.self'}" style="width: 100%;"></td>
            <td><input type="text" id="editExcludeMMSI${index}" value="${excludeMMSIValue}" style="width: 100%;" placeholder="123456789, 987654321"></td>
            <td>
                <button onclick="saveEdit(${index})" style="padding: 5px 10px; font-size: 12px; background: #28a745;">💾 Save</button>
                <button onclick="cancelEdit()" style="padding: 5px 10px; font-size: 12px; background: #6c757d;">❌ Cancel</button>
            </td>
        `;

        // Populate the edit form with enhanced dropdowns
        populateEditSignalKPaths(index, path.path);
        await populateEditRegimens(index, currentRegimens);
    }
}

// Populate SignalK paths dropdown for edit form
async function populateEditSignalKPaths(index, currentPath) {
    const dropdown = document.getElementById(`editPath${index}`);
    const customInput = document.getElementById(`editPathCustom${index}`);
    const filterType = document.querySelector(`input[name="editPathFilter${index}"]:checked`)?.value || 'self';

    try {
        const response = await fetch('/signalk/v1/api/');
        const data = await response.json();

        const allPaths = extractPathsFromSignalK(data, filterType);

        // Get defined commands to exclude
        const commandsResponse = await fetch(`${getPluginPath()}/api/commands`);
        const commandsData = await commandsResponse.json();

        const definedCommands = new Set();
        if (commandsData.success && commandsData.commands) {
            commandsData.commands.forEach(cmd => {
                definedCommands.add(`commands.${cmd.command}`);
            });
        }

        // Filter out defined command paths
        const availablePaths = allPaths.filter(path => !definedCommands.has(path));

        // Clear existing options (except default ones)
        while (dropdown.children.length > 2) {
            dropdown.removeChild(dropdown.lastChild);
        }

        // Add available paths
        availablePaths.forEach(path => {
            const option = document.createElement('option');
            option.value = path;
            option.textContent = path;
            dropdown.appendChild(option);
        });

        // Set current value and determine appropriate filter
        if (currentPath) {
            // Determine if current path is self or other vessel path
            const isSelfPath = !currentPath.includes('vessels.') || currentPath.startsWith('vessels.self.');
            const targetFilter = isSelfPath ? 'self' : 'others';

            // Set appropriate radio button
            const selfRadio = document.getElementById(`editPathFilterSelf${index}`);
            const othersRadio = document.getElementById(`editPathFilterOthers${index}`);
            if (targetFilter === 'self') {
                selfRadio.checked = true;
            } else {
                othersRadio.checked = true;
            }

            // Clean path for comparison (remove vessels.self. prefix if present)
            const cleanCurrentPath = currentPath.replace('vessels.self.', '');

            // Refresh paths with correct filter if needed
            if (targetFilter !== filterType) {
                updateEditPathFilter(index);
                return;
            }

            if (paths.includes(cleanCurrentPath)) {
                dropdown.value = cleanCurrentPath;
            } else if (paths.includes(currentPath)) {
                dropdown.value = currentPath;
            } else {
                dropdown.value = 'custom';
                customInput.style.display = 'block';
                customInput.value = currentPath;
            }
        }

        // Add change listener
        dropdown.addEventListener('change', function() {
            if (this.value === 'custom') {
                customInput.style.display = 'block';
                customInput.focus();
            } else {
                customInput.style.display = 'none';
            }
        });

    } catch (error) {
        console.log('Could not load real-time SignalK paths for edit:', error);
        // Fallback to custom input
        dropdown.value = 'custom';
        customInput.style.display = 'block';
        customInput.value = currentPath || '';
    }
}

// Update path filter for edit form
export function updateEditPathFilter(index) {
    const dropdown = document.getElementById(`editPath${index}`);
    const currentValue = dropdown.value;

    // Get current path from either dropdown or custom input
    const customInput = document.getElementById(`editPathCustom${index}`);
    const currentPath = currentValue === 'custom' ? customInput.value : currentValue;

    populateEditSignalKPaths(index, currentPath);
}

// Populate regimens for edit form
async function populateEditRegimens(index, currentRegimens) {
    const container = document.getElementById(`editRegimenCheckboxes${index}`);

    try {
        // Get defined commands to use as regimens
        const commandsResponse = await fetch(`${getPluginPath()}/api/commands`);
        const commandsData = await commandsResponse.json();

        const availableRegimens = [];
        if (commandsData.success && commandsData.commands) {
            commandsData.commands.forEach(cmd => {
                availableRegimens.push(cmd.command);
            });
        }

    container.innerHTML = '';

        // Add available regimens
        availableRegimens.forEach(regimen => {
            const label = document.createElement('label');
            label.style.display = 'flex';
            label.style.alignItems = 'center';
            label.style.marginBottom = '3px';
            label.style.cursor = 'pointer';
            label.style.fontSize = '0.8em';

            const span = document.createElement('span');
            span.textContent = regimen;
            span.style.flex = '1';
            span.style.marginRight = '6px';

            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.id = `editRegimen_${index}_${regimen}`;
            checkbox.value = regimen;
            checkbox.checked = currentRegimens.includes(regimen);
            checkbox.style.width = '14px';
            checkbox.style.height = '14px';

            label.appendChild(span);
            label.appendChild(checkbox);
            container.appendChild(label);
        });

        // Add custom regimens that aren't in the available list
        currentRegimens.forEach(regimen => {
            if (!availableRegimens.includes(regimen)) {
                const label = document.createElement('label');
                label.style.display = 'flex';
                label.style.alignItems = 'center';
                label.style.marginBottom = '3px';
                label.style.cursor = 'pointer';
                label.style.fontSize = '0.8em';
                label.style.fontStyle = 'italic';

                const span = document.createElement('span');
                span.textContent = `${regimen} (custom)`;
                span.style.flex = '1';
                span.style.marginRight = '6px';

                const checkbox = document.createElement('input');
                checkbox.type = 'checkbox';
                checkbox.id = `editRegimen_${index}_${regimen}`;
                checkbox.value = regimen;
                checkbox.checked = true;
                checkbox.style.width = '14px';
                checkbox.style.height = '14px';

                label.appendChild(span);
                label.appendChild(checkbox);
                container.appendChild(label);
            }
        });
    } catch (error) {
        console.log('Could not load regimens for edit:', error);
    }
}

export async function saveEdit(index) {
    const excludeMMSIInput = document.getElementById(`editExcludeMMSI${index}`).value.trim();
    const excludeMMSI = excludeMMSIInput ? excludeMMSIInput.split(',').map(mmsi => mmsi.trim()).filter(mmsi => mmsi) : [];

    // Get path value (either from dropdown or custom input)
    const pathDropdown = document.getElementById(`editPath${index}`);
    const pathCustom = document.getElementById(`editPathCustom${index}`);
    const selectedPath = pathDropdown.value === 'custom' ? pathCustom.value.trim() : pathDropdown.value.trim();

    // Get selected regimens
    const selectedRegimens = [];
    const checkboxes = document.querySelectorAll(`#editRegimenCheckboxes${index} input[type="checkbox"]:checked`);
    checkboxes.forEach(checkbox => {
        selectedRegimens.push(checkbox.value);
    });

    const updatedPath = {
        path: selectedPath,
        enabled: document.getElementById(`editEnabled${index}`).checked,
        regimen: selectedRegimens.join(','), // Join multiple regimens with comma
        source: document.getElementById(`editSource${index}`).value.trim() || undefined,
        context: document.getElementById(`editContext${index}`).value.trim() || 'vessels.self',
        excludeMMSI: excludeMMSI.length > 0 ? excludeMMSI : undefined
    };

    if (!updatedPath.path) {
        alert('SignalK path is required');
        return;
    }

    try {
        const response = await fetch(`${getPluginPath()}/api/config/paths/${index}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(updatedPath)
        });

        const result = await response.json();

        if (result.success) {
            editingIndex = -1;
            await loadPathConfigurations();
            alert('Path configuration updated successfully');
        } else {
            alert(`Error updating path configuration: ${result.error}`);
        }
    } catch (error) {
        alert(`Network error: ${error.message}`);
    }
}

export function cancelEdit() {
    editingIndex = -1;
    loadPathConfigurations();
}

// Command Management Functions
