/**
 * Home Port Management Module
 * Handles loading, saving, and setting home port location
 */

import { getPluginPath } from './utils.js';

/**
 * Load home port configuration on page load
 */
export async function loadHomePort() {
    try {
        const response = await fetch(`${getPluginPath()}/api/config/homeport`);
        const data = await response.json();

        if (data.success && data.latitude !== null && data.longitude !== null) {
            document.getElementById('homePortLat').value = data.latitude;
            document.getElementById('homePortLon').value = data.longitude;

            showHomePortStatus(`✅ Loaded: ${data.latitude.toFixed(6)}, ${data.longitude.toFixed(6)}`, 'success');
        } else {
            showHomePortStatus('ℹ️ No home port configured', 'info');
        }
    } catch (error) {
        console.error('Failed to load home port:', error);
        showHomePortStatus('❌ Failed to load home port', 'error');
    }
}

/**
 * Save home port configuration
 */
export async function saveHomePort() {
    const latInput = document.getElementById('homePortLat');
    const lonInput = document.getElementById('homePortLon');

    const latitude = parseFloat(latInput.value);
    const longitude = parseFloat(lonInput.value);

    // Validate input
    if (isNaN(latitude) || isNaN(longitude)) {
        showHomePortStatus('❌ Please enter valid latitude and longitude', 'error');
        return;
    }

    if (latitude < -90 || latitude > 90) {
        showHomePortStatus('❌ Latitude must be between -90 and 90', 'error');
        return;
    }

    if (longitude < -180 || longitude > 180) {
        showHomePortStatus('❌ Longitude must be between -180 and 180', 'error');
        return;
    }

    try {
        showHomePortStatus('💾 Saving...', 'info');

        const response = await fetch(`${getPluginPath()}/api/config/homeport`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                latitude,
                longitude,
            }),
        });

        const data = await response.json();

        if (data.success) {
            showHomePortStatus(`✅ Home port saved: ${latitude.toFixed(6)}, ${longitude.toFixed(6)}`, 'success');
        } else {
            showHomePortStatus(`❌ Failed to save: ${data.error}`, 'error');
        }
    } catch (error) {
        console.error('Failed to save home port:', error);
        showHomePortStatus('❌ Failed to save home port', 'error');
    }
}

/**
 * Set current vessel position as home port
 */
export async function setCurrentLocationAsHomePort() {
    try {
        showHomePortStatus('📍 Getting current position...', 'info');

        const response = await fetch(`${getPluginPath()}/api/position/current`);
        const data = await response.json();

        if (data.success) {
            document.getElementById('homePortLat').value = data.latitude;
            document.getElementById('homePortLon').value = data.longitude;

            showHomePortStatus(`✅ Current position loaded: ${data.latitude.toFixed(6)}, ${data.longitude.toFixed(6)}`, 'success');
        } else {
            showHomePortStatus('❌ Current position not available', 'error');
        }
    } catch (error) {
        console.error('Failed to get current position:', error);
        showHomePortStatus('❌ Failed to get current position', 'error');
    }
}

/**
 * Show status message for home port operations
 * @param {string} message - Status message to display
 * @param {string} type - Message type: 'success', 'error', or 'info'
 */
function showHomePortStatus(message, type) {
    const statusDiv = document.getElementById('homePortStatus');

    let color;
    switch (type) {
        case 'success':
            color = '#2e7d32';
            break;
        case 'error':
            color = '#c62828';
            break;
        case 'info':
        default:
            color = '#1976d2';
            break;
    }

    statusDiv.style.color = color;
    statusDiv.textContent = message;

    // Clear message after 5 seconds for success/info, 10 seconds for errors
    const timeout = type === 'error' ? 10000 : 5000;
    setTimeout(() => {
        if (statusDiv.textContent === message) {
            statusDiv.textContent = '';
        }
    }, timeout);
}

// Make functions available globally for onclick handlers
window.saveHomePort = saveHomePort;
window.setCurrentLocationAsHomePort = setCurrentLocationAsHomePort;