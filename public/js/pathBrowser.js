import { getPluginPath } from './utils.js';
import { setDataPathsQuery } from './dataPaths.js';
import { analyzeDataPath } from './analysis.js';

let availablePaths = [];
let dataDirectory = '';

export async function loadAvailablePaths() {
    try {
        const response = await fetch(`${getPluginPath()}/api/paths`);
        const result = await response.json();

        if (result.success) {
            availablePaths = result.paths;
            dataDirectory = result.dataDirectory;
            displayAvailablePaths();
        } else {
            document.getElementById('availablePaths').innerHTML =
                `<div class="error">Error loading paths: ${result.error}</div>`;
        }
    } catch (error) {
        document.getElementById('availablePaths').innerHTML =
            `<div class="error">Network error: ${error.message}</div>`;
    }
}

export function displayAvailablePaths() {
    const container = document.getElementById('availablePaths');

    if (availablePaths.length === 0) {
        container.innerHTML = '<p>No Parquet data files found. Start collecting data first.</p>';
        return;
    }

    let html = `
        <div class="path-dropdown-container">
            <div style="margin-bottom: 15px;">
                <label for="pathDropdown" style="display: block; margin-bottom: 5px; font-weight: 500;">Select Data Path:</label>
                <select id="pathDropdown" style="width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 4px; font-size: 14px;" onchange="updateSelectedPath()">
                    <option value="">-- Select a data path --</option>`;

    availablePaths.forEach((pathInfo, index) => {
        html += `<option value="${index}">${pathInfo.path} (${pathInfo.fileCount} files)</option>`;
    });

    html += `
                </select>
            </div>

            <div id="pathActions" style="display: none; margin-top: 15px; padding: 15px; background: #f8f9fa; border-radius: 6px; border: 1px solid #e9ecef;">
                <div style="margin-bottom: 10px;">
                    <strong id="selectedPathName"></strong>
                    <span id="selectedPathFiles" style="color: #666; margin-left: 10px;"></span>
                </div>
                <button id="generateQueryBtn" onclick="generateQueryForSelectedPath()" style="background: #28a745; color: white; border: none; padding: 10px 15px; border-radius: 4px; cursor: pointer; margin-right: 10px;">📋 Generate Query</button>
                <button id="analyzeBtn" onclick="analyzeSelectedPath()" style="background: #667eea; color: white; border: none; padding: 10px 15px; border-radius: 4px; cursor: pointer;">🧠 Analyze</button>
            </div>
        </div>`;

    container.innerHTML = html;
}

export function updateSelectedPath() {
    const dropdown = document.getElementById('pathDropdown');
    const actionsDiv = document.getElementById('pathActions');
    const pathNameSpan = document.getElementById('selectedPathName');
    const pathFilesSpan = document.getElementById('selectedPathFiles');

    if (dropdown.value === '') {
        actionsDiv.style.display = 'none';
        return;
    }

    const selectedIndex = parseInt(dropdown.value, 10);
    const pathInfo = availablePaths[selectedIndex];

    pathNameSpan.textContent = pathInfo.path;
    pathFilesSpan.textContent = `(${pathInfo.fileCount} files)`;
    actionsDiv.style.display = 'block';
}

export function generateQueryForSelectedPath() {
    const dropdown = document.getElementById('pathDropdown');
    if (dropdown.value === '') return;

    const selectedIndex = parseInt(dropdown.value, 10);
    const pathInfo = availablePaths[selectedIndex];
    generateQueryForPath(pathInfo.path, pathInfo.directory);
}

export function analyzeSelectedPath() {
    const dropdown = document.getElementById('pathDropdown');
    if (dropdown.value === '') return;

    const selectedIndex = parseInt(dropdown.value, 10);
    const pathInfo = availablePaths[selectedIndex];
    analyzeDataPath(pathInfo.path, pathInfo.directory);
}

export function generateQueryForPath(signalkPath, directory) {
    const query = `SELECT * FROM read_parquet('${directory}/*.parquet', union_by_name=true) ORDER BY received_timestamp DESC LIMIT 10`;
    setDataPathsQuery(query);
}

export function generateExampleQueries() {
    const container = document.getElementById('queryExamples');

    if (availablePaths.length === 0) {
        container.innerHTML = '<li><em>No data paths available yet. Start collecting data first.</em></li>';
        return;
    }

    let html = '';

    availablePaths.slice(0, 4).forEach(pathInfo => {
        const examples = [
            `SELECT * FROM read_parquet('${pathInfo.directory}/*.parquet', union_by_name=true) ORDER BY received_timestamp DESC LIMIT 10`,
            `SELECT COUNT(*) as total_records FROM read_parquet('${pathInfo.directory}/*.parquet', union_by_name=true)`,
            `SELECT received_timestamp, value, source_label FROM read_parquet('${pathInfo.directory}/*.parquet', union_by_name=true) WHERE value IS NOT NULL ORDER BY received_timestamp DESC LIMIT 10`
        ];

        examples.forEach(query => {
            html += `<li onclick="setQuery(this.textContent)"><code>${query}</code></li>`;
        });
    });

    container.innerHTML = html;
}

export function getAvailablePaths() {
    return availablePaths;
}

export function getDataDirectory() {
    return dataDirectory;
}
