import { getPluginPath } from './utils.js';

export async function executeQuery() {
  const query = document.getElementById('queryInput').value.trim();
  if (!query) {
    alert('Please enter a query');
    return;
  }

  const resultsContainer = document.getElementById(
    'customQueryResultsContainer'
  );
  resultsContainer.innerHTML = '<div class="loading">Executing query...</div>';

  try {
    const response = await fetch(`${getPluginPath()}/api/query`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query }),
    });

    const result = await response.json();

    if (result.success) {
      displayResults(result, 'customQueryResultsContainer');
    } else {
      resultsContainer.innerHTML = `<div class="error">Query error: ${result.error}</div>`;
    }
  } catch (error) {
    resultsContainer.innerHTML = `<div class="error">Network error: ${error.message}</div>`;
  }
}

export async function executeDataPathsQuery() {
  const query = document.getElementById('dataPathsQueryInput').value.trim();
  if (!query) {
    alert('Please enter a query');
    return;
  }

  const resultsContainer = document.getElementById('dataPathsResultsContainer');
  resultsContainer.innerHTML = '<div class="loading">Executing query...</div>';

  try {
    const response = await fetch(`${getPluginPath()}/api/query`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query }),
    });

    const result = await response.json();

    if (result.success) {
      displayResults(result, 'dataPathsResultsContainer');
    } else {
      resultsContainer.innerHTML = `<div class="error">Query error: ${result.error}</div>`;
    }
  } catch (error) {
    resultsContainer.innerHTML = `<div class="error">Network error: ${error.message}</div>`;
  }
}

function displayResults(result, containerId = 'resultsContainer') {
  const container = document.getElementById(containerId);

  if (!result.data || result.data.length === 0) {
    container.innerHTML = '<p>No data returned from query.</p>';
    return;
  }

  let statsHtml = `
        <div class="stats">
            <div class="stat-item">
                <div class="stat-value">${result.rowCount}</div>
                <div class="stat-label">Rows</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">${Object.keys(result.data[0]).length}</div>
                <div class="stat-label">Columns</div>
            </div>
        </div>`;

  const columns = Object.keys(result.data[0]);
  let tableHtml = '<div class="table-container"><table><thead><tr>';

  columns.forEach(col => {
    tableHtml += `<th>${col}</th>`;
  });
  tableHtml += '</tr></thead><tbody>';

  result.data.forEach(row => {
    tableHtml += '<tr>';
    columns.forEach(col => {
      let value = row[col];
      if (value === null || value === undefined) {
        value = '';
      } else if (typeof value === 'object') {
        value = JSON.stringify(value);
      }
      tableHtml += `<td>${value}</td>`;
    });
    tableHtml += '</tr>';
  });

  tableHtml += '</tbody></table></div>';

  container.innerHTML = statsHtml + tableHtml;
}

export function setQuery(query) {
  document.getElementById('queryInput').value = query;
}

export function setDataPathsQuery(query) {
  document.getElementById('dataPathsQueryInput').value = query;
}

export function clearQuery() {
  document.getElementById('queryInput').value = '';
  document.getElementById('customQueryResultsContainer').innerHTML =
    '<p>Run a query to see results here...</p>';
}

export function clearDataPathsQuery() {
  document.getElementById('dataPathsQueryInput').value = '';
  document.getElementById('dataPathsResultsContainer').innerHTML =
    '<p>Run a query to see results here...</p>';
}
