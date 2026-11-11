/** Escape a value for interpolation into innerHTML. */
export function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

export function getPluginPath() {
  const currentPath = window.location.pathname;
  const pathParts = currentPath.split('/');
  const pluginIndex = pathParts.indexOf('plugins');

  if (pluginIndex !== -1 && pathParts[pluginIndex + 1]) {
    return `/plugins/${pathParts[pluginIndex + 1]}`;
  }

  return '/plugins/signalk-parquet';
}

/**
 * Parse a fetch response as JSON. Signal K answers a plugin route the user
 * may not use with a plain-text 401/403, which would otherwise surface as a
 * cryptic JSON parse error.
 */
export async function readJson(response) {
  const text = await response.text();
  try {
    return JSON.parse(text);
  } catch {
    if (response.status === 401 || response.status === 403) {
      throw new Error(
        'Administrator login required. Log in to the Signal K Admin UI and reload this page.'
      );
    }
    throw new Error(`HTTP ${response.status}: ${text.slice(0, 200)}`);
  }
}
