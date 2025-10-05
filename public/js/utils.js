export function getPluginPath() {
  const currentPath = window.location.pathname;
  const pathParts = currentPath.split('/');
  const pluginIndex = pathParts.indexOf('plugins');

  if (pluginIndex !== -1 && pathParts[pluginIndex + 1]) {
    return `/plugins/${pathParts[pluginIndex + 1]}`;
  }

  return '/plugins/signalk-parquet';
}
