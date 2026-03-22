/**
 * Map Explorer Module
 * Spatial querying, map visualization, playback, and export of SignalK historical data.
 * Replicates ZedDisplay Historical Data Explorer widget functionality.
 */

import { getPluginPath } from './utils.js';

// =============================================================================
// A. STATE MANAGEMENT
// =============================================================================

const STATES = {
  IDLE: 'idle',
  DRAWING_BBOX: 'drawing_bbox',
  DRAWING_RADIUS: 'drawing_radius',
  QUERY_CONFIG: 'queryConfig',
  LOADING: 'loading',
  RESULTS: 'results',
};

const PATH_COLORS = ['#2196F3', '#FF9800', '#4CAF50', '#9C27B0', '#00BCD4'];

let state = {
  mode: STATES.IDLE,
  // Map
  map: null,
  mapInitialized: false,
  openSeaMapLayer: null,
  openSeaMapVisible: false,
  // Drawing
  drawClickCount: 0,
  drawStartLatLng: null,
  drawPreview: null,
  areaLayer: null,
  areaType: null, // 'bbox' or 'radius'
  areaGeometry: null, // {west,south,east,north} or {lat,lon,radius}
  handles: [],
  isDraggingArea: false,
  isDraggingHandle: null,
  dragStartLatLng: null,
  // Query
  timeMode: 'lookback', // 'lookback' or 'range'
  lookbackDays: 7,
  fromDate: null,
  toDate: null,
  context: 'self',
  selectedPaths: new Set(),    // selected SignalK path strings
  pathFilter: '',              // search filter text
  aggregation: 'average',
  smoothing: 'none',
  smoothingParam: 5,
  availablePaths: [],
  pathMeta: {},              // { 'nav.speedOverGround': { formula, symbol, displayFormat } }
  availableContexts: [],
  vesselNames: new Map(),
  // Results
  results: null,
  trackLayer: null,
  markerLayers: [],
  dataPoints: [],
  selectedIndex: -1,
  // Playback
  playing: false,
  playReverse: false,
  playSpeed: 1,
  playInterval: null,
  // Legend — ZedDisplay 3-state toggle: visible set + active index
  visibleSeriesIndices: new Set([0]),
  activeSeries: 0,
  // Saved areas
  savedAreas: [],
};

// =============================================================================
// B. MAP INITIALIZATION
// =============================================================================

export function initMapExplorer() {
  if (state.mapInitialized) {
    if (state.map) {
      setTimeout(() => state.map.invalidateSize(), 100);
    }
    return;
  }

  const container = document.getElementById('me-map');
  if (!container) return;

  const lat = parseFloat(document.getElementById('homePortLat')?.value) || 26.78;
  const lon = parseFloat(document.getElementById('homePortLon')?.value) || -80.05;

  state.map = L.map('me-map', {
    center: [lat, lon],
    zoom: 12,
    zoomControl: true,
  });

  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap contributors',
    maxZoom: 19,
  }).addTo(state.map);

  state.openSeaMapLayer = L.tileLayer(
    'https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png',
    {
      attribution: '&copy; OpenSeaMap contributors',
      maxZoom: 19,
      opacity: 0.8,
    }
  );

  // Map click handler for drawing
  state.map.on('click', onMapClick);
  state.map.on('mousemove', onMapMouseMove);

  state.mapInitialized = true;
  state._pathsStale = true;
  loadSavedAreas();
  loadAvailablePaths();
  updateQueryPanelState();

  setTimeout(() => state.map.invalidateSize(), 200);
}

// =============================================================================
// C. BBOX DRAWING
// =============================================================================

export function startBboxDraw() {
  clearDrawing();
  state.mode = STATES.DRAWING_BBOX;
  state.drawClickCount = 0;
  state.drawStartLatLng = null;
  document.getElementById('me-map').style.cursor = 'crosshair';
  setToolbarStatus('Click first corner of bounding box...');
}

export function startRadiusDraw() {
  clearDrawing();
  state.mode = STATES.DRAWING_RADIUS;
  state.drawClickCount = 0;
  state.drawStartLatLng = null;
  document.getElementById('me-map').style.cursor = 'crosshair';
  setToolbarStatus('Click center point...');
}

function onMapClick(e) {
  if (state.mode === STATES.DRAWING_BBOX) {
    handleBboxClick(e.latlng);
  } else if (state.mode === STATES.DRAWING_RADIUS) {
    handleRadiusClick(e.latlng);
  }
}

function onMapMouseMove(e) {
  if (state.mode === STATES.DRAWING_BBOX && state.drawClickCount === 1) {
    updateBboxPreview(e.latlng);
  } else if (state.mode === STATES.DRAWING_RADIUS && state.drawClickCount === 1) {
    updateRadiusPreview(e.latlng);
  }

  if (state.isDraggingArea && state.areaLayer) {
    handleAreaDrag(e.latlng);
  }
  if (state.isDraggingHandle !== null) {
    handleHandleDrag(e.latlng);
  }
}

function handleBboxClick(latlng) {
  if (state.drawClickCount === 0) {
    state.drawStartLatLng = latlng;
    state.drawClickCount = 1;
    setToolbarStatus('Click second corner...');
  } else {
    finalizeBbox(state.drawStartLatLng, latlng);
    state.mode = STATES.QUERY_CONFIG;
    state.drawClickCount = 0;
    document.getElementById('me-map').style.cursor = '';
    setToolbarStatus('');
  }
}

function updateBboxPreview(latlng) {
  if (state.drawPreview) {
    state.map.removeLayer(state.drawPreview);
  }
  const bounds = L.latLngBounds(state.drawStartLatLng, latlng);
  state.drawPreview = L.rectangle(bounds, {
    color: '#9C27B0',
    weight: 2,
    fillOpacity: 0.15,
    dashArray: '5,5',
  }).addTo(state.map);
}

function finalizeBbox(corner1, corner2) {
  if (state.drawPreview) {
    state.map.removeLayer(state.drawPreview);
    state.drawPreview = null;
  }

  const bounds = L.latLngBounds(corner1, corner2);
  const sw = bounds.getSouthWest();
  const ne = bounds.getNorthEast();

  state.areaType = 'bbox';
  state.areaGeometry = {
    west: sw.lng,
    south: sw.lat,
    east: ne.lng,
    north: ne.lat,
  };

  drawAreaOnMap();
  updateQueryPanelState();
}

// =============================================================================
// D. RADIUS DRAWING
// =============================================================================

function handleRadiusClick(latlng) {
  if (state.drawClickCount === 0) {
    state.drawStartLatLng = latlng;
    state.drawClickCount = 1;
    setToolbarStatus('Click to set radius...');
  } else {
    const dist = state.drawStartLatLng.distanceTo(latlng);
    finalizeRadius(state.drawStartLatLng, dist);
    state.mode = STATES.QUERY_CONFIG;
    state.drawClickCount = 0;
    document.getElementById('me-map').style.cursor = '';
    setToolbarStatus('');
  }
}

function updateRadiusPreview(latlng) {
  if (state.drawPreview) {
    state.map.removeLayer(state.drawPreview);
  }
  const dist = state.drawStartLatLng.distanceTo(latlng);
  state.drawPreview = L.circle(state.drawStartLatLng, {
    radius: dist,
    color: '#9C27B0',
    weight: 2,
    fillOpacity: 0.15,
    dashArray: '5,5',
  }).addTo(state.map);
}

function finalizeRadius(center, radius) {
  if (state.drawPreview) {
    state.map.removeLayer(state.drawPreview);
    state.drawPreview = null;
  }

  state.areaType = 'radius';
  state.areaGeometry = {
    lat: center.lat,
    lon: center.lng,
    radius: Math.round(radius),
  };

  drawAreaOnMap();
  updateQueryPanelState();
}

// =============================================================================
// AREA RENDERING + HANDLES
// =============================================================================

function drawAreaOnMap() {
  clearAreaLayer();

  if (state.areaType === 'bbox') {
    const { west, south, east, north } = state.areaGeometry;
    const bounds = L.latLngBounds([south, west], [north, east]);
    state.areaLayer = L.rectangle(bounds, {
      color: '#9C27B0',
      weight: 2,
      fillOpacity: 0.3,
      fillColor: '#9C27B0',
    }).addTo(state.map);

    // Drag to move
    enableAreaDrag();
    addBboxHandles();
  } else if (state.areaType === 'radius') {
    const { lat, lon, radius } = state.areaGeometry;
    state.areaLayer = L.circle([lat, lon], {
      radius,
      color: '#9C27B0',
      weight: 2,
      fillOpacity: 0.3,
      fillColor: '#9C27B0',
    }).addTo(state.map);

    enableAreaDrag();
    addRadiusHandles();
  }
}

function clearAreaLayer() {
  if (state.areaLayer) {
    state.map.removeLayer(state.areaLayer);
    state.areaLayer = null;
  }
  state.handles.forEach((h) => state.map.removeLayer(h));
  state.handles = [];
}

function updateQueryPanelState() {
  const panel = document.querySelector('.me-query-panel');
  if (!panel) return;
  if (state.areaGeometry) {
    panel.style.opacity = '1';
    panel.style.pointerEvents = 'auto';
  } else {
    panel.style.opacity = '0.4';
    panel.style.pointerEvents = 'none';
  }
}

function clearDrawing() {
  clearAreaLayer();
  if (state.drawPreview) {
    state.map.removeLayer(state.drawPreview);
    state.drawPreview = null;
  }
  state.areaType = null;
  state.areaGeometry = null;
  state.drawClickCount = 0;
  state.drawStartLatLng = null;
  document.getElementById('me-map').style.cursor = '';
  setToolbarStatus('');
  updateQueryPanelState();
}

function makeHandle(latlng, index) {
  const h = L.circleMarker(latlng, {
    radius: 6,
    color: '#9C27B0',
    fillColor: 'white',
    fillOpacity: 1,
    weight: 2,
    className: 'me-handle',
  }).addTo(state.map);

  h.on('mousedown', (e) => {
    L.DomEvent.stopPropagation(e);
    state.isDraggingHandle = index;
    state.map.dragging.disable();
  });

  state.map.on('mouseup', () => {
    if (state.isDraggingHandle !== null) {
      state.isDraggingHandle = null;
      state.map.dragging.enable();
      if (state.results) executeMapQuery();
    }
  });

  state.handles.push(h);
  return h;
}

function addBboxHandles() {
  const { west, south, east, north } = state.areaGeometry;
  const midLat = (south + north) / 2;
  const midLon = (west + east) / 2;

  // corners: 0=SW, 1=NW, 2=NE, 3=SE
  makeHandle(L.latLng(south, west), 0);
  makeHandle(L.latLng(north, west), 1);
  makeHandle(L.latLng(north, east), 2);
  makeHandle(L.latLng(south, east), 3);
  // edges: 4=W, 5=N, 6=E, 7=S
  makeHandle(L.latLng(midLat, west), 4);
  makeHandle(L.latLng(north, midLon), 5);
  makeHandle(L.latLng(midLat, east), 6);
  makeHandle(L.latLng(south, midLon), 7);
}

function addRadiusHandles() {
  const { lat, lon } = state.areaGeometry;
  // Center handle
  makeHandle(L.latLng(lat, lon), 0);
  // Edge handle (east)
  const edgePt = state.areaLayer.getBounds().getEast();
  makeHandle(L.latLng(lat, edgePt), 1);
}

function enableAreaDrag() {
  if (!state.areaLayer) return;

  state.areaLayer.on('mousedown', (e) => {
    L.DomEvent.stopPropagation(e);
    state.isDraggingArea = true;
    state.dragStartLatLng = e.latlng;
    state.map.dragging.disable();
  });

  state.map.on('mouseup', () => {
    if (state.isDraggingArea) {
      state.isDraggingArea = false;
      state.map.dragging.enable();
      if (state.results) executeMapQuery();
    }
  });
}

function handleAreaDrag(latlng) {
  if (!state.dragStartLatLng) return;
  const dLat = latlng.lat - state.dragStartLatLng.lat;
  const dLng = latlng.lng - state.dragStartLatLng.lng;
  state.dragStartLatLng = latlng;

  if (state.areaType === 'bbox') {
    state.areaGeometry.west += dLng;
    state.areaGeometry.east += dLng;
    state.areaGeometry.south += dLat;
    state.areaGeometry.north += dLat;
  } else {
    state.areaGeometry.lat += dLat;
    state.areaGeometry.lon += dLng;
  }
  drawAreaOnMap();
}

function handleHandleDrag(latlng) {
  if (state.areaType === 'bbox') {
    const g = state.areaGeometry;
    switch (state.isDraggingHandle) {
      case 0:
        g.south = latlng.lat;
        g.west = latlng.lng;
        break;
      case 1:
        g.north = latlng.lat;
        g.west = latlng.lng;
        break;
      case 2:
        g.north = latlng.lat;
        g.east = latlng.lng;
        break;
      case 3:
        g.south = latlng.lat;
        g.east = latlng.lng;
        break;
      case 4:
        g.west = latlng.lng;
        break;
      case 5:
        g.north = latlng.lat;
        break;
      case 6:
        g.east = latlng.lng;
        break;
      case 7:
        g.south = latlng.lat;
        break;
    }
    drawAreaOnMap();
  } else if (state.areaType === 'radius') {
    if (state.isDraggingHandle === 0) {
      // Move center
      state.areaGeometry.lat = latlng.lat;
      state.areaGeometry.lon = latlng.lng;
    } else {
      // Resize radius
      const center = L.latLng(state.areaGeometry.lat, state.areaGeometry.lon);
      state.areaGeometry.radius = Math.round(center.distanceTo(latlng));
    }
    drawAreaOnMap();
  }
}

// =============================================================================
// E. QUERY CONFIGURATION
// =============================================================================

export function setTimeMode(mode) {
  state.timeMode = mode;
  document.getElementById('me-lookback-btns').style.display =
    mode === 'lookback' ? 'flex' : 'none';
  document.getElementById('me-date-range').style.display =
    mode === 'range' ? 'flex' : 'none';
  document
    .querySelectorAll('#me-time-toggle button')
    .forEach((b) => b.classList.remove('me-active'));
  document
    .querySelector(`#me-time-toggle button[data-mode="${mode}"]`)
    .classList.add('me-active');

  // Only reload if we have valid date info (lookback always valid; range needs both inputs)
  if (mode === 'lookback' || (document.getElementById('me-from').value && document.getElementById('me-to').value)) {
    state._pathsStale = true;
    loadAvailablePaths();
    if (document.getElementById('me-lookup-contexts').checked) {
      lookupContexts();
    }
  }
}

export function onDateRangeChange() {
  const fromEl = document.getElementById('me-from');
  const toEl = document.getElementById('me-to');
  if (!fromEl.value || !toEl.value) return; // wait until both dates are set
  state._pathsStale = true;
  loadAvailablePaths();
  if (document.getElementById('me-lookup-contexts').checked) {
    lookupContexts();
  }
}

export function setLookback(days) {
  state.lookbackDays = days;
  state._pathsStale = true;
  document.querySelectorAll('#me-lookback-btns button').forEach((b) => {
    b.classList.toggle('me-active', parseInt(b.dataset.days) === days);
  });
  loadAvailablePaths();
  if (document.getElementById('me-lookup-contexts').checked) {
    lookupContexts();
  }
}

function contextDisplayName(ctx, vesselNames) {
  if (ctx === 'self' || ctx === 'vessels.self') return 'Self';
  const mmsiMatch = ctx.match(/mmsi:(\d+)/);
  const mmsi = mmsiMatch ? mmsiMatch[1] : null;
  const name = vesselNames ? vesselNames.get(ctx) : null;
  if (name && mmsi) return `${name} (${mmsi})`;
  if (name) return name;
  if (mmsi) return `MMSI ${mmsi}`;
  const lastDot = ctx.lastIndexOf('.');
  return lastDot >= 0 ? ctx.substring(lastDot + 1) : ctx;
}

async function fetchVesselNames(contexts, from, to) {
  const names = new Map();
  // Batch in groups of 10 to avoid overwhelming the server
  const ctxList = (Array.isArray(contexts) ? contexts : [])
    .map((c) => c.context || c)
    .filter((ctx) => ctx !== 'self' && ctx !== 'vessels.self');

  for (let i = 0; i < ctxList.length; i += 10) {
    const batch = ctxList.slice(i, i + 10);
    await Promise.all(batch.map(async (ctx) => {
      try {
        const url = `/signalk/v1/history/values?context=${encodeURIComponent(ctx)}&paths=name&from=${from}&to=${to}&resolution=86400`;
        const resp = await fetch(url);
        if (!resp.ok) return;
        const data = await resp.json();
        const result = Array.isArray(data) ? data[0] : data;
        if (result && result.data && result.data.length > 0) {
          const lastRow = result.data[result.data.length - 1];
          if (lastRow && lastRow[1]) {
            names.set(ctx, lastRow[1]);
          }
        }
      } catch (err) {
        // no name data for this context
      }
    }));
  }
  console.log(`Fetched ${names.size} vessel names from history API`);
  return names;
}

function renderContextList(filter) {
  const listEl = document.getElementById('me-context-list');
  if (!listEl) return;
  listEl.innerHTML = '';
  const filterLower = (filter || '').toLowerCase();

  // Self option
  if (!filterLower || 'self'.includes(filterLower)) {
    const row = document.createElement('label');
    row.className = 'me-path-row';
    row.innerHTML = `<input type="radio" name="me-ctx" value="self" ${state.context === 'self' ? 'checked' : ''} onchange="meSelectContext('self')" /> <span>Self</span>`;
    listEl.appendChild(row);
  }

  (Array.isArray(state.availableContexts) ? state.availableContexts : []).forEach((c) => {
    const ctx = c.context || c;
    const label = contextDisplayName(ctx, state.vesselNames);
    if (filterLower && !label.toLowerCase().includes(filterLower) && !ctx.toLowerCase().includes(filterLower)) return;
    const row = document.createElement('label');
    row.className = 'me-path-row';
    row.innerHTML = `<input type="radio" name="me-ctx" value="${ctx}" ${state.context === ctx ? 'checked' : ''} onchange="meSelectContext('${ctx}')" /> ${label}`;
    listEl.appendChild(row);
  });
}

export function filterContexts(value) {
  renderContextList(value);
}

export function selectContext(ctx) {
  state.context = ctx;
  state._pathsStale = true;
  loadAvailablePaths();
}

export async function lookupContexts() {
  const cb = document.getElementById('me-lookup-contexts');
  if (!cb.checked) {
    state.context = 'self';
    document.getElementById('me-context-select').style.display = 'none';
    return;
  }

  const { from, to } = getQueryTimeRange();
  let url;
  if (state.areaType === 'bbox') {
    const g = state.areaGeometry;
    url = `/api/history/contexts/spatial?from=${from}&to=${to}&bbox=${g.west},${g.south},${g.east},${g.north}`;
  } else if (state.areaType === 'radius') {
    const g = state.areaGeometry;
    url = `/api/history/contexts/spatial?from=${from}&to=${to}&radius=${g.lon},${g.lat},${g.radius}`;
  } else {
    url = `/signalk/v1/history/contexts?from=${from}&to=${to}`;
  }

  try {
    const resp = await fetch(url);
    const data = await resp.json();
    state.availableContexts = data.contexts || data || [];

    const countEl = document.getElementById('me-context-count');
    if (countEl) {
      const n = Array.isArray(state.availableContexts) ? state.availableContexts.length : 0;
      countEl.textContent = `${n} vessel${n !== 1 ? 's' : ''} found`;
    }
    // Clear filter and render list immediately with MMSI-only labels
    const filterEl = document.getElementById('me-context-filter');
    if (filterEl) filterEl.value = '';
    renderContextList('');
    document.getElementById('me-context-select').style.display = 'block';

    // Fetch vessel names in background, then re-render with names
    fetchVesselNames(state.availableContexts, from, to).then((names) => {
      state.vesselNames = names;
      renderContextList(filterEl ? filterEl.value : '');
    }).catch((err) => {
      console.error('Failed to fetch vessel names:', err);
    });
  } catch (err) {
    console.error('Failed to load contexts:', err);
  }
}

export async function loadAvailablePaths() {
  // Avoid redundant fetches — only reload if we have no paths yet
  if (state.availablePaths.length > 0 && !state._pathsStale) return;

  const { from, to } = getQueryTimeRange();
  const ctx = state.context === 'self' ? '' : state.context;

  // History API routes are on the main server router, not the plugin router
  const url = `/api/history/paths?context=${encodeURIComponent(ctx)}&from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`;

  try {
    const resp = await fetch(url);
    const data = await resp.json();
    // Response is a plain string array like ["navigation.speedOverGround", ...]
    state.availablePaths = Array.isArray(data) ? data : [];
    state._pathsStale = false;
    // Remove any selected paths that no longer exist in available paths
    for (const p of state.selectedPaths) {
      if (!state.availablePaths.includes(p)) state.selectedPaths.delete(p);
    }
    renderPathSelector();
  } catch (err) {
    console.error('Failed to load paths:', err);
  }
}

export function togglePathSelection(path) {
  if (state.selectedPaths.has(path)) {
    state.selectedPaths.delete(path);
  } else {
    if (state.selectedPaths.size >= 3) return;
    state.selectedPaths.add(path);
  }
  renderPathSelector();
}

export function removeSelectedPath(path) {
  state.selectedPaths.delete(path);
  renderPathSelector();
}

export function setPathFilter(value) {
  state.pathFilter = value.toLowerCase();
  renderPathSelector();
}

function renderPathSelector() {
  const container = document.getElementById('me-path-selector');
  if (!container) return;

  const chipsEl = container.querySelector('.me-path-chips');
  const listEl = container.querySelector('.me-path-list');
  const filterEl = container.querySelector('.me-path-filter');

  // Render chips
  chipsEl.innerHTML = '';
  state.selectedPaths.forEach((path) => {
    const chip = document.createElement('span');
    chip.className = 'me-path-chip';
    chip.innerHTML = `${path} <button onclick="meRemoveSelectedPath('${path}')">\u00d7</button>`;
    chipsEl.appendChild(chip);
  });

  // Preserve filter input value (don't replace it)
  if (filterEl && filterEl.value.toLowerCase() !== state.pathFilter) {
    filterEl.value = state.pathFilter;
  }

  // Render checkbox list
  listEl.innerHTML = '';
  const filter = state.pathFilter;
  const atMax = state.selectedPaths.size >= 3;

  state.availablePaths.forEach((path) => {
    if (filter && !path.toLowerCase().includes(filter)) return;
    const isSelected = state.selectedPaths.has(path);
    const disabled = atMax && !isSelected;

    const row = document.createElement('label');
    row.className = 'me-path-row' + (disabled ? ' me-path-disabled' : '');
    row.innerHTML = `<input type="checkbox" ${isSelected ? 'checked' : ''} ${disabled ? 'disabled' : ''} onchange="meTogglePathSelection('${path}')" /> ${path}`;
    listEl.appendChild(row);
  });
}

function getSelectedPaths() {
  const paths = [];
  state.selectedPaths.forEach((pathStr) => {
    let p = pathStr;
    const agg = document.getElementById('me-aggregation').value;
    if (agg && agg !== 'average') p += `:${agg}`;
    const smooth = document.getElementById('me-smoothing').value;
    if (smooth === 'sma') {
      p += `:sma:${document.getElementById('me-smooth-param').value || 5}`;
    } else if (smooth === 'ema') {
      p += `:ema:${document.getElementById('me-smooth-param').value || 0.3}`;
    }
    paths.push(p);
  });
  return paths;
}

function getQueryTimeRange() {
  if (state.timeMode === 'range') {
    const fromEl = document.getElementById('me-from');
    const toEl = document.getElementById('me-to');
    const fromDate = fromEl && fromEl.value ? new Date(fromEl.value) : null;
    const toDate = toEl && toEl.value ? new Date(toEl.value + 'T23:59:59') : null;
    // Fall back to 7-day lookback if inputs are empty/invalid
    if (!fromDate || isNaN(fromDate.getTime()) || !toDate || isNaN(toDate.getTime())) {
      const now = new Date();
      return { from: new Date(now.getTime() - 7 * 86400000).toISOString(), to: now.toISOString() };
    }
    return { from: fromDate.toISOString(), to: toDate.toISOString() };
  }
  const now = new Date();
  const from = new Date(now.getTime() - state.lookbackDays * 86400000);
  return { from: from.toISOString(), to: now.toISOString() };
}

// =============================================================================
// F. API CALLS
// =============================================================================

export async function executeMapQuery() {
  if (!state.areaGeometry) {
    setToolbarStatus('Draw an area on the map first.');
    return;
  }

  const paths = getSelectedPaths();
  if (paths.length === 0) {
    setToolbarStatus('Select at least one path.');
    return;
  }

  state.mode = STATES.LOADING;
  document.getElementById('me-loading').style.display = 'block';
  document.getElementById('me-results-panel').style.display = 'none';
  const exportBtns = document.getElementById('me-export-btns');
  if (exportBtns) exportBtns.style.display = 'none';

  const { from, to } = getQueryTimeRange();
  const ctx = state.context === 'self' ? '' : state.context;

  // Always include navigation.position for track
  const allPaths = ['navigation.position', ...paths.filter((p) => !p.startsWith('navigation.position'))];

  let spatialParam = '';
  if (state.areaType === 'bbox') {
    const g = state.areaGeometry;
    spatialParam = `&bbox=${g.west},${g.south},${g.east},${g.north}`;
  } else if (state.areaType === 'radius') {
    const g = state.areaGeometry;
    spatialParam = `&radius=${g.lon},${g.lat},${g.radius}`;
  }

  const url =
    `/signalk/v1/history/values?context=${ctx}` +
    `&from=${from}&to=${to}` +
    `&paths=${allPaths.join(',')}` +
    spatialParam;

  try {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();

    state.results = Array.isArray(data) ? data : [data];
    state.mode = STATES.RESULTS;
    state.selectedIndex = -1;

    processResults();
    const pathNames = getUniquePathNames();
    await fetchPathMeta(pathNames);
    renderMapResults();
    renderResultsTable();
    renderLegend();
    renderSummary();

    // Auto-select first data point so detail/sparklines are visible immediately
    if (state.dataPoints.length > 0) {
      selectPoint(0);
    }

    document.getElementById('me-loading').style.display = 'none';
    document.getElementById('me-results-panel').style.display = 'block';
    const exportBtns2 = document.getElementById('me-export-btns');
    if (exportBtns2) exportBtns2.style.display = 'flex';
  } catch (err) {
    console.error('Map query failed:', err);
    document.getElementById('me-loading').style.display = 'none';
    setToolbarStatus(`Query failed: ${err.message}`);
    state.mode = STATES.QUERY_CONFIG;
  }
}

function processResults() {
  state.dataPoints = [];

  if (!state.results || state.results.length === 0) return;

  // Find position result and value results
  let posResult = null;
  const valueResults = [];

  state.results.forEach((r) => {
    if (!r || !r.values) return;
    r.values.forEach((v, vi) => {
      if (v.path === 'navigation.position') {
        posResult = { result: r, valueIndex: vi };
      } else {
        valueResults.push({ result: r, valueIndex: vi, path: v.path });
      }
    });
  });

  if (!posResult) return;

  const posData = posResult.result.data || [];

  // Build points array with positions + values at matching timestamps
  posData.forEach((row, rowIdx) => {
    const ts = row[0];
    const posVal = row[posResult.valueIndex + 1];
    if (!posVal) return;

    // Position can be {latitude, longitude} or [lon, lat]
    let lat, lon;
    if (typeof posVal === 'object' && posVal.latitude !== undefined) {
      lat = posVal.latitude;
      lon = posVal.longitude;
    } else if (Array.isArray(posVal)) {
      lon = posVal[0];
      lat = posVal[1];
    } else {
      return;
    }

    const point = { ts, lat, lon, values: {} };

    // Collect values from each result set at same row index
    valueResults.forEach((vr) => {
      const vrData = vr.result.data || [];
      if (rowIdx < vrData.length) {
        const val = vrData[rowIdx][vr.valueIndex + 1];
        if (val !== null && val !== undefined) {
          point.values[vr.path] = val;
        }
      }
    });

    state.dataPoints.push(point);
  });

  // Initialize legend: first series visible and active
  state.activeSeries = 0;
  state.visibleSeriesIndices = new Set([0]);
}

function getUniquePathNames() {
  const names = new Set();
  state.dataPoints.forEach((pt) => {
    Object.keys(pt.values).forEach((k) => names.add(k));
  });
  return Array.from(names);
}

async function fetchPathMeta(paths) {
  // Build context prefix for meta API: vessels/self or vessels/urn:mrn:...
  let ctxPrefix = 'vessels/self';
  if (state.context && state.context !== 'self') {
    // context is like "vessels.urn:mrn:imo:mmsi:123456789"
    ctxPrefix = state.context.replace(/\./g, '/');
  }

  for (const p of paths) {
    const basePath = p.split(':')[0]; // strip aggregation/smoothing suffixes
    if (state.pathMeta[basePath]) continue;
    try {
      const skPath = basePath.replace(/\./g, '/');
      const resp = await fetch(`/signalk/v1/api/${ctxPrefix}/${skPath}/meta`);
      if (resp.ok) {
        const meta = await resp.json();
        if (meta.displayUnits) {
          state.pathMeta[basePath] = meta.displayUnits;
        }
      }
    } catch (err) {
      // no meta available for this path
    }
  }
}

function convertValue(value, pathName) {
  if (value === null || value === undefined) return value;
  const basePath = pathName.split(':')[0];
  const meta = state.pathMeta[basePath];
  if (!meta || !meta.formula) return value;
  try {
    // formula is like "value * 1.94384"
    return Function('value', `return ${meta.formula}`)(value);
  } catch {
    return value;
  }
}

function getUnitForPath(pathName) {
  const basePath = pathName.split(':')[0];
  const meta = state.pathMeta[basePath];
  if (meta && meta.symbol) return meta.symbol;
  return '';
}

function getDisplayFormat(pathName) {
  const basePath = pathName.split(':')[0];
  const meta = state.pathMeta[basePath];
  return meta?.displayFormat || null;
}

// =============================================================================
// G. MAP RESULTS RENDERING
// =============================================================================

function renderMapResults() {
  clearMapResults();

  if (state.dataPoints.length === 0) return;

  const pathNames = getUniquePathNames();
  const activePathName = pathNames[state.activeSeries] || pathNames[0];
  const activeColor = PATH_COLORS[state.activeSeries % PATH_COLORS.length];

  // Track polyline
  const trackCoords = state.dataPoints.map((pt) => [pt.lat, pt.lon]);
  state.trackLayer = L.polyline(trackCoords, {
    color: activeColor,
    weight: 2,
    opacity: 0.7,
  }).addTo(state.map);

  // Only render markers if at least one series is visible
  if (state.visibleSeriesIndices.size === 0) return;

  // Markers per data point — sized/colored by active series
  state.dataPoints.forEach((pt, idx) => {
    const val = pt.values[activePathName];
    const isSelected = idx === state.selectedIndex;

    // Grey dot if no data for active path, colored otherwise
    let markerColor, markerRadius, markerOpacity;
    if (val === undefined || val === null) {
      markerColor = '#999';
      markerRadius = 4;
      markerOpacity = 0.3;
    } else {
      markerColor = activeColor;
      markerRadius = scaleMarkerSize(val, activePathName);
      markerOpacity = 0.6;
    }

    const marker = L.circleMarker([pt.lat, pt.lon], {
      radius: markerRadius,
      color: isSelected ? '#FFD700' : markerColor,
      fillColor: isSelected ? '#FFD700' : markerColor,
      fillOpacity: isSelected ? 1 : markerOpacity,
      weight: isSelected ? 3 : 1,
    }).addTo(state.map);

    marker.on('click', () => selectPoint(idx));
    marker.bindTooltip(
      () => {
        let tip = formatTimestamp(pt.ts);
        Object.entries(pt.values).forEach(([p, v]) => {
          tip += `<br>${shortenPath(p)}: ${formatValue(v, p)} ${getUnitForPath(p)}`;
        });
        return tip;
      },
      { direction: 'top' }
    );

    state.markerLayers.push(marker);
  });

  // Bring selected marker to front
  if (state.selectedIndex >= 0 && state.markerLayers[state.selectedIndex]) {
    state.markerLayers[state.selectedIndex].bringToFront();
  }

  // Fit bounds to track
  if (state.trackLayer) {
    state.map.fitBounds(state.trackLayer.getBounds().pad(0.1));
  }
}

function scaleMarkerSize(val, pathName) {
  if (val === undefined || val === null) return 4;
  // Get min/max for this path
  let min = Infinity,
    max = -Infinity;
  state.dataPoints.forEach((pt) => {
    const v = pt.values[pathName];
    if (v !== undefined && v !== null) {
      if (v < min) min = v;
      if (v > max) max = v;
    }
  });
  if (min === max) return 6;
  const normalized = (val - min) / (max - min);
  return 4 + normalized * 8;
}

function clearMapResults() {
  if (state.trackLayer) {
    state.map.removeLayer(state.trackLayer);
    state.trackLayer = null;
  }
  state.markerLayers.forEach((m) => state.map.removeLayer(m));
  state.markerLayers = [];
}

// =============================================================================
// H. ALL POINTS TABLE
// =============================================================================

function renderResultsTable() {
  const container = document.getElementById('me-table-body');
  if (!container) return;

  const pathNames = getUniquePathNames();

  // Header
  const thead = document.getElementById('me-table-head');
  thead.innerHTML = `<tr><th>Time</th><th>Position</th>${pathNames.map((p) => `<th><span class="me-color-dot" style="background:${PATH_COLORS[pathNames.indexOf(p) % PATH_COLORS.length]}"></span>${shortenPath(p)}</th>`).join('')}</tr>`;

  // Body
  container.innerHTML = '';
  state.dataPoints.forEach((pt, idx) => {
    const row = document.createElement('tr');
    row.className = idx === state.selectedIndex ? 'me-selected-row' : '';
    row.onclick = () => selectPoint(idx);

    let cells = `<td>${formatTimestamp(pt.ts)}</td><td>${toDDM(pt.lat, pt.lon)}</td>`;
    pathNames.forEach((p) => {
      const v = pt.values[p];
      cells += `<td>${v !== undefined ? formatValue(v, p) + ' ' + getUnitForPath(p) : '-'}</td>`;
    });
    row.innerHTML = cells;
    container.appendChild(row);
  });
}

// =============================================================================
// I. DETAIL PANEL + PLAYBACK
// =============================================================================

export function selectPoint(idx) {
  if (idx < 0 || idx >= state.dataPoints.length) return;
  state.selectedIndex = idx;

  // Update map markers
  renderMapResults();

  // Update table highlight
  const rows = document.querySelectorAll('#me-table-body tr');
  rows.forEach((r, i) => {
    r.className = i === idx ? 'me-selected-row' : '';
  });

  // Scroll table row into view
  if (rows[idx]) {
    rows[idx].scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  }

  // Update detail panel
  renderDetailPanel();

  // Update playback slider
  const slider = document.getElementById('me-playback-slider');
  if (slider) slider.value = idx;
}

function renderDetailPanel() {
  const panel = document.getElementById('me-detail-content');
  if (!panel) return;

  if (state.selectedIndex < 0) {
    panel.innerHTML = '<p>Click a point to see details.</p>';
    return;
  }

  const pt = state.dataPoints[state.selectedIndex];
  const pathNames = getUniquePathNames();

  let html = `
    <div class="me-detail-header">
      <strong>${formatTimestamp(pt.ts)}</strong><br>
      <span>${toDDM(pt.lat, pt.lon)}</span>
    </div>
    <div class="me-detail-values">
  `;

  pathNames.forEach((p, pi) => {
    const v = pt.values[p];
    const unit = getUnitForPath(p);
    const color = PATH_COLORS[pi % PATH_COLORS.length];
    html += `
      <div class="me-detail-value-row">
        <span class="me-color-dot" style="background:${color}"></span>
        <strong>${shortenPath(p)}:</strong> ${v !== undefined ? formatValue(v, p) + ' ' + unit : 'N/A'}
      </div>
      <div id="me-sparkline-${pi}" class="me-sparkline"></div>
    `;
  });
  html += '</div>';
  panel.innerHTML = html;

  // Render sparklines with Plotly
  pathNames.forEach((p, pi) => {
    const container = document.getElementById(`me-sparkline-${pi}`);
    if (!container) return;

    const xData = [];
    const yData = [];
    state.dataPoints.forEach((dp) => {
      if (dp.values[p] !== undefined) {
        xData.push(new Date(dp.ts));
        yData.push(convertValue(dp.values[p], p));
      }
    });

    if (yData.length === 0) return;

    const color = PATH_COLORS[pi % PATH_COLORS.length];
    const selectedTs = new Date(pt.ts);

    Plotly.newPlot(
      container,
      [
        {
          x: xData,
          y: yData,
          type: 'scatter',
          mode: 'lines',
          line: { color, width: 1 },
          hoverinfo: 'skip',
        },
      ],
      {
        height: 80,
        margin: { t: 5, b: 20, l: 40, r: 5 },
        xaxis: { showgrid: false, showticklabels: false },
        yaxis: { showgrid: true, gridcolor: '#eee', title: { text: getUnitForPath(p), font: { size: 10 } } },
        shapes: [
          {
            type: 'line',
            x0: selectedTs,
            x1: selectedTs,
            y0: 0,
            y1: 1,
            yref: 'paper',
            line: { color: '#FF5722', width: 2 },
          },
        ],
        paper_bgcolor: 'transparent',
        plot_bgcolor: 'transparent',
      },
      { staticPlot: true, responsive: true }
    );
  });

  // Update slider range
  const slider = document.getElementById('me-playback-slider');
  if (slider) {
    slider.max = state.dataPoints.length - 1;
    slider.value = state.selectedIndex;
  }
}

export function onPlaybackSliderChange(value) {
  selectPoint(parseInt(value));
}

export function togglePlayback() {
  if (state.playing) {
    stopPlayback();
  } else {
    startPlayback();
  }
}

function startPlayback() {
  if (state.dataPoints.length === 0) return;
  state.playing = true;
  document.getElementById('me-play-btn').textContent = '\u23F8';

  if (state.selectedIndex < 0) state.selectedIndex = 0;

  const intervalMs = 200 / state.playSpeed;
  state.playInterval = setInterval(() => {
    const next = state.playReverse
      ? state.selectedIndex - 1
      : state.selectedIndex + 1;

    if (next < 0 || next >= state.dataPoints.length) {
      stopPlayback();
      return;
    }
    selectPoint(next);
  }, intervalMs);
}

function stopPlayback() {
  state.playing = false;
  if (state.playInterval) clearInterval(state.playInterval);
  state.playInterval = null;
  const btn = document.getElementById('me-play-btn');
  if (btn) btn.textContent = '\u25B6';
}

export function togglePlayReverse() {
  state.playReverse = !state.playReverse;
  const btn = document.getElementById('me-reverse-btn');
  if (btn) btn.classList.toggle('me-active', state.playReverse);
  if (state.playing) {
    stopPlayback();
    startPlayback();
  }
}

export function skipPlayback(delta) {
  if (state.dataPoints.length === 0) return;
  let next;
  if (delta === -Infinity) {
    next = 0;
  } else if (delta === Infinity) {
    next = state.dataPoints.length - 1;
  } else {
    next = Math.max(0, Math.min(state.dataPoints.length - 1, state.selectedIndex + delta));
  }
  selectPoint(next);
}

export function setPlaySpeed(speed) {
  state.playSpeed = parseFloat(speed);
  if (state.playing) {
    stopPlayback();
    startPlayback();
  }
}

// =============================================================================
// J. LEGEND
// =============================================================================

function renderLegend() {
  const container = document.getElementById('me-legend');
  if (!container) return;

  const pathNames = getUniquePathNames();
  container.innerHTML = '';

  pathNames.forEach((p, i) => {
    const color = PATH_COLORS[i % PATH_COLORS.length];
    const isVisible = state.visibleSeriesIndices.has(i);
    const isActive = i === state.activeSeries;
    const chip = document.createElement('div');
    chip.className = `me-legend-chip ${!isVisible ? 'me-hidden' : ''} ${isActive ? 'me-active-series' : ''}`;
    chip.innerHTML = `<span class="me-color-dot" style="background:${isVisible ? color : '#999'}"></span>${shortenPath(p)}`;
    chip.onclick = () => legendToggle(i);
    container.appendChild(chip);
  });
}

// ZedDisplay 3-state toggle:
// Hidden → Visible + Active
// Visible + Inactive → Active (no visibility change)
// Visible + Active → Hidden
function legendToggle(index) {
  const isVisible = state.visibleSeriesIndices.has(index);
  const isActive = index === state.activeSeries;

  if (!isVisible) {
    // Hidden → show and make active
    state.visibleSeriesIndices.add(index);
    state.activeSeries = index;
  } else if (isVisible && !isActive) {
    // Visible but not active → make active
    state.activeSeries = index;
  } else {
    // Visible + active → hide
    state.visibleSeriesIndices.delete(index);
    if (state.visibleSeriesIndices.size > 0) {
      state.activeSeries = state.visibleSeriesIndices.values().next().value;
    }
  }

  renderLegend();
  renderMapResults();
}

// =============================================================================
// K. SUMMARY BAR
// =============================================================================

function renderSummary() {
  const container = document.getElementById('me-summary');
  if (!container || !state.results) return;

  const pathNames = getUniquePathNames();
  const ctx = state.context === 'self' ? 'Self' : state.context;

  let areaStr = '';
  if (state.areaType === 'bbox') {
    const g = state.areaGeometry;
    areaStr = `Bbox [${g.south.toFixed(3)},${g.west.toFixed(3)} to ${g.north.toFixed(3)},${g.east.toFixed(3)}]`;
  } else if (state.areaType === 'radius') {
    const g = state.areaGeometry;
    areaStr = `Radius ${g.radius}m from ${g.lat.toFixed(4)},${g.lon.toFixed(4)}`;
  }

  const { from, to } = getQueryTimeRange();
  const fromStr = new Date(from).toLocaleDateString();
  const toStr = new Date(to).toLocaleDateString();

  let stats = '';
  pathNames.forEach((p, i) => {
    const vals = state.dataPoints
      .map((dp) => dp.values[p])
      .filter((v) => v !== undefined && v !== null);
    if (vals.length === 0) return;
    const min = Math.min(...vals);
    const max = Math.max(...vals);
    const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
    const unit = getUnitForPath(p);
    const color = PATH_COLORS[i % PATH_COLORS.length];
    stats += ` | <span style="color:${color}">${shortenPath(p)}</span>: ${formatValue(min, p)}/${formatValue(avg, p)}/${formatValue(max, p)} ${unit}`;
  });

  container.innerHTML = `${ctx} | ${areaStr} | ${fromStr}–${toStr} | ${state.dataPoints.length} pts | ${pathNames.length} paths${stats}`;
}

// =============================================================================
// L. EXPORT
// =============================================================================

export function exportCSV() {
  if (!state.dataPoints.length) return;

  const pathNames = getUniquePathNames();
  let csv = `# SignalK Parquet Map Explorer Export\n`;
  csv += `# Context: ${state.context}\n`;
  csv += `# Area: ${state.areaType} ${JSON.stringify(state.areaGeometry)}\n`;
  csv += `# Exported: ${new Date().toISOString()}\n`;
  csv += `timestamp,latitude,longitude,${pathNames.join(',')}\n`;

  state.dataPoints.forEach((pt) => {
    const vals = pathNames.map((p) =>
      pt.values[p] !== undefined ? pt.values[p] : ''
    );
    csv += `${pt.ts},${pt.lat},${pt.lon},${vals.join(',')}\n`;
  });

  downloadBlob(csv, 'map-explorer-export.csv', 'text/csv');
}

export function exportGeoJSON() {
  if (!state.dataPoints.length) return;

  const pathNames = getUniquePathNames();
  const features = [];

  // Search area feature
  if (state.areaType === 'bbox') {
    const g = state.areaGeometry;
    features.push({
      type: 'Feature',
      properties: { type: 'search_area', areaType: 'bbox' },
      geometry: {
        type: 'Polygon',
        coordinates: [
          [
            [g.west, g.south],
            [g.east, g.south],
            [g.east, g.north],
            [g.west, g.north],
            [g.west, g.south],
          ],
        ],
      },
    });
  } else if (state.areaType === 'radius') {
    const g = state.areaGeometry;
    features.push({
      type: 'Feature',
      properties: {
        type: 'search_area',
        areaType: 'radius',
        radiusMeters: g.radius,
      },
      geometry: { type: 'Point', coordinates: [g.lon, g.lat] },
    });
  }

  // Data point features
  state.dataPoints.forEach((pt) => {
    const props = { timestamp: pt.ts };
    pathNames.forEach((p) => {
      if (pt.values[p] !== undefined) props[p] = pt.values[p];
    });
    features.push({
      type: 'Feature',
      properties: props,
      geometry: { type: 'Point', coordinates: [pt.lon, pt.lat] },
    });
  });

  const geojson = { type: 'FeatureCollection', features };
  downloadBlob(
    JSON.stringify(geojson, null, 2),
    'map-explorer-export.geojson',
    'application/geo+json'
  );
}

export function exportKML() {
  if (!state.dataPoints.length) return;

  const pathNames = getUniquePathNames();

  let kml = `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
<Document>
  <name>SignalK Map Explorer Export</name>
  <description>Exported ${new Date().toISOString()}</description>
  <Style id="trackStyle">
    <LineStyle><color>ffF39621</color><width>2</width></LineStyle>
  </Style>
  <Style id="pointStyle">
    <IconStyle><scale>0.6</scale><Icon><href>http://maps.google.com/mapfiles/kml/paddle/blu-circle.png</href></Icon></IconStyle>
  </Style>
  <Placemark>
    <name>Track</name>
    <styleUrl>#trackStyle</styleUrl>
    <LineString>
      <coordinates>${state.dataPoints.map((pt) => `${pt.lon},${pt.lat},0`).join(' ')}</coordinates>
    </LineString>
  </Placemark>
`;

  state.dataPoints.forEach((pt) => {
    const extData = pathNames
      .filter((p) => pt.values[p] !== undefined)
      .map(
        (p) =>
          `        <Data name="${p}"><value>${pt.values[p]}</value></Data>`
      )
      .join('\n');

    kml += `  <Placemark>
    <name>${formatTimestamp(pt.ts)}</name>
    <TimeStamp><when>${pt.ts}</when></TimeStamp>
    <styleUrl>#pointStyle</styleUrl>
    <Point><coordinates>${pt.lon},${pt.lat},0</coordinates></Point>
    <ExtendedData>
${extData}
    </ExtendedData>
  </Placemark>
`;
  });

  kml += '</Document>\n</kml>';
  downloadBlob(kml, 'map-explorer-export.kml', 'application/vnd.google-earth.kml+xml');
}

// =============================================================================
// M. ROUTES, TRACKS & WAYPOINTS (SignalK Resources API)
// =============================================================================

export function saveAsWaypoint() {
  if (state.selectedIndex < 0) {
    setToolbarStatus('Select a point first.');
    return;
  }
  const pt = state.dataPoints[state.selectedIndex];
  const name = prompt('Waypoint name:', `Wpt ${formatTimestamp(pt.ts)}`);
  if (!name) return;

  const id = crypto.randomUUID();
  const data = {
    name,
    description: '',
    feature: {
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [pt.lon, pt.lat] },
      properties: {},
    },
  };

  fetch(`/signalk/v2/api/resources/waypoints/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
    .then((r) => {
      if (r.ok) setToolbarStatus(`Waypoint "${name}" saved.`);
      else setToolbarStatus('Failed to save waypoint.');
    })
    .catch(() => setToolbarStatus('Failed to save waypoint.'));
}

export function saveAsTrack() {
  if (state.dataPoints.length < 2) {
    setToolbarStatus('Need at least 2 points for a track.');
    return;
  }
  const name = prompt('Track name:', `Track ${new Date().toLocaleDateString()}`);
  if (!name) return;

  const coords = state.dataPoints.map((pt) => [pt.lon, pt.lat]);
  const id = crypto.randomUUID();
  const data = {
    feature: {
      type: 'Feature',
      geometry: { type: 'MultiLineString', coordinates: [coords] },
      properties: { name, description: `${coords.length} points` },
    },
  };

  fetch(`/signalk/v2/api/resources/tracks/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
    .then((r) => {
      if (r.ok) setToolbarStatus(`Track "${name}" saved (${coords.length} points).`);
      else setToolbarStatus('Failed to save track.');
    })
    .catch(() => setToolbarStatus('Failed to save track.'));
}

export function saveAsRoute() {
  if (state.dataPoints.length < 2) {
    setToolbarStatus('Need at least 2 points for a route.');
    return;
  }
  document.getElementById('me-route-modal').style.display = 'flex';
  updateRoutePreview();
}

export function closeRouteModal() {
  document.getElementById('me-route-modal').style.display = 'none';
}

export function updateRoutePreview() {
  const threshold = parseFloat(document.getElementById('me-route-tolerance').value) || 15;
  const coords = state.dataPoints.map((pt) => [pt.lon, pt.lat]);
  const simplified = simplifyTrack(coords, threshold);
  const dist = trackDistanceMeters(simplified);
  document.getElementById('me-route-preview').textContent =
    `${coords.length} points \u2192 ${simplified.length} waypoints \u2022 ${metersToNM(dist).toFixed(1)} NM`;
  document.getElementById('me-route-tolerance-val').textContent = `${threshold}\u00B0`;
}

export function confirmSaveRoute() {
  const name = document.getElementById('me-route-name').value.trim();
  if (!name) return;
  const desc = document.getElementById('me-route-desc').value.trim();
  const threshold = parseFloat(document.getElementById('me-route-tolerance').value) || 15;

  const coords = state.dataPoints.map((pt) => [pt.lon, pt.lat]);
  const simplified = simplifyTrack(coords, threshold);
  const dist = trackDistanceMeters(simplified);

  const id = crypto.randomUUID();
  const data = {
    name,
    description: desc,
    distance: Math.round(dist),
    feature: {
      type: 'Feature',
      geometry: { type: 'LineString', coordinates: simplified },
      properties: {
        coordinatesMeta: simplified.map((_, i) => ({ name: `WPT ${i + 1}` })),
      },
      id: '',
    },
  };

  fetch(`/signalk/v2/api/resources/routes/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
    .then((r) => {
      if (r.ok) setToolbarStatus(`Route "${name}" saved (${simplified.length} waypoints, ${metersToNM(dist).toFixed(1)} NM).`);
      else setToolbarStatus('Failed to save route.');
      closeRouteModal();
    })
    .catch(() => {
      setToolbarStatus('Failed to save route.');
      closeRouteModal();
    });
}

// Course-delta track simplification for marine route creation.
// Preserves waypoints at turns while removing redundant straight-line points.
// Unlike Ramer-Douglas-Peucker, this never cuts corners through dangerous waters.
//
// coords: [[lon, lat], ...], headingThresholdDeg: degrees, maxLegMeters: meters
function simplifyTrack(coords, headingThresholdDeg, maxLegMeters = 3704) {
  if (coords.length <= 2) return coords.slice();

  const result = [coords[0]];
  let lastKeptIndex = 0;
  let prevBearing = bearing(coords[0][1], coords[0][0], coords[1][1], coords[1][0]);

  for (let i = 1; i < coords.length - 1; i++) {
    const b = bearing(coords[i][1], coords[i][0], coords[i + 1][1], coords[i + 1][0]);
    let delta = Math.abs(b - prevBearing);
    if (delta > 180) delta = 360 - delta;

    const legDist = haversine(
      coords[lastKeptIndex][1], coords[lastKeptIndex][0],
      coords[i][1], coords[i][0]
    );

    if (delta >= headingThresholdDeg || legDist >= maxLegMeters) {
      result.push(coords[i]);
      lastKeptIndex = i;
    }
    prevBearing = b;
  }

  result.push(coords[coords.length - 1]);
  return result;
}

function bearing(lat1, lon1, lat2, lon2) {
  const toRad = Math.PI / 180;
  const dLon = (lon2 - lon1) * toRad;
  const y = Math.sin(dLon) * Math.cos(lat2 * toRad);
  const x = Math.cos(lat1 * toRad) * Math.sin(lat2 * toRad) -
    Math.sin(lat1 * toRad) * Math.cos(lat2 * toRad) * Math.cos(dLon);
  return ((Math.atan2(y, x) * 180 / Math.PI) + 360) % 360;
}

function trackDistanceMeters(coords) {
  let total = 0;
  for (let i = 1; i < coords.length; i++) {
    total += haversine(coords[i - 1][1], coords[i - 1][0], coords[i][1], coords[i][0]);
  }
  return total;
}

function metersToNM(m) { return m / 1852; }

function haversine(lat1, lon1, lat2, lon2) {
  const R = 6371000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// =============================================================================
// N. SAVED SEARCH AREAS
// =============================================================================

const SAVED_AREAS_KEY = 'signalk-parquet-saved-areas';

function loadSavedAreas() {
  try {
    state.savedAreas = JSON.parse(localStorage.getItem(SAVED_AREAS_KEY) || '[]');
  } catch {
    state.savedAreas = [];
  }
}

export function showSaveAreaDialog() {
  if (!state.areaGeometry) {
    setToolbarStatus('Draw an area first.');
    return;
  }
  document.getElementById('me-save-modal').style.display = 'flex';
}

export function closeSaveModal() {
  document.getElementById('me-save-modal').style.display = 'none';
}

export function saveArea() {
  const name = document.getElementById('me-save-name').value.trim();
  if (!name) return;
  const desc = document.getElementById('me-save-desc').value.trim();

  state.savedAreas.push({
    id: Date.now(),
    name,
    description: desc,
    type: state.areaType,
    geometry: { ...state.areaGeometry },
    created: new Date().toISOString(),
  });

  localStorage.setItem(SAVED_AREAS_KEY, JSON.stringify(state.savedAreas));
  document.getElementById('me-save-name').value = '';
  document.getElementById('me-save-desc').value = '';
  closeSaveModal();
  setToolbarStatus(`Area "${name}" saved.`);
}

export function showSavedAreasModal() {
  loadSavedAreas();
  const list = document.getElementById('me-saved-list');
  list.innerHTML = '';

  if (state.savedAreas.length === 0) {
    list.innerHTML = '<p>No saved areas yet.</p>';
  } else {
    state.savedAreas.forEach((area) => {
      const div = document.createElement('div');
      div.className = 'me-saved-item';
      div.innerHTML = `
        <div>
          <strong>${area.name}</strong>
          <span class="me-badge me-badge-${area.type}">${area.type}</span>
          ${area.description ? `<br><small>${area.description}</small>` : ''}
        </div>
        <div>
          <button onclick="window.meLoadArea(${area.id})" class="me-btn-sm">Load</button>
          <button onclick="window.meDeleteArea(${area.id})" class="me-btn-sm me-btn-danger">Delete</button>
        </div>
      `;
      list.appendChild(div);
    });
  }

  document.getElementById('me-saved-modal').style.display = 'flex';
}

export function closeSavedModal() {
  document.getElementById('me-saved-modal').style.display = 'none';
}

export function loadArea(id) {
  const area = state.savedAreas.find((a) => a.id === id);
  if (!area) return;

  state.areaType = area.type;
  state.areaGeometry = { ...area.geometry };
  drawAreaOnMap();
  updateQueryPanelState();

  // Fit to area
  if (state.areaLayer) {
    state.map.fitBounds(state.areaLayer.getBounds().pad(0.2));
  }

  closeSavedModal();
  state.mode = STATES.QUERY_CONFIG;
  setToolbarStatus(`Loaded area "${area.name}".`);
}

export function deleteArea(id) {
  state.savedAreas = state.savedAreas.filter((a) => a.id !== id);
  localStorage.setItem(SAVED_AREAS_KEY, JSON.stringify(state.savedAreas));
  showSavedAreasModal(); // Refresh the list
}

// =============================================================================
// N. UTILITIES
// =============================================================================

function toDDM(lat, lon) {
  const fmtDDM = (dec, posChar, negChar) => {
    const sign = dec >= 0 ? posChar : negChar;
    const abs = Math.abs(dec);
    const deg = Math.floor(abs);
    const min = (abs - deg) * 60;
    return `${deg}\u00B0 ${min.toFixed(3)}' ${sign}`;
  };
  return `${fmtDDM(lat, 'N', 'S')}, ${fmtDDM(lon, 'E', 'W')}`;
}

function formatValue(v, pathName) {
  if (v === null || v === undefined) return '-';
  if (typeof v === 'number') {
    const converted = pathName ? convertValue(v, pathName) : v;
    const fmt = pathName ? getDisplayFormat(pathName) : null;
    if (fmt) {
      // displayFormat like "0.0" = 1 decimal, "0.00" = 2, "0" = integer
      const decimals = fmt.includes('.') ? fmt.split('.')[1].length : 0;
      return converted.toFixed(decimals);
    }
    return Number.isInteger(converted) ? converted.toString() : converted.toFixed(2);
  }
  return String(v);
}

function formatTimestamp(ts) {
  return new Date(ts).toLocaleString();
}

function shortenPath(path) {
  // Remove aggregation/smoothing suffixes for display
  const base = path.split(':')[0];
  const parts = base.split('.');
  if (parts.length <= 2) return base;
  return parts.slice(-2).join('.');
}

function downloadBlob(content, filename, mimeType) {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function setToolbarStatus(msg) {
  const el = document.getElementById('me-status');
  if (el) el.textContent = msg;
}

export function toggleOpenSeaMap() {
  if (state.openSeaMapVisible) {
    state.map.removeLayer(state.openSeaMapLayer);
    state.openSeaMapVisible = false;
  } else {
    state.openSeaMapLayer.addTo(state.map);
    state.openSeaMapVisible = true;
  }
  document.getElementById('me-osm-toggle').classList.toggle('me-active', state.openSeaMapVisible);
}

export function showSmoothingParam() {
  const smooth = document.getElementById('me-smoothing').value;
  const paramDiv = document.getElementById('me-smooth-param-group');
  if (paramDiv) {
    paramDiv.style.display = smooth === 'none' ? 'none' : 'block';
  }
  const label = document.getElementById('me-smooth-param-label');
  if (label) {
    label.textContent = smooth === 'sma' ? 'Window size:' : 'Alpha:';
  }
  const input = document.getElementById('me-smooth-param');
  if (input) {
    input.value = smooth === 'sma' ? '5' : '0.3';
  }
}

export function showResultsTab(tabName) {
  document.querySelectorAll('.me-results-tab-btn').forEach((b) =>
    b.classList.toggle('me-active', b.dataset.tab === tabName)
  );
  document.getElementById('me-all-points').style.display = tabName === 'allpoints' ? 'block' : 'none';
  document.getElementById('me-detail').style.display = tabName === 'detail' ? 'block' : 'none';

  // Resize sparklines when detail tab becomes visible
  if (tabName === 'detail') {
    setTimeout(() => {
      document.querySelectorAll('.me-sparkline').forEach((el) => {
        if (el.data) Plotly.Plots.resize(el);
      });
    }, 50);
  }
}
