/**
 * Display units for threshold values (#74).
 *
 * Thresholds are stored in the path's base (SI) unit, which is what the
 * server compares live values against. People think in knots, °F and
 * degrees, so the editor takes and shows values in the unit the user has
 * chosen in the signalk-units-preference plugin, converting on the way in
 * and out. The plugin's public endpoint needs no authentication:
 *
 *   GET /signalk/v1/conversions/<path>
 *   -> { "<path>": { baseUnit, category, conversions: { <target>: { formula, inverseFormula, symbol } } } }
 *
 * with exactly one conversion, the user's preferred target unit. When the
 * plugin is not installed, the path has no conversion, or the target is the
 * base unit, the editor falls back to what it did before: degrees for
 * angular paths, the base unit for everything else.
 *
 * Formulas arrive as JavaScript expressions in `value` (e.g. "value * 1.94384",
 * "(value - 273.15) * 9/5 + 32"). They come from the local server's own
 * plugin, but are still checked against a small arithmetic alphabet before
 * being compiled, so a surprising response cannot run code in the page.
 */

const DEG_PER_RAD = 180 / Math.PI;

/** path -> unit descriptor, or null when the path has no display unit. */
const unitCache = new Map();

/** Degrees for angular paths when the units plugin has nothing for them. */
const DEGREES_FALLBACK = Object.freeze({
  baseUnit: 'rad',
  targetUnit: 'deg',
  symbol: '°',
  toBase: v => v / DEG_PER_RAD,
  toDisplay: v => v * DEG_PER_RAD,
  source: 'fallback',
});

/** Only digits, operators, parentheses, whitespace and the word `value`. */
const FORMULA_RE = /^[\d\s+\-*/().eE]*(value[\d\s+\-*/().eE]*)*$/;

function compileFormula(formula) {
  if (typeof formula !== 'string' || !FORMULA_RE.test(formula)) return null;
  try {
    const fn = new Function('value', `"use strict"; return (${formula});`);
    const probe = fn(1);
    return Number.isFinite(probe) ? fn : null;
  } catch {
    return null;
  }
}

/**
 * Fetch and cache the display unit for `path`. Resolves to the unit
 * descriptor, or null when there is nothing to convert. `dataType` is the
 * plugin's own type detection for the path, used for the degrees fallback.
 */
export async function loadThresholdUnit(path, dataType) {
  if (!path) return null;
  if (unitCache.has(path)) return unitCache.get(path);

  let unit = null;
  try {
    const response = await fetch(
      `/signalk/v1/conversions/${encodeURIComponent(path)}`,
      { headers: { Accept: 'application/json' } }
    );
    if (response.ok) {
      const body = await response.json();
      const meta = body && body[path];
      const conversions = meta && meta.conversions ? meta.conversions : {};
      const targetUnit = Object.keys(conversions)[0];
      if (
        targetUnit &&
        meta.baseUnit &&
        targetUnit !== meta.baseUnit &&
        targetUnit !== 'none'
      ) {
        const def = conversions[targetUnit];
        const toDisplay = compileFormula(def.formula);
        const toBase = compileFormula(def.inverseFormula);
        if (toDisplay && toBase) {
          unit = {
            baseUnit: meta.baseUnit,
            targetUnit,
            symbol: def.symbol || targetUnit,
            toBase,
            toDisplay,
            source: 'units-preference',
          };
        }
      }
    }
  } catch {
    // Plugin not installed or unreachable: fall through to the fallback.
  }

  if (!unit && dataType === 'angular') {
    unit = DEGREES_FALLBACK;
  }
  unitCache.set(path, unit);
  return unit;
}

/** True once loadThresholdUnit has run for the path (hit or miss). */
export function hasThresholdUnit(path) {
  return unitCache.has(path);
}

/**
 * The display unit for a path from the cache, with the degrees fallback for
 * angular paths that were never loaded. Synchronous so the value-field
 * renderers and the list views can use it directly.
 */
export function unitFor(path, dataType) {
  if (path && unitCache.has(path)) return unitCache.get(path);
  return dataType === 'angular' ? DEGREES_FALLBACK : null;
}

/** Display-unit number -> base-unit number (identity without a unit). */
export function toBaseValue(unit, value) {
  if (!unit || typeof value !== 'number' || !Number.isFinite(value)) {
    return value;
  }
  return unit.toBase(value);
}

/** Base-unit number -> display-unit number (identity without a unit). */
export function toDisplayValue(unit, value) {
  if (!unit || typeof value !== 'number' || !Number.isFinite(value)) {
    return value;
  }
  return roundDisplay(unit.toDisplay(value));
}

/** Trim float noise from a converted value: 0.5144 m/s -> 1 kn, not 0.99999. */
function roundDisplay(n) {
  return Number(n.toPrecision(6));
}

/**
 * A stored (base-unit) value rendered for a list: converted and suffixed
 * with the unit symbol when the path's unit is known, the raw value
 * otherwise.
 */
export function describeThresholdValue(path, value) {
  if (typeof value !== 'number') return value;
  const unit = unitFor(path);
  if (!unit) return value;
  return `${toDisplayValue(unit, value)} ${unit.symbol}`;
}
