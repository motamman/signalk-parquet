/**
 * Vessel identity as one object path per vessel.
 *
 * Signal K spreads a vessel's static identity over several paths that arrive
 * on different cadences: `name` and `mmsi` on the root bus from AIS static
 * reports, `design.*`, `communication.callsignVhf` and `sensors.ais.class`
 * as dotted paths. Recording each as its own path costs one file per path
 * per vessel per day in the hive layout, which on a coast with thousands of
 * AIS targets is most of the daily file count. So identity is folded into a
 * single object path, `identity`, whose components are the flattened values,
 * written once when a vessel is first recorded and again only when something
 * in it changes. The row is retention-exempt and never aggregated.
 *
 * Input (root delta):   { name: 'Ariel', mmsi: '244813000' }
 *   -> { name: 'Ariel', mmsi: '244813000' }
 * Input (design.aisShipType): { id: 36, name: 'Sailing' }
 *   -> { aisShipTypeId: 36, aisShipTypeName: 'Sailing' }
 */

export const IDENTITY_PATH = 'identity';

export function isIdentityPath(signalkPath: string | undefined): boolean {
  return signalkPath === IDENTITY_PATH;
}

/** Scalar components of the identity object, in a fixed order. */
export type IdentityComponents = {
  name?: string;
  mmsi?: string;
  aisShipTypeId?: number;
  aisShipTypeName?: string;
  lengthOverall?: number;
  beam?: number;
  callsignVhf?: string;
  aisClass?: string;
};

/** Root-bus keys that carry identity. */
export const IDENTITY_ROOT_KEYS = ['name', 'mmsi'] as const;

/** Dotted paths that carry identity. */
export const IDENTITY_PATHS = [
  'design.aisShipType',
  'design.length',
  'design.beam',
  'communication.callsignVhf',
  'sensors.ais.class',
] as const;

function asString(v: unknown): string | undefined {
  if (typeof v === 'string') {
    const trimmed = v.trim();
    return trimmed === '' ? undefined : trimmed;
  }
  if (typeof v === 'number' && Number.isFinite(v)) return String(v);
  return undefined;
}

function asNumber(v: unknown): number | undefined {
  return typeof v === 'number' && Number.isFinite(v) ? v : undefined;
}

/**
 * The identity components carried by one delta, or an empty object when the
 * delta carries none. `path` is the delta's path ('' for the root bus).
 */
export function identityFromDelta(
  path: string,
  value: unknown
): IdentityComponents {
  const out: IdentityComponents = {};
  if (path === '') {
    if (value === null || typeof value !== 'object') return out;
    const root = value as Record<string, unknown>;
    const name = asString(root.name);
    const mmsi = asString(root.mmsi);
    if (name !== undefined) out.name = name;
    if (mmsi !== undefined) out.mmsi = mmsi;
    return out;
  }
  switch (path) {
    case 'design.aisShipType': {
      if (value !== null && typeof value === 'object') {
        const o = value as Record<string, unknown>;
        const id = asNumber(o.id);
        const name = asString(o.name);
        if (id !== undefined) out.aisShipTypeId = id;
        if (name !== undefined) out.aisShipTypeName = name;
      } else {
        // Some sources emit the bare numeric type code.
        const id = asNumber(value);
        if (id !== undefined) out.aisShipTypeId = id;
      }
      return out;
    }
    case 'design.length': {
      if (value !== null && typeof value === 'object') {
        const overall = asNumber((value as Record<string, unknown>).overall);
        if (overall !== undefined) out.lengthOverall = overall;
      } else {
        const overall = asNumber(value);
        if (overall !== undefined) out.lengthOverall = overall;
      }
      return out;
    }
    case 'design.beam': {
      const beam = asNumber(value);
      if (beam !== undefined) out.beam = beam;
      return out;
    }
    case 'communication.callsignVhf': {
      const cs = asString(value);
      if (cs !== undefined) out.callsignVhf = cs;
      return out;
    }
    case 'sensors.ais.class': {
      const cls = asString(value);
      if (cls !== undefined) out.aisClass = cls;
      return out;
    }
    default:
      return out;
  }
}

/** Merge new components over known ones; true if anything changed. */
export function mergeIdentity(
  known: IdentityComponents,
  incoming: IdentityComponents
): boolean {
  let changed = false;
  for (const [key, value] of Object.entries(incoming) as Array<
    [keyof IdentityComponents, unknown]
  >) {
    if (value === undefined) continue;
    if (known[key] !== value) {
      (known as Record<string, unknown>)[key] = value;
      changed = true;
    }
  }
  return changed;
}
