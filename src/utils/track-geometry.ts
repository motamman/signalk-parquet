/**
 * Geometry helpers for the Track API provider.
 *
 * Splitting a time-ordered run of positions into segments at recording gaps,
 * Douglas-Peucker simplification that returns the *indices* it keeps (so the
 * arrays that run parallel to the coordinates — `coordTimes` and per-point
 * property values — stay aligned), and the bounding box of what was returned.
 *
 * Distances use an equirectangular projection around the segment's mean
 * latitude. At the scale simplification tolerances are given in (metres to
 * tens of metres) that is indistinguishable from great-circle distance, and it
 * keeps the hot loop free of trigonometry per point. A segment that crosses
 * the antimeridian is measured across the long way round, which over-states
 * its distances and so keeps more points than needed — the safe direction.
 */

export interface LonLat {
  lon: number;
  lat: number;
}

const EARTH_RADIUS_M = 6371000;
const DEG_TO_RAD = Math.PI / 180;

/**
 * Split time-ordered points into segments wherever two consecutive points are
 * more than `gapMs` apart. Order is preserved; no points are dropped.
 *
 * Input:  [t=0, t=60, t=120, t=7200, t=7260], gapMs=300
 * Output: [[t=0, t=60, t=120], [t=7200, t=7260]]
 */
export function splitIntoSegments<T extends { tMs: number }>(
  points: readonly T[],
  gapMs: number
): T[][] {
  const segments: T[][] = [];
  let current: T[] = [];
  for (const point of points) {
    const previous = current[current.length - 1];
    if (previous !== undefined && point.tMs - previous.tMs > gapMs) {
      segments.push(current);
      current = [];
    }
    current.push(point);
  }
  if (current.length > 0) {
    segments.push(current);
  }
  return segments;
}

/**
 * Hold a track to `maxPoints`, the Track API's upper bound on points per
 * track. Bucketing aims at the budget but can pass it: buckets are aligned to
 * the clock, so a window or a segment can touch one more than its share, and
 * a clipped track's crossing points come on top of its buckets.
 *
 * Crossing points (the fixes just outside a clipping box, which let a line
 * reach the edge of the view) are kept first while they fit, and the room
 * left goes to the other points, spread evenly over the track in time order
 * from its first to its last, so the time range returned does not narrow.
 * When the crossing points alone do not fit, the bound wins and they are
 * dropped. A segment left with no points is dropped.
 *
 * Input:  [[c, a, b, c], [c, d, c]] (c crossing), maxPoints 5
 * Output: [[c, a, c], [c, c]]
 */
export function capPoints<T extends { crossing?: boolean }>(
  segments: readonly T[][],
  maxPoints: number
): T[][] {
  const total = segments.reduce((n, s) => n + s.length, 0);
  if (total <= maxPoints) {
    return segments.map(s => [...s]);
  }
  const crossings = segments.reduce(
    (n, s) => n + s.filter(p => p.crossing).length,
    0
  );
  const keepCrossings = crossings <= maxPoints;
  const others = total - crossings;
  const room = Math.min(
    others,
    keepCrossings ? maxPoints - crossings : maxPoints
  );
  // `room` of the `others` points, evenly spaced from the first to the last:
  // dropping the last fix would end the track early and narrow the time range
  // it reports, which the Track API rules out. With room for one, the first.
  const chosen = new Set<number>();
  for (let i = 0; i < room; i++) {
    chosen.add(room === 1 ? 0 : Math.floor((i * (others - 1)) / (room - 1)));
  }

  const out: T[][] = [];
  let index = 0;
  for (const segment of segments) {
    const kept: T[] = [];
    for (const point of segment) {
      if (point.crossing) {
        if (keepCrossings) kept.push(point);
        continue;
      }
      if (chosen.has(index)) kept.push(point);
      index++;
    }
    if (kept.length > 0) out.push(kept);
  }
  return out;
}

function project(p: LonLat, cosLat: number): [number, number] {
  return [
    p.lon * DEG_TO_RAD * cosLat * EARTH_RADIUS_M,
    p.lat * DEG_TO_RAD * EARTH_RADIUS_M,
  ];
}

/**
 * Perpendicular distance in metres from `p` to the segment `a`–`b`, clamped
 * to the segment's ends (a point beyond either end measures to that end).
 */
export function perpendicularDistanceMetres(
  p: LonLat,
  a: LonLat,
  b: LonLat
): number {
  const cosLat = Math.cos(((a.lat + b.lat) / 2) * DEG_TO_RAD);
  const [px, py] = project(p, cosLat);
  const [ax, ay] = project(a, cosLat);
  const [bx, by] = project(b, cosLat);
  const dx = bx - ax;
  const dy = by - ay;
  const lengthSquared = dx * dx + dy * dy;
  if (lengthSquared === 0) {
    return Math.hypot(px - ax, py - ay);
  }
  const t = Math.max(
    0,
    Math.min(1, ((px - ax) * dx + (py - ay) * dy) / lengthSquared)
  );
  return Math.hypot(px - (ax + t * dx), py - (ay + t * dy));
}

/**
 * Douglas-Peucker simplification. Returns the ascending indices of the points
 * to keep; the first and last are always kept. An epsilon of zero or less, or
 * two points or fewer, keeps everything.
 *
 * Iterative rather than recursive so a track of hundreds of thousands of raw
 * fixes cannot overflow the stack.
 */
export function simplifyIndices(
  points: readonly LonLat[],
  epsilonMetres: number
): number[] {
  const n = points.length;
  if (n <= 2 || !(epsilonMetres > 0)) {
    return points.map((_, i) => i);
  }

  const keep = new Uint8Array(n);
  keep[0] = 1;
  keep[n - 1] = 1;
  const stack: Array<[number, number]> = [[0, n - 1]];

  while (stack.length > 0) {
    const [first, last] = stack.pop()!;
    let maxDistance = 0;
    let index = -1;
    for (let i = first + 1; i < last; i++) {
      const d = perpendicularDistanceMetres(
        points[i],
        points[first],
        points[last]
      );
      if (d > maxDistance) {
        maxDistance = d;
        index = i;
      }
    }
    if (index !== -1 && maxDistance > epsilonMetres) {
      keep[index] = 1;
      stack.push([first, index], [index, last]);
    }
  }

  const kept: number[] = [];
  for (let i = 0; i < n; i++) {
    if (keep[i]) kept.push(i);
  }
  return kept;
}

/**
 * Bounding box of a set of points as `[west, south, east, north]`, or
 * undefined for no points. Plain min/max: a track spanning the antimeridian
 * gets the wide box, which is the honest answer for a plain min/max field.
 */
export function boundingBoxOf(
  points: readonly LonLat[]
): [number, number, number, number] | undefined {
  if (points.length === 0) {
    return undefined;
  }
  let west = Infinity;
  let south = Infinity;
  let east = -Infinity;
  let north = -Infinity;
  for (const p of points) {
    if (p.lon < west) west = p.lon;
    if (p.lon > east) east = p.lon;
    if (p.lat < south) south = p.lat;
    if (p.lat > north) north = p.lat;
  }
  return [west, south, east, north];
}

/**
 * ISO 8601 duration for a number of milliseconds, in the largest single unit
 * that expresses it exactly, hours at most (the Track API normalises spacings
 * to hours and below).
 *
 * Input: 1000 -> PT1S; 720 -> PT0.72S; 120000 -> PT2M; 3600000 -> PT1H
 */
export function millisToIsoDuration(ms: number): string {
  if (ms > 0 && ms % 3_600_000 === 0) {
    return `PT${ms / 3_600_000}H`;
  }
  if (ms > 0 && ms % 60_000 === 0) {
    return `PT${ms / 60_000}M`;
  }
  const seconds = ms / 1000;
  const rendered = Number.isInteger(seconds)
    ? String(seconds)
    : String(parseFloat(seconds.toFixed(3)));
  return `PT${rendered}S`;
}
