/**
 * Minimal GPX parser
 *
 * Extracts track points (<trkpt>) from a GPX file. Waypoints and routes are
 * ignored — only trkpt elements carry the <time> tag needed for partitioning
 * time-series data into the parquet store.
 *
 * Supports GPX 1.0 and 1.1. Uses regex-based extraction rather than a full
 * XML parser to avoid adding a dependency; this is safe for GPX because the
 * schema is shallow and well-defined.
 *
 * Example input fragment:
 *   <trkpt lat="47.5" lon="8.7">
 *     <ele>412.5</ele>
 *     <time>2024-06-01T10:15:30Z</time>
 *     <speed>5.14</speed>
 *     <course>180.0</course>
 *   </trkpt>
 * Produces: { latitude: 47.5, longitude: 8.7, time: Date(...), elevation: 412.5, speedMs: 5.14, courseDeg: 180 }
 */

export interface GpxPoint {
  latitude: number;
  longitude: number;
  time?: Date;
  elevation?: number; // meters
  speedMs?: number; // m/s
  courseDeg?: number; // degrees true
}

export interface GpxTrack {
  name?: string;
  points: GpxPoint[];
}

export interface GpxParseResult {
  tracks: GpxTrack[];
  totalPoints: number;
  firstTime?: Date;
  lastTime?: Date;
}

// Real-world GPX exporters add namespace prefixes liberally (Garmin
// Connect uses "gpxx:", some OpenCPN configs use "ns3:", certain Suunto
// and Polar exporters use other ones). The (?:[\w-]+:)? group is
// non-capturing so existing capture-group indices stay the same.

// Match a single <trkpt ...>...</trkpt> block (possibly namespace-prefixed),
// capturing attributes and inner content. Multiline because trkpt contents
// span several lines.
const TRKPT_RE =
  /<(?:[\w-]+:)?trkpt\b([^>]*)>([\s\S]*?)<\/(?:[\w-]+:)?trkpt>|<(?:[\w-]+:)?trkpt\b([^>]*)\/>/g;

// Match <trk>...</trk>; within each we parse name and trkpt elements.
const TRK_RE = /<(?:[\w-]+:)?trk\b[^>]*>([\s\S]*?)<\/(?:[\w-]+:)?trk>/g;

const NAME_RE = /<(?:[\w-]+:)?name\b[^>]*>([\s\S]*?)<\/(?:[\w-]+:)?name>/;

function extractAttr(attrs: string, name: string): string | undefined {
  // e.g. extract lat="47.5" from ' lat="47.5" lon="8.7"'
  const re = new RegExp(`\\b${name}\\s*=\\s*"([^"]*)"`);
  const m = attrs.match(re);
  return m ? m[1] : undefined;
}

function extractTag(inner: string, tag: string): string | undefined {
  // Allow an optional namespace prefix on either the open or close tag,
  // matching the broader tolerance in TRKPT_RE / TRK_RE / NAME_RE.
  const re = new RegExp(
    `<(?:[\\w-]+:)?${tag}\\b[^>]*>([\\s\\S]*?)<\\/(?:[\\w-]+:)?${tag}>`,
    'i'
  );
  const m = inner.match(re);
  return m ? m[1].trim() : undefined;
}

function parseFloatOrUndef(s: string | undefined): number | undefined {
  if (s === undefined) return undefined;
  const n = parseFloat(s);
  return Number.isFinite(n) ? n : undefined;
}

function parsePoint(attrs: string, inner: string): GpxPoint | null {
  const lat = parseFloatOrUndef(extractAttr(attrs, 'lat'));
  const lon = parseFloatOrUndef(extractAttr(attrs, 'lon'));
  if (lat === undefined || lon === undefined) {
    return null;
  }

  const timeStr = extractTag(inner, 'time');
  let time: Date | undefined;
  if (timeStr) {
    const d = new Date(timeStr);
    if (!isNaN(d.getTime())) {
      time = d;
    }
  }

  return {
    latitude: lat,
    longitude: lon,
    time,
    elevation: parseFloatOrUndef(extractTag(inner, 'ele')),
    speedMs: parseFloatOrUndef(extractTag(inner, 'speed')),
    courseDeg: parseFloatOrUndef(extractTag(inner, 'course')),
  };
}

/**
 * Parse a GPX XML string into structured tracks.
 *
 * Points without a valid lat/lon are skipped. Points without <time> are
 * still returned (caller may drop them) — the time field is undefined.
 */
export function parseGpx(xml: string): GpxParseResult {
  const tracks: GpxTrack[] = [];
  let totalPoints = 0;
  let firstTime: Date | undefined;
  let lastTime: Date | undefined;

  let trkMatch: RegExpExecArray | null;
  TRK_RE.lastIndex = 0;

  while ((trkMatch = TRK_RE.exec(xml)) !== null) {
    const trkInner = trkMatch[1];
    const nameMatch = trkInner.match(NAME_RE);
    const name = nameMatch ? nameMatch[1].trim() : undefined;

    const points: GpxPoint[] = [];
    let ptMatch: RegExpExecArray | null;
    TRKPT_RE.lastIndex = 0;

    while ((ptMatch = TRKPT_RE.exec(trkInner)) !== null) {
      // Self-closing form uses group 3; otherwise group 1 + 2
      const attrs = ptMatch[1] ?? ptMatch[3] ?? '';
      const inner = ptMatch[2] ?? '';
      const pt = parsePoint(attrs, inner);
      if (!pt) continue;

      points.push(pt);
      totalPoints++;

      if (pt.time) {
        if (!firstTime || pt.time < firstTime) firstTime = pt.time;
        if (!lastTime || pt.time > lastTime) lastTime = pt.time;
      }
    }

    tracks.push({ name, points });
  }

  return { tracks, totalPoints, firstTime, lastTime };
}
