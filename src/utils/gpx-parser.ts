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
 * Two entry points share one tokenizer: `parseGpx(xml)` for a string, and
 * `GpxTokenizer` for a stream of chunks (#54), which emits a point as soon
 * as its element is complete and never holds more than the unfinished tail
 * of the input. A tag split across two chunks is handled by waiting for the
 * next chunk.
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

// Match a single trkpt element (possibly namespace-prefixed), either
// self-closing `<trkpt .../>` (group 1: attributes) or paired
// `<trkpt ...>...</trkpt>` (group 2: attributes, group 3: inner content).
// Multiline because trkpt contents span several lines.
//
// The self-closing alternative comes first and its attribute group is lazy:
// with the paired form first, `[^>]*` swallowed the trailing "/" of a
// self-closing tag and the lazy body then ran on to the next </trkpt>, so a
// self-closing point followed by a paired one collapsed into a single point
// (#70). The paired form cannot match a self-closing tag by accident now,
// because `[^>]*?` cannot cross the ">" and "/>" is tried before it.
const TRKPT_RE =
  /<(?:[\w-]+:)?trkpt\b([^>]*?)\/>|<(?:[\w-]+:)?trkpt\b([^>]*)>([\s\S]*?)<\/(?:[\w-]+:)?trkpt>/g;

// Opening <trk ...> tag. `\b` keeps <trkpt> and <trkseg> from matching.
const TRK_OPEN_RE = /<(?:[\w-]+:)?trk\b[^>]*>/g;

// Inside a track, the three things the tokenizer reacts to, whichever comes
// first: the closing </trk>, a trkpt, or a name element.
const TRACK_TOKEN_RE =
  /<(?:\/(?:[\w-]+:)?trk\s*>|(?:[\w-]+:)?trkpt\b|(?:[\w-]+:)?name\b)/g;

const NAME_RE = /<(?:[\w-]+:)?name\b[^>]*>([\s\S]*?)<\/(?:[\w-]+:)?name>/;

// Sticky variants for matching exactly at a token's position.
const TRKPT_AT_RE = new RegExp(TRKPT_RE.source, 'y');
const NAME_AT_RE = new RegExp(NAME_RE.source, 'y');

/**
 * Longest element the tokenizer will wait for. A trkpt or name element is
 * a few hundred bytes; an opening tag that never closes within this many
 * characters is treated as malformed and skipped rather than buffered to
 * the end of a 500 MB file.
 */
const MAX_PENDING_ELEMENT_CHARS = 1024 * 1024;

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

/** What the tokenizer emits, in document order. */
export type GpxEvent =
  | { type: 'track' }
  | { type: 'name'; name: string }
  | { type: 'point'; point: GpxPoint };

/**
 * Incremental GPX tokenizer. Feed it the file in any chunking with `push()`
 * and finish with `end()`; each call returns the events completed so far.
 *
 * Only what has not been consumed yet is kept: while waiting for the rest
 * of an element that straddles a chunk boundary, the buffer holds from that
 * element's start; otherwise it holds at most the tail after the last "<".
 *
 * Input (two pushes): '<trk><trkpt lat="1" lon="2"><ti' + 'me>2024-01-01T00:00:00Z</time></trkpt></trk>'
 * Output: [{type:'track'}], then [{type:'point', point:{latitude:1, longitude:2, time:…}}]
 */
export class GpxTokenizer {
  private buffer = '';
  private inTrack = false;
  private nameSeen = false;

  push(chunk: string): GpxEvent[] {
    this.buffer += chunk;
    return this.drain(false);
  }

  end(): GpxEvent[] {
    const events = this.drain(true);
    this.buffer = '';
    return events;
  }

  private drain(final: boolean): GpxEvent[] {
    const events: GpxEvent[] = [];
    const buffer = this.buffer;
    let pos = 0;
    let waiting = false;

    for (;;) {
      if (!this.inTrack) {
        TRK_OPEN_RE.lastIndex = pos;
        const open = TRK_OPEN_RE.exec(buffer);
        if (!open) break;
        this.inTrack = true;
        this.nameSeen = false;
        events.push({ type: 'track' });
        pos = open.index + open[0].length;
        continue;
      }

      TRACK_TOKEN_RE.lastIndex = pos;
      const token = TRACK_TOKEN_RE.exec(buffer);
      if (!token) break;
      const at = token.index;

      if (token[0].startsWith('</')) {
        this.inTrack = false;
        pos = at + token[0].length;
        continue;
      }

      const isName = /name\b$/.test(token[0]);
      if (isName && this.nameSeen) {
        pos = at + 1;
        continue;
      }

      const re = isName ? NAME_AT_RE : TRKPT_AT_RE;
      re.lastIndex = at;
      const m = re.exec(buffer);
      if (m) {
        if (isName) {
          this.nameSeen = true;
          events.push({ type: 'name', name: m[1].trim() });
        } else {
          // Self-closing form uses group 1; the paired form groups 2 + 3.
          const pt = parsePoint(m[1] ?? m[2] ?? '', m[3] ?? '');
          if (pt) events.push({ type: 'point', point: pt });
        }
        pos = at + m[0].length;
        continue;
      }

      // The element is not complete in the buffer. Wait for more input
      // unless this is the end or it has grown past any plausible size.
      if (final || buffer.length - at > MAX_PENDING_ELEMENT_CHARS) {
        pos = at + 1;
        continue;
      }
      pos = at;
      waiting = true;
      break;
    }

    if (waiting) {
      this.buffer = buffer.slice(pos);
    } else {
      // Nothing more to match; keep only a tail that could be the start of
      // a token cut by the chunk boundary.
      const lastOpen = buffer.lastIndexOf('<');
      const keepFrom =
        lastOpen >= pos && buffer.length - lastOpen <= MAX_PENDING_ELEMENT_CHARS
          ? lastOpen
          : buffer.length;
      this.buffer = final ? '' : buffer.slice(keepFrom);
    }
    return events;
  }
}

/**
 * Parse a GPX XML string into structured tracks.
 *
 * Points without a valid lat/lon are skipped. Points without <time> are
 * still returned (caller may drop them) — the time field is undefined.
 */
export function parseGpx(xml: string): GpxParseResult {
  const tokenizer = new GpxTokenizer();
  return collect([...tokenizer.push(xml), ...tokenizer.end()]);
}

/** Assemble tokenizer events into the structured result. */
export function collect(events: Iterable<GpxEvent>): GpxParseResult {
  const tracks: GpxTrack[] = [];
  let totalPoints = 0;
  let firstTime: Date | undefined;
  let lastTime: Date | undefined;
  let current: GpxTrack | undefined;

  for (const event of events) {
    if (event.type === 'track') {
      current = { name: undefined, points: [] };
      tracks.push(current);
    } else if (event.type === 'name') {
      if (current) current.name = event.name;
    } else if (current) {
      current.points.push(event.point);
      totalPoints++;
      const time = event.point.time;
      if (time) {
        if (!firstTime || time < firstTime) firstTime = time;
        if (!lastTime || time > lastTime) lastTime = time;
      }
    }
  }

  return { tracks, totalPoints, firstTime, lastTime };
}
