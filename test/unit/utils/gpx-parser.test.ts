/**
 * Unit tests for the regex-based GPX track parser. The parser feeds historic
 * track imports into the parquet store, so these tests pin coordinate/child
 * extraction, namespace tolerance, time handling (all times are parsed from
 * ISO strings with a Z suffix, so assertions via toISOString() are timezone
 * independent), and the exact behaviour of the regex edge cases.
 */
import { expect } from 'chai';
import { parseGpx } from '../../../src/utils/gpx-parser';

describe('parseGpx', () => {
  describe('minimal plain GPX', () => {
    const xml = `<?xml version="1.0" encoding="UTF-8"?>
<gpx version="1.1" creator="unit-test">
  <trk>
    <name>  Morning Sail  </name>
    <trkseg>
      <trkpt lat="47.5" lon="8.7">
        <ele>412.5</ele>
        <time>2024-06-01T10:15:30Z</time>
        <speed>5.14</speed>
        <course>180.0</course>
      </trkpt>
      <trkpt lat="47.501" lon="8.701">
        <time>2024-06-01T10:15:40Z</time>
      </trkpt>
      <trkpt lat="-33.85" lon="151.21">
        <time>2024-06-01T10:15:50Z</time>
      </trkpt>
    </trkseg>
  </trk>
</gpx>`;

    it('parses one track with all points in document order', () => {
      const result = parseGpx(xml);

      expect(result.tracks.length).to.equal(1);
      expect(result.totalPoints).to.equal(3);
      const coords = result.tracks[0].points.map(p => [
        p.latitude,
        p.longitude,
      ]);
      expect(coords).to.deep.equal([
        [47.5, 8.7],
        [47.501, 8.701],
        [-33.85, 151.21],
      ]);
    });

    it('extracts the trimmed track name', () => {
      const result = parseGpx(xml);
      expect(result.tracks[0].name).to.equal('Morning Sail');
    });

    it('parses time, elevation, speed and course from point children', () => {
      const first = parseGpx(xml).tracks[0].points[0];

      expect(first.time?.toISOString()).to.equal('2024-06-01T10:15:30.000Z');
      expect(first.elevation).to.equal(412.5);
      expect(first.speedMs).to.equal(5.14);
      expect(first.courseDeg).to.equal(180);
    });

    it('leaves optional children undefined when absent', () => {
      const second = parseGpx(xml).tracks[0].points[1];

      expect(second.elevation).to.equal(undefined);
      expect(second.speedMs).to.equal(undefined);
      expect(second.courseDeg).to.equal(undefined);
    });

    it('computes firstTime and lastTime from the points', () => {
      const result = parseGpx(xml);

      expect(result.firstTime?.toISOString()).to.equal(
        '2024-06-01T10:15:30.000Z'
      );
      expect(result.lastTime?.toISOString()).to.equal(
        '2024-06-01T10:15:50.000Z'
      );
    });
  });

  describe('time handling', () => {
    const point = (timeTag: string): string =>
      `<gpx><trk><trkseg><trkpt lat="1.5" lon="2.5">${timeTag}</trkpt></trkseg></trk></gpx>`;

    it('includes a point without <time>, leaving time undefined', () => {
      const result = parseGpx(point(''));

      expect(result.totalPoints).to.equal(1);
      expect(result.tracks[0].points[0].time).to.equal(undefined);
      expect(result.firstTime).to.equal(undefined);
      expect(result.lastTime).to.equal(undefined);
    });

    it('keeps the point but drops an unparseable time string', () => {
      // new Date('not-a-timestamp') is an Invalid Date; the parser guards
      // with isNaN(getTime()) and leaves time undefined instead of storing it.
      const result = parseGpx(point('<time>not-a-timestamp</time>'));

      expect(result.totalPoints).to.equal(1);
      expect(result.tracks[0].points[0].time).to.equal(undefined);
      expect(result.firstTime).to.equal(undefined);
    });

    it('treats an empty <time></time> as no time', () => {
      const result = parseGpx(point('<time></time>'));

      expect(result.totalPoints).to.equal(1);
      expect(result.tracks[0].points[0].time).to.equal(undefined);
    });

    it('parses fractional-second timestamps', () => {
      const result = parseGpx(point('<time>2024-06-01T10:15:30.250Z</time>'));

      expect(result.tracks[0].points[0].time?.toISOString()).to.equal(
        '2024-06-01T10:15:30.250Z'
      );
    });

    it('computes firstTime/lastTime as min/max even when points are out of order', () => {
      const xml = `<gpx><trk><trkseg>
        <trkpt lat="1" lon="1"><time>2024-06-01T12:00:00Z</time></trkpt>
        <trkpt lat="2" lon="2"><time>2024-06-01T08:00:00Z</time></trkpt>
        <trkpt lat="3" lon="3"><time>2024-06-01T10:00:00Z</time></trkpt>
      </trkseg></trk></gpx>`;
      const result = parseGpx(xml);

      // Document order is preserved in the points array...
      expect(result.tracks[0].points.map(p => p.latitude)).to.deep.equal([
        1, 2, 3,
      ]);
      // ...while the time range is the true min/max.
      expect(result.firstTime?.toISOString()).to.equal(
        '2024-06-01T08:00:00.000Z'
      );
      expect(result.lastTime?.toISOString()).to.equal(
        '2024-06-01T12:00:00.000Z'
      );
    });
  });

  describe('point filtering', () => {
    it('skips points missing the lon attribute', () => {
      const xml = `<gpx><trk><trkseg>
        <trkpt lat="47.5"><time>2024-06-01T10:00:00Z</time></trkpt>
        <trkpt lat="47.6" lon="8.7"><time>2024-06-01T10:00:10Z</time></trkpt>
      </trkseg></trk></gpx>`;
      const result = parseGpx(xml);

      expect(result.totalPoints).to.equal(1);
      expect(result.tracks[0].points[0].latitude).to.equal(47.6);
    });

    it('skips points with a non-numeric lat attribute', () => {
      const xml = `<gpx><trk><trkseg>
        <trkpt lat="abc" lon="8.7"></trkpt>
        <trkpt lat="47.6" lon="8.7"></trkpt>
      </trkseg></trk></gpx>`;

      expect(parseGpx(xml).totalPoints).to.equal(1);
    });

    it('keeps a point at exactly 0/0 (zero is a valid coordinate)', () => {
      const xml =
        '<gpx><trk><trkseg><trkpt lat="0" lon="0"></trkpt></trkseg></trk></gpx>';
      const result = parseGpx(xml);

      expect(result.totalPoints).to.equal(1);
      expect(result.tracks[0].points[0].latitude).to.equal(0);
      expect(result.tracks[0].points[0].longitude).to.equal(0);
    });

    it('skips points using single-quoted attributes (only double quotes are matched)', () => {
      // extractAttr's regex is lat\s*=\s*"..." — single-quoted attributes do
      // not match, so lat/lon come back undefined and the point is dropped.
      const xml = `<gpx><trk><trkseg><trkpt lat='47.5' lon='8.7'></trkpt></trkseg></trk></gpx>`;

      expect(parseGpx(xml).totalPoints).to.equal(0);
    });

    it('ignores an invalid elevation value but keeps the point', () => {
      const xml = `<gpx><trk><trkseg>
        <trkpt lat="47.5" lon="8.7"><ele>n/a</ele></trkpt>
      </trkseg></trk></gpx>`;
      const result = parseGpx(xml);

      expect(result.totalPoints).to.equal(1);
      expect(result.tracks[0].points[0].elevation).to.equal(undefined);
    });
  });

  describe('namespace-prefixed tags', () => {
    // All four regexes (trk, trkpt, name, child tags) accept an optional
    // `[\w-]+:` prefix on both the opening and closing tag.
    // Input:  <ns3:trkpt lat="47.5" lon="8.7">
    //           <ns3:time>2024-06-01T10:15:30Z</ns3:time>
    //         </ns3:trkpt>
    // Output: { latitude: 47.5, longitude: 8.7, time: 2024-06-01T10:15:30.000Z }
    it('parses ns3-prefixed track, name, point and child tags', () => {
      const xml = `<ns3:gpx xmlns:ns3="http://www.topografix.com/GPX/1/1">
        <ns3:trk>
          <ns3:name>Prefixed</ns3:name>
          <ns3:trkseg>
            <ns3:trkpt lat="47.5" lon="8.7">
              <ns3:ele>412.5</ns3:ele>
              <ns3:time>2024-06-01T10:15:30Z</ns3:time>
            </ns3:trkpt>
          </ns3:trkseg>
        </ns3:trk>
      </ns3:gpx>`;
      const result = parseGpx(xml);

      expect(result.tracks.length).to.equal(1);
      expect(result.tracks[0].name).to.equal('Prefixed');
      expect(result.totalPoints).to.equal(1);
      const pt = result.tracks[0].points[0];
      expect(pt.latitude).to.equal(47.5);
      expect(pt.longitude).to.equal(8.7);
      expect(pt.elevation).to.equal(412.5);
      expect(pt.time?.toISOString()).to.equal('2024-06-01T10:15:30.000Z');
    });

    it('parses hyphenated prefixes such as gpx-ext:', () => {
      // The prefix character class is [\w-], so hyphens are allowed.
      const xml = `<gpx><gpx-ext:trk><gpx-ext:trkseg>
        <gpx-ext:trkpt lat="1" lon="2"></gpx-ext:trkpt>
      </gpx-ext:trkseg></gpx-ext:trk></gpx>`;

      expect(parseGpx(xml).totalPoints).to.equal(1);
    });
  });

  describe('multiple <trk> blocks', () => {
    const xml = `<gpx>
      <trk>
        <name>Leg 1</name>
        <trkseg>
          <trkpt lat="1" lon="1"><time>2024-06-02T10:00:00Z</time></trkpt>
          <trkpt lat="2" lon="2"><time>2024-06-02T11:00:00Z</time></trkpt>
        </trkseg>
      </trk>
      <trk>
        <trkseg>
          <trkpt lat="3" lon="3"><time>2024-06-01T09:00:00Z</time></trkpt>
        </trkseg>
      </trk>
    </gpx>`;

    it('produces one GpxTrack per <trk> with points assigned correctly', () => {
      const result = parseGpx(xml);

      expect(result.tracks.length).to.equal(2);
      expect(result.tracks[0].name).to.equal('Leg 1');
      expect(result.tracks[0].points.length).to.equal(2);
      expect(result.tracks[1].name).to.equal(undefined);
      expect(result.tracks[1].points.length).to.equal(1);
      expect(result.totalPoints).to.equal(3);
    });

    it('computes firstTime/lastTime across all tracks', () => {
      const result = parseGpx(xml);

      // The earliest point lives in the second track.
      expect(result.firstTime?.toISOString()).to.equal(
        '2024-06-01T09:00:00.000Z'
      );
      expect(result.lastTime?.toISOString()).to.equal(
        '2024-06-02T11:00:00.000Z'
      );
    });
  });

  describe('self-closing trkpt', () => {
    it('parses self-closing points (no children, time undefined)', () => {
      const xml = `<gpx><trk><trkseg>
        <trkpt lat="1" lon="2"/>
        <trkpt lat="3" lon="4" />
      </trkseg></trk></gpx>`;
      const result = parseGpx(xml);

      expect(result.totalPoints).to.equal(2);
      expect(
        result.tracks[0].points.map(p => [p.latitude, p.longitude])
      ).to.deep.equal([
        [1, 2],
        [3, 4],
      ]);
      expect(result.tracks[0].points[0].time).to.equal(undefined);
    });

    it('merges a self-closing point into the following paired point (documents current behaviour)', () => {
      // QUIRK: TRKPT_RE tries the paired-tag alternative first. For a
      // self-closing trkpt FOLLOWED by a paired trkpt, `<trkpt\b([^>]*)>`
      // consumes the trailing "/" into the attribute group and the lazy inner
      // match runs to the next </trkpt>, swallowing the second point.
      // Input:
      //   <trkpt lat="1" lon="2"/>
      //   <trkpt lat="3" lon="4"><time>2024-01-01T00:00:00Z</time></trkpt>
      // Output: ONE point with the first point's coordinates and the second
      // point's time. Real exports use a single style throughout, so this
      // only bites on hand-mixed files.
      const xml = `<gpx><trk><trkseg>
        <trkpt lat="1" lon="2"/>
        <trkpt lat="3" lon="4"><time>2024-01-01T00:00:00Z</time></trkpt>
      </trkseg></trk></gpx>`;
      const result = parseGpx(xml);

      expect(result.totalPoints).to.equal(1);
      const pt = result.tracks[0].points[0];
      expect(pt.latitude).to.equal(1);
      expect(pt.longitude).to.equal(2);
      expect(pt.time?.toISOString()).to.equal('2024-01-01T00:00:00.000Z');
    });
  });

  describe('malformed or empty input', () => {
    const emptyShape = {
      tracks: [],
      totalPoints: 0,
      firstTime: undefined,
      lastTime: undefined,
    };

    it('returns an empty result for the empty string', () => {
      expect(parseGpx('')).to.deep.equal(emptyShape);
    });

    it('returns an empty result for non-XML garbage', () => {
      expect(parseGpx('this is { not XML ] at all >>>')).to.deep.equal(
        emptyShape
      );
    });

    it('returns an empty result for XML without any <trk>', () => {
      const xml =
        '<gpx><wpt lat="47.5" lon="8.7"><name>Buoy</name></wpt></gpx>';

      expect(parseGpx(xml)).to.deep.equal(emptyShape);
    });

    it('ignores trkpt elements outside a <trk> block', () => {
      const xml = `<gpx>
        <trkpt lat="9" lon="9"><time>2024-06-01T00:00:00Z</time></trkpt>
        <trk><trkseg><trkpt lat="1" lon="1"></trkpt></trkseg></trk>
      </gpx>`;
      const result = parseGpx(xml);

      expect(result.totalPoints).to.equal(1);
      expect(result.tracks[0].points[0].latitude).to.equal(1);
    });
  });

  describe('real-world export', () => {
    // Black-box use case: a short evening sail on Lake Zurich recorded by a
    // Garmin device whose Connect export (JAXB serializer) prefixes every
    // element with ns3:. The file carries an XML declaration, metadata with
    // its own <time> (which must not leak into the track range), and one
    // track segment with elevation/speed/course per point.
    const xml = `<?xml version="1.0" encoding="UTF-8"?>
<ns3:gpx xmlns:ns3="http://www.topografix.com/GPX/1/1" version="1.1" creator="Garmin Connect">
  <ns3:metadata>
    <ns3:time>2024-08-10T12:59:00.000Z</ns3:time>
  </ns3:metadata>
  <ns3:trk>
    <ns3:name>Lake Zurich evening sail</ns3:name>
    <ns3:type>sailing</ns3:type>
    <ns3:trkseg>
      <ns3:trkpt lat="47.3438" lon="8.5573">
        <ns3:ele>406.0</ns3:ele>
        <ns3:time>2024-08-10T13:00:05.000Z</ns3:time>
        <ns3:speed>2.57</ns3:speed>
        <ns3:course>184.5</ns3:course>
      </ns3:trkpt>
      <ns3:trkpt lat="47.3431" lon="8.5570">
        <ns3:ele>406.2</ns3:ele>
        <ns3:time>2024-08-10T13:00:35.000Z</ns3:time>
        <ns3:speed>2.83</ns3:speed>
        <ns3:course>189.0</ns3:course>
      </ns3:trkpt>
      <ns3:trkpt lat="47.3424" lon="8.5566">
        <ns3:ele>406.1</ns3:ele>
        <ns3:time>2024-08-10T13:01:05.000Z</ns3:time>
        <ns3:speed>3.09</ns3:speed>
        <ns3:course>191.2</ns3:course>
      </ns3:trkpt>
    </ns3:trkseg>
  </ns3:trk>
</ns3:gpx>`;

    it('parses the full Garmin-style track', () => {
      const result = parseGpx(xml);

      expect(result.tracks.length).to.equal(1);
      expect(result.tracks[0].name).to.equal('Lake Zurich evening sail');
      expect(result.totalPoints).to.equal(3);

      const first = result.tracks[0].points[0];
      expect(first.latitude).to.equal(47.3438);
      expect(first.longitude).to.equal(8.5573);
      expect(first.elevation).to.equal(406.0);
      expect(first.speedMs).to.equal(2.57);
      expect(first.courseDeg).to.equal(184.5);

      // The metadata <time> sits outside the track and must not widen the
      // range below the first point's timestamp.
      expect(result.firstTime?.toISOString()).to.equal(
        '2024-08-10T13:00:05.000Z'
      );
      expect(result.lastTime?.toISOString()).to.equal(
        '2024-08-10T13:01:05.000Z'
      );
    });
  });
});
