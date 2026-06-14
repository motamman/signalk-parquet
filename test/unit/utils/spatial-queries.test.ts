/**
 * Unit tests for the History API spatial filter layer: query-parameter
 * parsing, DuckDB WHERE-clause generation, in-memory buffer filtering, and
 * the position-path predicate.
 */
import { expect } from 'chai';
import {
  parseBboxParam,
  parseRadiusParam,
  parseSpatialParams,
  buildSpatialSqlClause,
  filterBufferRecordsSpatially,
  isPositionPath,
  SpatialFilter,
} from '../../../src/utils/spatial-queries';
import { DataRecord } from '../../../src/types';

function makeRecord(extra: Partial<DataRecord>): DataRecord {
  return {
    received_timestamp: '2026-06-01T10:00:00.000Z',
    signalk_timestamp: '2026-06-01T10:00:00.000Z',
    context: 'vessels.self',
    path: 'navigation.position',
    value: null,
    ...extra,
  };
}

describe('parseBboxParam', () => {
  it('parses west,south,east,north', () => {
    expect(parseBboxParam('8,47,9,48')).to.deep.equal({
      west: 8,
      south: 47,
      east: 9,
      north: 48,
    });
  });

  it('tolerates whitespace around components', () => {
    expect(parseBboxParam(' 8 , 47 , 9 , 48 ')).to.deep.equal({
      west: 8,
      south: 47,
      east: 9,
      north: 48,
    });
  });

  it('accepts west greater than east for antimeridian boxes', () => {
    expect(parseBboxParam('170,-20,-170,-10')).to.deep.equal({
      west: 170,
      south: -20,
      east: -170,
      north: -10,
    });
  });

  it('accepts the full coordinate extremes', () => {
    expect(parseBboxParam('-180,-90,180,90')).to.deep.equal({
      west: -180,
      south: -90,
      east: 180,
      north: 90,
    });
  });

  const invalid: Array<[string, string]> = [
    ['empty string', ''],
    ['three components', '8,47,9'],
    ['five components', '8,47,9,48,1'],
    ['non-numeric component', '8,abc,9,48'],
    ['south above north', '8,49,9,48'],
    ['south below -90', '8,-91,9,48'],
    ['north above 90', '8,47,9,91'],
    ['west below -180', '-181,47,9,48'],
    ['east above 180', '8,47,181,48'],
  ];
  invalid.forEach(([label, input]) => {
    it(`returns null for ${label}`, () => {
      expect(parseBboxParam(input)).to.equal(null);
    });
  });
});

describe('parseRadiusParam', () => {
  it('parses lon,lat,meters in GeoJSON order', () => {
    const filter = parseRadiusParam('8.5,47.4,1000');
    expect(filter).to.not.equal(null);
    expect(filter!.type).to.equal('radius');
    expect(filter!.centerLon).to.equal(8.5);
    expect(filter!.centerLat).to.equal(47.4);
    expect(filter!.radiusMeters).to.equal(1000);
  });

  it('derives a bounding box that contains the center', () => {
    const filter = parseRadiusParam('8.5,47.4,1000')!;
    expect(filter.bbox.south).to.be.below(47.4);
    expect(filter.bbox.north).to.be.above(47.4);
    expect(filter.bbox.west).to.be.below(8.5);
    expect(filter.bbox.east).to.be.above(8.5);
  });

  const invalid: Array<[string, string]> = [
    ['empty string', ''],
    ['two components', '8.5,47.4'],
    ['four components', '8.5,47.4,1000,5'],
    ['non-numeric component', '8.5,x,1000'],
    ['latitude out of range', '8.5,91,1000'],
    ['longitude out of range', '181,47.4,1000'],
    ['zero radius', '8.5,47.4,0'],
    ['negative radius', '8.5,47.4,-10'],
  ];
  invalid.forEach(([label, input]) => {
    it(`returns null for ${label}`, () => {
      expect(parseRadiusParam(input)).to.equal(null);
    });
  });
});

describe('parseSpatialParams', () => {
  it('returns null when neither parameter is given', () => {
    expect(parseSpatialParams(undefined, undefined)).to.equal(null);
  });

  it('builds a bbox filter from bbox only', () => {
    const filter = parseSpatialParams('8,47,9,48', undefined);
    expect(filter).to.deep.equal({
      type: 'bbox',
      bbox: { west: 8, south: 47, east: 9, north: 48 },
    });
  });

  it('builds a radius filter from radius only', () => {
    const filter = parseSpatialParams(undefined, '8.5,47.4,1000');
    expect(filter!.type).to.equal('radius');
  });

  it('prefers radius when both are present', () => {
    const filter = parseSpatialParams('8,47,9,48', '8.5,47.4,1000');
    expect(filter!.type).to.equal('radius');
  });

  it('returns null when an invalid radius shadows a valid bbox', () => {
    // Radius takes precedence even when it fails to parse; the bbox is not
    // used as a fallback.
    expect(parseSpatialParams('8,47,9,48', 'garbage')).to.equal(null);
  });

  it('returns null for an invalid bbox alone', () => {
    expect(parseSpatialParams('garbage', undefined)).to.equal(null);
  });
});

describe('buildSpatialSqlClause', () => {
  const bboxFilter: SpatialFilter = {
    type: 'bbox',
    bbox: { west: 8, south: 47, east: 9, north: 48 },
  };

  it('emits inclusive latitude and longitude bounds', () => {
    const sql = buildSpatialSqlClause(bboxFilter);
    expect(sql).to.include('TRY_CAST(value_latitude AS DOUBLE) >= 47');
    expect(sql).to.include('TRY_CAST(value_latitude AS DOUBLE) <= 48');
    expect(sql).to.include('TRY_CAST(value_longitude AS DOUBLE) >= 8');
    expect(sql).to.include('TRY_CAST(value_longitude AS DOUBLE) <= 9');
    expect(sql).to.not.include('ST_Distance_Spheroid');
  });

  it('uses OR for boxes crossing the antimeridian', () => {
    const sql = buildSpatialSqlClause({
      type: 'bbox',
      bbox: { west: 170, south: -20, east: -170, north: -10 },
    });
    expect(sql).to.include(
      '(TRY_CAST(value_longitude AS DOUBLE) >= 170 OR TRY_CAST(value_longitude AS DOUBLE) <= -170)'
    );
  });

  it('honours custom column expressions', () => {
    const sql = buildSpatialSqlClause(bboxFilter, 'lat', 'lon');
    expect(sql).to.include('lat >= 47');
    expect(sql).to.include('lon >= 8');
  });

  it('adds a precise spheroid distance check for radius filters', () => {
    const sql = buildSpatialSqlClause({
      type: 'radius',
      bbox: { west: 8.4, south: 47.3, east: 8.6, north: 47.5 },
      centerLat: 47.4,
      centerLon: 8.5,
      radiusMeters: 1000,
    });
    expect(sql).to.include('ST_Distance_Spheroid');
    expect(sql).to.include('ST_Point(8.5, 47.4)');
    expect(sql).to.include('<= 1000');
  });

  it('falls back to bbox-only SQL when radius metadata is missing', () => {
    const sql = buildSpatialSqlClause({
      type: 'radius',
      bbox: { west: 8.4, south: 47.3, east: 8.6, north: 47.5 },
    });
    expect(sql).to.not.include('ST_Distance_Spheroid');
  });
});

describe('filterBufferRecordsSpatially', () => {
  const bboxFilter: SpatialFilter = {
    type: 'bbox',
    bbox: { west: 8, south: 47, east: 9, north: 48 },
  };

  it('keeps records inside and drops records outside the bbox', () => {
    const inside = makeRecord({ value_latitude: 47.5, value_longitude: 8.5 });
    const outside = makeRecord({ value_latitude: 50, value_longitude: 8.5 });
    const result = filterBufferRecordsSpatially([inside, outside], bboxFilter);
    expect(result).to.deep.equal([inside]);
  });

  it('reads positions from a value object', () => {
    const record = makeRecord({ value: { latitude: 47.5, longitude: 8.5 } });
    expect(filterBufferRecordsSpatially([record], bboxFilter)).to.have.lengthOf(
      1
    );
  });

  it('reads positions from a value_json object', () => {
    const record = makeRecord({
      value_json: { latitude: 50, longitude: 8.5 },
    });
    expect(filterBufferRecordsSpatially([record], bboxFilter)).to.have.lengthOf(
      0
    );
  });

  it('reads positions from a value_json string', () => {
    const record = makeRecord({
      value_json: JSON.stringify({ latitude: 47.5, longitude: 8.5 }),
    });
    expect(filterBufferRecordsSpatially([record], bboxFilter)).to.have.lengthOf(
      1
    );
  });

  it('keeps records without position data', () => {
    const record = makeRecord({ value: 12.3, path: 'environment.depth' });
    expect(filterBufferRecordsSpatially([record], bboxFilter)).to.have.lengthOf(
      1
    );
  });

  it('keeps records with unparseable value_json', () => {
    const record = makeRecord({ value_json: 'not json' });
    expect(filterBufferRecordsSpatially([record], bboxFilter)).to.have.lengthOf(
      1
    );
  });

  it('applies the precise distance check for radius filters', () => {
    // The bbox around a circle includes its corners; a point tucked into the
    // corner is inside the bbox but outside the circle and must be dropped.
    const filter: SpatialFilter = {
      type: 'radius',
      bbox: { west: 8.4, south: 47.31, east: 8.6, north: 47.49 },
      centerLat: 47.4,
      centerLon: 8.5,
      radiusMeters: 10_000,
    };
    const nearCenter = makeRecord({
      value_latitude: 47.41,
      value_longitude: 8.51,
    });
    const inCorner = makeRecord({
      value_latitude: 47.489,
      value_longitude: 8.599,
    });
    const result = filterBufferRecordsSpatially([nearCenter, inCorner], filter);
    expect(result).to.deep.equal([nearCenter]);
  });
});

describe('isPositionPath', () => {
  const positives = [
    'navigation.position',
    'NAVIGATION.POSITION',
    'navigation.gnss.antennaPosition',
    'navigation.anchor.position',
    'navigation.destination.waypoint.position',
    'sensors.ais.target.position',
    'navigation.position.value',
  ];
  positives.forEach(path => {
    it(`recognizes ${path}`, () => {
      expect(isPositionPath(path)).to.equal(true);
    });
  });

  const negatives = [
    'navigation.speedOverGround',
    'environment.wind.angleApparent',
    'position',
    'navigation.positioning',
  ];
  negatives.forEach(path => {
    it(`rejects ${path}`, () => {
      expect(isPositionPath(path)).to.equal(false);
    });
  });
});

describe('parseBboxParam coordinate boundaries', () => {
  // Each coordinate is validated independently; these pin the exact inclusive
  // limits (-90/90 latitude, -180/180 longitude) so a comparison operator that
  // drifts by one (< vs <=) changes a result.
  it('accepts the southern limit -90', () => {
    expect(parseBboxParam('0,-90,1,0')).to.not.equal(null);
  });

  it('rejects just below the southern limit', () => {
    expect(parseBboxParam('0,-90.001,1,0')).to.equal(null);
  });

  it('accepts the northern limit 90', () => {
    expect(parseBboxParam('0,0,1,90')).to.not.equal(null);
  });

  it('rejects just above the northern limit', () => {
    expect(parseBboxParam('0,0,1,90.001')).to.equal(null);
  });

  it('accepts the western limit -180', () => {
    expect(parseBboxParam('-180,0,1,1')).to.not.equal(null);
  });

  it('rejects just below the western limit', () => {
    expect(parseBboxParam('-180.001,0,1,1')).to.equal(null);
  });

  it('accepts the eastern limit 180', () => {
    expect(parseBboxParam('0,0,180,1')).to.not.equal(null);
  });

  it('rejects just above the eastern limit', () => {
    expect(parseBboxParam('0,0,180.001,1')).to.equal(null);
  });

  it('accepts a zero-height box where south equals north', () => {
    expect(parseBboxParam('0,5,1,5')).to.not.equal(null);
  });

  it('rejects when south is just above north', () => {
    expect(parseBboxParam('0,5.001,1,5')).to.equal(null);
  });
});

describe('parseRadiusParam coordinate boundaries', () => {
  it('accepts latitude at +/-90 and longitude at +/-180', () => {
    expect(parseRadiusParam('0,90,100')).to.not.equal(null);
    expect(parseRadiusParam('0,-90,100')).to.not.equal(null);
    expect(parseRadiusParam('180,0,100')).to.not.equal(null);
    expect(parseRadiusParam('-180,0,100')).to.not.equal(null);
  });

  it('rejects latitude just outside +/-90', () => {
    expect(parseRadiusParam('0,90.001,100')).to.equal(null);
    expect(parseRadiusParam('0,-90.001,100')).to.equal(null);
  });

  it('rejects longitude just outside +/-180', () => {
    expect(parseRadiusParam('180.001,0,100')).to.equal(null);
  });

  it('accepts the smallest positive radius', () => {
    expect(parseRadiusParam('0,0,0.001')).to.not.equal(null);
  });
});

describe('buildSpatialSqlClause branch selection', () => {
  it('uses the AND (non-wrap) form when west equals east', () => {
    // west <= east must take the non-wrap branch; a strict < would wrongly
    // switch to the antimeridian OR form for a zero-width box.
    const sql = buildSpatialSqlClause({
      type: 'bbox',
      bbox: { west: 9, south: 0, east: 9, north: 1 },
    });
    expect(sql).to.not.include(' OR ');
    expect(sql).to.include('<= 9');
  });

  it('omits the distance check for a non-radius filter even if it carries center fields', () => {
    // The distance clause is gated on type === 'radius'; a bbox filter that
    // happens to carry center/radius fields must not get a spheroid check.
    const sql = buildSpatialSqlClause({
      type: 'bbox',
      bbox: { west: 8, south: 47, east: 9, north: 48 },
      centerLat: 47.5,
      centerLon: 8.5,
      radiusMeters: 1000,
    } as SpatialFilter);
    expect(sql).to.not.include('ST_Distance_Spheroid');
  });
});

describe('filterBufferRecordsSpatially extractor branches', () => {
  // Out-of-bbox positions per storage format: a broken extractor would return
  // null and the record would be kept as "non-position", so these only pass
  // when the extractor reads the coordinate and the point is dropped.
  const bbox: SpatialFilter = {
    type: 'bbox',
    bbox: { west: 8, south: 47, east: 9, north: 48 },
  };

  it('drops an out-of-range latitude from a value object', () => {
    const r = makeRecord({ value: { latitude: 50, longitude: 8.5 } });
    expect(filterBufferRecordsSpatially([r], bbox)).to.have.lengthOf(0);
  });

  it('drops an out-of-range longitude from a value object', () => {
    const r = makeRecord({ value: { latitude: 47.5, longitude: 20 } });
    expect(filterBufferRecordsSpatially([r], bbox)).to.have.lengthOf(0);
  });

  it('drops an out-of-range latitude from a value_json string', () => {
    const r = makeRecord({
      value_json: JSON.stringify({ latitude: 50, longitude: 8.5 }),
    });
    expect(filterBufferRecordsSpatially([r], bbox)).to.have.lengthOf(0);
  });

  it('drops an out-of-range longitude from a value_json string', () => {
    const r = makeRecord({
      value_json: JSON.stringify({ latitude: 47.5, longitude: 20 }),
    });
    expect(filterBufferRecordsSpatially([r], bbox)).to.have.lengthOf(0);
  });

  it('drops an out-of-range longitude from a value_json object', () => {
    const r = makeRecord({ value_json: { latitude: 47.5, longitude: 20 } });
    expect(filterBufferRecordsSpatially([r], bbox)).to.have.lengthOf(0);
  });

  it('drops an out-of-range longitude from the value_longitude column', () => {
    const r = makeRecord({ value_latitude: 47.5, value_longitude: 20 });
    expect(filterBufferRecordsSpatially([r], bbox)).to.have.lengthOf(0);
  });

  it('does not apply a distance check for a bbox filter carrying center fields', () => {
    // A point inside the bbox but outside the would-be radius is kept, because
    // the radius branch is gated on type === 'radius'.
    const filter = {
      type: 'bbox',
      bbox: { west: 8, south: 47, east: 9, north: 48 },
      centerLat: 47.5,
      centerLon: 8.5,
      radiusMeters: 10,
    } as SpatialFilter;
    const farButInBox = makeRecord({
      value_latitude: 47.95,
      value_longitude: 8.95,
    });
    expect(
      filterBufferRecordsSpatially([farButInBox], filter)
    ).to.have.lengthOf(1);
  });
});
