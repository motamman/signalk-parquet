import { expect } from 'chai';
import { Instant, ZonedDateTime } from '@js-joda/core';
import { isoBound, isoInstant } from '../../../src/utils/iso-time';

describe('iso-time', () => {
  const midnight = ZonedDateTime.parse('2026-09-16T00:00:00Z');

  it('formats a bound with fixed milliseconds from every input type', () => {
    expect(isoBound(midnight)).to.equal('2026-09-16T00:00:00.000Z');
    expect(isoBound(midnight.toInstant())).to.equal('2026-09-16T00:00:00.000Z');
    expect(isoBound(new Date('2026-09-16T00:00:00Z'))).to.equal('2026-09-16T00:00:00.000Z');
    expect(isoBound(Instant.parse('2026-09-16T00:00:00.250Z'))).to.equal('2026-09-16T00:00:00.250Z');
  });

  it('sorts correctly against millisecond and second-precision rows', () => {
    const to = isoBound(midnight);
    // A row stamped exactly at `to` is not below it: excluded by `< to`.
    expect('2026-09-16T00:00:00.000Z' < to).to.equal(false);
    // js-joda's spelling let it in: the defect this module removes.
    expect('2026-09-16T00:00:00.000Z' < isoInstant(midnight)).to.equal(true);
    // A legacy row without milliseconds sorts above the bound, where it belongs.
    expect('2026-09-16T00:00:00Z' >= to).to.equal(true);
    // The last row of the previous day is below it.
    expect('2026-09-15T23:59:59.999Z' < to).to.equal(true);
    // Used as `from`: a row half a second in is not below it, so included.
    expect('2026-09-16T00:00:00.500Z' >= to).to.equal(true);
  });

  it('keeps the display form for response fields', () => {
    expect(isoInstant(midnight)).to.equal('2026-09-16T00:00:00Z');
    expect(isoInstant(midnight.toInstant())).to.equal('2026-09-16T00:00:00Z');
  });
});
