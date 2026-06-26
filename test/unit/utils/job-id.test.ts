/**
 * Unit tests for the shared background-job id helper.
 */
import { expect } from 'chai';
import { newJobId } from '../../../src/utils/job-id';

describe('newJobId', () => {
  it('prefixes the id with the given prefix', () => {
    expect(newJobId('val')).to.match(/^val_/);
    expect(newJobId('posagg')).to.match(/^posagg_/);
  });

  it('produces the prefix_millis_random shape', () => {
    expect(newJobId('rep')).to.match(/^rep_\d+_[a-z0-9]+$/);
  });

  it('produces distinct ids on successive calls', () => {
    const ids = new Set(Array.from({ length: 100 }, () => newJobId('x')));
    expect(ids.size).to.equal(100);
  });
});
