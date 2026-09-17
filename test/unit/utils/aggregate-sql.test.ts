import { expect } from 'chai';
import {
  firstSql,
  lastSql,
  middleIndexSql,
} from '../../../src/utils/aggregate-sql';

describe('aggregate-sql', () => {
  it('orders first and last by the timestamp column', () => {
    expect(firstSql('TRY_CAST(value AS DOUBLE)', 'signalk_timestamp')).to.equal(
      'FIRST(TRY_CAST(value AS DOUBLE) ORDER BY signalk_timestamp)'
    );
    expect(lastSql('value_avg', 'bucket_time')).to.equal(
      'LAST(value_avg ORDER BY bucket_time)'
    );
  });

  it('orders middle_index by the timestamp column', () => {
    expect(middleIndexSql('value', 'signalk_timestamp')).to.equal(
      'list(value ORDER BY signalk_timestamp)[(count(*) + 1) // 2]'
    );
  });
});
