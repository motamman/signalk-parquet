/**
 * Unit tests for the SQL string literal escaping helper. Everything the query
 * builders splice into single-quoted SQL literals goes through this function,
 * so the doubling behaviour must hold for every quote position and count.
 */
import { expect } from 'chai';
import { escapeSqlString } from '../../../src/utils/sql-escape';

describe('escapeSqlString', () => {
  it('returns strings without quotes unchanged', () => {
    expect(escapeSqlString('vessels.self')).to.equal('vessels.self');
  });

  it('returns the empty string unchanged', () => {
    expect(escapeSqlString('')).to.equal('');
  });

  it('doubles a single embedded quote', () => {
    expect(escapeSqlString("O'Brien")).to.equal("O''Brien");
  });

  it('doubles every quote when several are present', () => {
    expect(escapeSqlString("It's a 'test'")).to.equal("It''s a ''test''");
  });

  it('doubles consecutive quotes individually', () => {
    expect(escapeSqlString("'''")).to.equal("''''''");
  });

  it('handles quotes at the start and end of the string', () => {
    expect(escapeSqlString("'wrapped'")).to.equal("''wrapped''");
  });

  it('leaves backslashes and double quotes alone', () => {
    expect(escapeSqlString('a\\b"c')).to.equal('a\\b"c');
  });
});
