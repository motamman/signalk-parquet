/**
 * Unit tests for filesystem path helpers: the special-directory skip list and
 * the context/path to file-path translations used by the legacy flat layout.
 */
import { expect } from 'chai';
import * as path from 'path';
import { Context } from '@signalk/server-api';
import {
  SPECIAL_DIRECTORIES,
  shouldSkipDirectory,
  toContextFilePath,
  toParquetFilePath,
} from '../../../src/utils/path-helpers';

describe('shouldSkipDirectory', () => {
  it('skips every special directory', () => {
    for (const dir of SPECIAL_DIRECTORIES) {
      expect(shouldSkipDirectory(dir), dir).to.equal(true);
    }
  });

  it('covers the expected special directory names', () => {
    expect([...SPECIAL_DIRECTORIES].sort()).to.deep.equal([
      'claude-schemas',
      'failed',
      'processed',
      'quarantine',
      'repaired',
    ]);
  });

  it('does not skip regular data directories', () => {
    expect(shouldSkipDirectory('navigation')).to.equal(false);
    expect(shouldSkipDirectory('vessels')).to.equal(false);
  });

  it('is case-sensitive', () => {
    expect(shouldSkipDirectory('Processed')).to.equal(false);
  });
});

describe('toContextFilePath', () => {
  it('converts dots to path separators', () => {
    expect(toContextFilePath('vessels.self' as Context)).to.equal(
      'vessels/self'
    );
  });

  it('converts colons in URN contexts to underscores', () => {
    // Input:  vessels.urn:mrn:signalk:uuid:xxx
    // Output: vessels/urn_mrn_signalk_uuid_xxx
    expect(
      toContextFilePath('vessels.urn:mrn:signalk:uuid:xxx' as Context)
    ).to.equal('vessels/urn_mrn_signalk_uuid_xxx');
  });
});

describe('toParquetFilePath', () => {
  it('joins data dir, context path, and dotted path into a glob', () => {
    expect(
      toParquetFilePath('/data', 'vessels/self', 'navigation.position')
    ).to.equal(
      path.join(
        '/data',
        'vessels',
        'self',
        'navigation',
        'position',
        '*.parquet'
      )
    );
  });

  it('keeps single-segment paths intact', () => {
    expect(toParquetFilePath('/data', 'vessels/self', 'name')).to.equal(
      path.join('/data', 'vessels', 'self', 'name', '*.parquet')
    );
  });
});
