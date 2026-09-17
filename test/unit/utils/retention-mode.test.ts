import { expect } from 'chai';
import {
  resolveRetentionStamp,
  retentionModeOf,
} from '../../../src/utils/retention-mode';
import {
  EXPORTED_BUFFER_ONLY,
  EXPORTED_PENDING,
} from '../../../src/utils/sqlite-buffer';
import type { PathConfig } from '../../../src/types';

const config = (over: Partial<PathConfig> = {}): PathConfig =>
  ({ path: 'navigation.rateOfTurn', ...over }) as PathConfig;

describe('retention mode', () => {
  describe('retentionModeOf', () => {
    it('defaults to full, so every existing config is unchanged', () => {
      expect(retentionModeOf(config())).to.equal('full');
      expect(retentionModeOf(config({ retention: undefined }))).to.equal('full');
      expect(retentionModeOf(config({ retention: 'full' }))).to.equal('full');
    });

    it('reads an explicit buffer mode', () => {
      expect(retentionModeOf(config({ retention: 'buffer' }))).to.equal('buffer');
    });
  });

  describe('resolveRetentionStamp', () => {
    it('stamps a full path as owing an export', () => {
      expect(resolveRetentionStamp(config(), new Set())).to.equal(
        EXPORTED_PENDING
      );
    });

    it('stamps a buffer-only path as never to be exported', () => {
      expect(
        resolveRetentionStamp(config({ retention: 'buffer' }), new Set())
      ).to.equal(EXPORTED_BUFFER_ONLY);
    });

    it('promotes a buffer-only path while its regimen is active', () => {
      const path = config({
        retention: 'buffer',
        fullWhileRegimen: 'capturePassage',
      });
      expect(resolveRetentionStamp(path, new Set(['capturePassage']))).to.equal(
        EXPORTED_PENDING
      );
      expect(resolveRetentionStamp(path, new Set(['other']))).to.equal(
        EXPORTED_BUFFER_ONLY
      );
      expect(resolveRetentionStamp(path, new Set())).to.equal(
        EXPORTED_BUFFER_ONLY
      );
    });

    it('ignores the promotion regimen on a full path', () => {
      // A full path is already exported; the regimen has nothing to promote.
      expect(
        resolveRetentionStamp(
          config({ fullWhileRegimen: 'capturePassage' }),
          new Set()
        )
      ).to.equal(EXPORTED_PENDING);
    });

    it('tolerates whitespace and an absent regimen set', () => {
      const path = config({
        retention: 'buffer',
        fullWhileRegimen: '  capturePassage  ',
      });
      expect(resolveRetentionStamp(path, new Set(['capturePassage']))).to.equal(
        EXPORTED_PENDING
      );
      expect(resolveRetentionStamp(path, undefined)).to.equal(
        EXPORTED_BUFFER_ONLY
      );
    });

    it('treats an empty promotion regimen as none', () => {
      expect(
        resolveRetentionStamp(
          config({ retention: 'buffer', fullWhileRegimen: '   ' }),
          new Set([''])
        )
      ).to.equal(EXPORTED_BUFFER_ONLY);
    });
  });
});
