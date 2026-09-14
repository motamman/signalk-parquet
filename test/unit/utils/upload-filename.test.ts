/**
 * Unit tests for the staged upload filename (#68): unique within a request,
 * readable, and confined to the staging directory.
 */
import { expect } from 'chai';
import { uploadFilename } from '../../../src/utils/upload-filename';

describe('uploadFilename', () => {
  const fixed = (s: string) => () => s;

  it('keeps a sanitised stem and extension and appends a six-character suffix', () => {
    expect(uploadFilename('nav 1.gpx', fixed('k3f9a1zz'))).to.equal(
      'nav_1_k3f9a1.gpx'
    );
  });

  it('gives colliding sanitised names distinct results', () => {
    // Use case: 'nav 1.gpx' and 'nav@1.gpx' both sanitise to 'nav_1.gpx'.
    const a = uploadFilename('nav 1.gpx');
    const b = uploadFilename('nav@1.gpx');
    expect(a).to.match(/^nav_1_[a-z0-9]{6}\.gpx$/);
    expect(b).to.match(/^nav_1_[a-z0-9]{6}\.gpx$/);
    expect(a).to.not.equal(b);
  });

  it('drops directory components so the name cannot leave the staging dir', () => {
    expect(uploadFilename('../../etc/x.gpx', fixed('8b2c0d'))).to.equal(
      'x_8b2c0d.gpx'
    );
    expect(uploadFilename('C:\\tracks\\x.gpx', fixed('8b2c0d'))).to.match(
      /_8b2c0d\.gpx$/
    );
  });

  it('falls back to a generic stem and .gpx when the name has neither', () => {
    expect(uploadFilename('.gpx', fixed('2f7e11'))).to.equal(
      'upload_2f7e11.gpx'
    );
    expect(uploadFilename('track', fixed('2f7e11'))).to.equal(
      'track_2f7e11.gpx'
    );
  });

  it('pads a short random string until the suffix is full', () => {
    let calls = 0;
    const short = () => {
      calls++;
      return 'ab';
    };
    expect(uploadFilename('t.gpx', short)).to.equal('t_ababab.gpx');
    expect(calls).to.equal(3);
  });
});
