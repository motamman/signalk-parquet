/**
 * On-disk name for an uploaded file.
 *
 * Every file of one multipart request is staged in the same directory, so
 * the stored name has to be unique within the request as well as safe. The
 * stem is sanitised for readability in logs and a random suffix is appended
 * so two originals that sanitise to the same stem (`nav 1.gpx`, `nav@1.gpx`)
 * do not overwrite each other (#68). Directory components are dropped, so a
 * name like `../../etc/x.gpx` cannot escape the staging directory.
 *
 * Input:  'nav 1.gpx'         -> 'nav_1_k3f9a1.gpx'
 *         '../../etc/x.gpx'   -> 'x_8b2c0d.gpx'
 *         '.gpx'              -> 'upload_2f7e11.gpx'
 */
import * as path from 'path';

const SUFFIX_LENGTH = 6;

export function uploadFilename(
  originalname: string,
  random: () => string = () => Math.random().toString(36).slice(2)
): string {
  // Split on the last dot by hand: path.parse() treats a leading-dot name
  // such as '.gpx' as a dotfile with no extension.
  const base = path.basename(originalname);
  const dot = base.lastIndexOf('.');
  const rawStem = dot >= 0 ? base.slice(0, dot) : base;
  const rawExt = dot >= 0 ? base.slice(dot) : '';
  const stem = rawStem.replace(/[^\w-]/g, '_') || 'upload';
  const ext = rawExt.replace(/[^\w.-]/g, '_') || '.gpx';
  let suffix = '';
  while (suffix.length < SUFFIX_LENGTH) {
    suffix += random();
  }
  return `${stem}_${suffix.slice(0, SUFFIX_LENGTH)}${ext}`;
}
