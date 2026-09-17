/**
 * The webapp config has to survive a save/load round trip intact.
 *
 * `loadWebAppConfig` applies a migration and then writes the result straight
 * back to disk, so a field it does not carry through is not merely ignored —
 * it is deleted from the stored config. That made a setting saved in the UI
 * appear not to take effect, and it could not be recovered by saving again.
 * These tests pin the round trip for the fields PathConfig has today and for
 * fields it does not know about at all, so the next one added cannot be lost
 * the same way.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as path from 'path';
import { loadWebAppConfig, saveWebAppConfig } from '../../src/commands';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import type { CommandConfig, PathConfig } from '../../src/types';

describe('webapp config round trip', () => {
  let host: FakeSignalK;

  const configFile = () =>
    path.join(host.dataDir, 'signalk-parquet', 'webapp-config.json');

  const stored = () => fs.readJsonSync(configFile()) as { paths: PathConfig[] };

  beforeEach(() => {
    host = createFakeSignalK();
  });

  afterEach(async () => {
    await host?.cleanup();
  });

  it('preserves the retention mode a path was saved with', () => {
    const paths = [
      {
        path: 'navigation.rateOfTurn',
        enabled: true,
        context: 'vessels.self',
        retention: 'buffer',
      },
    ] as unknown as PathConfig[];
    saveWebAppConfig(paths, [] as CommandConfig[], host.app);

    const loaded = loadWebAppConfig(host.app);
    expect(loaded.paths[0].retention).to.equal('buffer');
    // Loading rewrites the file, so the setting must survive that too.
    expect(stored().paths[0].retention).to.equal('buffer');
    expect(loadWebAppConfig(host.app).paths[0].retention).to.equal('buffer');
  });

  it('preserves the promotion regimen alongside it', () => {
    const paths = [
      {
        path: 'navigation.rateOfTurn',
        enabled: true,
        retention: 'buffer',
        fullWhileRegimen: 'capturePassage',
      },
    ] as unknown as PathConfig[];
    saveWebAppConfig(paths, [] as CommandConfig[], host.app);

    const loaded = loadWebAppConfig(host.app);
    expect(loaded.paths[0].fullWhileRegimen).to.equal('capturePassage');
    expect(stored().paths[0].fullWhileRegimen).to.equal('capturePassage');
  });

  it('keeps every existing field through the round trip', () => {
    const entry = {
      path: 'navigation.position',
      name: 'Position',
      enabled: true,
      regimen: 'capturePassage',
      source: 'gps.main',
      context: 'vessels.*',
      excludeMMSI: ['123456789'],
      autoDiscovered: true,
    } as unknown as PathConfig;
    saveWebAppConfig([entry], [] as CommandConfig[], host.app);

    expect(loadWebAppConfig(host.app).paths[0]).to.deep.equal(entry);
  });

  it('does not drop a field PathConfig has never heard of', () => {
    // The failure mode was structural, not specific to one field: a whitelist
    // deleted anything unknown. A future setting must not repeat it.
    const paths = [
      { path: 'navigation.speedOverGround', enabled: true, somethingNew: 42 },
    ] as unknown as PathConfig[];
    saveWebAppConfig(paths, [] as CommandConfig[], host.app);

    loadWebAppConfig(host.app);
    expect(
      (stored().paths[0] as unknown as { somethingNew?: number }).somethingNew
    ).to.equal(42);
  });

  it('still normalises an empty source filter to undefined', () => {
    // The one transformation the migration exists for.
    const paths = [
      { path: 'navigation.speedOverGround', enabled: true, source: '' },
    ] as unknown as PathConfig[];
    saveWebAppConfig(paths, [] as CommandConfig[], host.app);

    expect(loadWebAppConfig(host.app).paths[0].source).to.equal(undefined);
  });
});
