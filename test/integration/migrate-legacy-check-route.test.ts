/**
 * Pins GET /api/migrate/legacy-check: the Status tab shows the migration
 * panel only when this reports a legacy flat-layout vessels/ directory. It
 * must answer from the directory alone, never by scanning the store.
 */
import { expect } from 'chai';
import * as path from 'path';
import * as fs from 'fs-extra';
import { Router } from 'express';
import { registerApiRoutes } from '../../src/api-routes';
import { PluginState } from '../../src/types';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';

type Handler = (req: unknown, res: unknown) => Promise<void> | void;

function createRecordingRouter(): { router: Router; gets: Map<string, Handler> } {
  const gets = new Map<string, Handler>();
  const noop = () => router;
  const router = {
    use: noop,
    post: noop,
    put: noop,
    delete: noop,
    patch: noop,
    get: (routePath: string, ...handlers: Handler[]) => {
      gets.set(routePath, handlers[handlers.length - 1]);
      return router;
    },
  } as unknown as Router;
  return { router, gets };
}

describe('GET /api/migrate/legacy-check', () => {
  let host: FakeSignalK;
  let handler: Handler;

  const app = {
    debug: () => {},
    error: () => {},
    getMetadata: () => undefined,
    getSelfPath: () => undefined,
    selfId: 'legacyself',
    selfContext: 'vessels.legacyself',
  };

  beforeEach(() => {
    host = createFakeSignalK({ selfId: 'legacyself' });
    const state = {
      getDataDirPath: () => host.dataDir,
      currentConfig: { outputDirectory: host.dataDir },
    } as unknown as PluginState;
    const { router, gets } = createRecordingRouter();
    registerApiRoutes(router, state, app as never);
    handler = gets.get('/api/migrate/legacy-check') as Handler;
    expect(handler).to.be.a('function');
  });

  afterEach(async () => {
    await host?.cleanup();
  });

  async function check(): Promise<{ success: boolean; legacyDirectoryPresent: boolean }> {
    let body: { success: boolean; legacyDirectoryPresent: boolean } | undefined;
    const res = {
      status() {
        return res;
      },
      json(payload: typeof body) {
        body = payload;
        return res;
      },
    };
    await handler({ params: {}, query: {}, body: {} }, res);
    return body!;
  }

  it('reports no legacy data on a hive-only store', async () => {
    await fs.ensureDir(path.join(host.dataDir, 'tier=raw', 'context=vessels__x'));
    expect(await check()).to.deep.equal({
      success: true,
      legacyDirectoryPresent: false,
    });
  });

  it('reports legacy data when a vessels/ directory exists', async () => {
    await fs.ensureDir(path.join(host.dataDir, 'vessels', 'self', 'navigation'));
    expect(await check()).to.deep.equal({
      success: true,
      legacyDirectoryPresent: true,
    });
  });

  it('ignores a vessels file that is not a directory', async () => {
    await fs.writeFile(path.join(host.dataDir, 'vessels'), '');
    expect((await check()).legacyDirectoryPresent).to.equal(false);
  });
});
