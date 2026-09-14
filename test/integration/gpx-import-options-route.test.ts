/**
 * Pins GET /api/import/gpx/options (#55): the import page builds its path
 * checkboxes from this response, so it has to list exactly the paths the
 * importer accepts, with the metadata the page renders.
 */
import { expect } from 'chai';
import { Router } from 'express';
import { registerApiRoutes } from '../../src/api-routes';
import {
  DEFAULT_IMPORT_PATHS,
  GPX_IMPORT_PATH_OPTIONS,
} from '../../src/services/gpx-import-service';
import { PluginState } from '../../src/types';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';

type Handler = (req: unknown, res: unknown) => Promise<void> | void;

/** Minimal router double recording the GET handlers the plugin registers. */
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

describe('GET /api/import/gpx/options', () => {
  let host: FakeSignalK;
  let handler: Handler;

  const app = {
    debug: () => {},
    error: () => {},
    getMetadata: () => undefined,
    getSelfPath: () => undefined,
    selfId: 'optionsself',
    selfContext: 'vessels.optionsself',
  };

  beforeEach(() => {
    host = createFakeSignalK({ selfId: 'optionsself' });
    const state = {
      getDataDirPath: () => host.dataDir,
      currentConfig: { outputDirectory: host.dataDir },
    } as unknown as PluginState;
    const { router, gets } = createRecordingRouter();
    registerApiRoutes(router, state, app as never);
    const captured = gets.get('/api/import/gpx/options');
    expect(captured, 'the plugin registers the options route').to.be.a(
      'function'
    );
    handler = captured as Handler;
  });

  afterEach(async () => {
    await host?.cleanup();
  });

  it('returns the importable paths with their UI metadata', async () => {
    let body: { success: boolean; paths: typeof GPX_IMPORT_PATH_OPTIONS } | undefined;
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

    expect(body?.success).to.equal(true);
    expect(body?.paths).to.deep.equal(GPX_IMPORT_PATH_OPTIONS);
    for (const option of body?.paths ?? []) {
      expect(option.from).to.be.a('string').and.not.equal('');
      expect(option.unit).to.be.a('string').and.not.equal('');
      expect(option.defaultChecked).to.be.a('boolean');
    }
  });

  it('lists exactly the paths the importer accepts', () => {
    // The route validates requested paths against DEFAULT_IMPORT_PATHS, so
    // the UI must never offer a path the import would reject.
    expect(GPX_IMPORT_PATH_OPTIONS.map(o => o.path)).to.deep.equal(
      DEFAULT_IMPORT_PATHS
    );
  });
});
