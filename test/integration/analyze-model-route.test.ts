/**
 * Pins that the model picked in the analysis UI reaches the analyzer.
 *
 * /api/analyze accepts `claudeModel` in its body. The handler is obtained by
 * running the plugin's route registration against a recording router (see
 * raw-sql-guard-http.test.ts for why), and ClaudeAnalyzer.analyzeData is
 * replaced by a recorder, so no API call is made.
 */
import { expect } from 'chai';
import { Router } from 'express';
import { registerApiRoutes } from '../../src/api-routes';
import {
  AnalysisRequest,
  AnalysisResponse,
  ClaudeAnalyzer,
} from '../../src/claude-analyzer';
import { CLAUDE_MODELS } from '../../src/claude-models';
import { PluginState } from '../../src/types';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';

const SELF_ID = 'analyzeself';

type Handler = (req: unknown, res: unknown) => Promise<void> | void;

/** Minimal router double recording the POST handlers the plugin registers. */
function createRecordingRouter(): {
  router: Router;
  posts: Map<string, Handler>;
} {
  const posts = new Map<string, Handler>();
  const noop = () => router;
  const router = {
    use: noop,
    get: noop,
    put: noop,
    delete: noop,
    patch: noop,
    post: (routePath: string, ...handlers: Handler[]) => {
      posts.set(routePath, handlers[handlers.length - 1]);
      return router;
    },
  } as unknown as Router;
  return { router, posts };
}

describe('analysis model selection over /api/analyze', function () {
  this.timeout(30000);

  const originalAnalyzeData = ClaudeAnalyzer.prototype.analyzeData;
  let host: FakeSignalK;
  let handler: Handler;
  let received: AnalysisRequest[];

  const app = {
    debug: () => {},
    error: () => {},
    getMetadata: () => undefined,
    getSelfPath: () => undefined,
    selfId: SELF_ID,
    selfContext: `vessels.${SELF_ID}`,
  };

  before(() => {
    ClaudeAnalyzer.prototype.analyzeData = async function (
      request: AnalysisRequest
    ): Promise<AnalysisResponse> {
      received.push(request);
      return {
        id: 'analysis_1',
        analysis: 'ok',
        insights: [],
        confidence: 1,
        dataQuality: 'ok',
        timestamp: new Date().toISOString(),
        metadata: {
          dataPath: request.dataPath,
          analysisType: request.analysisType,
          recordCount: 0,
        },
      };
    };
  });

  after(() => {
    ClaudeAnalyzer.prototype.analyzeData = originalAnalyzeData;
  });

  beforeEach(() => {
    host = createFakeSignalK({ selfId: SELF_ID });
    received = [];

    // Only the slice of PluginState the /api/analyze handler reads.
    const state = {
      getDataDirPath: () => host.dataDir,
      currentConfig: {
        outputDirectory: host.dataDir,
        claudeIntegration: {
          enabled: true,
          apiKey: 'test-key-not-used',
          model: CLAUDE_MODELS.HAIKU_4_5,
        },
      },
    } as unknown as PluginState;

    const { router, posts } = createRecordingRouter();
    registerApiRoutes(router, state, app as never);
    const captured = posts.get('/api/analyze');
    expect(captured, 'the plugin registers a POST /api/analyze route').to.be.a(
      'function'
    );
    handler = captured as Handler;
  });

  afterEach(async () => {
    await host?.cleanup();
  });

  /** POSTs a custom analysis and returns the response status. */
  async function analyze(body: Record<string, unknown>): Promise<number> {
    let status = 200;
    const res = {
      status(code: number) {
        status = code;
        return res;
      },
      json() {
        return res;
      },
    };
    await handler(
      {
        body: { dataPath: 'navigation.speedOverGround', ...body },
        params: {},
        query: {},
      },
      res
    );
    return status;
  }

  it('passes the model picked in the UI to the analyzer', async () => {
    expect(await analyze({ claudeModel: CLAUDE_MODELS.OPUS_5 })).to.equal(200);
    expect(received[0].model).to.equal(CLAUDE_MODELS.OPUS_5);
  });

  it('leaves the model unset without a pick, so the configured one applies', async () => {
    expect(await analyze({})).to.equal(200);
    expect(received[0].model).to.equal(undefined);
  });

  // Use case: a browser tab opened before an upgrade still sends the id the
  // old page offered.
  it('maps an older model id to the current model of its tier', async () => {
    await analyze({ claudeModel: 'claude-opus-4-1-20250805' });
    expect(received[0].model).to.equal(CLAUDE_MODELS.OPUS_5);
  });
});
