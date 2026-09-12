/**
 * Request shape of ClaudeAnalyzer calls. The Anthropic client is swapped for
 * a recorder, so these tests pin what would be sent (which model, whether a
 * temperature is included, the output budget) and how the answer is read back
 * when a thinking block precedes it. Data loading, prompt building and schema
 * discovery are stubbed: the request shape does not depend on them.
 */
import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'os';
import * as path from 'path';
import type Anthropic from '@anthropic-ai/sdk';
import {
  AnalysisRequest,
  ClaudeAnalyzer,
  responseText,
} from '../../src/claude-analyzer';
import { CLAUDE_MODELS, ClaudeModel } from '../../src/claude-models';

type Params = Record<string, unknown>;

/** A reply whose answer follows a thinking block, as Opus 5 and Sonnet 5 send by default. */
function thinkingThenText(text: string): Params {
  return {
    stop_reason: 'end_turn',
    usage: { input_tokens: 1, output_tokens: 1 },
    content: [
      { type: 'thinking', thinking: '', signature: 'sig' },
      { type: 'text', text },
    ],
  };
}

describe('ClaudeAnalyzer model selection', () => {
  let dataDir: string;
  let requests: Params[];
  let reply: Params;

  beforeEach(async () => {
    dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'claude-model-'));
    requests = [];
    reply = thinkingThenText('{"analysis": "steady", "insights": ["ok"]}');
  });

  afterEach(async () => {
    await fs.remove(dataDir);
  });

  /** An analyzer on `model` with its client and data loading stubbed. */
  function newAnalyzer(model: ClaudeModel): ClaudeAnalyzer {
    const analyzer = new ClaudeAnalyzer(
      { apiKey: 'test-key-not-used', model, maxTokens: 4000, temperature: 0.3 },
      undefined,
      dataDir
    );
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const internals = analyzer as any;
    internals.client = {
      messages: {
        create: async (params: Params) => {
          requests.push(params);
          return reply;
        },
      },
    };
    internals.prepareDataForAnalysis = async () => ({
      summary: {},
      sampleData: [],
      originalCount: 0,
    });
    internals.buildAnalysisPrompt = () => 'prompt';
    internals.vesselContextManager = {
      refreshVesselInfo: async () => {},
      generateClaudeContext: () => '',
    };
    internals.getEnhancedSchemaForClaude = async () => 'schema '.repeat(20);
    return analyzer;
  }

  /** A summary analysis request for one path, with the given overrides. */
  function request(overrides: Partial<AnalysisRequest> = {}): AnalysisRequest {
    return {
      dataPath: 'navigation.speedOverGround',
      analysisType: 'summary',
      ...overrides,
    };
  }

  describe('sampling analysis', () => {
    it('uses the model requested for the analysis over the configured one', async () => {
      await newAnalyzer(CLAUDE_MODELS.SONNET_5).analyzeData(
        request({ model: CLAUDE_MODELS.OPUS_5 })
      );
      expect(requests[0].model).to.equal(CLAUDE_MODELS.OPUS_5);
    });

    it('uses the configured model when none is requested', async () => {
      await newAnalyzer(CLAUDE_MODELS.HAIKU_4_5).analyzeData(request());
      expect(requests[0].model).to.equal(CLAUDE_MODELS.HAIKU_4_5);
    });

    it('omits temperature and leaves room for thinking on Sonnet 5', async () => {
      await newAnalyzer(CLAUDE_MODELS.SONNET_5).analyzeData(request());
      expect(requests[0]).to.not.have.property('temperature');
      expect(requests[0].max_tokens).to.equal(16000);
    });

    it('sends the configured temperature and budget to Haiku 4.5', async () => {
      await newAnalyzer(CLAUDE_MODELS.HAIKU_4_5).analyzeData(request());
      expect(requests[0].temperature).to.equal(0.3);
      expect(requests[0].max_tokens).to.equal(4000);
    });

    it('reads the answer that follows a thinking block', async () => {
      const result = await newAnalyzer(CLAUDE_MODELS.SONNET_5).analyzeData(
        request()
      );
      expect(result.analysis).to.equal('steady');
    });

    it('reports a refusal instead of returning an empty analysis', async () => {
      reply = {
        stop_reason: 'refusal',
        usage: { input_tokens: 1, output_tokens: 0 },
        content: [],
      };
      let message = '';
      try {
        await newAnalyzer(CLAUDE_MODELS.SONNET_5).analyzeData(request());
      } catch (error) {
        message = (error as Error).message;
      }
      expect(message).to.contain('declined');
    });
  });

  describe('database-access analysis', () => {
    it('uses the requested model without a temperature', async () => {
      reply = thinkingThenText('The boat averaged 5 knots.');
      const result = await newAnalyzer(CLAUDE_MODELS.HAIKU_4_5).analyzeData(
        request({ useDatabaseAccess: true, model: CLAUDE_MODELS.OPUS_5 })
      );
      expect(requests[0].model).to.equal(CLAUDE_MODELS.OPUS_5);
      expect(requests[0]).to.not.have.property('temperature');
      expect(result.analysis).to.equal('The boat averaged 5 knots.');
    });
  });

  describe('connection test', () => {
    it('disables thinking and reads the text after any thinking block', async () => {
      reply = thinkingThenText('Claude AI connection successful');
      const result = await newAnalyzer(CLAUDE_MODELS.OPUS_5).testConnection();
      expect(result).to.deep.equal({ success: true });
      expect(requests[0].model).to.equal(CLAUDE_MODELS.OPUS_5);
      expect(requests[0].thinking).to.deep.equal({ type: 'disabled' });
    });
  });
});

describe('responseText', () => {
  it('joins the text blocks and skips thinking and tool_use blocks', () => {
    const message = {
      content: [
        { type: 'thinking', thinking: '', signature: 'sig' },
        { type: 'text', text: 'first' },
        { type: 'tool_use', id: 't1', name: 'q', input: {} },
        { type: 'text', text: 'second' },
      ],
    } as unknown as Anthropic.Message;
    expect(responseText(message)).to.equal('first\nsecond');
  });

  it('returns an empty string when there is no text block', () => {
    expect(
      responseText({ content: [] } as unknown as Anthropic.Message)
    ).to.equal('');
  });
});
