/**
 * Unit tests for the Claude model registry helpers. The plugin persists a
 * model id in its config and must coerce unknown/missing values to a safe
 * default before calling the Anthropic API, so the validation and fallback
 * logic is what stops a stale config from sending an invalid model id. The
 * request helper decides which parameters each model accepts.
 */
import { expect } from 'chai';
import {
  CLAUDE_MODELS,
  SUPPORTED_CLAUDE_MODELS,
  DEFAULT_CLAUDE_MODEL,
  CLAUDE_MODEL_DESCRIPTIONS,
  THINKING_MIN_MAX_TOKENS,
  isValidClaudeModel,
  getValidClaudeModel,
  modelRequestParams,
} from '../../src/claude-models';

describe('claude-models registry', () => {
  it('lists every defined model as supported', () => {
    expect([...SUPPORTED_CLAUDE_MODELS].sort()).to.deep.equal(
      Object.values(CLAUDE_MODELS).sort()
    );
  });

  it('uses a supported model as the default', () => {
    expect(SUPPORTED_CLAUDE_MODELS).to.include(DEFAULT_CLAUDE_MODEL);
  });

  it('describes every supported model', () => {
    for (const model of SUPPORTED_CLAUDE_MODELS) {
      expect(CLAUDE_MODEL_DESCRIPTIONS, model).to.have.property(model);
      expect(CLAUDE_MODEL_DESCRIPTIONS[model]).to.be.a('string').and.not.empty;
    }
  });

  it('has no descriptions for unsupported models', () => {
    expect(Object.keys(CLAUDE_MODEL_DESCRIPTIONS).sort()).to.deep.equal(
      [...SUPPORTED_CLAUDE_MODELS].sort()
    );
  });
});

describe('isValidClaudeModel', () => {
  it('accepts every supported model id', () => {
    for (const model of SUPPORTED_CLAUDE_MODELS) {
      expect(isValidClaudeModel(model), model).to.equal(true);
    }
  });

  it('rejects an unknown model id', () => {
    expect(isValidClaudeModel('claude-imaginary-9')).to.equal(false);
  });

  it('rejects the empty string', () => {
    expect(isValidClaudeModel('')).to.equal(false);
  });

  it('is exact, not a prefix or substring match', () => {
    expect(isValidClaudeModel(CLAUDE_MODELS.SONNET_5 + '-extra')).to.equal(
      false
    );
    expect(isValidClaudeModel('sonnet')).to.equal(false);
  });
});

describe('getValidClaudeModel', () => {
  it('passes through a valid model id unchanged', () => {
    expect(getValidClaudeModel(CLAUDE_MODELS.OPUS_5)).to.equal(
      CLAUDE_MODELS.OPUS_5
    );
  });

  it('falls back to the default for an unknown model', () => {
    expect(getValidClaudeModel('nope')).to.equal(DEFAULT_CLAUDE_MODEL);
  });

  it('falls back to the default for undefined', () => {
    expect(getValidClaudeModel(undefined)).to.equal(DEFAULT_CLAUDE_MODEL);
  });

  it('falls back to the default for the empty string', () => {
    expect(getValidClaudeModel('')).to.equal(DEFAULT_CLAUDE_MODEL);
  });

  // Use case: a config or an open browser tab still carrying an id from an
  // older model generation, some of them already retired by the API.
  it('maps older Opus ids to the current Opus', () => {
    expect(getValidClaudeModel('claude-opus-4-1-20250805')).to.equal(
      CLAUDE_MODELS.OPUS_5
    );
    expect(getValidClaudeModel('claude-opus-4-20250514')).to.equal(
      CLAUDE_MODELS.OPUS_5
    );
  });

  it('maps older Haiku ids to the current Haiku', () => {
    expect(getValidClaudeModel('claude-3-haiku-20240307')).to.equal(
      CLAUDE_MODELS.HAIKU_4_5
    );
  });

  it('maps older Sonnet ids to the default, the current Sonnet', () => {
    expect(DEFAULT_CLAUDE_MODEL).to.equal(CLAUDE_MODELS.SONNET_5);
    expect(getValidClaudeModel('claude-sonnet-4-20250514')).to.equal(
      CLAUDE_MODELS.SONNET_5
    );
    expect(getValidClaudeModel('claude-sonnet-4-5-20250929')).to.equal(
      CLAUDE_MODELS.SONNET_5
    );
  });
});

describe('modelRequestParams', () => {
  it('builds params for every supported model', () => {
    for (const model of SUPPORTED_CLAUDE_MODELS) {
      expect(modelRequestParams(model, 1000, 0.5).model, model).to.equal(model);
    }
  });

  for (const model of [CLAUDE_MODELS.OPUS_5, CLAUDE_MODELS.SONNET_5]) {
    it(`omits temperature for ${model}, which rejects it`, () => {
      expect(modelRequestParams(model, 4000, 0.3)).to.not.have.property(
        'temperature'
      );
    });

    it(`raises a budget below the thinking floor for ${model}`, () => {
      expect(
        modelRequestParams(model, THINKING_MIN_MAX_TOKENS - 1, 0).max_tokens
      ).to.equal(THINKING_MIN_MAX_TOKENS);
    });

    it(`keeps a budget above the thinking floor for ${model}`, () => {
      expect(
        modelRequestParams(model, THINKING_MIN_MAX_TOKENS + 1, 0).max_tokens
      ).to.equal(THINKING_MIN_MAX_TOKENS + 1);
    });
  }

  it('passes temperature and budget through for Haiku 4.5', () => {
    expect(
      modelRequestParams(CLAUDE_MODELS.HAIKU_4_5, 4000, 0.3)
    ).to.deep.equal({
      model: CLAUDE_MODELS.HAIKU_4_5,
      max_tokens: 4000,
      temperature: 0.3,
    });
  });

  it('passes a zero temperature through for Haiku 4.5', () => {
    expect(
      modelRequestParams(CLAUDE_MODELS.HAIKU_4_5, 8000, 0).temperature
    ).to.equal(0);
  });
});
