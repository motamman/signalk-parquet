/**
 * Unit tests for the Claude model registry helpers. The plugin persists a
 * model id in its config and must coerce unknown/missing values to a safe
 * default before calling the Anthropic API, so the validation and fallback
 * logic is what stops a stale config from sending an invalid model id.
 */
import { expect } from 'chai';
import {
  CLAUDE_MODELS,
  SUPPORTED_CLAUDE_MODELS,
  DEFAULT_CLAUDE_MODEL,
  CLAUDE_MODEL_DESCRIPTIONS,
  isValidClaudeModel,
  getValidClaudeModel,
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
    expect(isValidClaudeModel(CLAUDE_MODELS.SONNET_4_5 + '-extra')).to.equal(
      false
    );
    expect(isValidClaudeModel('sonnet')).to.equal(false);
  });
});

describe('getValidClaudeModel', () => {
  it('passes through a valid model id unchanged', () => {
    expect(getValidClaudeModel(CLAUDE_MODELS.OPUS_4_1)).to.equal(
      CLAUDE_MODELS.OPUS_4_1
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
});
