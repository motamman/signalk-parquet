/**
 * Claude Model Definitions - Single Source of Truth
 * Update this file when Anthropic releases new models
 */

export const CLAUDE_MODELS = {
  OPUS_4_1: 'claude-opus-4-1-20250805',
  OPUS_4: 'claude-opus-4-20250514',
  SONNET_4: 'claude-sonnet-4-20250514',
  SONNET_4_5: 'claude-sonnet-4-5-20250929',
} as const;

export const SUPPORTED_CLAUDE_MODELS = [
  CLAUDE_MODELS.OPUS_4_1,
  CLAUDE_MODELS.OPUS_4,
  CLAUDE_MODELS.SONNET_4,
  CLAUDE_MODELS.SONNET_4_5,
] as const;

export const DEFAULT_CLAUDE_MODEL = CLAUDE_MODELS.SONNET_4_5;

export const CLAUDE_MODEL_DESCRIPTIONS = {
  [CLAUDE_MODELS.OPUS_4_1]: 'Claude Opus 4.1 (Most Capable & Intelligent)',
  [CLAUDE_MODELS.OPUS_4]: 'Claude Opus 4 (Previous Flagship)',
  [CLAUDE_MODELS.SONNET_4]: 'Claude Sonnet 4 (Balanced Performance)',
  [CLAUDE_MODELS.SONNET_4_5]: 'Claude Sonnet 4.5 (Latest Sonnet)',
} as const;

export type ClaudeModel = typeof SUPPORTED_CLAUDE_MODELS[number];

/**
 * Check if a model string is a valid supported Claude model
 */
export function isValidClaudeModel(model: string): model is ClaudeModel {
  return SUPPORTED_CLAUDE_MODELS.includes(model as ClaudeModel);
}

/**
 * Get a valid Claude model, falling back to default if invalid
 */
export function getValidClaudeModel(model?: string): ClaudeModel {
  if (!model || !isValidClaudeModel(model)) {
    return DEFAULT_CLAUDE_MODEL;
  }
  return model;
}