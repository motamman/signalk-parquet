/**
 * Claude Model Definitions - Single Source of Truth
 * Update this file when Anthropic releases new models.
 *
 * When adding a model:
 * 1. Add its id to CLAUDE_MODELS and SUPPORTED_CLAUDE_MODELS
 * 2. Describe it in CLAUDE_MODEL_DESCRIPTIONS
 * 3. Record its request traits in MODEL_TRAITS (the compiler enforces this)
 * 4. Offer it in the analysis model <select> in public/index.html
 */

export const CLAUDE_MODELS = {
  OPUS_5: 'claude-opus-5',
  SONNET_5: 'claude-sonnet-5',
  HAIKU_4_5: 'claude-haiku-4-5',
} as const;

export const SUPPORTED_CLAUDE_MODELS = [
  CLAUDE_MODELS.OPUS_5,
  CLAUDE_MODELS.SONNET_5,
  CLAUDE_MODELS.HAIKU_4_5,
] as const;

export const DEFAULT_CLAUDE_MODEL = CLAUDE_MODELS.SONNET_5;

export const CLAUDE_MODEL_DESCRIPTIONS = {
  [CLAUDE_MODELS.OPUS_5]: 'Claude Opus 5 (Most Capable)',
  [CLAUDE_MODELS.SONNET_5]: 'Claude Sonnet 5 (Balanced, Recommended)',
  [CLAUDE_MODELS.HAIKU_4_5]: 'Claude Haiku 4.5 (Fastest, Lowest Cost)',
} as const;

export type ClaudeModel = (typeof SUPPORTED_CLAUDE_MODELS)[number];

interface ModelTraits {
  /** Whether the API accepts a `temperature` for this model. */
  acceptsTemperature: boolean;
  /** Whether the model runs adaptive thinking when `thinking` is omitted. */
  thinksByDefault: boolean;
}

// Opus 5 and Sonnet 5 reject sampling parameters with a 400 and think by
// default, spending output tokens before the answer. Haiku 4.5 accepts a
// temperature and only thinks when asked to.
const MODEL_TRAITS: Record<ClaudeModel, ModelTraits> = {
  [CLAUDE_MODELS.OPUS_5]: { acceptsTemperature: false, thinksByDefault: true },
  [CLAUDE_MODELS.SONNET_5]: {
    acceptsTemperature: false,
    thinksByDefault: true,
  },
  [CLAUDE_MODELS.HAIKU_4_5]: {
    acceptsTemperature: true,
    thinksByDefault: false,
  },
};

/**
 * Output budget floor for models that think by default: max_tokens caps
 * thinking and answer together, so a budget sized for the answer alone would
 * truncate it.
 */
export const THINKING_MIN_MAX_TOKENS = 16000;

/**
 * Check if a model string is a valid supported Claude model
 */
export function isValidClaudeModel(model: string): model is ClaudeModel {
  return SUPPORTED_CLAUDE_MODELS.includes(model as ClaudeModel);
}

/**
 * Get a valid Claude model. An id from an older model generation, such as one
 * saved in a config while it was current, maps to the current model of the
 * same tier; anything else falls back to the default.
 *
 * Examples:
 *   'claude-opus-4-1-20250805' -> 'claude-opus-5'
 *   'claude-3-haiku-20240307'  -> 'claude-haiku-4-5'
 *   'claude-sonnet-4-20250514' -> 'claude-sonnet-5' (the default)
 */
export function getValidClaudeModel(model?: string): ClaudeModel {
  if (!model) {
    return DEFAULT_CLAUDE_MODEL;
  }
  if (isValidClaudeModel(model)) {
    return model;
  }
  if (model.includes('opus')) {
    return CLAUDE_MODELS.OPUS_5;
  }
  if (model.includes('haiku')) {
    return CLAUDE_MODELS.HAIKU_4_5;
  }
  return DEFAULT_CLAUDE_MODEL;
}

/**
 * The model-dependent fields of a Messages API request: the model id, an
 * output budget with room for thinking where the model thinks by default, and
 * `temperature` only where the model accepts one.
 *
 * Example: modelRequestParams('claude-sonnet-5', 4000, 0.3)
 *   -> { model: 'claude-sonnet-5', max_tokens: 16000 }
 */
export function modelRequestParams(
  model: ClaudeModel,
  maxTokens: number,
  temperature: number
): { model: ClaudeModel; max_tokens: number; temperature?: number } {
  const traits = MODEL_TRAITS[model];
  return {
    model,
    max_tokens: traits.thinksByDefault
      ? Math.max(maxTokens, THINKING_MIN_MAX_TOKENS)
      : maxTokens,
    ...(traits.acceptsTemperature ? { temperature } : {}),
  };
}
