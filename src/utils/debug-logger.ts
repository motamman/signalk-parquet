/**
 * Simple debug logger for utility files
 * Can be disabled via environment variable or compile-time constant
 */

// Set to false in production to disable all debug logging from utilities
const DEBUG_ENABLED = process.env.SIGNALK_PARQUET_DEBUG === 'true' || false;

export const debugLogger = {
  log: (message: string, ...args: unknown[]): void => {
    if (DEBUG_ENABLED) {
      // eslint-disable-next-line no-console
      console.log(`[SignalK-Parquet] ${message}`, ...args);
    }
  },

  warn: (message: string, ...args: unknown[]): void => {
    if (DEBUG_ENABLED) {
      // eslint-disable-next-line no-console
      console.warn(`[SignalK-Parquet] ${message}`, ...args);
    }
  },

  error: (message: string, ...args: unknown[]): void => {
    // Always log errors
    // eslint-disable-next-line no-console
    console.error(`[SignalK-Parquet] ${message}`, ...args);
  },
};
