import { DEFAULT_CONFIG } from './constants.js';

export class ShardProviderError extends Error {
  constructor(code, message, options = {}) {
    super(message);
    this.name = 'ShardProviderError';
    this.code = code || DEFAULT_CONFIG.ERROR_CODES.UNKNOWN;
    this.field = options.field || null;
    this.collection = options.collection || null;
    this.operation = options.operation || null;
    this.cause = options.cause || null;
    this.details = options.details || null;
    this.publicMessage = options.publicMessage || DEFAULT_CONFIG.SAFE_PUBLIC_MESSAGES[this.code] || DEFAULT_CONFIG.SAFE_PUBLIC_MESSAGES.UNKNOWN;
    this.safeDetails = options.safeDetails || null;
  }
}

export class ValidationError extends ShardProviderError {
  constructor(code, message, options = {}) {
    super(code, message, options);
    this.name = 'ValidationError';
  }
}

export class QueryError extends ShardProviderError {
  constructor(code, message, options = {}) {
    super(code, message, options);
    this.name = 'QueryError';
  }
}

export class IndexGenerationError extends ShardProviderError {
  constructor(code, message, options = {}) {
    super(code, message, options);
    this.name = 'IndexGenerationError';
  }
}

export class RulesGenerationError extends ShardProviderError {
  constructor(code, message, options = {}) {
    super(code, message, options);
    this.name = 'RulesGenerationError';
  }
}

export function getFriendlyMessage(error) {
  if (!error) return DEFAULT_CONFIG.SAFE_PUBLIC_MESSAGES.UNKNOWN;
  if (typeof error === 'string') return error;
  return error.publicMessage || DEFAULT_CONFIG.SAFE_PUBLIC_MESSAGES[error.code] || DEFAULT_CONFIG.SAFE_PUBLIC_MESSAGES.UNKNOWN;
}

export function toShardProviderError(error, fallback = {}) {
  if (error instanceof ShardProviderError) return error;
  return new ShardProviderError(
    fallback.code || DEFAULT_CONFIG.ERROR_CODES.UNKNOWN,
    error?.message || 'Unknown error',
    {
      ...fallback,
      cause: error,
      publicMessage: fallback.publicMessage || DEFAULT_CONFIG.SAFE_PUBLIC_MESSAGES[fallback.code || DEFAULT_CONFIG.ERROR_CODES.UNKNOWN],
    }
  );
}
