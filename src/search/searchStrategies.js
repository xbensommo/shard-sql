import { DEFAULT_CONFIG } from '../constants.js';
import { QueryError } from '../errors.js';
import { createPrefixes, tokenize } from './tokenizer.js';

export function prepareSearchFields(definition, data = {}) {
  const mode = definition.search?.mode || DEFAULT_CONFIG.SEARCH_MODES.NONE;
  const fields = definition.search?.fields || [];
  const values = fields.map((field) => data[field]).filter((value) => value !== undefined && value !== null);
  const text = values.join(' ');

  switch (mode) {
    case DEFAULT_CONFIG.SEARCH_MODES.NONE:
      return {};
    case DEFAULT_CONFIG.SEARCH_MODES.TOKEN_ARRAY:
      return { _searchTokens: tokenize(text) };
    case DEFAULT_CONFIG.SEARCH_MODES.PREFIX:
      return { _searchPrefixes: createPrefixes(text) };
    case DEFAULT_CONFIG.SEARCH_MODES.EXTERNAL:
      return {};
    default:
      return {};
  }
}

export function buildSearchQuery(definition, term) {
  const mode = definition.search?.mode || DEFAULT_CONFIG.SEARCH_MODES.NONE;
  const normalized = String(term || '').trim();
  if (!normalized) {
    throw new QueryError(
      DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT,
      'Search term cannot be empty.',
      { collection: definition.name, operation: 'search' }
    );
  }

  switch (mode) {
    case DEFAULT_CONFIG.SEARCH_MODES.TOKEN_ARRAY:
      return { field: '_searchTokens', op: 'array-contains', value: tokenize(normalized)[0] || normalized.toLowerCase() };
    case DEFAULT_CONFIG.SEARCH_MODES.PREFIX:
      return { field: '_searchPrefixes', op: 'array-contains', value: normalized.toLowerCase() };
    case DEFAULT_CONFIG.SEARCH_MODES.EXTERNAL:
      throw new QueryError(
        DEFAULT_CONFIG.ERROR_CODES.SEARCH_NOT_CONFIGURED,
        'External search must be handled outside provider-managed SQL actions.',
        { collection: definition.name, operation: 'search' }
      );
    default:
      throw new QueryError(
        DEFAULT_CONFIG.ERROR_CODES.SEARCH_NOT_CONFIGURED,
        `Search is not configured for collection '${definition.name}'.`,
        { collection: definition.name, operation: 'search' }
      );
  }
}
