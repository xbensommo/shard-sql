import { DEFAULT_CONFIG } from '../constants.js';
import { QueryError } from '../errors.js';
import { createPrefixes, normalizeSearchText, tokenize } from './tokenizer.js';

function uniqueStrings(values = []) {
  return [...new Set(values.filter((value) => typeof value === 'string' && value.trim()))];
}

function createTokenPrefixes(text) {
  const normalized = normalizeSearchText(text);
  if (!normalized) return [];

  const prefixes = new Set(createPrefixes(normalized));
  for (const token of tokenize(normalized)) {
    for (const prefix of createPrefixes(token)) {
      prefixes.add(prefix);
    }
  }

  return [...prefixes];
}

function extractSearchText(definition, data = {}) {
  const fields = definition.search?.fields || [];
  return fields
    .map((field) => data?.[field])
    .filter((value) => value !== undefined && value !== null)
    .join(' ');
}

function ensureSearchTerm(definition, term) {
  const normalized = normalizeSearchText(term);
  if (!normalized) {
    throw new QueryError(
      DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT,
      'Search term cannot be empty.',
      { collection: definition.name, operation: 'search' }
    );
  }
  return normalized;
}

export function prepareSearchFields(definition, data = {}) {
  const mode = definition.search?.mode || DEFAULT_CONFIG.SEARCH_MODES.NONE;
  const text = extractSearchText(definition, data);
  const normalizedText = normalizeSearchText(text);
  const tokens = tokenize(normalizedText);

  switch (mode) {
    case DEFAULT_CONFIG.SEARCH_MODES.NONE:
      return {};
    case DEFAULT_CONFIG.SEARCH_MODES.TOKEN_ARRAY:
      return {
        _searchText: normalizedText,
        _searchTokens: tokens,
      };
    case DEFAULT_CONFIG.SEARCH_MODES.PREFIX:
      return {
        _searchText: normalizedText,
        _searchTokens: tokens,
        _searchPrefixes: createTokenPrefixes(normalizedText),
      };
    case DEFAULT_CONFIG.SEARCH_MODES.EXTERNAL:
      return {
        _searchText: normalizedText,
      };
    default:
      return {};
  }
}

export function buildSearchPlan(definition, term, options = {}) {
  const mode = definition.search?.mode || DEFAULT_CONFIG.SEARCH_MODES.NONE;
  const normalized = ensureSearchTerm(definition, term);
  const tokens = uniqueStrings(tokenize(normalized));
  const primaryToken = tokens[0] || normalized;

  switch (mode) {
    case DEFAULT_CONFIG.SEARCH_MODES.TOKEN_ARRAY:
      return {
        mode,
        term: String(term),
        normalized,
        tokens,
        primaryFilter: { field: '_searchTokens', op: 'array-contains', value: primaryToken },
        limit: options.limit || DEFAULT_CONFIG.DEFAULT_PAGE_SIZE,
      };
    case DEFAULT_CONFIG.SEARCH_MODES.PREFIX:
      return {
        mode,
        term: String(term),
        normalized,
        tokens,
        primaryFilter: { field: '_searchPrefixes', op: 'array-contains', value: primaryToken },
        limit: options.limit || DEFAULT_CONFIG.DEFAULT_PAGE_SIZE,
      };
    case DEFAULT_CONFIG.SEARCH_MODES.EXTERNAL:
      return {
        mode,
        term: String(term),
        normalized,
        tokens,
        primaryFilter: null,
        limit: options.limit || DEFAULT_CONFIG.DEFAULT_PAGE_SIZE,
      };
    default:
      throw new QueryError(
        DEFAULT_CONFIG.ERROR_CODES.SEARCH_NOT_CONFIGURED,
        `Search is not configured for collection '${definition.name}'.`,
        { collection: definition.name, operation: 'search' }
      );
  }
}

export function buildSearchQuery(definition, term, options = {}) {
  return buildSearchPlan(definition, term, options).primaryFilter;
}

export function getSearchText(definition, record) {
  const data = record?.data && typeof record.data === 'object' ? record.data : record;
  if (!data || typeof data !== 'object') return '';
  if (typeof data._searchText === 'string' && data._searchText.trim()) {
    return normalizeSearchText(data._searchText);
  }
  return normalizeSearchText(extractSearchText(definition, data));
}

export function getSearchTokens(definition, record) {
  const data = record?.data && typeof record.data === 'object' ? record.data : record;
  if (Array.isArray(data?._searchTokens)) {
    return uniqueStrings(data._searchTokens.map((token) => normalizeSearchText(token)).filter(Boolean));
  }
  return uniqueStrings(tokenize(getSearchText(definition, record)));
}

export function getSearchPrefixes(definition, record) {
  const data = record?.data && typeof record.data === 'object' ? record.data : record;
  if (Array.isArray(data?._searchPrefixes)) {
    return uniqueStrings(data._searchPrefixes.map((value) => normalizeSearchText(value)).filter(Boolean));
  }
  return createTokenPrefixes(getSearchText(definition, record));
}

export function matchesSearchRecord(definition, record, plan) {
  const normalized = plan?.normalized || '';
  const tokens = Array.isArray(plan?.tokens) ? plan.tokens : [];
  if (!normalized) return false;

  const text = getSearchText(definition, record);
  const recordTokens = getSearchTokens(definition, record);
  const recordPrefixes = plan?.mode === DEFAULT_CONFIG.SEARCH_MODES.PREFIX
    ? getSearchPrefixes(definition, record)
    : [];

  if (plan?.mode === DEFAULT_CONFIG.SEARCH_MODES.TOKEN_ARRAY) {
    return tokens.every((token) => recordTokens.includes(token));
  }

  if (plan?.mode === DEFAULT_CONFIG.SEARCH_MODES.PREFIX) {
    if (text.startsWith(normalized)) return true;
    return tokens.every((token) => {
      if (recordPrefixes.includes(token)) return true;
      return recordTokens.some((candidate) => candidate.startsWith(token));
    });
  }

  return false;
}

export function scoreSearchRecord(definition, record, plan) {
  const normalized = plan?.normalized || '';
  if (!normalized) return 0;

  const text = getSearchText(definition, record);
  const tokens = Array.isArray(plan?.tokens) ? plan.tokens : [];
  const recordTokens = getSearchTokens(definition, record);
  let score = 0;

  if (!text) return score;
  if (text === normalized) score += 1000;
  if (text.startsWith(normalized)) score += 700;
  if (text.includes(normalized)) score += 300;

  for (const token of tokens) {
    if (recordTokens.includes(token)) score += 120;
    else if (recordTokens.some((candidate) => candidate.startsWith(token))) score += 60;
  }

  if (plan?.mode === DEFAULT_CONFIG.SEARCH_MODES.PREFIX) {
    const prefixes = getSearchPrefixes(definition, record);
    for (const token of tokens) {
      if (prefixes.includes(token)) score += 45;
    }
  }

  const updatedAt = record?.data?.updatedAt || record?.updatedAt;
  if (updatedAt) {
    const parsed = Date.parse(updatedAt);
    if (Number.isFinite(parsed)) {
      score += Math.floor(parsed / 1000000000);
    }
  }

  return score;
}

export function rankSearchRecords(definition, records = [], plan) {
  return [...records].sort((left, right) => {
    const scoreDelta = scoreSearchRecord(definition, right, plan) - scoreSearchRecord(definition, left, plan);
    if (scoreDelta !== 0) return scoreDelta;

    const leftUpdatedAt = Date.parse(left?.data?.updatedAt || left?.updatedAt || left?.data?.createdAt || left?.createdAt || 0) || 0;
    const rightUpdatedAt = Date.parse(right?.data?.updatedAt || right?.updatedAt || right?.data?.createdAt || right?.createdAt || 0) || 0;
    if (rightUpdatedAt !== leftUpdatedAt) return rightUpdatedAt - leftUpdatedAt;

    const leftId = String(left?.id || '');
    const rightId = String(right?.id || '');
    return leftId.localeCompare(rightId);
  });
}
