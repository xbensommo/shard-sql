import { DEFAULT_CONFIG } from '../constants.js';

export function normalizeSearchText(text) {
  return String(text || '')
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

export function tokenize(text) {
  const normalized = normalizeSearchText(text);
  if (!normalized) return [];
  return [...new Set(normalized.split(' ').filter(Boolean))];
}

export function createPrefixes(text, maxLength = DEFAULT_CONFIG.TOKEN_PREFIX_LIMIT) {
  const normalized = normalizeSearchText(text);
  if (!normalized) return [];
  const compact = normalized.replace(/\s+/g, ' ');
  const prefixes = [];
  for (let i = 1; i <= Math.min(compact.length, maxLength); i += 1) {
    prefixes.push(compact.slice(0, i));
  }
  return [...new Set(prefixes)];
}
