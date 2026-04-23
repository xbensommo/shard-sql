import { DEFAULT_CONFIG } from '../constants.js';
import { QueryError } from '../errors.js';
import { normalizeQueryFieldAliases } from './relationNaming.js';

function normalizeIndex(index) {
  if (Array.isArray(index)) return { fields: index };
  return { order: 'ASCENDING', ...index };
}

function hasSupportingIndex(definition, filters = [], orderBy = []) {
  const indexes = (definition.indexes || []).map(normalizeIndex);
  if (filters.length === 0 && orderBy.length === 0) return true;
  if (indexes.length === 0) return filters.length <= 1 && orderBy.length <= 1;

  const needed = [...new Set([
    ...filters.map((item) => item.field),
    ...orderBy.map((item) => item.field),
  ])];

  return indexes.some((idx) => {
    const fields = idx.fields || [];
    return needed.every((field) => fields.includes(field));
  });
}

export function inspectQuery(definition, query = {}, options = {}) {
  const collection = definition.name;
  const normalizedQuery = normalizeQueryFieldAliases(definition, query, { collection, operation: options.operation || 'query' });
  const filters = Array.isArray(normalizedQuery.filters) ? normalizedQuery.filters : [];
  const orderBy = Array.isArray(normalizedQuery.orderBy) ? normalizedQuery.orderBy : [];
  const rangeFilters = filters.filter((item) => ['<', '<=', '>', '>='].includes(item.op));

  filters.forEach((filter) => {
    const fieldConfig = definition.schema[filter.field];
    if (!fieldConfig || !fieldConfig.filterable) {
      throw new QueryError(
        DEFAULT_CONFIG.ERROR_CODES.QUERY_NOT_ALLOWED,
        `Filtering by '${filter.field}' is not allowed for collection '${collection}'.`,
        { collection, operation: options.operation || 'query', field: filter.field }
      );
    }
  });

  orderBy.forEach((sort) => {
    const fieldConfig = definition.schema[sort.field];
    if (!fieldConfig || !fieldConfig.sortable) {
      throw new QueryError(
        DEFAULT_CONFIG.ERROR_CODES.QUERY_NOT_ALLOWED,
        `Sorting by '${sort.field}' is not allowed for collection '${collection}'.`,
        { collection, operation: options.operation || 'query', field: sort.field }
      );
    }
  });

  if (rangeFilters.length > 1) {
    const distinctFields = [...new Set(rangeFilters.map((item) => item.field))];
    if (distinctFields.length > 1) {
      throw new QueryError(
        DEFAULT_CONFIG.ERROR_CODES.QUERY_NOT_ALLOWED,
        'Multiple range filters on different fields are not supported by this package configuration.',
        { collection, operation: options.operation || 'query' }
      );
    }
  }

  if (!hasSupportingIndex(definition, filters, orderBy)) {
    throw new QueryError(
      DEFAULT_CONFIG.ERROR_CODES.INDEX_REQUIRED,
      `Query for collection '${collection}' is not covered by declared indexes.`,
      { collection, operation: options.operation || 'query', details: { filters, orderBy } }
    );
  }

  return {
    ok: true,
    normalizedQuery,
    shardsFanout: definition.shard?.type === DEFAULT_CONFIG.SHARD_TYPES.NONE ? 1 : (normalizedQuery.shards?.length || 'dynamic'),
  };
}
