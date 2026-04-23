import { DEFAULT_CONFIG, FIELD_TYPES } from '../constants.js';
import { defineCollection } from '../core/defineCollection.js';

export function createLegacyCollectionDefinition(collectionName, options = {}) {
  const nonSharded = Array.isArray(options.nonShardedCollections) && options.nonShardedCollections.includes(collectionName);
  return defineCollection({
    name: collectionName,
    legacy: true,
    shard: {
      type: nonSharded ? DEFAULT_CONFIG.SHARD_TYPES.NONE : (options.defaultShardType || DEFAULT_CONFIG.SHARD_TYPES.MONTHLY),
      field: options.defaultDateField || DEFAULT_CONFIG.DEFAULT_DATE_FIELD,

      strategy: DEFAULT_CONFIG.SHARD_STRATEGIES.SUFFIX,
    },
    schema: {
      createdAt: { type: FIELD_TYPES.TIMESTAMP, readonly: true, system: true, sortable: true, filterable: true },
      updatedAt: { type: FIELD_TYPES.TIMESTAMP, readonly: true, system: true, sortable: true },
      createdBy: { type: FIELD_TYPES.STRING, readonly: true, system: true, filterable: true },
      updatedBy: { type: FIELD_TYPES.STRING, readonly: true, system: true },
      isDeleted: { type: FIELD_TYPES.BOOLEAN, readonly: true, system: true, filterable: true },
      deletedAt: { type: FIELD_TYPES.TIMESTAMP, readonly: true, system: true, sortable: true, filterable: true },
      deletedBy: { type: FIELD_TYPES.STRING, readonly: true, system: true, filterable: true },
      _searchTokens: { type: FIELD_TYPES.ARRAY, readonly: true, system: true },
      _searchPrefixes: { type: FIELD_TYPES.ARRAY, readonly: true, system: true },
    },
    writableFields: [],
    updateableFields: [],
    indexes: [],
    search: { mode: DEFAULT_CONFIG.SEARCH_MODES.NONE, fields: [] },
    rules: {},
  });
}
