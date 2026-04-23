/** @file index.js */
export const VERSION = '1.1.0';

export { DEFAULT_CONFIG, FIELD_TYPES } from './constants.js';
export { SqlShardProvider } from './runtime/SqlShardProvider.js';
export { IncludeEngine } from './runtime/includeEngine.js';
export { createShardedActions } from './actions/createShardedActions.js';
export { defineCollection } from './core/defineCollection.js';
export { CollectionRegistry } from './core/registry.js';
export { createShardProvider, createSqlShardProvider, createDataConnectProvider } from './core/providerFactory.js';
export { inspectQuery } from './core/queryDoctor.js';
export { createSqlAdapter } from './adapters/createSqlAdapter.js';
export { createDataConnectAdapter } from './adapters/createDataConnectAdapter.js';

export {
  getCollectionGroupId,
  getLegacyFallbackShardNames,
  getLegacySeparatorShardNames,
  getShardConfig,
  isBucketParentStrategy,
  normalizeShardRange,
  resolveShardKey,
} from './core/shardResolver.js';

export {
  buildFieldAliases,
  deriveForeignKeyField,
  derivePrimaryKeyField,
  normalizeDataFieldAliases,
  normalizeQueryFieldAliases,
  singularizeCollectionName,
  toCamelCase,
  toSnakeCase,
  uniqueFieldNames,
} from './core/relationNaming.js';

export { createValidator, validateCreatePayload, validateUpdatePayload } from './validators.js';
export {
  ShardProviderError,
  ValidationError,
  QueryError,
  IndexGenerationError,
  RulesGenerationError,
  getFriendlyMessage,
  toShardProviderError,
} from './errors.js';
