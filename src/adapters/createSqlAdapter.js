import { DEFAULT_CONFIG } from '../constants.js';
import { ValidationError } from '../errors.js';

function ensureCollectionMap(collections) {
  if (!collections || typeof collections !== 'object' || Array.isArray(collections)) {
    throw new ValidationError(
      DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT,
      'createSqlAdapter requires a collections map.',
      { field: 'collections', operation: 'createSqlAdapter' }
    );
  }
  return collections;
}

export function createSqlAdapter(config = {}) {
  const collections = ensureCollectionMap(config.collections);
  return {
    type: 'sql-adapter',
    collections,
    onError: config.onError,
    resolveCollection(collectionName) {
      return collections[collectionName] || null;
    },
    async commitWriteOperations(context) {
      if (typeof config.commitWriteOperations === 'function') {
        return config.commitWriteOperations(context);
      }
      const { operations = [], provider, options = {} } = context || {};
      const results = [];
      for (const operation of operations) {
        const collectionName = operation?.collection || operation?.collectionName;
        const data = operation?.data || operation?.payload || {};
        const id = operation?.id || operation?.recordId || operation?.key;
        const type = operation?.type || operation?.action || operation?.method;

        switch (type) {
          case 'set':
          case 'create':
            results.push(id ? await provider.set(collectionName, id, data, options) : await provider.create(collectionName, data, options));
            break;
          case 'update':
          case 'patch':
            results.push(await provider.update(collectionName, String(id), data, options));
            break;
          case 'delete':
          case 'remove':
            results.push(await provider.destroy(collectionName, String(id), options));
            break;
          default:
            throw new ValidationError(
              DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT,
              `Unsupported write operation type: ${type}`,
              { field: 'type', operation: 'commitWriteOperations', value: type }
            );
        }
      }
      return results;
    },
  };
}
