export function normalizeProviderOptions(options = {}) {
  return {
    adapter: options.adapter,
    collections: options.collections || options.definitions || [],
    strictSchemas: Boolean(options.strictSchemas),
    productionSafeErrors: options.productionSafeErrors !== false,
    environment: options.environment || options.env,
    nonShardedCollections: options.nonShardedCollections || options.nonSharded || [],
    onError: options.onError,
    ...options,
  };
}
