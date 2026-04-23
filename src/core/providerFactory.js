import { DEFAULT_CONFIG } from '../constants.js';
import { SqlShardProvider } from '../runtime/SqlShardProvider.js';

export function createSqlShardProvider(options = {}) {
  return new SqlShardProvider({ ...options, engine: options.engine || options.backend || DEFAULT_CONFIG.BACKENDS.SQL });
}

export function createDataConnectProvider(options = {}) {
  return new SqlShardProvider({ ...options, engine: DEFAULT_CONFIG.BACKENDS.DATACONNECT });
}

export function createShardProvider(options = {}) {
  const engine = String(options.engine || options.backend || DEFAULT_CONFIG.BACKENDS.SQL).toLowerCase();
  if (engine !== DEFAULT_CONFIG.BACKENDS.SQL && engine != DEFAULT_CONFIG.BACKENDS.DATACONNECT && engine != DEFAULT_CONFIG.BACKENDS.POSTGRES) {
    throw new Error(`Unsupported provider engine for standalone SQL package: ${engine}`);
  }
  return new SqlShardProvider({ ...options, engine });
}
