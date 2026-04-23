import { DEFAULT_CONFIG } from '../constants.js';
import { ValidationError } from '../errors.js';
import { defineCollection } from './defineCollection.js';

export class CollectionRegistry {
  constructor(definitions = []) {
    this.map = new Map();
    definitions.forEach((definition) => this.register(definition));
  }

  register(definition) {
    const normalized = definition?.kind === 'ShardProviderCollectionDefinition'
      ? definition
      : defineCollection(definition);

    if (this.map.has(normalized.name)) {
      throw new ValidationError(
        DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION,
        `Collection '${normalized.name}' is already registered.`,
        { collection: normalized.name }
      );
    }

    this.map.set(normalized.name, normalized);
    return normalized;
  }

  get(name) {
    return this.map.get(name) || null;
  }

  has(name) {
    return this.map.has(name);
  }

  list() {
    return Array.from(this.map.values());
  }
}
