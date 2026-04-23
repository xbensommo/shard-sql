import { DEFAULT_CONFIG, FIELD_TYPES } from '../constants.js';
import { ValidationError } from '../errors.js';
import {
  buildFieldAliases,
  deriveForeignKeyField,
  derivePrimaryKeyField,
  uniqueFieldNames,
} from './relationNaming.js';

function registerAlias(map, alias, canonical, collectionName) {
  if (typeof alias !== 'string' || !alias.trim()) return;

  const cleanAlias = alias.trim();
  const existing = map[cleanAlias];
  if (existing && existing !== canonical) {
    throw new ValidationError(
      DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION,
      `Field alias '${cleanAlias}' is ambiguous in collection '${collectionName}'.`,
      { field: cleanAlias, collection: collectionName }
    );
  }

  map[cleanAlias] = canonical;
}

function normalizeFieldConfig(name, config = {}) {
  const normalized = typeof config === 'string' ? { type: config } : { ...config };
  normalized.name = name;
  normalized.type = normalized.type || FIELD_TYPES.ANY;
  normalized.required = Boolean(normalized.required);
  normalized.readonly = Boolean(normalized.readonly);
  normalized.immutable = Boolean(normalized.immutable);
  normalized.system = Boolean(normalized.system);
  normalized.filterable = Boolean(normalized.filterable);
  normalized.sortable = Boolean(normalized.sortable);
  normalized.searchable = Boolean(normalized.searchable);
  normalized.aliases = uniqueFieldNames([
    ...(Array.isArray(normalized.aliases) ? normalized.aliases : []),
    ...buildFieldAliases(name),
  ]).filter((alias) => alias !== name);
  normalized.default = normalized.default;
  normalized.enum = Array.isArray(normalized.enum) ? normalized.enum : undefined;
  return normalized;
}

function normalizeIdentity(input, collectionName, schema) {
  const legacyPrimaryKey = typeof input.primaryKey === 'object' && !Array.isArray(input.primaryKey)
    ? input.primaryKey
    : null;
  const identityInput = (input.identity && typeof input.identity === 'object' && !Array.isArray(input.identity))
    ? input.identity
    : {};

  const configuredField =
    identityInput.field
    || identityInput.primaryKey
    || legacyPrimaryKey?.field
    || (typeof input.primaryKey === 'string' ? input.primaryKey : null)
    || input.defaultPrimaryKey
    || null;

  const field = configuredField || derivePrimaryKeyField(collectionName);
  const aliases = uniqueFieldNames([
    ...(Array.isArray(identityInput.aliases) ? identityInput.aliases : []),
    ...(Array.isArray(legacyPrimaryKey?.aliases) ? legacyPrimaryKey.aliases : []),
    ...(Array.isArray(input.primaryKeyAliases) ? input.primaryKeyAliases : []),
    ...buildFieldAliases(field),
  ]).filter((alias) => alias !== field);

  const explicitEnabled = identityInput.enabled;
  const enabled = explicitEnabled !== undefined
    ? Boolean(explicitEnabled)
    : Boolean(configuredField || schema[field]);

  if (enabled && !schema[field]) {
    throw new ValidationError(
      DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION,
      `Primary key field '${field}' must exist in collection '${collectionName}'.`,
      { field, collection: collectionName }
    );
  }

  return Object.freeze({
    enabled,
    field,
    aliases,
    derived: !configuredField,
    immutable: identityInput.immutable ?? legacyPrimaryKey?.immutable ?? true,
    required: Boolean(identityInput.required ?? legacyPrimaryKey?.required),
  });
}

function normalizeRelationEntries(input) {
  return Object.entries(input.relations || input.includes || {});
}

function normalizeRelationConfig(key, config = {}, context = {}) {
  const relation = typeof config === 'string' ? { collection: config } : { ...config };
  const targetCollection = relation.collection || relation.fromCollection || relation.from;

  if (!targetCollection || typeof targetCollection !== 'string') {
    throw new ValidationError(
      DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION,
      `Relation '${key}' requires a target collection name.`,
      { field: `relations.${key}`, collection: context.collectionName }
    );
  }

  const localField = relation.localField || deriveForeignKeyField(targetCollection);
  const foreignField = relation.foreignField || relation.targetField || relation.referencesField || derivePrimaryKeyField(targetCollection);
  const as = relation.as || key;

  if (localField !== 'id' && !context.schema[localField]) {
    throw new ValidationError(
      DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION,
      `Relation '${key}' references missing local field '${localField}'.`,
      { field: `relations.${key}.localField`, collection: context.collectionName }
    );
  }

  return Object.freeze({
    key,
    collection: targetCollection,
    as,
    localField,
    foreignField,
    aliases: uniqueFieldNames([
      ...(Array.isArray(relation.aliases) ? relation.aliases : []),
      ...buildFieldAliases(localField),
    ]).filter((alias) => alias !== localField),
    many: Boolean(relation.many || relation.mode === 'many' || relation.type === 'many'),
    includeDeleted: Boolean(relation.includeDeleted),
    required: Boolean(relation.required),
    limit: Number.isFinite(relation.limit) && relation.limit > 0 ? relation.limit : null,
    orderBy: Array.isArray(relation.orderBy)
      ? relation.orderBy.map((item) => ({ field: item.field, direction: item.direction || 'asc' }))
      : [],
    range: relation.range || null,
    shardDate: relation.shardDate || null,
    description: relation.description || null,
  });
}

function buildFieldAliasMap(collectionName, schema, identity, relations) {
  const aliases = {};

  Object.entries(schema).forEach(([field, config]) => {
    registerAlias(aliases, field, field, collectionName);
    buildFieldAliases(field).forEach((alias) => registerAlias(aliases, alias, field, collectionName));
    (config.aliases || []).forEach((alias) => registerAlias(aliases, alias, field, collectionName));
  });

  if (identity?.enabled) {
    registerAlias(aliases, identity.field, identity.field, collectionName);
    (identity.aliases || []).forEach((alias) => registerAlias(aliases, alias, identity.field, collectionName));
  }

  Object.values(relations || {}).forEach((relation) => {
    registerAlias(aliases, relation.localField, relation.localField, collectionName);
    (relation.aliases || []).forEach((alias) => registerAlias(aliases, alias, relation.localField, collectionName));
  });

  return Object.freeze({ ...aliases });
}

export function defineCollection(input = {}) {
  if (!input.name || typeof input.name !== 'string') {
    throw new ValidationError(
      DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION,
      'Collection definition requires a non-empty name.',
      { field: 'name', collection: input.name || null }
    );
  }

  const schemaEntries = Object.entries(input.schema || {}).map(([name, config]) => [name, normalizeFieldConfig(name, config)]);
  const schema = Object.fromEntries(schemaEntries);

  const shard = {
    type: input.shard?.type || DEFAULT_CONFIG.SHARD_TYPES.NONE,
    field: input.shard?.field || DEFAULT_CONFIG.DEFAULT_DATE_FIELD,
    format: input.shard?.format || null,
    strategy: input.shard?.strategy || DEFAULT_CONFIG.DEFAULT_SHARD_STRATEGY,
    root: input.shard?.root || `${input.name}Shards`,
    collectionId: input.shard?.collectionId || input.path || input.name,
    metadataField: input.shard?.metadataField || DEFAULT_CONFIG.DEFAULT_SHARD_METADATA_FIELD,
    legacyReadFallback: input.shard?.legacyReadFallback || null,
  };

  if (!Object.values(DEFAULT_CONFIG.SHARD_TYPES).includes(shard.type)) {
    throw new ValidationError(
      DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION,
      `Unsupported shard type: ${shard.type}`,
      { field: 'shard.type', collection: input.name }
    );
  }

  if (!Object.values(DEFAULT_CONFIG.SHARD_STRATEGIES).includes(shard.strategy)) {
    throw new ValidationError(
      DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION,
      `Unsupported shard strategy: ${shard.strategy}`,
      { field: 'shard.strategy', collection: input.name }
    );
  }

  const search = {
    mode: input.search?.mode || DEFAULT_CONFIG.SEARCH_MODES.NONE,
    fields: Array.isArray(input.search?.fields) ? [...new Set(input.search.fields)] : [],
  };

  const backend = {
    engine: input.backend?.engine || input.storage?.engine || DEFAULT_CONFIG.BACKENDS.SQL,
    table: input.backend?.table || input.storage?.table || input.path || input.name,
    connector: input.backend?.connector || input.storage?.connector || null,
    schema: input.backend?.schema || input.storage?.schema || null,
  };

  if (!Object.values(DEFAULT_CONFIG.SEARCH_MODES).includes(search.mode)) {
    throw new ValidationError(
      DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION,
      `Unsupported search mode: ${search.mode}`,
      { field: 'search.mode', collection: input.name }
    );
  }

  const writableFields = Array.isArray(input.writableFields)
    ? [...new Set(input.writableFields)]
    : Object.keys(schema).filter((key) => !schema[key].readonly && !schema[key].system);

  const updateableFields = Array.isArray(input.updateableFields)
    ? [...new Set(input.updateableFields)]
    : writableFields.filter((key) => !schema[key]?.immutable);

  const identity = normalizeIdentity(input, input.name, schema);
  const relations = Object.freeze(Object.fromEntries(
    normalizeRelationEntries(input).map(([key, config]) => [
      key,
      normalizeRelationConfig(key, config, { collectionName: input.name, schema, identity }),
    ])
  ));
  const fieldAliases = buildFieldAliasMap(input.name, schema, identity, relations);

  return Object.freeze({
    kind: 'ShardProviderCollectionDefinition',
    name: input.name,
    path: input.path || input.name,
    shard,
    schema,
    writableFields,
    updateableFields,
    indexes: Array.isArray(input.indexes)
      ? input.indexes.map((idx) => (Array.isArray(idx) ? [...idx] : { ...idx }))
      : [],
    search,
    backend,
    storage: backend,
    rules: { ...(input.rules || {}) },
    metadata: { ...(input.metadata || {}) },
    identity,
    primaryKey: identity,
    fieldAliases,
    relations,
    includes: relations,
    legacy: Boolean(input.legacy),
  });
}
