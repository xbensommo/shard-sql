import { DEFAULT_CONFIG } from '../constants.js';
import { ValidationError } from '../errors.js';

const DEFAULT_PRIMARY_KEY_SUFFIX = '_id';

function uniqueStrings(values = []) {
  return [
    ...new Set(
      values
        .filter((value) => typeof value === 'string' && value.trim())
        .map((value) => value.trim())
    ),
  ];
}

export function toSnakeCase(value = '') {
  return String(value)
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/[\s\-]+/g, '_')
    .replace(/__+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase();
}

export function toCamelCase(value = '') {
  const normalized = String(value).trim();
  if (!normalized) return '';
  if (!normalized.includes('_') && !normalized.includes('-') && !normalized.includes(' ')) {
    return normalized;
  }

  return normalized
    .toLowerCase()
    .replace(/[_\-\s]+([a-z0-9])/g, (_, char) => char.toUpperCase());
}

export function singularizeCollectionName(collectionName = '') {
  const normalized = toSnakeCase(collectionName);
  if (!normalized) return '';

  if (normalized.endsWith('ies') && normalized.length > 3) {
    return `${normalized.slice(0, -3)}y`;
  }

  if (/(xes|zes|ches|shes|sses)$/.test(normalized)) {
    return normalized.slice(0, -2);
  }

  if (normalized.endsWith('s') && !normalized.endsWith('ss')) {
    return normalized.slice(0, -1);
  }

  return normalized;
}

export function derivePrimaryKeyField(collectionName, options = {}) {
  const suffix =
    typeof options.suffix === 'string' && options.suffix.trim()
      ? options.suffix.trim()
      : DEFAULT_PRIMARY_KEY_SUFFIX;

  const singular = singularizeCollectionName(collectionName);
  return `${singular || toSnakeCase(collectionName)}${suffix}`;
}

export function deriveForeignKeyField(collectionName, options = {}) {
  return derivePrimaryKeyField(collectionName, options);
}

export function buildFieldAliases(field = '') {
  return uniqueStrings([field, toSnakeCase(field), toCamelCase(field)]);
}

export function uniqueFieldNames(values = []) {
  return uniqueStrings(values);
}

function registerAlias(map, alias, canonical, collectionName) {
  if (typeof alias !== 'string' || !alias.trim()) return;

  const cleanAlias = alias.trim();
  const existing = map[cleanAlias];

  if (existing && existing !== canonical) {
    throw new ValidationError(
      DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION,
      `Field alias '${cleanAlias}' is ambiguous in collection '${collectionName}'.`,
      { collection: collectionName, field: cleanAlias }
    );
  }

  map[cleanAlias] = canonical;
}

function resolvePrimaryKeyInput(primaryKeyInput) {
  if (!primaryKeyInput) return null;

  if (typeof primaryKeyInput === 'string') {
    return { field: primaryKeyInput.trim() };
  }

  if (typeof primaryKeyInput === 'object' && !Array.isArray(primaryKeyInput)) {
    return { ...primaryKeyInput };
  }

  return null;
}

export function normalizePrimaryKeyConfig(collectionName, schema = {}, primaryKeyInput = null) {
  const explicit = resolvePrimaryKeyInput(primaryKeyInput);
  const derivedField = derivePrimaryKeyField(collectionName, explicit || {});
  const enabled = Boolean(explicit) || Boolean(schema[derivedField]);
  const field = explicit?.field ? explicit.field.trim() : derivedField;

  if (enabled && !schema[field]) {
    throw new ValidationError(
      DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION,
      `Primary key field '${field}' must exist in the schema for collection '${collectionName}'.`,
      { collection: collectionName, field }
    );
  }

  const aliases = uniqueStrings([
    ...(explicit?.aliases || []),
    toSnakeCase(field),
    toCamelCase(field),
  ]).filter((alias) => alias !== field);

  return Object.freeze({
    enabled,
    field,
    aliases,
    derivedField,
    source: explicit ? 'explicit' : enabled ? 'derived' : 'disabled',
    immutable: explicit?.immutable !== false,
    required: Boolean(explicit?.required),
  });
}

export function buildFieldAliasMap(collectionName, schema = {}, primaryKey = null, relations = {}) {
  const aliases = {};

  Object.keys(schema).forEach((field) => {
    registerAlias(aliases, field, field, collectionName);
    registerAlias(aliases, toSnakeCase(field), field, collectionName);
    registerAlias(aliases, toCamelCase(field), field, collectionName);
  });

  if (primaryKey?.enabled) {
    registerAlias(aliases, primaryKey.field, primaryKey.field, collectionName);
    (primaryKey.aliases || []).forEach((alias) => {
      registerAlias(aliases, alias, primaryKey.field, collectionName);
    });
  }

  Object.values(relations || {}).forEach((relation) => {
    registerAlias(aliases, relation.localField, relation.localField, collectionName);
    (relation.aliases || []).forEach((alias) => {
      registerAlias(aliases, alias, relation.localField, collectionName);
    });
  });

  return Object.freeze({ ...aliases });
}

export function normalizeRelationsConfig(
  collectionName,
  schema = {},
  relationsInput = {},
  knownFieldAliases = {}
) {
  if (!relationsInput || typeof relationsInput !== 'object' || Array.isArray(relationsInput)) {
    return Object.freeze({});
  }

  const normalized = {};

  Object.entries(relationsInput).forEach(([localFieldInput, config]) => {
    if (!config || typeof config !== 'object' || Array.isArray(config)) {
      throw new ValidationError(
        DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION,
        `Relation '${localFieldInput}' in collection '${collectionName}' must be an object.`,
        { collection: collectionName, field: localFieldInput }
      );
    }

    const localField = knownFieldAliases[localFieldInput] || localFieldInput;

    if (!schema[localField]) {
      throw new ValidationError(
        DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION,
        `Relation field '${localFieldInput}' must exist in the schema for collection '${collectionName}'.`,
        { collection: collectionName, field: localFieldInput }
      );
    }

    const targetCollection = config.collection || config.fromCollection || config.references || null;

    if (!targetCollection || typeof targetCollection !== 'string') {
      throw new ValidationError(
        DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION,
        `Relation '${localFieldInput}' in collection '${collectionName}' requires a target collection.`,
        { collection: collectionName, field: localFieldInput }
      );
    }

    const targetField =
      typeof config.targetField === 'string' && config.targetField.trim()
        ? config.targetField.trim()
        : derivePrimaryKeyField(targetCollection);

    normalized[localField] = Object.freeze({
      localField,
      collection: targetCollection.trim(),
      targetField,
      aliases: uniqueStrings([
        ...(config.aliases || []),
        toSnakeCase(localField),
        toCamelCase(localField),
      ]).filter((alias) => alias !== localField),
    });
  });

  return Object.freeze(normalized);
}

export function resolveCanonicalField(definition, fieldName) {
  if (typeof fieldName !== 'string' || !fieldName.trim()) return fieldName;
  const cleanField = fieldName.trim();
  return definition?.fieldAliases?.[cleanField] || cleanField;
}

function aliasConflictMessage(rawField, canonicalField, collection, operation) {
  return `Field '${rawField}' conflicts with canonical field '${canonicalField}' in collection '${collection}'.`;
}

export function normalizeDataFieldAliases(definition, data = {}, options = {}) {
  if (!data || typeof data !== 'object' || Array.isArray(data)) return data;

  const collection = options.collection || definition?.name || null;
  const operation = options.operation || 'normalizeData';
  const normalized = {};
  const seen = new Map();

  Object.entries(data).forEach(([rawField, value]) => {
    const canonicalField = resolveCanonicalField(definition, rawField);
    const seenFrom = seen.get(canonicalField);

    if (seenFrom && seenFrom !== rawField) {
      throw new ValidationError(
        DEFAULT_CONFIG.ERROR_CODES.INVALID_DATA,
        aliasConflictMessage(rawField, canonicalField, collection, operation),
        { collection, operation, field: rawField }
      );
    }

    seen.set(canonicalField, rawField);
    normalized[canonicalField] = value;
  });

  return normalized;
}

export function normalizeQueryFieldAliases(definition, queryInput = {}, options = {}) {
  const collection = options.collection || definition?.name || null;
  const operation = options.operation || 'query';
  const normalized = { ...queryInput };
  const seen = new Map();

  const rewriteFieldEntries = (entries = []) =>
    entries.map((entry) => {
      if (!entry || typeof entry !== 'object') return entry;

      const rawField = entry.field;
      const canonicalField = resolveCanonicalField(definition, rawField);
      const seenFrom = seen.get(canonicalField);

      if (rawField && seenFrom && seenFrom !== rawField) {
        throw new ValidationError(
          DEFAULT_CONFIG.ERROR_CODES.INVALID_DATA,
          aliasConflictMessage(rawField, canonicalField, collection, operation),
          { collection, operation, field: rawField }
        );
      }

      if (rawField) seen.set(canonicalField, rawField);
      return { ...entry, field: canonicalField };
    });

  normalized.filters = rewriteFieldEntries(Array.isArray(queryInput.filters) ? queryInput.filters : []);
  normalized.orderBy = rewriteFieldEntries(Array.isArray(queryInput.orderBy) ? queryInput.orderBy : []);

  return normalized;
}