import { DEFAULT_CONFIG, FIELD_TYPES } from '../constants.js';
import { ValidationError } from '../errors.js';
import { normalizeDataFieldAliases } from './relationNaming.js';

function isPlainObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function isTimestampLike(value) {
  if (!value) return false;
  if (value instanceof Date) return true;
  if (typeof value === 'string' || typeof value === 'number') return !Number.isNaN(new Date(value).getTime());
  if (typeof value === 'object' && typeof value.toDate === 'function') return true;
  if (typeof value === 'object' && 'seconds' in value && 'nanoseconds' in value) return true;
  return false;
}

function validateType(fieldName, value, config, collection, operation) {
  if (value === undefined || value === null) return;
  const type = config.type;

  const fail = () => {
    throw new ValidationError(
      DEFAULT_CONFIG.ERROR_CODES.INVALID_FIELD_TYPE,
      `Field '${fieldName}' must be of type '${type}'.`,
      { field: fieldName, collection, operation }
    );
  };

  switch (type) {
    case FIELD_TYPES.ANY:
      return;
    case FIELD_TYPES.STRING:
      if (typeof value !== 'string') fail();
      return;
    case FIELD_TYPES.NUMBER:
      if (typeof value !== 'number' || Number.isNaN(value)) fail();
      return;
    case FIELD_TYPES.BOOLEAN:
      if (typeof value !== 'boolean') fail();
      return;
    case FIELD_TYPES.ARRAY:
      if (!Array.isArray(value)) fail();
      return;
    case FIELD_TYPES.OBJECT:
      if (!isPlainObject(value)) fail();
      return;
    case FIELD_TYPES.TIMESTAMP:
      if (!isTimestampLike(value)) fail();
      return;
    case FIELD_TYPES.REFERENCE:
      if (!isPlainObject(value) && typeof value !== 'string') fail();
      return;
    default:
      return;
  }
}

function resolveDefault(fieldConfig) {
  if (typeof fieldConfig.default === 'function') return fieldConfig.default();
  if (fieldConfig.default === 'serverTimestamp') return new Date();
  return fieldConfig.default;
}

export function compileCollectionSchema(definition) {
  const schema = definition?.schema || {};
  const fields = Object.keys(schema);
  const writableFields = new Set(definition?.writableFields || []);
  const updateableFields = new Set(definition?.updateableFields || []);
  const identity = definition?.identity || definition?.primaryKey || null;

  function assertKnownFields(data, collection, operation) {
    Object.keys(data).forEach((field) => {
      if (!schema[field]) {
        throw new ValidationError(
          DEFAULT_CONFIG.ERROR_CODES.UNKNOWN_FIELD,
          `Field '${field}' is not allowed in collection '${collection}'.`,
          { field, collection, operation }
        );
      }
    });
  }

  function validateEnums(fieldName, value, config, collection, operation) {
    if (value === undefined || value === null || !config.enum) return;
    if (!config.enum.includes(value)) {
      throw new ValidationError(
        DEFAULT_CONFIG.ERROR_CODES.ENUM_MISMATCH,
        `Field '${fieldName}' must be one of: ${config.enum.join(', ')}`,
        { field: fieldName, collection, operation }
      );
    }
  }

  function applyDefaults(data) {
    const result = { ...data };
    fields.forEach((field) => {
      const config = schema[field];
      if (result[field] === undefined && config.default !== undefined) {
        result[field] = resolveDefault(config);
      }
    });
    return result;
  }

  function normalizeInputData(data, collection, operation) {
    return normalizeDataFieldAliases(definition, data, { collection, operation });
  }

  function validateCreate(data, options = {}) {
    const collection = options.collection || definition.name;
    const operation = options.operation || 'create';
    if (!isPlainObject(data)) {
      throw new ValidationError(
        DEFAULT_CONFIG.ERROR_CODES.INVALID_DATA,
        'Create payload must be a non-null object.',
        { collection, operation, field: 'data' }
      );
    }

    const normalizedData = normalizeInputData(data, collection, operation);
    const payload = applyDefaults(normalizedData);
    assertKnownFields(payload, collection, operation);

    fields.forEach((field) => {
      const config = schema[field];
      if (config.required && (payload[field] === undefined || payload[field] === null || payload[field] === '')) {
        throw new ValidationError(
          DEFAULT_CONFIG.ERROR_CODES.REQUIRED_FIELD,
          `Field '${field}' is required.`,
          { field, collection, operation }
        );
      }
      if (payload[field] !== undefined) {
        if (!writableFields.has(field) && !config.system) {
          throw new ValidationError(
            DEFAULT_CONFIG.ERROR_CODES.READONLY_FIELD,
            `Field '${field}' cannot be set directly.`,
            { field, collection, operation }
          );
        }
        validateType(field, payload[field], config, collection, operation);
        validateEnums(field, payload[field], config, collection, operation);
      }
    });

    if (identity?.enabled && identity.required && (payload[identity.field] === undefined || payload[identity.field] === null || payload[identity.field] === '')) {
      throw new ValidationError(
        DEFAULT_CONFIG.ERROR_CODES.REQUIRED_FIELD,
        `Primary key field '${identity.field}' is required.`,
        { field: identity.field, collection, operation }
      );
    }

    return payload;
  }

  function validateUpdate(data, existingDoc = null, options = {}) {
    const collection = options.collection || definition.name;
    const operation = options.operation || 'update';
    if (!isPlainObject(data)) {
      throw new ValidationError(
        DEFAULT_CONFIG.ERROR_CODES.INVALID_DATA,
        'Update payload must be a non-null object.',
        { collection, operation, field: 'data' }
      );
    }

    const normalizedData = normalizeInputData(data, collection, operation);
    assertKnownFields(normalizedData, collection, operation);

    Object.keys(normalizedData).forEach((field) => {
      const config = schema[field];
      if (!updateableFields.has(field) || config.readonly || config.system) {
        throw new ValidationError(
          DEFAULT_CONFIG.ERROR_CODES.READONLY_FIELD,
          `Field '${field}' cannot be updated.`,
          { field, collection, operation }
        );
      }
      if (config.immutable && existingDoc && existingDoc[field] !== undefined && existingDoc[field] !== normalizedData[field]) {
        throw new ValidationError(
          DEFAULT_CONFIG.ERROR_CODES.IMMUTABLE_FIELD,
          `Field '${field}' is immutable and cannot be changed.`,
          { field, collection, operation }
        );
      }
      if (identity?.enabled && identity.immutable && field === identity.field && existingDoc && existingDoc[field] !== undefined && existingDoc[field] !== normalizedData[field]) {
        throw new ValidationError(
          DEFAULT_CONFIG.ERROR_CODES.IMMUTABLE_FIELD,
          `Primary key field '${field}' is immutable and cannot be changed.`,
          { field, collection, operation }
        );
      }
      validateType(field, normalizedData[field], config, collection, operation);
      validateEnums(field, normalizedData[field], config, collection, operation);
    });

    return { ...normalizedData };
  }

  return {
    definition,
    fields,
    schema,
    writableFields,
    updateableFields,
    validateCreate,
    validateUpdate,
    applyDefaults,
  };
}
