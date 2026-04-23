import { DEFAULT_CONFIG } from './constants.js';
import { ValidationError } from './errors.js';
import { compileCollectionSchema } from './core/schemaCompiler.js';

export const createValidator = (collectionName, definition = null) => {
  const compiled = definition ? compileCollectionSchema(definition) : null;

  const validateId = (id, operation) => {
    if (!id || typeof id !== 'string' || id.trim() === '') {
      throw new ValidationError(
        DEFAULT_CONFIG.ERROR_CODES.INVALID_ID,
        `${operation}: ID must be a non-empty string`,
        { field: 'id', collection: collectionName, operation }
      );
    }
    return id.trim();
  };

  const validateData = (data, operation) => {
    if (!data || typeof data !== 'object' || Array.isArray(data)) {
      throw new ValidationError(
        DEFAULT_CONFIG.ERROR_CODES.INVALID_DATA,
        `${operation}: Data must be a non-null object`,
        { field: 'data', collection: collectionName, operation }
      );
    }
    if (compiled) {
      return operation === 'update'
        ? compiled.validateUpdate(data, null, { collection: collectionName, operation })
        : compiled.validateCreate(data, { collection: collectionName, operation });
    }
    return { ...data };
  };

  const validatePageSize = (pageSize) => {
    const size = Number(pageSize);
    if (Number.isNaN(size) || size <= 0 || size > DEFAULT_CONFIG.MAX_PAGE_SIZE) {
      throw new ValidationError(
        DEFAULT_CONFIG.ERROR_CODES.INVALID_PAGE_SIZE,
        `Page size must be a positive number between 1 and ${DEFAULT_CONFIG.MAX_PAGE_SIZE}`,
        { field: 'pageSize', collection: collectionName, operation: 'pagination' }
      );
    }
    return size;
  };

  return {
    validateId,
    validateData,
    validatePageSize,
  };
};

export const validateCreatePayload = (collectionDefinition, data) => compileCollectionSchema(collectionDefinition).validateCreate(data);
export const validateUpdatePayload = (collectionDefinition, data, existingDoc = null) => compileCollectionSchema(collectionDefinition).validateUpdate(data, existingDoc);
