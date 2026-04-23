import { DEFAULT_CONFIG } from '../constants.js';
import { deriveForeignKeyField, derivePrimaryKeyField } from '../core/relationNaming.js';
import { ShardProviderError, ValidationError } from '../errors.js';

function chunkArray(values = [], size = 10) {
  const chunks = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
}

function getRecordFieldValue(record, field) {
  if (!record) return undefined;
  if (field === 'id') return record.id;
  return record.data?.[field];
}

function flattenIncludedRecord(record) {
  if (!record) return null;
  return {
    id: record.id,
    shard: record.shard,
    ...(record.data || {}),
  };
}

function cloneRecord(record) {
  return {
    ...record,
    data: { ...(record?.data || {}) },
  };
}

function defaultShardRange() {
  const end = new Date();
  const start = new Date(Date.UTC(end.getUTCFullYear() - 1, end.getUTCMonth(), 1));
  return { start, end };
}

function resolveIncludeRange(include, options = {}) {
  return include.range || options.includeRange || options.range || defaultShardRange();
}

function resolveIncludeShardDate(include, options = {}) {
  return include.shardDate || options.includeShardDate || options.shardDate || null;
}

function sanitizeInlineInclude(include = {}) {
  return {
    key: include.key || include.name || include.as || include.collection,
    collection: include.collection || include.fromCollection || include.from,
    as: include.as || include.key || include.name || include.collection,
    localField: include.localField || deriveForeignKeyField(include.collection || include.fromCollection || include.from || ''),
    foreignField: include.foreignField || include.targetField || include.referencesField || derivePrimaryKeyField(include.collection || include.fromCollection || include.from || ''),
    many: Boolean(include.many || include.mode === 'many' || include.type === 'many'),
    includeDeleted: Boolean(include.includeDeleted),
    required: Boolean(include.required),
    limit: Number.isFinite(include.limit) && include.limit > 0 ? include.limit : null,
    orderBy: Array.isArray(include.orderBy)
      ? include.orderBy.map((item) => ({ field: item.field, direction: item.direction || 'asc' }))
      : [],
    range: include.range || null,
    shardDate: include.shardDate || null,
    includes: include.includes,
  };
}

export class IncludeEngine {
  constructor(provider) {
    this.provider = provider;
  }

  normalizeIncludeRequests(collectionName, includes = []) {
    const definition = this.provider.getCollectionDefinition(collectionName);
    const relationMap = definition.relations || definition.includes || {};
    const requested = Array.isArray(includes) ? includes : (includes ? [includes] : []);

    return requested.map((include) => {
      if (typeof include === 'string') {
        const relation = relationMap[include];
        if (!relation) {
          throw new ValidationError(
            DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT,
            `Include '${include}' is not configured on collection '${collectionName}'.`,
            { collection: collectionName, operation: 'includes', field: include }
          );
        }
        return relation;
      }

      if (!include || typeof include !== 'object') {
        throw new ValidationError(
          DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT,
          `Invalid include descriptor provided for collection '${collectionName}'.`,
          { collection: collectionName, operation: 'includes' }
        );
      }

      const relationKey = include.relation || include.key || include.name;
      const baseRelation = relationKey ? relationMap[relationKey] : null;
      const normalized = sanitizeInlineInclude(baseRelation ? { ...baseRelation, ...include } : include);

      if (!normalized.collection) {
        throw new ValidationError(
          DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT,
          `Include '${relationKey || normalized.as || 'unknown'}' requires a target collection.`,
          { collection: collectionName, operation: 'includes' }
        );
      }

      return normalized;
    });
  }

  async hydrateRecord(collectionName, record, includes = [], options = {}) {
    const hydrated = await this.hydrateRecords(collectionName, record ? [record] : [], includes, options);
    return hydrated[0] || record;
  }

  async hydrateRecords(collectionName, records = [], includes = [], options = {}) {
    if (!Array.isArray(records) || records.length === 0) return Array.isArray(records) ? records : [];
    const normalizedIncludes = this.normalizeIncludeRequests(collectionName, includes);
    if (normalizedIncludes.length === 0) return records;

    let hydrated = records.map(cloneRecord);
    for (const include of normalizedIncludes) {
      hydrated = await this.applyInclude(collectionName, hydrated, include, options);
    }
    return hydrated;
  }

  async applyInclude(sourceCollectionName, records = [], include = {}, options = {}) {
    const targetCollection = include.collection;
    const targetDefinition = this.provider.getCollectionDefinition(targetCollection);
    const localValues = [...new Set(records
      .map((record) => getRecordFieldValue(record, include.localField))
      .filter((value) => value !== undefined && value !== null && value !== ''))];

    const attachEmpty = (record) => ({
      ...record,
      data: {
        ...(record.data || {}),
        [include.as]: include.many ? [] : null,
      },
    });

    if (localValues.length === 0) return records.map(attachEmpty);

    let related = [];
    if (include.foreignField === 'id') {
      const fetched = await Promise.all(localValues.map((value) => this.provider.getById(targetCollection, String(value), {
        silentNotFound: true,
        includeDeleted: include.includeDeleted,
        shardDate: resolveIncludeShardDate(include, options),
        range: resolveIncludeRange(include, options),
        includes: include.includes,
      })));
      related = fetched.filter(Boolean);
    } else {
      if (!targetDefinition.schema[include.foreignField]) {
        throw new ValidationError(
          DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT,
          `Include '${include.as}' targets unknown field '${include.foreignField}' on collection '${targetCollection}'.`,
          { collection: sourceCollectionName, operation: 'includes', field: include.foreignField }
        );
      }

      const batches = chunkArray(localValues, 10);
      for (const values of batches) {
        const filters = [{
          field: include.foreignField,
          op: values.length === 1 ? '==' : 'in',
          value: values.length === 1 ? values[0] : values,
        }];
        const chunkResults = await this.provider.fetchByFilters(targetCollection, {
          filters,
          orderBy: include.orderBy || [],
          limit: include.limit || (include.many ? Math.max(values.length * 25, DEFAULT_CONFIG.DEFAULT_PAGE_SIZE) : values.length),
          range: resolveIncludeRange(include, options),
          includeDeleted: include.includeDeleted,
          includes: include.includes,
        }, {
          includeDeleted: include.includeDeleted,
          range: resolveIncludeRange(include, options),
          shardDate: resolveIncludeShardDate(include, options),
        });
        related.push(...chunkResults);
      }
    }

    const relationMap = new Map();
    related.forEach((record) => {
      const key = getRecordFieldValue(record, include.foreignField);
      if (key === undefined || key === null || key === '') return;
      if (include.many) {
        const bucket = relationMap.get(key) || [];
        bucket.push(flattenIncludedRecord(record));
        relationMap.set(key, bucket);
      } else if (!relationMap.has(key)) {
        relationMap.set(key, flattenIncludedRecord(record));
      }
    });

    return records.map((record) => {
      const localValue = getRecordFieldValue(record, include.localField);
      const relatedValue = relationMap.get(localValue);
      if (include.required && !relatedValue) {
        throw new ShardProviderError(
          DEFAULT_CONFIG.ERROR_CODES.NOT_FOUND,
          `Required include '${include.as}' could not be resolved for collection '${sourceCollectionName}'.`,
          { collection: sourceCollectionName, operation: 'includes', field: include.as }
        );
      }
      return {
        ...record,
        data: {
          ...(record.data || {}),
          [include.as]: include.many ? (Array.isArray(relatedValue) ? relatedValue : []) : (relatedValue || null),
        },
      };
    });
  }
}
