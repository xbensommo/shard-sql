import { DEFAULT_CONFIG } from '../constants.js';
import { createLegacyCollectionDefinition } from '../compat/legacyCollectionConfig.js';
import { normalizeProviderOptions } from '../compat/legacyProviderBridge.js';
import { mapAdapterError } from '../core/providerErrorMapper.js';
import { CollectionRegistry } from '../core/registry.js';
import { derivePrimaryKeyField } from '../core/relationNaming.js';
import { ShardProviderError, ValidationError } from '../errors.js';
import { prepareSearchFields } from '../search/searchStrategies.js';
import { CollectionRuntime } from './collectionRuntime.js';
import { IncludeEngine } from './includeEngine.js';
import { runSafeCallback } from './safeCallback.js';

function ensureStringId(id, operation, collectionName) {
  if (!id || typeof id !== 'string' || id.trim() === '') {
    throw new ValidationError(
      DEFAULT_CONFIG.ERROR_CODES.INVALID_ID,
      `${operation}: ID must be a non-empty string.`,
      { field: 'id', collection: collectionName, operation }
    );
  }
  return id.trim();
}

function defaultRange() {
  const end = new Date();
  const start = new Date(Date.UTC(end.getUTCFullYear() - 1, end.getUTCMonth(), 1));
  return { start, end };
}

function normalizePageSize(value) {
  const parsed = Number(value || DEFAULT_CONFIG.DEFAULT_PAGE_SIZE);
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_CONFIG.DEFAULT_PAGE_SIZE;
  return Math.min(Math.floor(parsed), DEFAULT_CONFIG.MAX_PAGE_SIZE);
}

function nowIso() {
  return new Date().toISOString();
}

function buildSoftDeletePayload(options = {}, isDeleted = true) {
  const deletedBy = options.deletedBy ?? options.userId ?? options.uid ?? null;
  return {
    [DEFAULT_CONFIG.SOFT_DELETE_FIELD]: isDeleted,
    [DEFAULT_CONFIG.DELETED_AT_FIELD]: isDeleted ? nowIso() : null,
    [DEFAULT_CONFIG.DELETED_BY_FIELD]: isDeleted ? deletedBy : null,
    updatedAt: nowIso(),
  };
}

function coerceRecordId(record, definition, collectionName) {
  if (!record) return null;
  const identityField = definition?.identity?.field || derivePrimaryKeyField(collectionName);

  if (typeof record.id === 'string' && record.id.trim()) return record.id.trim();

  const data = record.data && typeof record.data === 'object' ? record.data : record;
  const candidate = data?.[identityField] ?? data?.id;
  if (candidate === undefined || candidate === null || candidate === '') return null;
  return String(candidate).trim();
}

function normalizeRecord(record, definition, collectionName) {
  if (!record) return null;
  if (record.id && record.data && typeof record.data === 'object') {
    return {
      id: String(record.id),
      shard: record.shard ?? null,
      data: { ...record.data },
      ...(record.softDeleted !== undefined ? { softDeleted: record.softDeleted } : {}),
      ...(record.restored !== undefined ? { restored: record.restored } : {}),
      ...(record.destroyed !== undefined ? { destroyed: record.destroyed } : {}),
      ...(record.hardDeleted !== undefined ? { hardDeleted: record.hardDeleted } : {}),
    };
  }

  if (typeof record !== 'object' || Array.isArray(record)) {
    throw new ValidationError(
      DEFAULT_CONFIG.ERROR_CODES.INVALID_DATA,
      `SQL adapter for '${collectionName}' returned an invalid record shape.`,
      { collection: collectionName, operation: 'normalizeRecord' }
    );
  }

  const id = coerceRecordId(record, definition, collectionName);
  if (!id) {
    throw new ValidationError(
      DEFAULT_CONFIG.ERROR_CODES.INVALID_ID,
      `SQL adapter for '${collectionName}' must return an id or identity field value.`,
      { collection: collectionName, operation: 'normalizeRecord', field: definition?.identity?.field || 'id' }
    );
  }

  return {
    id,
    shard: record.shard ?? null,
    data: { ...record },
  };
}

function normalizeRecordList(records, definition, collectionName) {
  return (Array.isArray(records) ? records : []).filter(Boolean).map((record) => normalizeRecord(record, definition, collectionName));
}

function normalizePageResult(result, definition, collectionName, queryInput = {}) {
  if (Array.isArray(result)) {
    const items = normalizeRecordList(result, definition, collectionName);
    const offset = Number.isFinite(Number(queryInput.offset)) ? Math.max(0, Number(queryInput.offset)) : 0;
    const pageSize = normalizePageSize(queryInput.limit || queryInput.pageSize);
    return {
      items,
      total: items.length,
      hasMore: false,
      lastVisible: null,
      pagination: {
        mode: 'offset',
        offset,
        pageSize,
        nextOffset: null,
        hasMore: false,
        total: items.length,
      },
      filters: Array.isArray(queryInput.filters) ? queryInput.filters : [],
      activeFilters: Array.isArray(queryInput.filters) ? queryInput.filters : [],
      orderBy: Array.isArray(queryInput.orderBy) ? queryInput.orderBy : [],
      pageSize,
      includeDeleted: queryInput.includeDeleted === true,
      range: queryInput.range || null,
      queriedShards: [],
    };
  }

  const items = normalizeRecordList(result?.items, definition, collectionName);
  const total = typeof result?.total === 'number' ? result.total : items.length;
  const pageSize = normalizePageSize(result?.pageSize || result?.pagination?.pageSize || queryInput.limit || queryInput.pageSize);
  const offset = Number.isFinite(Number(result?.pagination?.offset))
    ? Number(result.pagination.offset)
    : (Number.isFinite(Number(queryInput.offset)) ? Math.max(0, Number(queryInput.offset)) : 0);
  const hasMore = typeof result?.hasMore === 'boolean'
    ? result.hasMore
    : (offset + items.length) < total;
  const nextOffset = result?.pagination?.nextOffset ?? (hasMore ? offset + items.length : null);

  return {
    items,
    total,
    hasMore,
    lastVisible: nextOffset,
    pagination: {
      mode: result?.pagination?.mode || 'offset',
      offset,
      pageSize,
      nextOffset,
      hasMore,
      total,
      ...(result?.pagination && typeof result.pagination === 'object' ? result.pagination : {}),
    },
    filters: Array.isArray(result?.filters) ? result.filters : (Array.isArray(queryInput.filters) ? queryInput.filters : []),
    activeFilters: Array.isArray(result?.activeFilters) ? result.activeFilters : (Array.isArray(queryInput.filters) ? queryInput.filters : []),
    orderBy: Array.isArray(result?.orderBy) ? result.orderBy : (Array.isArray(queryInput.orderBy) ? queryInput.orderBy : []),
    pageSize,
    includeDeleted: result?.includeDeleted === true || queryInput.includeDeleted === true,
    range: result?.range || queryInput.range || null,
    queriedShards: Array.isArray(result?.queriedShards) ? result.queriedShards : [],
  };
}

export class SqlShardProvider {
  constructor(rawOptions = {}) {
    const options = normalizeProviderOptions(rawOptions);
    if (!options.adapter) {
      throw new ValidationError(
        DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT,
        'SqlShardProvider requires an adapter.',
        { field: 'adapter', operation: 'constructor' }
      );
    }

    this.adapter = options.adapter;
    this.engine = options.engine || options.backend || DEFAULT_CONFIG.BACKENDS.SQL;
    this.environment = options.environment || process?.env?.NODE_ENV || DEFAULT_CONFIG.ENVIRONMENTS.PRODUCTION;
    this.productionSafeErrors = options.productionSafeErrors !== false;
    this.strictSchemas = Boolean(options.strictSchemas);
    this.nonShardedCollections = Array.isArray(options.nonShardedCollections) ? options.nonShardedCollections : [];
    this.onError = options.onError;
    this.registry = new CollectionRegistry(options.collections || []);
    this.runtimeCache = new Map();
    this.includeEngine = new IncludeEngine(this);
    this.maxBatchSize = Number.isInteger(options.maxBatchSize) && options.maxBatchSize > 0 ? options.maxBatchSize : null;
  }

  isProduction() {
    return this.environment === DEFAULT_CONFIG.ENVIRONMENTS.PRODUCTION;
  }

  registerCollection(definition) {
    const normalized = this.registry.register(definition);
    this.runtimeCache.delete(normalized.name);
    return normalized;
  }

  listCollections() {
    return this.registry.list();
  }

  getCollectionDefinition(collectionName) {
    return this.registry.get(collectionName)
      || createLegacyCollectionDefinition(collectionName, {
        nonShardedCollections: this.nonShardedCollections,
      });
  }

  getCollectionRuntime(collectionName) {
    if (!this.runtimeCache.has(collectionName)) {
      this.runtimeCache.set(collectionName, new CollectionRuntime(this.getCollectionDefinition(collectionName)));
    }
    return this.runtimeCache.get(collectionName);
  }

  supportsSoftDelete(definition) {
    return definition?.metadata?.softDelete !== false;
  }

  normalizeBulkIds(collectionNameOrIds = null, idsOrCollectionName = null, operation = 'bulkMutation') {
    const collectionName = Array.isArray(collectionNameOrIds)
      ? (typeof idsOrCollectionName === 'string' ? idsOrCollectionName : null)
      : (typeof collectionNameOrIds === 'string' ? collectionNameOrIds : null);

    const ids = Array.isArray(collectionNameOrIds)
      ? collectionNameOrIds
      : (Array.isArray(idsOrCollectionName) ? idsOrCollectionName : null);

    if (!Array.isArray(ids)) {
      throw new ValidationError(
        DEFAULT_CONFIG.ERROR_CODES.INVALID_ID,
        'Bulk mutation requires an array of document ids.',
        { field: 'ids', collection: collectionName, operation }
      );
    }

    const normalized = [];
    const seen = new Set();
    ids.forEach((rawId) => {
      const cleanId = typeof rawId === 'string' ? rawId.trim() : '';
      if (!cleanId) return;
      if (!seen.has(cleanId)) {
        seen.add(cleanId);
        normalized.push(cleanId);
      }
    });

    if (normalized.length === 0) {
      throw new ValidationError(
        DEFAULT_CONFIG.ERROR_CODES.INVALID_ID,
        'Bulk mutation requires at least one valid document id.',
        { field: 'ids', collection: collectionName, operation }
      );
    }

    return normalized;
  }

  async applyBulkMutation(collectionName, ids = [], mutator, options = {}) {
    const validIds = this.normalizeBulkIds(ids, collectionName, options.operation || 'bulkMutation');
    const results = [];
    for (const id of validIds) {
      const result = await mutator(id);
      results.push(result);
    }
    return results;
  }

  resolveCollectionAdapter(collectionName) {
    const definition = this.getCollectionDefinition(collectionName);
    const resolved = typeof this.adapter.resolveCollection === 'function'
      ? this.adapter.resolveCollection(collectionName, definition, this)
      : this.adapter.collections?.[collectionName] || this.adapter[collectionName];

    if (!resolved || typeof resolved !== 'object') {
      throw new ValidationError(
        DEFAULT_CONFIG.ERROR_CODES.COLLECTION_NOT_REGISTERED,
        `No SQL adapter is registered for collection '${collectionName}'.`,
        { collection: collectionName, operation: 'resolveCollectionAdapter' }
      );
    }

    return resolved;
  }

  handleError(error, context = {}) {
    const mapped = error instanceof ShardProviderError ? error : mapAdapterError(error, context);
    runSafeCallback(this.onError, mapped, null);
    runSafeCallback(this.adapter?.onError, mapped, context);
    throw mapped;
  }

  normalizeIncludeRequests(collectionName, includes = []) {
    return this.includeEngine.normalizeIncludeRequests(collectionName, includes);
  }

  async hydrateRecord(collectionName, record, includes = [], options = {}) {
    return this.includeEngine.hydrateRecord(collectionName, record, includes, options);
  }

  async hydrateRecords(collectionName, records = [], includes = [], options = {}) {
    return this.includeEngine.hydrateRecords(collectionName, records, includes, options);
  }

  async applyInclude(sourceCollectionName, records = [], include = {}, options = {}) {
    return this.includeEngine.applyInclude(sourceCollectionName, records, include, options);
  }

  async create(collectionName, data, options = {}) {
    try {
      const runtime = this.getCollectionRuntime(collectionName);
      const definition = runtime.definition;
      const operations = this.resolveCollectionAdapter(collectionName);
      if (typeof operations.create !== 'function') {
        throw new ValidationError(
          DEFAULT_CONFIG.ERROR_CODES.QUERY_NOT_ALLOWED,
          `SQL adapter for '${collectionName}' does not implement create().`,
          { collection: collectionName, operation: 'create' }
        );
      }

      const payload = this.strictSchemas || !definition.legacy
        ? runtime.validateCreate(data, { collection: collectionName, operation: 'create' })
        : { ...data };

      const finalPayload = {
        ...payload,
        ...prepareSearchFields(definition, payload),
        createdAt: payload.createdAt || nowIso(),
        updatedAt: nowIso(),
        ...(this.supportsSoftDelete(definition) ? { [DEFAULT_CONFIG.SOFT_DELETE_FIELD]: false } : {}),
      };

      const created = normalizeRecord(
        await operations.create({ collectionName, definition, data: finalPayload, options, provider: this }),
        definition,
        collectionName
      );
      return options.includes ? this.hydrateRecord(collectionName, created, options.includes, options) : created;
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'create' });
    }
  }

  async set(collectionName, id, data, options = {}) {
    try {
      const cleanId = ensureStringId(id, 'set', collectionName);
      const runtime = this.getCollectionRuntime(collectionName);
      const definition = runtime.definition;
      const operations = this.resolveCollectionAdapter(collectionName);
      if (typeof operations.set !== 'function') {
        throw new ValidationError(
          DEFAULT_CONFIG.ERROR_CODES.QUERY_NOT_ALLOWED,
          `SQL adapter for '${collectionName}' does not implement set().`,
          { collection: collectionName, operation: 'set' }
        );
      }

      const payload = this.strictSchemas || !definition.legacy
        ? runtime.validateCreate(data, { collection: collectionName, operation: 'set' })
        : { ...data };

      const finalPayload = {
        ...payload,
        ...prepareSearchFields(definition, payload),
        updatedAt: nowIso(),
        ...(this.supportsSoftDelete(definition) && payload[DEFAULT_CONFIG.SOFT_DELETE_FIELD] === undefined
          ? { [DEFAULT_CONFIG.SOFT_DELETE_FIELD]: false }
          : {}),
      };

      const written = normalizeRecord(
        await operations.set({ collectionName, definition, id: cleanId, data: finalPayload, options, provider: this }),
        definition,
        collectionName
      );
      return options.includes ? this.hydrateRecord(collectionName, written, options.includes, options) : written;
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'set' });
    }
  }

  async update(collectionName, id, data, options = {}) {
    try {
      const cleanId = ensureStringId(id, 'update', collectionName);
      const runtime = this.getCollectionRuntime(collectionName);
      const definition = runtime.definition;
      const operations = this.resolveCollectionAdapter(collectionName);
      if (typeof operations.update !== 'function') {
        throw new ValidationError(
          DEFAULT_CONFIG.ERROR_CODES.QUERY_NOT_ALLOWED,
          `SQL adapter for '${collectionName}' does not implement update().`,
          { collection: collectionName, operation: 'update' }
        );
      }

      const existing = await this.getById(collectionName, cleanId, { silentNotFound: false, includeDeleted: true });
      const payload = this.strictSchemas || !definition.legacy
        ? runtime.validateUpdate(data, existing?.data || null, { collection: collectionName, operation: 'update' })
        : { ...data };

      const finalPayload = {
        ...payload,
        ...prepareSearchFields(definition, { ...(existing?.data || {}), ...payload }),
        updatedAt: nowIso(),
      };

      const updated = normalizeRecord(
        await operations.update({ collectionName, definition, id: cleanId, existing, data: finalPayload, options, provider: this }),
        definition,
        collectionName
      );
      return options.includes ? this.hydrateRecord(collectionName, updated, options.includes, options) : updated;
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'update' });
    }
  }

  async remove(collectionName, id, options = {}) {
    try {
      const cleanId = ensureStringId(id, 'remove', collectionName);
      const definition = this.getCollectionRuntime(collectionName).definition;
      const operations = this.resolveCollectionAdapter(collectionName);
      const existing = await this.getById(collectionName, cleanId, { silentNotFound: false, includeDeleted: true });

      if (!this.supportsSoftDelete(definition) || options.hardDelete === true) {
        if (typeof operations.destroy !== 'function' && typeof operations.remove !== 'function') {
          throw new ValidationError(
            DEFAULT_CONFIG.ERROR_CODES.QUERY_NOT_ALLOWED,
            `SQL adapter for '${collectionName}' does not implement destroy() or remove().`,
            { collection: collectionName, operation: 'remove' }
          );
        }
        await (operations.destroy || operations.remove)({ collectionName, definition, id: cleanId, existing, options, provider: this });
        return { id: cleanId, shard: null, hardDeleted: true };
      }

      const payload = buildSoftDeletePayload(options, true);
      if (typeof operations.update !== 'function') {
        throw new ValidationError(
          DEFAULT_CONFIG.ERROR_CODES.QUERY_NOT_ALLOWED,
          `SQL adapter for '${collectionName}' must implement update() for soft deletes.`,
          { collection: collectionName, operation: 'remove' }
        );
      }
      await operations.update({ collectionName, definition, id: cleanId, existing, data: payload, options, provider: this });
      return {
        id: cleanId,
        shard: null,
        data: { ...(existing.data || {}), ...payload, [DEFAULT_CONFIG.SOFT_DELETE_FIELD]: true },
        softDeleted: true,
      };
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'remove' });
    }
  }

  async restore(collectionName, id, options = {}) {
    try {
      const cleanId = ensureStringId(id, 'restore', collectionName);
      const definition = this.getCollectionRuntime(collectionName).definition;
      const operations = this.resolveCollectionAdapter(collectionName);
      const existing = await this.getById(collectionName, cleanId, { silentNotFound: false, includeDeleted: true });
      if (typeof operations.update !== 'function') {
        throw new ValidationError(
          DEFAULT_CONFIG.ERROR_CODES.QUERY_NOT_ALLOWED,
          `SQL adapter for '${collectionName}' must implement update() for restore().`,
          { collection: collectionName, operation: 'restore' }
        );
      }
      const payload = buildSoftDeletePayload(options, false);
      await operations.update({ collectionName, definition, id: cleanId, existing, data: payload, options, provider: this });
      const restored = {
        id: cleanId,
        shard: null,
        data: { ...(existing.data || {}), ...payload, [DEFAULT_CONFIG.SOFT_DELETE_FIELD]: false },
        restored: true,
      };
      return options.includes ? this.hydrateRecord(collectionName, restored, options.includes, options) : restored;
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'restore' });
    }
  }

  async destroy(collectionName, id, options = {}) {
    try {
      const cleanId = ensureStringId(id, 'destroy', collectionName);
      const operations = this.resolveCollectionAdapter(collectionName);
      const existing = await this.getById(collectionName, cleanId, { silentNotFound: false, includeDeleted: true });
      if (typeof operations.destroy !== 'function' && typeof operations.remove !== 'function') {
        throw new ValidationError(
          DEFAULT_CONFIG.ERROR_CODES.QUERY_NOT_ALLOWED,
          `SQL adapter for '${collectionName}' does not implement destroy() or remove().`,
          { collection: collectionName, operation: 'destroy' }
        );
      }
      await (operations.destroy || operations.remove)({ collectionName, definition: this.getCollectionDefinition(collectionName), id: cleanId, existing, options, provider: this });
      return { id: cleanId, shard: null, destroyed: true };
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'destroy' });
    }
  }

  async bulkSet(collectionName, records = [], options = {}) {
    try {
      const operations = this.resolveCollectionAdapter(collectionName);
      const definition = this.getCollectionDefinition(collectionName);
      if (typeof operations.bulkSet === 'function') {
        return operations.bulkSet({ collectionName, definition, records, options, provider: this });
      }
      const results = [];
      for (const entry of Array.isArray(records) ? records : []) {
        const id = ensureStringId(entry?.id, 'bulkSet', collectionName);
        const result = await this.set(collectionName, id, entry?.data || entry?.payload || {}, options);
        results.push(result);
      }
      return results;
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'bulkSet' });
    }
  }

  async bulkUpdateStatus(collectionName, ids = [], status, options = {}) {
    try {
      const statusField = options.statusField || DEFAULT_CONFIG.DEFAULT_BULK_STATUS_FIELD;
      const extraData = options.extraData && typeof options.extraData === 'object' ? options.extraData : {};
      return this.applyBulkMutation(collectionName, ids, async (id) => {
        return this.update(collectionName, id, { [statusField]: status, ...extraData }, options);
      }, { operation: 'bulkUpdateStatus' });
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'bulkUpdateStatus' });
    }
  }

  async bulkDelete(collectionName, ids = [], options = {}) {
    try {
      return this.applyBulkMutation(collectionName, ids, async (id) => this.remove(collectionName, id, options), { operation: 'bulkDelete' });
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'bulkDelete' });
    }
  }

  async bulkRestore(collectionName, ids = [], options = {}) {
    try {
      return this.applyBulkMutation(collectionName, ids, async (id) => this.restore(collectionName, id, options), { operation: 'bulkRestore' });
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'bulkRestore' });
    }
  }

  async bulkDestroy(collectionName, ids = [], options = {}) {
    try {
      return this.applyBulkMutation(collectionName, ids, async (id) => this.destroy(collectionName, id, options), { operation: 'bulkDestroy' });
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'bulkDestroy' });
    }
  }

  async getById(collectionName, id, options = {}) {
    try {
      const cleanId = ensureStringId(id, 'getById', collectionName);
      const definition = this.getCollectionRuntime(collectionName).definition;
      const operations = this.resolveCollectionAdapter(collectionName);
      if (typeof operations.getById !== 'function') {
        throw new ValidationError(
          DEFAULT_CONFIG.ERROR_CODES.QUERY_NOT_ALLOWED,
          `SQL adapter for '${collectionName}' does not implement getById().`,
          { collection: collectionName, operation: 'getById' }
        );
      }

      const found = await operations.getById({ collectionName, definition, id: cleanId, options, provider: this });
      if (!found) {
        if (options.silentNotFound) return null;
        throw new ShardProviderError(
          DEFAULT_CONFIG.ERROR_CODES.NOT_FOUND,
          `Document '${cleanId}' was not found in collection '${collectionName}'.`,
          { collection: collectionName, operation: 'getById' }
        );
      }

      const normalized = normalizeRecord(found, definition, collectionName);
      if (!options.includeDeleted && this.supportsSoftDelete(definition) && normalized.data?.[DEFAULT_CONFIG.SOFT_DELETE_FIELD] === true) {
        if (options.silentNotFound) return null;
        throw new ShardProviderError(
          DEFAULT_CONFIG.ERROR_CODES.NOT_FOUND,
          `Document '${cleanId}' was not found in collection '${collectionName}'.`,
          { collection: collectionName, operation: 'getById' }
        );
      }

      return options.includes ? this.hydrateRecord(collectionName, normalized, options.includes, options) : normalized;
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'getById' });
    }
  }

  async getByPrimaryKey(collectionName, value, options = {}) {
    try {
      const definition = this.getCollectionDefinition(collectionName);
      const field = options.field || definition.identity?.field || derivePrimaryKeyField(collectionName);
      if (field === 'id' || field === definition.identity?.field) {
        return this.getById(collectionName, String(value), options);
      }

      const results = await this.fetchByFilters(collectionName, {
        filters: [{ field, op: '==', value }],
        limit: 1,
        range: options.range,
        includeDeleted: options.includeDeleted,
        includes: options.includes,
      }, options);

      const match = results[0] || null;
      if (match) return match;
      if (options.silentNotFound) return null;

      throw new ShardProviderError(
        DEFAULT_CONFIG.ERROR_CODES.NOT_FOUND,
        `Primary key '${value}' was not found in collection '${collectionName}'.`,
        { collection: collectionName, operation: 'getByPrimaryKey', field }
      );
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'getByPrimaryKey' });
    }
  }

  async fetchByForeignKey(collectionName, foreignField, valueOrValues, options = {}) {
    try {
      const values = Array.isArray(valueOrValues) ? valueOrValues : [valueOrValues];
      const uniqueValues = [...new Set(values.filter((value) => value !== undefined && value !== null && value !== ''))];
      if (uniqueValues.length === 0) return [];

      if (foreignField === 'id') {
        const results = await Promise.all(uniqueValues.map((value) => this.getById(collectionName, String(value), {
          silentNotFound: true,
          includeDeleted: options.includeDeleted,
          includes: options.includes,
        })));
        return results.filter(Boolean);
      }

      return this.fetchByFilters(collectionName, {
        filters: [{
          field: foreignField,
          op: uniqueValues.length === 1 ? '==' : 'in',
          value: uniqueValues.length === 1 ? uniqueValues[0] : uniqueValues,
        }],
        orderBy: options.orderBy || [],
        limit: options.limit || Math.max(DEFAULT_CONFIG.DEFAULT_PAGE_SIZE, uniqueValues.length * 25),
        range: options.range || defaultRange(),
        includeDeleted: options.includeDeleted,
        includes: options.includes,
      }, options);
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'fetchByForeignKey', field: foreignField });
    }
  }

  async fetchPage(collectionName, queryInput = {}, options = {}) {
    try {
      const definition = this.getCollectionRuntime(collectionName).definition;
      const operations = this.resolveCollectionAdapter(collectionName);
      const normalizedQuery = {
        filters: Array.isArray(queryInput.filters) ? queryInput.filters : [],
        orderBy: Array.isArray(queryInput.orderBy) ? queryInput.orderBy : [],
        limit: normalizePageSize(queryInput.limit || options.limit || queryInput.pageSize || options.pageSize),
        offset: Number.isFinite(Number(queryInput.offset)) ? Math.max(0, Number(queryInput.offset)) : 0,
        includeDeleted: queryInput.includeDeleted === true,
        includes: queryInput.includes || options.includes,
        range: queryInput.range || options.range || null,
      };

      let pageResult;
      if (typeof operations.fetchPage === 'function') {
        pageResult = await operations.fetchPage({ collectionName, definition, queryInput: normalizedQuery, options, provider: this });
      } else if (typeof operations.fetchByFilters === 'function') {
        pageResult = await operations.fetchByFilters({ collectionName, definition, queryInput: normalizedQuery, options, provider: this });
      } else {
        throw new ValidationError(
          DEFAULT_CONFIG.ERROR_CODES.QUERY_NOT_ALLOWED,
          `SQL adapter for '${collectionName}' does not implement fetchPage() or fetchByFilters().`,
          { collection: collectionName, operation: 'fetchByFilters' }
        );
      }

      const page = normalizePageResult(pageResult, definition, collectionName, normalizedQuery);
      const includes = normalizedQuery.includes;
      page.items = includes
        ? await this.hydrateRecords(collectionName, page.items, includes, options)
        : page.items;
      page.total = typeof page.total === 'number' ? page.total : page.items.length;
      return page;
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'fetchByFilters' });
    }
  }

  async fetchByFilters(collectionName, queryInput = {}, options = {}) {
    const page = await this.fetchPage(collectionName, queryInput, options);
    return Array.isArray(page?.items) ? page.items : [];
  }

  async search(collectionName, term, options = {}) {
    try {
      const { buildSearchQuery } = await import('../search/searchStrategies.js');
      const runtime = this.getCollectionRuntime(collectionName);
      const filter = buildSearchQuery(runtime.definition, term);
      const orderField = runtime.definition.schema.createdAt?.sortable ? 'createdAt' : null;
      return this.fetchByFilters(collectionName, {
        filters: [filter],
        orderBy: orderField ? [{ field: orderField, direction: 'desc' }] : [],
        limit: options.limit || DEFAULT_CONFIG.DEFAULT_PAGE_SIZE,
        range: options.range || defaultRange(),
        includes: options.includes,
      }, options);
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'search' });
    }
  }

  getBatchLimit(options = {}) {
    return [
      options.maxBatchSize,
      options.batchSize,
      options.chunkSize,
      options.maxOperationsPerBatch,
      options.writeBatchSize,
      options.batchLimit,
      this.maxBatchSize,
      this.batchSize,
      this.writeBatchSize,
      this.batchLimit,
    ].find((value) => Number.isInteger(value) && value > 0) || 500;
  }

  async commitWriteOperations(operations = [], options = {}) {
    try {
      const normalizedOperations = Array.isArray(operations) ? operations.filter(Boolean) : [];
      if (!normalizedOperations.length) return [];

      if (typeof this.adapter.commitWriteOperations === 'function') {
        await this.adapter.commitWriteOperations({ operations: normalizedOperations, options, provider: this });
        return normalizedOperations;
      }

      for (const operation of normalizedOperations) {
        const collectionName = operation?.collection || operation?.collectionName;
        if (!collectionName) {
          throw new ValidationError(
            DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT,
            'Write operations require a collection name.',
            { field: 'collection', operation: 'commitWriteOperations' }
          );
        }

        const type = operation?.type || operation?.action || operation?.method;
        const id = operation?.id || operation?.recordId || operation?.key;
        const data = operation?.data || operation?.payload || {};
        switch (type) {
          case 'set':
          case 'create':
            if (id) await this.set(collectionName, id, data, options);
            else await this.create(collectionName, data, options);
            break;
          case 'update':
          case 'patch':
            await this.update(collectionName, ensureStringId(String(id), 'commitWriteOperations', collectionName), data, options);
            break;
          case 'delete':
          case 'remove':
            await this.destroy(collectionName, ensureStringId(String(id), 'commitWriteOperations', collectionName), options);
            break;
          default:
            throw new ValidationError(
              DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT,
              `Unsupported write operation type: ${type}`,
              { field: 'type', operation: 'commitWriteOperations', value: type }
            );
        }
      }

      return normalizedOperations;
    } catch (error) {
      this.handleError(error, { operation: 'commitWriteOperations' });
    }
  }
}
