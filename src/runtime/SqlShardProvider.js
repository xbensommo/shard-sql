import { DEFAULT_CONFIG } from '../constants.js';
import { createLegacyCollectionDefinition } from '../compat/legacyCollectionConfig.js';
import { normalizeProviderOptions } from '../compat/legacyProviderBridge.js';
import { mapAdapterError } from '../core/providerErrorMapper.js';
import { CollectionRegistry } from '../core/registry.js';
import { derivePrimaryKeyField } from '../core/relationNaming.js';
import { ShardProviderError, ValidationError } from '../errors.js';
import { buildSearchPlan, matchesSearchRecord, prepareSearchFields, rankSearchRecords } from '../search/searchStrategies.js';
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

function shouldUseDefaultRange(definition) {
  const shardType = definition?.shard?.type || DEFAULT_CONFIG.SHARD_TYPES.NONE;
  return shardType && shardType !== DEFAULT_CONFIG.SHARD_TYPES.NONE;
}

function chunkArray(values = [], size = 10) {
  const chunkSize = Number.isInteger(size) && size > 0 ? size : 10;
  const chunks = [];
  for (let index = 0; index < values.length; index += chunkSize) {
    chunks.push(values.slice(index, index + chunkSize));
  }
  return chunks;
}

function encodeCursor(payload = {}) {
  return Buffer.from(JSON.stringify(payload)).toString('base64url');
}

function decodeCursor(cursor) {
  if (!cursor || typeof cursor !== 'string') return null;
  try {
    const decoded = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
    return decoded && typeof decoded === 'object' ? decoded : null;
  } catch {
    return null;
  }
}

function normalizePageSize(value) {
  const parsed = Number(value || DEFAULT_CONFIG.DEFAULT_PAGE_SIZE);
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_CONFIG.DEFAULT_PAGE_SIZE;
  return Math.min(Math.floor(parsed), DEFAULT_CONFIG.MAX_PAGE_SIZE);
}

function normalizePaginationInput(queryInput = {}, options = {}) {
  const pagination = {
    ...(options.pagination && typeof options.pagination === 'object' ? options.pagination : {}),
    ...(queryInput.pagination && typeof queryInput.pagination === 'object' ? queryInput.pagination : {}),
  };

  const requestedMode = pagination.mode || queryInput.pageMode || options.pageMode || (pagination.cursor || queryInput.cursor || queryInput.pageToken || queryInput.after ? 'cursor' : 'offset');
  const mode = requestedMode === 'cursor' ? 'cursor' : 'offset';
  const pageSize = normalizePageSize(queryInput.limit || options.limit || queryInput.pageSize || options.pageSize || pagination.limit || pagination.pageSize);
  const decodedCursor = decodeCursor(pagination.cursor || queryInput.cursor || queryInput.pageToken || queryInput.after || options.cursor || options.pageToken || options.after);
  const cursor = pagination.cursor || queryInput.cursor || queryInput.pageToken || queryInput.after || options.cursor || options.pageToken || options.after || null;
  const offsetFromCursor = Number.isFinite(Number(decodedCursor?.offset)) ? Math.max(0, Number(decodedCursor.offset)) : 0;
  const offset = Number.isFinite(Number(pagination.offset))
    ? Math.max(0, Number(pagination.offset))
    : (Number.isFinite(Number(queryInput.offset)) ? Math.max(0, Number(queryInput.offset)) : (mode === 'cursor' ? offsetFromCursor : 0));

  return {
    mode,
    pageSize,
    limit: pageSize,
    offset,
    cursor,
    decodedCursor,
    direction: pagination.direction || queryInput.direction || options.direction || 'forward',
  };
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
  const paginationInput = normalizePaginationInput(queryInput);
  const requestedMode = result?.pagination?.mode || result?.mode || paginationInput.mode;
  const mode = requestedMode === 'cursor' ? 'cursor' : 'offset';

  if (Array.isArray(result)) {
    const items = normalizeRecordList(result, definition, collectionName);
    const offset = paginationInput.offset;
    const pageSize = paginationInput.pageSize;
    const nextOffset = items.length >= pageSize ? offset + items.length : null;
    const nextCursor = mode === 'cursor' && nextOffset !== null ? encodeCursor({ mode: 'offset', offset: nextOffset }) : null;
    const prevOffset = offset > 0 ? Math.max(0, offset - pageSize) : null;
    const prevCursor = mode === 'cursor' && prevOffset !== null ? encodeCursor({ mode: 'offset', offset: prevOffset }) : null;
    return {
      items,
      total: mode === 'cursor' ? null : items.length,
      hasMore: mode === 'cursor' ? Boolean(nextCursor) : false,
      lastVisible: mode === 'cursor' ? nextCursor : null,
      nextCursor,
      prevCursor,
      pagination: {
        mode,
        offset,
        cursor: paginationInput.cursor,
        pageSize,
        nextOffset,
        prevOffset,
        nextCursor,
        prevCursor,
        hasMore: mode === 'cursor' ? Boolean(nextCursor) : false,
        total: mode === 'cursor' ? null : items.length,
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
  const pageSize = normalizePageSize(result?.pageSize || result?.pagination?.pageSize || queryInput.limit || queryInput.pageSize);
  const offset = Number.isFinite(Number(result?.pagination?.offset))
    ? Math.max(0, Number(result.pagination.offset))
    : paginationInput.offset;
  const total = typeof result?.total === 'number' ? result.total : (mode === 'cursor' ? null : items.length);
  const nextOffset = result?.pagination?.nextOffset ?? result?.nextOffset ?? ((mode === 'offset' && typeof total === 'number' && (offset + items.length) < total) ? offset + items.length : null);
  const prevOffset = result?.pagination?.prevOffset ?? result?.prevOffset ?? (offset > 0 ? Math.max(0, offset - pageSize) : null);
  const nextCursor = result?.pagination?.nextCursor
    ?? result?.nextCursor
    ?? (mode === 'cursor' && nextOffset !== null ? encodeCursor({ mode: 'offset', offset: nextOffset }) : null);
  const prevCursor = result?.pagination?.prevCursor
    ?? result?.prevCursor
    ?? (mode === 'cursor' && prevOffset !== null ? encodeCursor({ mode: 'offset', offset: prevOffset }) : null);
  const hasMore = typeof result?.hasMore === 'boolean'
    ? result.hasMore
    : (mode === 'cursor' ? Boolean(nextCursor) : (typeof total === 'number' ? (offset + items.length) < total : Boolean(nextOffset)));

  return {
    items,
    total,
    hasMore,
    lastVisible: mode === 'cursor' ? nextCursor : nextOffset,
    nextCursor,
    prevCursor,
    pagination: {
      mode,
      offset,
      cursor: result?.pagination?.cursor ?? result?.cursor ?? paginationInput.cursor,
      pageSize,
      nextOffset,
      prevOffset,
      nextCursor,
      prevCursor,
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
    this.searchProvider = options.searchProvider || null;
    this.searchScanLimit = Number.isInteger(options.searchScanLimit) && options.searchScanLimit > 0 ? options.searchScanLimit : 1000;
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
    const concurrency = Number.isInteger(options.concurrency) && options.concurrency > 0 ? options.concurrency : 10;
    const chunks = chunkArray(validIds, concurrency);
    const results = [];

    for (const batch of chunks) {
      const settled = await Promise.allSettled(batch.map((id) => mutator(id)));
      settled.forEach((entry) => {
        if (entry.status === 'fulfilled') results.push(entry.value);
      });
      const rejected = settled.find((entry) => entry.status === 'rejected');
      if (rejected) throw rejected.reason;
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

  resolveSearchOrderBy(definition, queryInput = {}) {
    if (Array.isArray(queryInput.orderBy) && queryInput.orderBy.length) return queryInput.orderBy;
    if (definition.schema.updatedAt?.sortable) return [{ field: 'updatedAt', direction: 'desc' }];
    if (definition.schema.createdAt?.sortable) return [{ field: 'createdAt', direction: 'desc' }];
    return [];
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
        range: options.range || (shouldUseDefaultRange(this.getCollectionDefinition(collectionName)) ? defaultRange() : null),
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
      const pagination = normalizePaginationInput(queryInput, options);
      const normalizedQuery = {
        filters: Array.isArray(queryInput.filters) ? queryInput.filters : [],
        orderBy: Array.isArray(queryInput.orderBy) ? queryInput.orderBy : [],
        limit: pagination.limit,
        pageSize: pagination.pageSize,
        offset: pagination.offset,
        cursor: pagination.cursor,
        pageToken: pagination.cursor,
        after: pagination.cursor,
        pageMode: pagination.mode,
        direction: pagination.direction,
        pagination,
        includeDeleted: queryInput.includeDeleted === true || options.includeDeleted === true,
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
          { collection: collectionName, operation: 'fetchPage' }
        );
      }

      const page = normalizePageResult(pageResult, definition, collectionName, normalizedQuery);
      const includes = normalizedQuery.includes;
      page.items = includes
        ? await this.hydrateRecords(collectionName, page.items, includes, options)
        : page.items;
      return page;
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'fetchPage' });
    }
  }

  async fetchByFilters(collectionName, queryInput = {}, options = {}) {
    const page = await this.fetchPage(collectionName, queryInput, options);
    return Array.isArray(page?.items) ? page.items : [];
  }

  async searchPage(collectionName, term, queryInput = {}, options = {}) {
    try {
      const runtime = this.getCollectionRuntime(collectionName);
      const definition = runtime.definition;
      const operations = this.resolveCollectionAdapter(collectionName);
      const mergedInput = { ...(queryInput || {}) };
      const pagination = normalizePaginationInput(mergedInput, options);
      const plan = buildSearchPlan(definition, term, { limit: pagination.limit });
      const orderBy = this.resolveSearchOrderBy(definition, mergedInput);
      const includeDeleted = mergedInput.includeDeleted === true || options.includeDeleted === true;
      const includes = mergedInput.includes || options.includes;

      if (plan.mode === DEFAULT_CONFIG.SEARCH_MODES.EXTERNAL) {
        if (this.searchProvider) {
          const externalResult = typeof this.searchProvider.searchPage === 'function'
            ? await this.searchProvider.searchPage({ collectionName, definition, term, plan, queryInput: { ...mergedInput, orderBy, includeDeleted, includes, pagination }, options, provider: this })
            : await this.searchProvider.search({ collectionName, definition, term, plan, queryInput: { ...mergedInput, orderBy, includeDeleted, includes, pagination }, options, provider: this });
          const page = normalizePageResult(externalResult, definition, collectionName, { ...mergedInput, orderBy, includeDeleted, includes, pagination, pageMode: pagination.mode, limit: pagination.limit, offset: pagination.offset, cursor: pagination.cursor });
          page.items = includes ? await this.hydrateRecords(collectionName, page.items, includes, options) : page.items;
          return page;
        }

        if (typeof operations.search !== 'function') {
          throw new ValidationError(
            DEFAULT_CONFIG.ERROR_CODES.SEARCH_NOT_CONFIGURED,
            `External search is configured for '${collectionName}' but no search provider or adapter search() is available.`,
            { collection: collectionName, operation: 'search' }
          );
        }
      }

      if (typeof operations.search === 'function') {
        const adapterResult = await operations.search({ collectionName, definition, term, plan, queryInput: { ...mergedInput, orderBy, includeDeleted, includes, pagination, pageMode: pagination.mode, limit: pagination.limit, offset: pagination.offset, cursor: pagination.cursor }, options, provider: this });
        const page = normalizePageResult(adapterResult, definition, collectionName, { ...mergedInput, orderBy, includeDeleted, includes, pagination, pageMode: pagination.mode, limit: pagination.limit, offset: pagination.offset, cursor: pagination.cursor });
        page.items = includes ? await this.hydrateRecords(collectionName, page.items, includes, options) : page.items;
        return page;
      }

      const baseFilters = [
        ...(Array.isArray(mergedInput.filters) ? mergedInput.filters : []),
        ...(plan.primaryFilter ? [plan.primaryFilter] : []),
      ];
      const scanPageSize = Math.min(DEFAULT_CONFIG.MAX_PAGE_SIZE, Math.max(pagination.limit * 4, 50));
      const scanLimit = Number.isInteger(options.searchScanLimit) && options.searchScanLimit > 0 ? options.searchScanLimit : this.searchScanLimit;
      const matches = [];
      const seenIds = new Set();
      let rawOffset = 0;
      let scanned = 0;
      let hasMore = true;

      while (hasMore && scanned < scanLimit) {
        const rawPage = await this.fetchPage(collectionName, {
          filters: baseFilters,
          orderBy,
          limit: scanPageSize,
          offset: rawOffset,
          includeDeleted,
          range: mergedInput.range || options.range || null,
        }, { ...options, includes: null, includeDeleted });

        const candidates = Array.isArray(rawPage?.items) ? rawPage.items : [];
        for (const record of candidates) {
          const recordId = String(record?.id || '');
          if (!recordId || seenIds.has(recordId)) continue;
          seenIds.add(recordId);
          if (matchesSearchRecord(definition, record, plan)) {
            matches.push(record);
          }
        }

        scanned += candidates.length;
        hasMore = Boolean(rawPage?.hasMore) && candidates.length > 0;
        rawOffset = Number.isFinite(Number(rawPage?.pagination?.nextOffset))
          ? Number(rawPage.pagination.nextOffset)
          : rawOffset + candidates.length;

        if (!candidates.length) break;
      }

      const ranked = rankSearchRecords(definition, matches, plan);
      const total = ranked.length;
      const sliceStart = pagination.offset;
      const sliceEnd = sliceStart + pagination.limit;
      const items = ranked.slice(sliceStart, sliceEnd);
      const pageHasMore = sliceEnd < total;
      const nextOffset = pageHasMore ? sliceEnd : null;
      const prevOffset = sliceStart > 0 ? Math.max(0, sliceStart - pagination.limit) : null;
      const page = normalizePageResult({
        items,
        total,
        hasMore: pageHasMore,
        pagination: {
          mode: pagination.mode,
          offset: sliceStart,
          pageSize: pagination.limit,
          nextOffset,
          prevOffset,
          nextCursor: pagination.mode === 'cursor' && nextOffset !== null ? encodeCursor({ mode: 'offset', offset: nextOffset }) : null,
          prevCursor: pagination.mode === 'cursor' && prevOffset !== null ? encodeCursor({ mode: 'offset', offset: prevOffset }) : null,
          hasMore: pageHasMore,
          total,
        },
        filters: baseFilters,
        orderBy,
        includeDeleted,
      }, definition, collectionName, { ...mergedInput, includeDeleted, includes, pagination, pageMode: pagination.mode, limit: pagination.limit, offset: sliceStart, cursor: pagination.cursor });

      page.items = includes ? await this.hydrateRecords(collectionName, page.items, includes, options) : page.items;
      page.search = {
        term: String(term),
        normalized: plan.normalized,
        mode: plan.mode,
        tokens: plan.tokens,
        scanned,
        scanLimit,
        truncated: hasMore && scanned >= scanLimit,
      };
      return page;
    } catch (error) {
      this.handleError(error, { collection: collectionName, operation: 'search' });
    }
  }

  async search(collectionName, term, options = {}) {
    const page = await this.searchPage(collectionName, term, options, options);
    return Array.isArray(page?.items) ? page.items : [];
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

      const executeOperations = async () => {
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
      };

      if (typeof this.adapter.transaction === 'function') {
        return this.adapter.transaction(async (transaction) => executeOperations(transaction));
      }

      return executeOperations();
    } catch (error) {
      this.handleError(error, { operation: 'commitWriteOperations' });
    }
  }
}
