import { DEFAULT_CONFIG, FIELD_TYPES } from '../src/constants.js';
import { defineCollection } from '../src/core/defineCollection.js';
import { createDataConnectAdapter } from '../src/adapters/createDataConnectAdapter.js';
import { SqlShardProvider } from '../src/runtime/SqlShardProvider.js';

export function makeRef(initialValue) {
  return { value: initialValue };
}

export function defineAccountCollection(overrides = {}) {
  return defineCollection({
    name: 'accounts',
    backend: { engine: 'sql', table: 'accounts' },
    metadata: { softDelete: true },
    primaryKey: 'account_id',
    search: { mode: DEFAULT_CONFIG.SEARCH_MODES.TOKEN_ARRAY, fields: ['name'] },
    schema: {
      account_id: { type: FIELD_TYPES.STRING, required: true, immutable: true },
      name: { type: FIELD_TYPES.STRING, required: true, filterable: true, sortable: true, searchable: true },
      status: { type: FIELD_TYPES.STRING, enum: ['lead', 'active', 'inactive'], default: 'lead', filterable: true },
      tier: { type: FIELD_TYPES.STRING, default: 'standard', filterable: true },
      createdAt: { type: FIELD_TYPES.TIMESTAMP, sortable: true },
      updatedAt: { type: FIELD_TYPES.TIMESTAMP },
      deletedAt: { type: FIELD_TYPES.TIMESTAMP },
      deletedBy: { type: FIELD_TYPES.STRING },
      isDeleted: { type: FIELD_TYPES.BOOLEAN, filterable: true },
      _searchText: { type: FIELD_TYPES.STRING, system: true },
      _searchTokens: { type: FIELD_TYPES.ARRAY, system: true },
      _searchPrefixes: { type: FIELD_TYPES.ARRAY, system: true },
    },
    ...overrides,
  });
}

export function defineContactCollection(overrides = {}) {
  return defineCollection({
    name: 'contacts',
    backend: { engine: 'sql', table: 'contacts' },
    primaryKey: 'contact_id',
    schema: {
      contact_id: { type: FIELD_TYPES.STRING, required: true, immutable: true },
      account_id: { type: FIELD_TYPES.STRING, required: true, filterable: true },
      name: { type: FIELD_TYPES.STRING, required: true, filterable: true },
      isDeleted: { type: FIELD_TYPES.BOOLEAN, filterable: true },
      createdAt: { type: FIELD_TYPES.TIMESTAMP, sortable: true },
      updatedAt: { type: FIELD_TYPES.TIMESTAMP },
    },
    ...overrides,
  });
}

export function createMemorySqlAdapter(options = {}) {
  const tables = {
    accounts: new Map(),
    contacts: new Map(),
  };

  const callLog = [];
  const hooks = options.hooks || {};

  const seed = options.seed || {};
  for (const [tableName, rows] of Object.entries(seed)) {
    if (!tables[tableName]) tables[tableName] = new Map();
    for (const row of rows) {
      const idField = tableName === 'contacts' ? 'contact_id' : 'account_id';
      tables[tableName].set(String(row[idField]), { ...row });
    }
  }

  function recordCall(type, payload) {
    callLog.push({ type, ...payload });
  }

  const getIdField = (collectionName) => (collectionName === 'contacts' ? 'contact_id' : 'account_id');

  const compareValues = (a, b) => {
    if (a === b) return 0;
    if (a == null) return 1;
    if (b == null) return -1;
    return a > b ? 1 : -1;
  };

  const filterRows = (rows, filters = []) => rows.filter((row) => {
    return filters.every((filter) => {
      if (!filter) return true;
      const value = row?.[filter.field];
      switch (filter.op || '==') {
        case '==': return value === filter.value;
        case 'in': return Array.isArray(filter.value) && filter.value.includes(value);
        case 'array-contains': return Array.isArray(value) && value.includes(filter.value);
        default: throw new Error(`Unsupported filter op in test adapter: ${filter.op}`);
      }
    });
  });

  const sortRows = (rows, orderBy = []) => {
    if (!Array.isArray(orderBy) || !orderBy.length) return [...rows];
    return [...rows].sort((left, right) => {
      for (const sort of orderBy) {
        const direction = (sort.direction || 'asc').toLowerCase() === 'desc' ? -1 : 1;
        const result = compareValues(left?.[sort.field], right?.[sort.field]);
        if (result !== 0) return result * direction;
      }
      return 0;
    });
  };

  function getRows(collectionName) {
    return [...(tables[collectionName] || new Map()).values()];
  }

  const collections = {
    accounts: {
      async getById({ id }) {
        recordCall('getById', { collectionName: 'accounts', id });
        return hooks.getById ? hooks.getById({ collectionName: 'accounts', id, tables }) : (tables.accounts.get(id) || null);
      },
      async fetchPage({ queryInput }) {
        recordCall('fetchPage', { collectionName: 'accounts', queryInput });
        if (hooks.fetchPage) return hooks.fetchPage({ collectionName: 'accounts', queryInput, tables });
        const rows = sortRows(filterRows(getRows('accounts'), queryInput.filters || []), queryInput.orderBy || []);
        const offset = Number(queryInput.offset || 0);
        const limit = Number(queryInput.limit || 20);
        return {
          items: rows.slice(offset, offset + limit),
          total: rows.length,
          hasMore: offset + limit < rows.length,
          pagination: {
            offset,
            pageSize: limit,
            nextOffset: offset + limit < rows.length ? offset + limit : null,
            hasMore: offset + limit < rows.length,
            total: rows.length,
          },
        };
      },
      async fetchByFilters({ queryInput }) {
        recordCall('fetchByFilters', { collectionName: 'accounts', queryInput });
        if (hooks.fetchByFilters) return hooks.fetchByFilters({ collectionName: 'accounts', queryInput, tables });
        return sortRows(filterRows(getRows('accounts'), queryInput.filters || []), queryInput.orderBy || []);
      },
      async create({ data }) {
        recordCall('create', { collectionName: 'accounts', data });
        if (hooks.create) return hooks.create({ collectionName: 'accounts', data, tables });
        const row = { account_id: data.account_id, ...data };
        tables.accounts.set(row.account_id, row);
        return row;
      },
      async set({ id, data }) {
        recordCall('set', { collectionName: 'accounts', id, data });
        if (hooks.set) return hooks.set({ collectionName: 'accounts', id, data, tables });
        const row = { account_id: id, ...data };
        tables.accounts.set(id, row);
        return row;
      },
      async update({ id, data, existing }) {
        recordCall('update', { collectionName: 'accounts', id, data });
        if (hooks.update) return hooks.update({ collectionName: 'accounts', id, data, existing, tables });
        const row = { ...(existing?.data || {}), ...data, account_id: id };
        tables.accounts.set(id, row);
        return row;
      },
      async destroy({ id }) {
        recordCall('destroy', { collectionName: 'accounts', id });
        if (hooks.destroy) return hooks.destroy({ collectionName: 'accounts', id, tables });
        tables.accounts.delete(id);
        return { account_id: id };
      },
    },
    contacts: {
      async getById({ id }) {
        recordCall('getById', { collectionName: 'contacts', id });
        return hooks.getById ? hooks.getById({ collectionName: 'contacts', id, tables }) : (tables.contacts.get(id) || null);
      },
      async fetchByFilters({ queryInput }) {
        recordCall('fetchByFilters', { collectionName: 'contacts', queryInput });
        if (hooks.fetchByFilters && options.allowContactHook !== false) return hooks.fetchByFilters({ collectionName: 'contacts', queryInput, tables });
        return sortRows(filterRows(getRows('contacts'), queryInput.filters || []), queryInput.orderBy || []);
      },
      async create({ data }) {
        recordCall('create', { collectionName: 'contacts', data });
        const row = { contact_id: data.contact_id, ...data };
        tables.contacts.set(row.contact_id, row);
        return row;
      },
      async set({ id, data }) {
        recordCall('set', { collectionName: 'contacts', id, data });
        const row = { contact_id: id, ...data };
        tables.contacts.set(id, row);
        return row;
      },
      async update({ id, data, existing }) {
        recordCall('update', { collectionName: 'contacts', id, data });
        const row = { ...(existing?.data || {}), ...data, contact_id: id };
        tables.contacts.set(id, row);
        return row;
      },
      async destroy({ id }) {
        recordCall('destroy', { collectionName: 'contacts', id });
        tables.contacts.delete(id);
        return { contact_id: id };
      },
    },
  };

  const adapter = createDataConnectAdapter({
    collections,
    onError: options.onError,
    async commitWriteOperations(context) {
      if (hooks.commitWriteOperations) return hooks.commitWriteOperations({ ...context, tables, callLog });
      return null;
    },
  });

  return {
    adapter,
    tables,
    callLog,
    getRows,
    getIdField,
  };
}

export function createSqlProvider(config = {}) {
  const accountDefinition = config.accounts || defineAccountCollection(config.accountOverrides || {});
  const contactDefinition = config.contacts || defineContactCollection(config.contactOverrides || {});
  const adapterBundle = config.adapterBundle || createMemorySqlAdapter(config.adapterOptions || {});
  const collections = config.collections || [
    config.withRelations === false
      ? accountDefinition
      : defineCollection({
          ...accountDefinition,
          relations: {
            contacts: {
              collection: 'contacts',
              localField: 'account_id',
              foreignField: 'account_id',
              many: true,
              as: 'contacts',
              ...(config.contactRelation || {}),
            },
          },
        }),
    contactDefinition,
  ];

  return {
    provider: new SqlShardProvider({
      engine: 'sql',
      adapter: adapterBundle.adapter,
      collections,
      strictSchemas: config.strictSchemas,
      onError: config.onError,
    }),
    ...adapterBundle,
    accountDefinition,
    contactDefinition,
  };
}
