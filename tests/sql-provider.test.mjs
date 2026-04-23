import test from 'node:test';
import assert from 'node:assert/strict';

import { DEFAULT_CONFIG, FIELD_TYPES } from '../src/constants.js';
import { defineCollection } from '../src/core/defineCollection.js';
import { createDataConnectAdapter } from '../src/adapters/createDataConnectAdapter.js';
import { SqlShardProvider } from '../src/runtime/SqlShardProvider.js';
import { createSqlProvider, createMemorySqlAdapter, defineAccountCollection } from './_helpers.mjs';

function expectCode(error, code) {
  return error?.code === code;
}

test('SqlShardProvider constructor requires adapter', () => {
  assert.throws(() => new SqlShardProvider({ collections: [] }), (error) => error?.code === DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT);
});

test('SqlShardProvider create/update/getById/fetchPage/remove/restore works with adapter', async () => {
  const { provider, tables } = createSqlProvider({
    adapterOptions: {
      seed: {
        contacts: [{
          contact_id: 'con-1',
          account_id: 'acc-1',
          name: 'Jane Client',
          isDeleted: false,
          createdAt: '2026-01-01T00:00:00.000Z',
          updatedAt: '2026-01-01T00:00:00.000Z',
        }],
      },
    },
  });

  const created = await provider.create('accounts', { account_id: 'acc-1', name: 'Acme Ltd' });
  assert.equal(created.id, 'acc-1');
  assert.equal(created.data.isDeleted, false);
  assert.ok(Array.isArray(created.data._searchTokens));

  const updated = await provider.update('accounts', 'acc-1', { name: 'Acme Group' });
  assert.equal(updated.data.name, 'Acme Group');

  const fetched = await provider.getById('accounts', 'acc-1', { includes: ['contacts'] });
  assert.equal(fetched.data.name, 'Acme Group');
  assert.equal(fetched.data.contacts.length, 1);
  assert.equal(fetched.data.contacts[0].name, 'Jane Client');

  const page = await provider.fetchPage('accounts', {
    filters: [{ field: DEFAULT_CONFIG.SOFT_DELETE_FIELD, op: '==', value: false }],
    orderBy: [{ field: 'name', direction: 'asc' }],
    limit: 10,
  });
  assert.equal(page.items.length, 1);
  assert.equal(page.total, 1);

  const removed = await provider.remove('accounts', 'acc-1', { deletedBy: 'usr-1' });
  assert.equal(removed.softDeleted, true);
  assert.equal(removed.data.deletedBy, 'usr-1');

  const afterRemove = await provider.getById('accounts', 'acc-1', { includeDeleted: true });
  assert.equal(afterRemove.data.isDeleted, true);

  const restored = await provider.restore('accounts', 'acc-1');
  assert.equal(restored.restored, true);

  const afterRestore = await provider.getById('accounts', 'acc-1');
  assert.equal(afterRestore.data.isDeleted, false);
  assert.equal(tables.accounts.get('acc-1').isDeleted, false);
});

test('SqlShardProvider rejects invalid adapter record shapes and missing ids', async () => {
  const { provider } = createSqlProvider({
    adapterOptions: {
      hooks: {
        create() {
          return [];
        },
      },
    },
  });

  await assert.rejects(() => provider.create('accounts', { account_id: 'acc-1', name: 'Broken' }), (error) => expectCode(error, DEFAULT_CONFIG.ERROR_CODES.INVALID_DATA));

  const badIdBundle = createMemorySqlAdapter({
    hooks: {
      create() {
        return { name: 'No id' };
      },
    },
  });
  const badIdProvider = new SqlShardProvider({ adapter: badIdBundle.adapter, collections: [defineAccountCollection()] });
  await assert.rejects(() => badIdProvider.create('accounts', { account_id: 'acc-2', name: 'Broken' }), (error) => expectCode(error, DEFAULT_CONFIG.ERROR_CODES.INVALID_ID));
});

test('SqlShardProvider maps adapter errors and triggers provider and adapter onError callbacks', async () => {
  const seen = [];
  const { adapter } = createMemorySqlAdapter({
    hooks: {
      create() {
        const error = new Error('duplicate key');
        error.sqlState = '23505';
        throw error;
      },
    },
    onError(error) {
      seen.push(['adapter', error.code]);
    },
  });
  const provider = new SqlShardProvider({
    adapter,
    collections: [defineAccountCollection()],
    onError(error) {
      seen.push(['provider', error.code]);
    },
  });

  await assert.rejects(() => provider.create('accounts', { account_id: 'acc-1', name: 'Acme' }), (error) => expectCode(error, DEFAULT_CONFIG.ERROR_CODES.ALREADY_EXISTS));
  assert.deepEqual(seen, [
    ['provider', DEFAULT_CONFIG.ERROR_CODES.ALREADY_EXISTS],
    ['adapter', DEFAULT_CONFIG.ERROR_CODES.ALREADY_EXISTS],
  ]);
});

test('SqlShardProvider getById respects silentNotFound and soft-delete visibility', async () => {
  const { provider } = createSqlProvider({
    adapterOptions: {
      seed: {
        accounts: [{
          account_id: 'acc-1',
          name: 'Acme',
          status: 'lead',
          tier: 'standard',
          isDeleted: true,
          createdAt: '2026-01-01T00:00:00.000Z',
          updatedAt: '2026-01-01T00:00:00.000Z',
        }],
      },
    },
  });

  await assert.rejects(() => provider.getById('accounts', 'acc-1'), (error) => expectCode(error, DEFAULT_CONFIG.ERROR_CODES.NOT_FOUND));
  const hidden = await provider.getById('accounts', 'acc-1', { silentNotFound: true });
  assert.equal(hidden, null);
  const visible = await provider.getById('accounts', 'acc-1', { includeDeleted: true });
  assert.equal(visible.data.isDeleted, true);
});

test('SqlShardProvider fetchByForeignKey handles id path, dedupes empty values, and default pagination', async () => {
  const { provider, callLog } = createSqlProvider({
    adapterOptions: {
      seed: {
        accounts: [
          { account_id: 'acc-1', name: 'A', status: 'lead', tier: 'standard', isDeleted: false, createdAt: '2026-01-01T00:00:00.000Z', updatedAt: '2026-01-01T00:00:00.000Z' },
          { account_id: 'acc-2', name: 'B', status: 'lead', tier: 'standard', isDeleted: false, createdAt: '2026-01-02T00:00:00.000Z', updatedAt: '2026-01-02T00:00:00.000Z' },
        ],
      },
    },
  });

  const empty = await provider.fetchByForeignKey('accounts', 'account_id', ['', null, undefined]);
  assert.deepEqual(empty, []);

  const byId = await provider.fetchByForeignKey('accounts', 'id', ['acc-1', 'acc-1', 'acc-2']);
  assert.equal(byId.length, 2);
  assert.equal(callLog.filter((entry) => entry.type === 'getById').length, 2);

  const byFk = await provider.fetchByForeignKey('accounts', 'account_id', ['acc-1', 'acc-2']);
  assert.equal(byFk.length, 2);
  const fetchByFiltersCall = callLog.find((entry) => entry.type === 'fetchPage');
  assert.equal(fetchByFiltersCall.queryInput.filters[0].op, 'in');
});

test('SqlShardProvider fetchPage normalizes plain arrays and caps excessive page size', async () => {
  const adapter = createDataConnectAdapter({
    collections: {
      accounts: {
        async fetchByFilters() {
          return [
            { account_id: 'acc-1', name: 'A', status: 'lead', tier: 'standard', isDeleted: false },
            { account_id: 'acc-2', name: 'B', status: 'lead', tier: 'standard', isDeleted: false },
          ];
        },
      },
    },
  });
  const provider = new SqlShardProvider({ adapter, collections: [defineAccountCollection()] });
  const page = await provider.fetchPage('accounts', { limit: 999 });
  assert.equal(page.items.length, 2);
  assert.equal(page.hasMore, false);
  assert.equal(page.pagination.pageSize, DEFAULT_CONFIG.MAX_PAGE_SIZE);
});

test('SqlShardProvider remove hard-deletes collections without softDelete and destroy works', async () => {
  const accountDefinition = defineAccountCollection({ metadata: { softDelete: false } });
  const { adapter, tables } = createMemorySqlAdapter({
    seed: {
      accounts: [{ account_id: 'acc-1', name: 'A', status: 'lead', tier: 'standard', isDeleted: false, createdAt: '2026-01-01T00:00:00.000Z', updatedAt: '2026-01-01T00:00:00.000Z' }],
    },
  });
  const provider = new SqlShardProvider({ adapter, collections: [accountDefinition] });
  const removed = await provider.remove('accounts', 'acc-1');
  assert.equal(removed.hardDeleted, true);
  assert.equal(tables.accounts.has('acc-1'), false);
});

test('SqlShardProvider bulk operations dedupe ids and validate input', async () => {
  const { provider, callLog } = createSqlProvider({
    adapterOptions: {
      seed: {
        accounts: [
          { account_id: 'acc-1', name: 'A', status: 'lead', tier: 'standard', isDeleted: false, createdAt: '2026-01-01T00:00:00.000Z', updatedAt: '2026-01-01T00:00:00.000Z' },
          { account_id: 'acc-2', name: 'B', status: 'lead', tier: 'standard', isDeleted: false, createdAt: '2026-01-02T00:00:00.000Z', updatedAt: '2026-01-02T00:00:00.000Z' },
        ],
      },
    },
  });

  const deleted = await provider.bulkDelete('accounts', ['acc-1', 'acc-1', ' ', null, 'acc-2']);
  assert.equal(deleted.length, 2);
  assert.equal(callLog.filter((entry) => entry.type === 'update').length, 2);

  await assert.rejects(() => provider.bulkDelete('accounts', []), (error) => expectCode(error, DEFAULT_CONFIG.ERROR_CODES.INVALID_ID));
  await assert.rejects(() => provider.bulkDelete('accounts', null), (error) => expectCode(error, DEFAULT_CONFIG.ERROR_CODES.INVALID_ID));
});

test('SqlShardProvider commitWriteOperations fallback and custom adapter path both work', async () => {
  const fallbackBundle = createMemorySqlAdapter();
  const fallbackProvider = new SqlShardProvider({
    adapter: { collections: fallbackBundle.adapter.collections },
    collections: [defineAccountCollection()],
  });

  await fallbackProvider.commitWriteOperations([
    { type: 'create', collection: 'accounts', data: { account_id: 'acc-1', name: 'Acme' } },
    { type: 'update', collection: 'accounts', id: 'acc-1', data: { status: 'active' } },
    { type: 'remove', collection: 'accounts', id: 'acc-1' },
  ]);
  assert.equal(fallbackBundle.tables.accounts.has('acc-1'), false);

  const custom = createSqlProvider({
    adapterOptions: {
      hooks: {
        async commitWriteOperations({ operations }) {
          return operations.map((entry) => ({ ok: true, type: entry.type }));
        },
      },
    },
  });
  const committed = await custom.provider.commitWriteOperations([{ type: 'create', collection: 'accounts', data: { account_id: 'acc-2', name: 'Beta' } }]);
  assert.equal(committed.length, 1);

  await assert.rejects(() => fallbackProvider.commitWriteOperations([{ type: '???', collection: 'accounts', id: 'acc-2', data: {} }]), (error) => expectCode(error, DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT));
});

test('SqlShardProvider search supports token mode and rejects non-configured/external search', async () => {
  const { provider } = createSqlProvider({
    adapterOptions: {
      seed: {
        accounts: [{ account_id: 'acc-1', name: 'Acme Namibia', status: 'lead', tier: 'standard', isDeleted: false, _searchTokens: ['acme', 'namibia'], createdAt: '2026-01-01T00:00:00.000Z', updatedAt: '2026-01-01T00:00:00.000Z' }],
      },
    },
  });
  const results = await provider.search('accounts', 'Acme');
  assert.equal(results.length, 1);

  const noSearchDefinition = defineAccountCollection({ search: { mode: DEFAULT_CONFIG.SEARCH_MODES.NONE, fields: [] } });
  const noSearchProvider = new SqlShardProvider({ adapter: createMemorySqlAdapter().adapter, collections: [noSearchDefinition] });
  await assert.rejects(() => noSearchProvider.search('accounts', 'Acme'), (error) => expectCode(error, DEFAULT_CONFIG.ERROR_CODES.SEARCH_NOT_CONFIGURED));

  const externalSearchDefinition = defineAccountCollection({ search: { mode: DEFAULT_CONFIG.SEARCH_MODES.EXTERNAL, fields: ['name'] } });
  const externalSearchProvider = new SqlShardProvider({ adapter: createMemorySqlAdapter().adapter, collections: [externalSearchDefinition] });
  await assert.rejects(() => externalSearchProvider.search('accounts', 'Acme'), (error) => expectCode(error, DEFAULT_CONFIG.ERROR_CODES.SEARCH_NOT_CONFIGURED));
});

test('SqlShardProvider include engine handles required includes, unknown includes, and inline descriptors', async () => {
  const { provider } = createSqlProvider({
    adapterOptions: {
      seed: {
        accounts: [{ account_id: 'acc-1', name: 'Acme', status: 'lead', tier: 'standard', isDeleted: false, createdAt: '2026-01-01T00:00:00.000Z', updatedAt: '2026-01-01T00:00:00.000Z' }],
      },
    },
    contactRelation: { required: true },
  });

  await assert.rejects(() => provider.getById('accounts', 'acc-1', { includes: ['contacts'] }), (error) => expectCode(error, DEFAULT_CONFIG.ERROR_CODES.NOT_FOUND));
  await assert.rejects(() => provider.getById('accounts', 'acc-1', { includes: ['missing_relation'] }), (error) => expectCode(error, DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT));

  const hydrated = await provider.getById('accounts', 'acc-1', {
    includes: [{ collection: 'contacts', localField: 'account_id', foreignField: 'account_id', many: true, as: 'contactList' }],
  });
  assert.deepEqual(hydrated.data.contactList, []);

  await assert.rejects(() => provider.getById('accounts', 'acc-1', { includes: [{ localField: 'account_id' }] }), (error) => expectCode(error, DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT));
});

test('SqlShardProvider missing collection adapter or methods surface clear errors', async () => {
  const provider = new SqlShardProvider({ adapter: createDataConnectAdapter({ collections: {} }), collections: [defineAccountCollection()] });
  await assert.rejects(() => provider.getById('accounts', 'acc-1'), (error) => expectCode(error, DEFAULT_CONFIG.ERROR_CODES.COLLECTION_NOT_REGISTERED));

  const badAdapter = createDataConnectAdapter({ collections: { accounts: {} } });
  const badProvider = new SqlShardProvider({ adapter: badAdapter, collections: [defineAccountCollection()] });
  await assert.rejects(() => badProvider.getById('accounts', 'acc-1'), (error) => expectCode(error, DEFAULT_CONFIG.ERROR_CODES.QUERY_NOT_ALLOWED));
});

test('SqlShardProvider set and update enforce schema rules and immutable ids', async () => {
  const { provider } = createSqlProvider({
    adapterOptions: {
      seed: {
        accounts: [{ account_id: 'acc-1', name: 'Acme', status: 'lead', tier: 'standard', isDeleted: false, createdAt: '2026-01-01T00:00:00.000Z', updatedAt: '2026-01-01T00:00:00.000Z' }],
      },
    },
  });

  await assert.rejects(() => provider.set('accounts', 'acc-2', { name: 'No PK' }), (error) => expectCode(error, DEFAULT_CONFIG.ERROR_CODES.REQUIRED_FIELD));
  await assert.rejects(() => provider.update('accounts', 'acc-1', { account_id: 'changed' }), (error) => expectCode(error, DEFAULT_CONFIG.ERROR_CODES.READONLY_FIELD));
});
