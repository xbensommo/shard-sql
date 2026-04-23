import test from 'node:test';
import assert from 'node:assert/strict';

import { createShardedActions } from '../src/actions/createShardedActions.js';
import { DEFAULT_CONFIG } from '../src/constants.js';
import { createSqlProvider, makeRef } from './_helpers.mjs';

function createState(shape = 'object') {
  if (shape === 'array') {
    return {
      accounts: makeRef([]),
      accountsMeta: makeRef({ lastFilter: null }),
      isLoading: makeRef(false),
    };
  }
  return {
    accounts: makeRef({ items: [], total: 0, pagination: { nextOffset: null, hasMore: false } }),
    accountsMeta: makeRef({ lastFilter: null }),
    isLoading: makeRef(false),
  };
}

test('createShardedActions fetchInitialPage replaces state and fetchNextPage appends', async () => {
  const { provider } = createSqlProvider({
    adapterOptions: {
      seed: {
        accounts: [
          { account_id: 'acc-1', name: 'Zulu', status: 'lead', tier: 'standard', isDeleted: false, createdAt: '2026-01-03T00:00:00.000Z', updatedAt: '2026-01-03T00:00:00.000Z' },
          { account_id: 'acc-2', name: 'Bravo', status: 'lead', tier: 'standard', isDeleted: false, createdAt: '2026-01-02T00:00:00.000Z', updatedAt: '2026-01-02T00:00:00.000Z' },
          { account_id: 'acc-3', name: 'Alpha', status: 'lead', tier: 'standard', isDeleted: false, createdAt: '2026-01-01T00:00:00.000Z', updatedAt: '2026-01-01T00:00:00.000Z' },
        ],
      },
    },
  });
  const state = createState();
  const actions = createShardedActions('accounts', state, provider);

  const firstPage = await actions.fetchInitialPage({ limit: 2, orderBy: [{ field: 'name', direction: 'asc' }] });
  assert.equal(firstPage.items.length, 2);
  assert.equal(state.accounts.value.items.length, 2);
  assert.equal(state.accounts.value.items[0].data.name, 'Alpha');
  assert.equal(state.accountsMeta.value.total, 3);

  const nextPage = await actions.fetchNextPage({ orderBy: [{ field: 'name', direction: 'asc' }] });
  assert.equal(nextPage.items.length, 1);
  assert.equal(state.accounts.value.items.length, 3);
  assert.equal(state.accounts.value.items[2].data.name, 'Zulu');
  assert.equal(state.isLoading.value, false);
});

test('createShardedActions handles no-more-pages, array state targets, and updateState false', async () => {
  const { provider } = createSqlProvider({
    adapterOptions: {
      seed: {
        accounts: [{ account_id: 'acc-1', name: 'Acme', status: 'lead', tier: 'standard', isDeleted: false, createdAt: '2026-01-01T00:00:00.000Z', updatedAt: '2026-01-01T00:00:00.000Z' }],
      },
    },
  });
  const state = createState('array');
  const actions = createShardedActions('accounts', state, provider);

  await actions.fetchInitialPage({ limit: 5 });
  assert.equal(Array.isArray(state.accounts.value), true);
  assert.equal(state.accounts.value.length, 1);

  const page = await actions.fetchNextPage();
  assert.equal(page.items.length, 0);
  assert.equal(page.hasMore, false);

  const before = [...state.accounts.value];
  const results = await actions.fetchByFilters([{ field: 'name', op: '==', value: 'Acme' }], { updateState: false });
  assert.equal(results.length, 1);
  assert.deepEqual(state.accounts.value, before);
});

test('createShardedActions add/update/remove/restore/bulk flows keep state in sync', async () => {
  const { provider } = createSqlProvider();
  const state = createState();
  const actions = createShardedActions('accounts', state, provider);

  await actions.add({ account_id: 'acc-1', name: 'Acme' });
  await actions.add({ account_id: 'acc-2', name: 'Beta' });
  assert.equal(state.accounts.value.items.length, 2);

  await actions.update('acc-1', { status: 'active' });
  assert.equal(state.accounts.value.items.find((entry) => entry.id === 'acc-1').data.status, 'active');

  await actions.remove('acc-2', { keepInState: true });
  assert.equal(state.accounts.value.items.find((entry) => entry.id === 'acc-2').isDeleted, true);
  assert.equal(state.accounts.value.items.find((entry) => entry.id === 'acc-2').data.isDeleted, false);

  await actions.restore('acc-2');
  assert.equal(state.accounts.value.items.find((entry) => entry.id === 'acc-2').data.isDeleted, false);

  await actions.bulkDelete(['acc-1', 'acc-2']);
  assert.equal(state.accounts.value.items.length, 0);

  await actions.add({ account_id: 'acc-3', name: 'Gamma' });
  await actions.bulkDestroy(['acc-3']);
  assert.equal(state.accounts.value.items.length, 0);
});

test('createShardedActions keepInState delete patches top-level state field and loading resets on failure', async () => {
  const { provider } = createSqlProvider();
  const state = createState();
  const actions = createShardedActions('accounts', state, provider);

  await actions.add({ account_id: 'acc-1', name: 'Acme' });
  await actions.remove('acc-1', { keepInState: true });
  assert.equal(state.accounts.value.items[0][DEFAULT_CONFIG.SOFT_DELETE_FIELD], true);

  await assert.rejects(() => actions.add({ name: 'Missing id' }), /required/i);
  assert.equal(state.isLoading.value, false);
});
