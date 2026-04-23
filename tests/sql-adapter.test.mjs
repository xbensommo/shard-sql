import test from 'node:test';
import assert from 'node:assert/strict';

import { DEFAULT_CONFIG } from '../src/constants.js';
import { createSqlAdapter } from '../src/adapters/createSqlAdapter.js';
import { createSqlProvider } from './_helpers.mjs';

function expectCode(fn, code) {
  assert.throws(fn, (error) => error?.code === code);
}

test('createSqlAdapter requires collections map', () => {
  expectCode(() => createSqlAdapter(), DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT);
  expectCode(() => createSqlAdapter({ collections: [] }), DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT);
});

test('createSqlAdapter resolveCollection returns mapped collection or null', () => {
  const adapter = createSqlAdapter({ collections: { accounts: { getById() { return null; } } } });
  assert.ok(adapter.resolveCollection('accounts'));
  assert.equal(adapter.resolveCollection('missing'), null);
});

test('createSqlAdapter commitWriteOperations dispatches create/set/update/delete and rejects invalid types', async () => {
  const { provider, tables } = createSqlProvider();
  const adapter = createSqlAdapter({ collections: { accounts: provider.resolveCollectionAdapter('accounts') } });

  await adapter.commitWriteOperations({
    provider,
    options: {},
    operations: [
      { type: 'create', collection: 'accounts', data: { account_id: 'acc-1', name: 'Acme' } },
      { type: 'set', collection: 'accounts', id: 'acc-2', data: { account_id: 'acc-2', name: 'Beta' } },
      { type: 'update', collection: 'accounts', id: 'acc-2', data: { status: 'active' } },
      { type: 'delete', collection: 'accounts', id: 'acc-1' },
    ],
  });

  assert.equal(tables.accounts.has('acc-1'), false);
  assert.equal(tables.accounts.get('acc-2').status, 'active');

  await assert.rejects(() => adapter.commitWriteOperations({
    provider,
    operations: [{ type: 'explode', collection: 'accounts', id: 'acc-2', data: {} }],
  }), (error) => error?.code === DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT);
});
