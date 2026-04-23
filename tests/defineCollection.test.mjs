import test from 'node:test';
import assert from 'node:assert/strict';

import { DEFAULT_CONFIG, FIELD_TYPES } from '../src/constants.js';
import { defineCollection } from '../src/core/defineCollection.js';
import { validateCreatePayload, validateUpdatePayload } from '../src/validators.js';

function expectCode(fn, code) {
  assert.throws(fn, (error) => error?.code === code);
}

test('defineCollection stores backend metadata, relations, aliases, and identity', () => {
  const definition = defineCollection({
    name: 'crm_accounts',
    backend: { engine: 'sql', table: 'crm_accounts', connector: 'crm' },
    primaryKey: { field: 'account_id', required: true },
    schema: {
      account_id: { type: FIELD_TYPES.STRING, required: true, immutable: true },
      ownerId: { type: FIELD_TYPES.STRING, required: true },
      status: { type: FIELD_TYPES.STRING, enum: ['lead', 'active'] },
    },
    relations: {
      owner: {
        collection: 'users',
        localField: 'ownerId',
        foreignField: 'user_id',
        many: false,
        as: 'owner',
      },
    },
  });

  assert.equal(definition.backend.engine, 'sql');
  assert.equal(definition.backend.table, 'crm_accounts');
  assert.equal(definition.identity.field, 'account_id');
  assert.equal(definition.identity.required, true);
  assert.equal(definition.fieldAliases.owner_id, 'ownerId');
  assert.equal(definition.relations.owner.foreignField, 'user_id');
  assert.equal(definition.storage.table, 'crm_accounts');
});

test('defineCollection rejects ambiguous aliases from snake/camel duplicates', () => {
  expectCode(() => defineCollection({
    name: 'broken',
    schema: {
      user_id: { type: FIELD_TYPES.STRING },
      userId: { type: FIELD_TYPES.STRING },
    },
  }), DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION);
});

test('defineCollection rejects missing primary key field', () => {
  expectCode(() => defineCollection({
    name: 'broken_pk',
    primaryKey: 'missing_id',
    schema: {
      name: { type: FIELD_TYPES.STRING },
    },
  }), DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION);
});

test('defineCollection rejects invalid relation local field and invalid search/shard config', () => {
  expectCode(() => defineCollection({
    name: 'broken_rel',
    schema: { account_id: { type: FIELD_TYPES.STRING } },
    relations: {
      contacts: { collection: 'contacts', localField: 'missing_field' },
    },
  }), DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION);

  expectCode(() => defineCollection({
    name: 'broken_search',
    search: { mode: 'wild-west' },
    schema: { account_id: { type: FIELD_TYPES.STRING } },
  }), DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION);

  expectCode(() => defineCollection({
    name: 'broken_shard',
    shard: { type: 'weekly' },
    schema: { account_id: { type: FIELD_TYPES.STRING } },
  }), DEFAULT_CONFIG.ERROR_CODES.INVALID_COLLECTION_DEFINITION);
});

test('validators normalize aliases, apply defaults, and reject invalid create/update payloads', () => {
  const definition = defineCollection({
    name: 'accounts',
    primaryKey: { field: 'account_id', required: true },
    schema: {
      account_id: { type: FIELD_TYPES.STRING, required: true, immutable: true },
      ownerId: { type: FIELD_TYPES.STRING, required: true },
      status: { type: FIELD_TYPES.STRING, enum: ['lead', 'active'], default: 'lead' },
      notes: { type: FIELD_TYPES.STRING, readonly: true },
      profile: { type: FIELD_TYPES.OBJECT },
    },
  });

  const payload = validateCreatePayload(definition, {
    accountId: 'acc-1',
    owner_id: 'usr-1',
    profile: { branch: 'windhoek' },
  });

  assert.deepEqual(payload, {
    account_id: 'acc-1',
    ownerId: 'usr-1',
    status: 'lead',
    profile: { branch: 'windhoek' },
  });

  expectCode(() => validateCreatePayload(definition, { account_id: 'acc-1', ownerId: 'usr-1', notes: 'blocked' }), DEFAULT_CONFIG.ERROR_CODES.READONLY_FIELD);
  expectCode(() => validateCreatePayload(definition, { account_id: 'acc-1', ownerId: 'usr-1', unknownField: true }), DEFAULT_CONFIG.ERROR_CODES.UNKNOWN_FIELD);
  expectCode(() => validateCreatePayload(definition, { account_id: 'acc-1', ownerId: 'usr-1', status: 'archived' }), DEFAULT_CONFIG.ERROR_CODES.ENUM_MISMATCH);
  expectCode(() => validateCreatePayload(definition, { account_id: 'acc-1', ownerId: 'usr-1', profile: [] }), DEFAULT_CONFIG.ERROR_CODES.INVALID_FIELD_TYPE);
  expectCode(() => validateCreatePayload(definition, { account_id: 'acc-1', accountId: 'acc-2', ownerId: 'usr-1' }), DEFAULT_CONFIG.ERROR_CODES.INVALID_DATA);

  const existing = { account_id: 'acc-1', ownerId: 'usr-1', status: 'lead' };
  const updatePayload = validateUpdatePayload(definition, { status: 'active' }, existing);
  assert.deepEqual(updatePayload, { status: 'active' });
  expectCode(() => validateUpdatePayload(definition, { accountId: 'acc-2' }, existing), DEFAULT_CONFIG.ERROR_CODES.READONLY_FIELD);
  expectCode(() => validateUpdatePayload(definition, { owner_id: 'usr-2', ownerId: 'usr-1' }, existing), DEFAULT_CONFIG.ERROR_CODES.INVALID_DATA);
});
