# @xbensommo/shard-provider-sql

Standalone SQL-first package extracted from the mixed shard-provider build.

This package is for:
- PostgreSQL-backed apps
- Firebase Data Connect / SQL Connect projects
- adapter-driven CRUD where each collection maps to explicit SQL operations
- schema validation, include hydration, pagination, bulk actions, and store helpers

This package is **not** the Firestore package.
It intentionally removes:
- Firestore runtime/provider
- Firestore rules generation
- Firestore index generation
- Firebase peer dependency coupling

## Why this standalone package exists

The mixed package worked, but expansion was going to get messy:
- SQL and Firestore have different execution models
- Data Connect relies on generated operations, not runtime collection discovery
- package boundaries matter if you want SQL support to grow cleanly

So this package isolates the SQL architecture and keeps the public surface focused.

## Core pieces

- `SqlShardProvider` — runtime provider
- `createSqlAdapter()` — generic SQL adapter wrapper
- `createDataConnectAdapter()` — alias helper for Firebase Data Connect generated operations
- `defineCollection()` — schema, identity, relations, search, metadata
- `createShardedActions()` — state/store helper
- `IncludeEngine` — relation hydration across collections

## Install

```bash
npm install @xbensommo/shard-provider-sql
```

## Minimal usage

```js
import {
  SqlShardProvider,
  createDataConnectAdapter,
  defineCollection,
  FIELD_TYPES,
} from '@xbensommo/shard-provider-sql';

const accounts = defineCollection({
  name: 'accounts',
  backend: { engine: 'sql', table: 'accounts' },
  primaryKey: 'account_id',
  schema: {
    account_id: { type: FIELD_TYPES.STRING, required: true, immutable: true },
    name: { type: FIELD_TYPES.STRING, required: true, filterable: true, sortable: true },
    status: { type: FIELD_TYPES.STRING, enum: ['lead', 'active', 'inactive'], filterable: true },
    createdAt: { type: FIELD_TYPES.TIMESTAMP, sortable: true },
    updatedAt: { type: FIELD_TYPES.TIMESTAMP },
    isDeleted: { type: FIELD_TYPES.BOOLEAN, filterable: true },
  },
});

const adapter = createDataConnectAdapter({
  collections: {
    accounts: {
      async getById({ id }) {
        return await getAccountById(id);
      },
      async fetchPage({ queryInput }) {
        return await listAccounts(queryInput);
      },
      async create({ data }) {
        return await createAccount(data);
      },
      async update({ id, data }) {
        return await updateAccount(id, data);
      },
      async destroy({ id }) {
        return await deleteAccount(id);
      },
    },
  },
});

const provider = new SqlShardProvider({
  adapter,
  collections: [accounts],
  strictSchemas: true,
});
```

## Architectural note

Firebase Data Connect / SQL Connect is schema-and-operation driven.
That means this package does **not** auto-discover SQL CRUD at runtime.
You wire each collection to explicit generated queries/mutations through the adapter.

That is the clean design, and it is why this package is easier to expand now.

## Tests

```bash
npm test
```
"# shard-sql" 
