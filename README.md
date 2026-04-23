# @xbensommo/shard-sql

Standalone SQL-first provider package for adapter-driven CRUD, schema validation, relation includes, pagination, bulk actions, and Firebase Data Connect integration.

This package exists so SQL can grow cleanly **without** carrying Firestore runtime baggage.

## What it is for

- PostgreSQL-backed business apps
- Firebase Data Connect / SQL Connect projects
- adapter-driven CRUD where every collection maps to explicit operations
- shared collection definitions, validation, includes, and store actions

## What it intentionally does not include

- Firestore runtime
- Firestore rules generation
- Firestore composite index generation
- Firebase peer dependencies as a hard requirement

## Install

```bash
npm install @xbensommo/shard-sql
```

## Quick start

```js
import {
  SqlShardProvider,
  createDataConnectAdapter,
  defineCollection,
  FIELD_TYPES,
} from '@xbensommo/shard-sql';

const accounts = defineCollection({
  name: 'accounts',
  backend: { engine: 'sql', table: 'accounts', connector: 'crm' },
  primaryKey: 'account_id',
  metadata: { softDelete: true },
  schema: {
    account_id: { type: FIELD_TYPES.STRING, required: true, immutable: true },
    name: { type: FIELD_TYPES.STRING, required: true, sortable: true, searchable: true },
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

## Package structure

```text
src/
  actions/
  adapters/
  compat/
  core/
  runtime/
  search/
  constants.js
  errors.js
  index.js
  validators.js

docs/
  ADAPTER_GUIDE.md
  ARCHITECTURE.md
  DATA_CONNECT.md
  MIGRATION.md
  PUBLISHING.md

tests/
.github/workflows/
```

## Scripts

```bash
npm run build       # build JS bundles into dist/
npm run build:types # emit declaration files into dist/
npm run build:all   # full build
npm test            # run test suite
npm run test:ci     # build + tests + smoke test
npm run smoke       # validate dist exports load correctly
```

## Build output

The package publishes from `dist/` and includes:
- ESM builds
- CommonJS builds
- declaration files
- source maps

## Publishing

This repository includes workflows for:
- CI
- npm publish
- GitHub Packages publish

Read:
- [Publishing guide](docs/PUBLISHING.md)
- [Architecture notes](docs/ARCHITECTURE.md)
- [Adapter guide](docs/ADAPTER_GUIDE.md)
- [Data Connect notes](docs/DATA_CONNECT.md)
- [Migration notes](docs/MIGRATION.md)

## Design rule

SQL/Data Connect is **not** runtime-discovered like Firestore.
The right design is explicit adapter wiring.
That is why this package focuses on provider orchestration and leaves execution to adapters.

## Development

```bash
npm install
npm run test:ci
```

## License

MIT
