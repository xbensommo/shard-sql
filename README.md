# @xbensommo/shard-sql

Standalone SQL-first provider package for adapter-driven CRUD, schema validation, relation includes, **proper page objects**, **cursor-ready pagination**, bulk actions, and improved built-in search for SQL and Firebase Data Connect projects.

This package exists so SQL can grow cleanly **without** carrying Firestore runtime baggage.

## What it is for

- PostgreSQL-backed business apps
- Firebase Data Connect / SQL Connect projects
- adapter-driven CRUD where every collection maps to explicit operations
- shared collection definitions, validation, includes, search, and store actions

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
  DEFAULT_CONFIG,
} from '@xbensommo/shard-sql';

const accounts = defineCollection({
  name: 'accounts',
  backend: { engine: 'sql', table: 'accounts', connector: 'crm' },
  primaryKey: 'account_id',
  metadata: { softDelete: true },
  search: {
    mode: DEFAULT_CONFIG.SEARCH_MODES.TOKEN_ARRAY,
    fields: ['name'],
  },
  schema: {
    account_id: { type: FIELD_TYPES.STRING, required: true, immutable: true },
    name: { type: FIELD_TYPES.STRING, required: true, sortable: true, searchable: true },
    status: { type: FIELD_TYPES.STRING, enum: ['lead', 'active', 'inactive'], filterable: true },
    createdAt: { type: FIELD_TYPES.TIMESTAMP, sortable: true },
    updatedAt: { type: FIELD_TYPES.TIMESTAMP, sortable: true },
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
      async search({ term, queryInput }) {
        return await searchAccounts(term, queryInput);
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

const page = await provider.fetchPage('accounts', {
  pageMode: 'cursor',
  limit: 20,
  orderBy: [{ field: 'createdAt', direction: 'desc' }],
});

const nextPage = await provider.fetchPage('accounts', {
  pageMode: 'cursor',
  cursor: page.pagination.nextCursor,
  limit: 20,
  orderBy: [{ field: 'createdAt', direction: 'desc' }],
});

const searchPage = await provider.searchPage('accounts', 'jane smith', {
  limit: 10,
  offset: 0,
});
```

## Pagination

`fetchPage()` returns a normalized page object.

```js
const page = await provider.fetchPage('accounts', {
  limit: 20,
  pageMode: 'cursor', // or 'offset'
  cursor: null,
  orderBy: [{ field: 'createdAt', direction: 'desc' }],
});
```

Returned shape:

```js
{
  items: [...],
  total: 42,
  hasMore: true,
  nextCursor: '...',
  prevCursor: '...',
  pagination: {
    mode: 'cursor',
    offset: 0,
    pageSize: 20,
    nextOffset: 20,
    prevOffset: null,
    nextCursor: '...',
    prevCursor: null,
    hasMore: true,
    total: 42,
  }
}
```

Notes:
- offset and cursor requests are both supported
- offset-backed adapters can still be used with cursor mode through provider-generated cursor tokens
- adapters can also return native cursor pagination directly

## Search

Two search paths are supported:

1. **Adapter/provider search** — preferred for production scale
2. **Built-in search fallback** — provider scans ranked candidates using `_searchText`, `_searchTokens`, and `_searchPrefixes`

Supported search modes:
- `token-array` — full multi-token matching
- `prefix` — partial multi-token matching
- `external` — handled by `searchProvider` or adapter `search()`

Use `searchPage()` when you need page metadata:

```js
const page = await provider.searchPage('accounts', 'jan smi', {
  limit: 10,
  pageMode: 'offset',
});
```

Use `search()` when you only need items:

```js
const items = await provider.search('accounts', 'jane smith');
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
