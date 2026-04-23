# Migration from the mixed SQL experiment

## What changed

The earlier SQL support lived inside a mixed package layout.
This standalone package changes that in a few important ways:

- source now lives under `src/`
- publish output lives under `dist/`
- package exports point to built artifacts, not raw source files
- docs now describe SQL-first usage only
- Firestore runtime and generator concerns are gone
- page responses are normalized consistently
- cursor-ready pagination is now supported
- search is now stronger and supports `searchPage()`

## Import changes

Old mixed-import idea:

```js
import { SqlShardProvider } from '@xbensommo/shard-provider';
```

New standalone import:

```js
import { SqlShardProvider } from '@xbensommo/shard-sql';
```

## Build changes

Old package state: source archive only.
New package state: buildable and publishable repository with CI workflows.

## Runtime changes

### Pagination
Old behavior was mostly offset normalization.
New behavior supports:
- offset pages
- cursor-ready pages
- adapter-native cursor pagination
- provider-generated cursor tokens for offset-backed adapters

### Search
Old behavior was minimal token/prefix filtering.
New behavior supports:
- `searchPage()` for page metadata
- better multi-token token search
- better multi-token prefix search
- external/provider search hooks
- adapter-native `search()` when available

## What did not change

The main app-facing concepts remain familiar:
- `defineCollection()`
- `SqlShardProvider`
- adapter-driven CRUD
- `createShardedActions()`
- include hydration
