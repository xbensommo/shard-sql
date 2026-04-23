# Migration from the mixed SQL experiment

## What changed

The earlier SQL support lived inside a mixed package layout.
This standalone package changes that in a few important ways:

- source now lives under `src/`
- publish output lives under `dist/`
- package exports point to built artifacts, not raw source files
- docs now describe SQL-first usage only
- Firestore runtime and generator concerns are gone

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

## What did not change

The main app-facing concepts remain familiar:
- `defineCollection()`
- `SqlShardProvider`
- adapter-driven CRUD
- `createShardedActions()`
- include hydration
