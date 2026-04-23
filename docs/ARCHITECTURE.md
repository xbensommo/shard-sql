# Architecture

## Goal

`@xbensommo/shard-sql` is the SQL-first sibling to the Firestore shard-provider. It keeps the same high-level ergonomics where that still makes sense—collection definitions, schema validation, relation includes, pagination helpers, search orchestration, and sharded actions—but it removes Firestore runtime assumptions.

## Core boundaries

### 1. Collection definition
`defineCollection()` is still the central contract.
It defines:
- collection name
- backend metadata such as engine/table/connector
- schema
- identity / primary key
- relation metadata
- search metadata
- soft-delete metadata

### 2. Provider runtime
`SqlShardProvider` is the orchestrator.
It is responsible for:
- validating collection requests
- normalizing ids and page responses
- supporting offset and cursor pagination
- calling adapter methods
- applying strict schema rules
- resolving includes through `IncludeEngine`
- mapping adapter errors into provider errors
- normalizing soft delete / restore behavior
- providing built-in search fallback when adapters do not expose native search

### 3. Adapter contract
Adapters are the execution layer.
The provider does **not** talk to SQL directly.
It expects explicit operations such as:
- `getById`
- `fetchPage`
- `fetchByFilters`
- `search` (recommended)
- `create`
- `set`
- `update`
- `destroy`
- optional `commitWriteOperations`
- optional `transaction`

This is what keeps the package expandable.
Different engines can share the same provider contract while using different execution strategies.

### 4. Search boundary
Search can be handled in three ways:
- adapter-native search via `operations.search()`
- external provider search via `searchProvider`
- provider-managed fallback search using normalized `_searchText`, `_searchTokens`, and `_searchPrefixes`

For enterprise-scale datasets, adapter-native or external search is the right path.
The built-in fallback is best for moderate datasets and compatibility.

### 5. Includes and relations
Relations remain provider-level, not adapter-level.
That means:
- adapters return rows
- provider normalizes rows into records
- include engine performs relation hydration

This prevents each adapter from reinventing relation loading.

## Why it is standalone

SQL and Firestore have different realities:
- Firestore is collection/document oriented and runtime-friendly
- SQL/Data Connect is schema/operation driven

Keeping them in one package makes versioning, testing, and expansion harder.
The standalone package keeps SQL decisions isolated and future-safe.
