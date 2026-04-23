# Adapter Guide

## Minimum contract

Each collection adapter should expose the operations your app needs.
The safest baseline is:

```js
{
  async getById({ id, definition, options, provider }) {},
  async fetchPage({ queryInput, definition, options, provider }) {},
  async fetchByFilters({ queryInput, definition, options, provider }) {},
  async create({ data, definition, options, provider }) {},
  async set({ id, data, definition, options, provider }) {},
  async update({ id, data, existing, definition, options, provider }) {},
  async destroy({ id, existing, definition, options, provider }) {}
}
```

## Returned record shape

The provider accepts either:

### Plain row
```js
{ account_id: 'acc-1', name: 'Acme Ltd' }
```

### Explicit record
```js
{
  id: 'acc-1',
  data: { account_id: 'acc-1', name: 'Acme Ltd' },
  shard: null,
}
```

Plain rows are easier. The provider will normalize them.

## Page shape

`fetchPage()` can return either a plain array or a structured page object.
The preferred object is:

```js
{
  items: [...],
  total: 42,
  hasMore: true,
  pagination: {
    mode: 'offset',
    offset: 0,
    pageSize: 20,
    nextOffset: 20,
    hasMore: true,
    total: 42,
  }
}
```

## Data Connect pattern

For Firebase Data Connect, map each collection to generated queries and mutations.
Do not try to recreate Firestore-style dynamic CRUD.
Instead, wire explicit operations to your generated SDK.

## Error mapping

Let your adapter throw raw driver errors when needed.
The provider will normalize many of them into friendly package errors.
If you already normalize errors inside the adapter, keep the error shape stable.
