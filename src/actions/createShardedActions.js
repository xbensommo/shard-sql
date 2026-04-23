import { DEFAULT_CONFIG } from '../constants.js';

export function createShardedActions(collectionName, state, provider) {
  if (!collectionName || typeof collectionName !== 'string') {
    throw new Error('createShardedActions requires a non-empty collection name.');
  }
  if (!provider) {
    throw new Error(`createShardedActions('${collectionName}') requires a provider instance.`);
  }

  const getStoreRef = () => state?.[collectionName];
  const getCollectionStateValue = () => getStoreRef()?.value;
  const setCollectionState = (value) => {
    const target = getStoreRef();
    if (target?.value === undefined) return;

    const current = target.value;
    if (Array.isArray(current)) {
      target.value = Array.isArray(value) ? value : (Array.isArray(value?.items) ? value.items : value);
      return;
    }

    if (current && typeof current === 'object' && !Array.isArray(current)) {
      if (Array.isArray(value)) {
        target.value = { ...current, items: value };
        return;
      }
      if (value && typeof value === 'object') {
        target.value = { ...current, ...value };
        return;
      }
    }

    target.value = value;
  };

  const getItems = () => {
    const value = getCollectionStateValue();
    if (Array.isArray(value)) return value;
    if (Array.isArray(value?.items)) return value.items;
    return [];
  };

  const setItems = (items) => {
    const target = getStoreRef();
    if (target?.value === undefined) return;
    const current = target.value;
    if (Array.isArray(current)) {
      target.value = items;
      return;
    }
    if (current && typeof current === 'object') {
      if (Object.prototype.hasOwnProperty.call(current, 'items')) {
        target.value = { ...current, items };
        return;
      }
    }
    target.value = items;
  };

  const setFetchedState = (result, options = {}) => {
    const incomingItems = Array.isArray(result?.items) ? result.items : (Array.isArray(result) ? result : []);
    const shouldAppend = options.append === true;
    const target = getStoreRef();

    if (target?.value !== undefined) {
      const current = target.value;

      if (Array.isArray(current)) {
        if (!shouldAppend) {
          target.value = incomingItems;
        } else {
          const merged = [...current];
          const seen = new Map(current.map((item, index) => [item?.id, index]));
          incomingItems.forEach((item) => {
            const idx = item?.id ? seen.get(item.id) : undefined;
            if (idx !== undefined) merged[idx] = item;
            else merged.push(item);
          });
          target.value = merged;
        }
      } else if (current && typeof current === 'object') {
        const baseItems = Array.isArray(current.items) ? current.items : [];
        const nextItems = !shouldAppend
          ? incomingItems
          : (() => {
              const merged = [...baseItems];
              const seen = new Map(baseItems.map((item, index) => [item?.id, index]));
              incomingItems.forEach((item) => {
                const idx = item?.id ? seen.get(item.id) : undefined;
                if (idx !== undefined) merged[idx] = item;
                else merged.push(item);
              });
              return merged;
            })();

        const nextState = { ...current, items: nextItems };
        if (result && typeof result === 'object' && !Array.isArray(result)) {
          Object.entries(result).forEach(([key, value]) => {
            if (key === 'items') return;
            nextState[key] = value;
          });
        }
        if (!(result && typeof result === 'object' && !Array.isArray(result) && typeof result.total === 'number')) {
          nextState.total = nextItems.length;
        }
        target.value = nextState;
      } else {
        target.value = incomingItems;
      }
    }

    const metaRef = state?.[`${collectionName}Meta`];
    if (metaRef?.value !== undefined && result && typeof result === 'object' && !Array.isArray(result)) {
      metaRef.value = {
        ...(metaRef.value && typeof metaRef.value === 'object' ? metaRef.value : {}),
        ...(Object.fromEntries(Object.entries(result).filter(([key]) => key !== 'items'))),
      };
    }
  };

  const mergeItem = (item) => {
    const items = getItems();
    const idx = items.findIndex((entry) => entry.id === item.id);
    const next = [...items];
    if (idx >= 0) next.splice(idx, 1, item);
    else next.unshift(item);
    setItems(next);
  };

  const patchItem = (id, patch = {}) => {
    const items = getItems();
    setItems(items.map((entry) => (entry.id === id ? { ...entry, ...patch } : entry)));
  };

  const removeItem = (id) => {
    const items = getItems();
    setItems(items.filter((entry) => entry.id !== id));
  };

  const withLoading = async (fn) => {
    try {
      if (state?.isLoading?.value !== undefined) state.isLoading.value = true;
      return await fn();
    } finally {
      if (state?.isLoading?.value !== undefined) state.isLoading.value = false;
    }
  };

  const actions = {
    async add(data, options = {}) {
      return withLoading(async () => {
        const result = await provider.create(collectionName, data, options);
        mergeItem(result);
        return result;
      });
    },

    async set(id, data, options = {}) {
      return withLoading(async () => {
        const result = await provider.set(collectionName, id, data, options);
        mergeItem(result);
        return result;
      });
    },

    async update(id, data, options = {}) {
      return withLoading(async () => {
        const result = await provider.update(collectionName, id, data, options);
        mergeItem(result);
        return result;
      });
    },

    async remove(id, options = {}) {
      return withLoading(async () => {
        const result = await provider.remove(collectionName, id, options);
        if (options.keepInState) patchItem(id, { [DEFAULT_CONFIG.SOFT_DELETE_FIELD]: true });
        else removeItem(id);
        return result;
      });
    },

    async restore(id, options = {}) {
      return withLoading(async () => {
        const result = await provider.restore(collectionName, id, options);
        mergeItem(result);
        return result;
      });
    },

    async destroy(id, options = {}) {
      return withLoading(async () => {
        const result = await provider.destroy(collectionName, id, options);
        removeItem(id);
        return result;
      });
    },

    async getById(id, options = {}) {
      return withLoading(() => provider.getById(collectionName, id, options));
    },

    async getByPrimaryKey(value, options = {}) {
      return withLoading(() => provider.getByPrimaryKey(collectionName, value, options));
    },

    async fetchByForeignKey(field, valueOrValues, options = {}) {
      return withLoading(async () => {
        const results = await provider.fetchByForeignKey(collectionName, field, valueOrValues, options);
        if (options.updateState !== false) setFetchedState(results);
        return results;
      });
    },

    async fetchByFilters(filters = [], options = {}) {
      return withLoading(async () => {
        const results = await provider.fetchByFilters(collectionName, {
          filters,
          orderBy: options.orderBy || [],
          limit: options.limit || options.pageSize || DEFAULT_CONFIG.DEFAULT_PAGE_SIZE,
          range: options.range,
          includeDeleted: options.includeDeleted === true,
          includes: options.includes,
        }, options);
        if (options.updateState !== false) setFetchedState(results);
        return results;
      });
    },

    async fetchInitialPage(options = {}) {
      return withLoading(async () => {
        const page = await provider.fetchPage(collectionName, {
          filters: options.filters || [],
          orderBy: options.orderBy || [{ field: 'createdAt', direction: 'desc' }],
          limit: options.limit || options.pageSize || DEFAULT_CONFIG.DEFAULT_PAGE_SIZE,
          range: options.range,
          includeDeleted: options.includeDeleted === true,
          includes: options.includes,
          offset: 0,
        }, options);
        setFetchedState(page, { append: options.append === true });
        return page;
      });
    },

    async fetchNextPage(options = {}) {
      return withLoading(async () => {
        const current = getCollectionStateValue() || {};
        const currentPagination = current?.pagination || {};
        const pageSize = options.limit || options.pageSize || current.pageSize || currentPagination.pageSize || DEFAULT_CONFIG.DEFAULT_PAGE_SIZE;
        const nextOffset = Number.isFinite(Number(options.offset))
          ? Number(options.offset)
          : (Number.isFinite(Number(currentPagination.nextOffset)) ? Number(currentPagination.nextOffset) : getItems().length);

        if (currentPagination.hasMore === false || currentPagination.nextOffset === null) {
          return {
            items: [],
            total: typeof current.total === 'number' ? current.total : getItems().length,
            hasMore: false,
            lastVisible: current.lastVisible ?? null,
            pagination: {
              ...(currentPagination && typeof currentPagination === 'object' ? currentPagination : {}),
              hasMore: false,
            },
            filters: Array.isArray(current.activeFilters) ? current.activeFilters : (Array.isArray(current.filters) ? current.filters : []),
            orderBy: Array.isArray(current.orderBy) ? current.orderBy : (current.orderBy ? [current.orderBy] : []),
            pageSize,
            includeDeleted: current.includeDeleted === true,
            range: current.range,
          };
        }

        const page = await provider.fetchPage(collectionName, {
          filters: options.filters || (Array.isArray(current.activeFilters) ? current.activeFilters : (Array.isArray(current.filters) ? current.filters : [])),
          orderBy: options.orderBy || (Array.isArray(current.orderBy) ? current.orderBy : (current.orderBy ? [current.orderBy] : [])),
          limit: pageSize,
          range: options.range || current.range,
          includeDeleted: options.includeDeleted === true || current.includeDeleted === true,
          includes: options.includes || current.includes,
          offset: nextOffset,
        }, options);

        if (options.updateState !== false) setFetchedState(page, { append: true });
        return page;
      });
    },

    async fetchNext(options = {}) {
      return actions.fetchNextPage(options);
    },

    async bulkUpdateStatus(ids = [], status, options = {}) {
      return withLoading(async () => {
        const results = await provider.bulkUpdateStatus(collectionName, ids, status, options);
        const updatedIds = new Set(results.map((item) => item.id));
        const items = getItems().map((entry) => {
          if (!updatedIds.has(entry.id)) return entry;
          const match = results.find((item) => item.id === entry.id);
          return match?.data ? { ...entry, ...match.data } : entry;
        });
        setItems(items);
        return results;
      });
    },

    async bulkDelete(ids = [], options = {}) {
      return withLoading(async () => {
        const results = await provider.bulkDelete(collectionName, ids, options);
        if (options.keepInState) {
          const deletedIds = new Set(results.map((item) => item.id));
          setItems(getItems().map((entry) => deletedIds.has(entry.id)
            ? { ...entry, [DEFAULT_CONFIG.SOFT_DELETE_FIELD]: true }
            : entry));
        } else {
          const deletedIds = new Set(results.map((item) => item.id));
          setItems(getItems().filter((entry) => !deletedIds.has(entry.id)));
        }
        return results;
      });
    },

    async bulkRestore(ids = [], options = {}) {
      return withLoading(async () => {
        const results = await provider.bulkRestore(collectionName, ids, options);
        const items = [...getItems()];
        results.forEach((result) => {
          const idx = items.findIndex((entry) => entry.id === result.id);
          if (idx >= 0) items[idx] = { ...items[idx], ...(result.data || {}) };
          else items.unshift(result);
        });
        setItems(items);
        return results;
      });
    },

    async bulkDestroy(ids = [], options = {}) {
      return withLoading(async () => {
        const results = await provider.bulkDestroy(collectionName, ids, options);
        const destroyedIds = new Set(results.map((item) => item.id));
        setItems(getItems().filter((entry) => !destroyedIds.has(entry.id)));
        return results;
      });
    },

    async search(term, options = {}) {
      return withLoading(async () => {
        const results = await provider.search(collectionName, term, options);
        if (options.updateState !== false) setFetchedState(results);
        return results;
      });
    },

    explainQuery(queryInput = {}) {
      const runtime = provider.getCollectionRuntime(collectionName);
      const { inspectQuery } = provider.constructor.inspectQueryModule || {};
      if (inspectQuery) return inspectQuery(runtime.definition, queryInput, { operation: 'explainQuery' });
      return { ok: true, note: 'Runtime explanation module not attached.' };
    },
  };

  actions.setById = actions.set;
  actions.removePermanently = actions.destroy;
  actions.archive = actions.remove;

  return actions;
}
