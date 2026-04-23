/**
 * @file shardResolver.js
 */
import { DEFAULT_CONFIG } from '../constants.js';

function toDate(value) {
  if (!value) return new Date();
  if (value instanceof Date) return value;
  if (typeof value?.toDate === 'function') return value.toDate();
  if (typeof value === 'object' && value.seconds !== undefined) {
    return new Date(value.seconds * 1000);
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return new Date();
  return date;
}

function pad(value) {
  return String(value).padStart(2, '0');
}

export function getShardConfig(definition = {}, collectionName = '') {
  const shard = definition?.shard || {};

  return {
    type: shard.type || DEFAULT_CONFIG.SHARD_TYPES.NONE,
    field: shard.field || DEFAULT_CONFIG.DEFAULT_DATE_FIELD,
    strategy: shard.strategy || DEFAULT_CONFIG.DEFAULT_SHARD_STRATEGY,
    root: shard.root || `${collectionName || definition?.name || 'items'}Shards`,
    collectionId: shard.collectionId || definition?.path || definition?.name || collectionName,
    metadataField: shard.metadataField || DEFAULT_CONFIG.DEFAULT_SHARD_METADATA_FIELD,
    legacyReadFallback: shard.legacyReadFallback || null,
    separator: typeof shard.separator === 'string' && shard.separator ? shard.separator : '_',
    legacySeparators: Array.isArray(shard.legacySeparators) ? shard.legacySeparators.filter(Boolean) : ['-'],
  };
}

export function isBucketParentStrategy(definition = {}, collectionName = '') {
  return getShardConfig(definition, collectionName).strategy === DEFAULT_CONFIG.SHARD_STRATEGIES.BUCKET_PARENT;
}

function formatSuffixShardKey(date, shardType) {
  switch (shardType) {
    case DEFAULT_CONFIG.SHARD_TYPES.MONTHLY:
      return `${date.getUTCFullYear()}_${pad(date.getUTCMonth() + 1)}`;
    case DEFAULT_CONFIG.SHARD_TYPES.YEARLY:
      return `${date.getUTCFullYear()}`;
    default:
      return '';
  }
}

export function formatBucketShardKey(date, shardType) {
  switch (shardType) {
    case DEFAULT_CONFIG.SHARD_TYPES.MONTHLY:
      return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}`;
    case DEFAULT_CONFIG.SHARD_TYPES.YEARLY:
      return `${date.getUTCFullYear()}`;
    default:
      return '';
  }
}

export function resolveShardKey(definition, data = {}, options = {}) {
  const shard = getShardConfig(definition, definition?.name);

  if (shard.type === DEFAULT_CONFIG.SHARD_TYPES.NONE) {
    return null;
  }

  const date = toDate(options.shardDate || data[shard.field]);

  if (shard.strategy === DEFAULT_CONFIG.SHARD_STRATEGIES.BUCKET_PARENT) {
    return formatBucketShardKey(date, shard.type);
  }

  return formatSuffixShardKey(date, shard.type);
}

export function resolveShardMetadata(definition, data = {}, options = {}) {
  const shard = getShardConfig(definition, definition?.name);

  if (shard.type === DEFAULT_CONFIG.SHARD_TYPES.NONE) {
    return null;
  }

  const date = toDate(options.shardDate || data[shard.field]);

  return {
    key:
      shard.strategy === DEFAULT_CONFIG.SHARD_STRATEGIES.BUCKET_PARENT
        ? formatBucketShardKey(date, shard.type)
        : formatSuffixShardKey(date, shard.type),
    field: shard.metadataField,
  };
}

export function normalizeShardRange(range = {}) {
  if (!range || typeof range !== 'object') {
    const now = new Date();
    return { start: now, end: now };
  }

  const startSource = range.start ?? range.from ?? range.shardStart ?? range.begin ?? range.date ?? null;
  const endSource = range.end ?? range.to ?? range.shardEnd ?? range.until ?? range.date ?? startSource;

  const start = startSource ? toDate(startSource) : new Date();
  const end = endSource ? toDate(endSource) : start;

  return start <= end ? { start, end } : { start: end, end: start };
}

function enumerateShardDates(range = {}) {
  const { start, end } = normalizeShardRange(range);

  const dates = [];
  const cursor = new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), 1));
  const limit = new Date(Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), 1));

  while (cursor <= limit) {
    dates.push(new Date(cursor));
    cursor.setUTCMonth(cursor.getUTCMonth() + 1);
  }

  return dates;
}

function enumerateYearDates(range = {}) {
  const { start, end } = normalizeShardRange(range);

  const years = [];
  for (let year = start.getUTCFullYear(); year <= end.getUTCFullYear(); year += 1) {
    years.push(new Date(Date.UTC(year, 0, 1)));
  }

  return years;
}

export function resolveShardName(collectionName, definition, data = {}, options = {}) {
  const shard = getShardConfig(definition, collectionName);

  if (shard.type === DEFAULT_CONFIG.SHARD_TYPES.NONE) {
    return shard.collectionId;
  }

  const shardKey = resolveShardKey(definition, data, options);

  if (shard.strategy === DEFAULT_CONFIG.SHARD_STRATEGIES.BUCKET_PARENT) {
    return `${shard.root}/${shardKey}/${shard.collectionId}`;
  }

  return `${shard.collectionId}${shard.separator}${shardKey}`;
}

export function getLegacySeparatorShardNames(collectionName, definition, range = {}) {
  const shard = getShardConfig(definition, collectionName);

  if (shard.type === DEFAULT_CONFIG.SHARD_TYPES.NONE) {
    return [];
  }

  if (shard.strategy === DEFAULT_CONFIG.SHARD_STRATEGIES.BUCKET_PARENT) {
    return [];
  }

  const dates =
    shard.type === DEFAULT_CONFIG.SHARD_TYPES.MONTHLY
      ? enumerateShardDates(range)
      : enumerateYearDates(range);

  const separators = [...new Set((Array.isArray(shard.legacySeparators) ? shard.legacySeparators : []).filter((separator) => separator && separator !== shard.separator))];

  if (separators.length === 0) {
    return [];
  }

  return [
    ...new Set(
      dates.flatMap((date) => {
        const shardKey = resolveShardKey(definition, { [shard.field]: date }, { shardDate: date });
        return separators.map((separator) => `${shard.collectionId}${separator}${shardKey}`);
      }),
    ),
  ];
}


export function enumerateShardNames(collectionName, definition, range = {}) {
  const shard = getShardConfig(definition, collectionName);

  if (shard.type === DEFAULT_CONFIG.SHARD_TYPES.NONE) {
    return [shard.collectionId];
  }

  const dates =
    shard.type === DEFAULT_CONFIG.SHARD_TYPES.MONTHLY
      ? enumerateShardDates(range)
      : enumerateYearDates(range);

  return [
    ...new Set(
      dates.map((date) =>
        resolveShardName(
          collectionName,
          definition,
          { [shard.field]: date },
          { shardDate: date },
        ),
      ),
    ),
  ];
}

export function getCollectionGroupId(collectionName, definition) {
  return getShardConfig(definition, collectionName).collectionId;
}

export function getLegacyFallbackShardNames(collectionName, definition, range = {}) {
  const shard = getShardConfig(definition, collectionName);

  if (
    shard.strategy !== DEFAULT_CONFIG.SHARD_STRATEGIES.BUCKET_PARENT ||
    shard.legacyReadFallback !== DEFAULT_CONFIG.SHARD_STRATEGIES.SUFFIX
  ) {
    return [];
  }

  const legacyDefinition = {
    ...definition,
    shard: {
      ...(definition?.shard || {}),
      ...shard,
      strategy: DEFAULT_CONFIG.SHARD_STRATEGIES.SUFFIX,
      collectionId: definition?.path || definition?.name || collectionName,
    },
  };

  return enumerateShardNames(collectionName, legacyDefinition, range);
}