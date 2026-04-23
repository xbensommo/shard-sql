export function runSafeCallback(callback, ...args) {
  if (typeof callback !== 'function') return;
  try {
    return callback(...args);
  } catch (error) {
    console.error('[shard-sql] callback error', error);
    return undefined;
  }
}
