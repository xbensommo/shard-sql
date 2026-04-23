export function runSafeCallback(fn, payload, fallback = null) {
  if (typeof fn !== 'function') return fallback;
  try {
    return fn(payload);
  } catch (error) {
    return fallback;
  }
}
