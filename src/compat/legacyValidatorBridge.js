export function mergeLegacyPayload(existingDoc = {}, incoming = {}) {
  return { ...existingDoc, ...incoming };
}
