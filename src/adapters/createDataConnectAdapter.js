import { createSqlAdapter } from './createSqlAdapter.js';

/**
 * Thin helper for Firebase SQL Connect / Data Connect generated SDK wiring.
 *
 * Important:
 * - SQL Connect generates app-specific operations, so this package cannot auto-discover CRUD.
 * - You must provide per-collection handlers that call your generated SDK/query functions.
 */
export function createDataConnectAdapter(config = {}) {
  return createSqlAdapter(config);
}
