import { DEFAULT_CONFIG } from '../constants.js';
import { ShardProviderError, toShardProviderError } from '../errors.js';

const genericCodeMap = {
  'permission-denied': DEFAULT_CONFIG.ERROR_CODES.PERMISSION_DENIED,
  permission_denied: DEFAULT_CONFIG.ERROR_CODES.PERMISSION_DENIED,
  not_found: DEFAULT_CONFIG.ERROR_CODES.NOT_FOUND,
  'not-found': DEFAULT_CONFIG.ERROR_CODES.NOT_FOUND,
  unavailable: DEFAULT_CONFIG.ERROR_CODES.UNAVAILABLE,
  timeout: DEFAULT_CONFIG.ERROR_CODES.DEADLINE_EXCEEDED,
  'deadline-exceeded': DEFAULT_CONFIG.ERROR_CODES.DEADLINE_EXCEEDED,
  aborted: DEFAULT_CONFIG.ERROR_CODES.ABORTED,
  conflict: DEFAULT_CONFIG.ERROR_CODES.ABORTED,
  'already-exists': DEFAULT_CONFIG.ERROR_CODES.ALREADY_EXISTS,
  duplicate: DEFAULT_CONFIG.ERROR_CODES.ALREADY_EXISTS,
  unique_violation: DEFAULT_CONFIG.ERROR_CODES.ALREADY_EXISTS,
  failed_precondition: DEFAULT_CONFIG.ERROR_CODES.FAILED_PRECONDITION,
  'failed-precondition': DEFAULT_CONFIG.ERROR_CODES.FAILED_PRECONDITION,
  invalid_argument: DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT,
  'invalid-argument': DEFAULT_CONFIG.ERROR_CODES.INVALID_ARGUMENT,
  internal: DEFAULT_CONFIG.ERROR_CODES.INTERNAL,
};

export function mapAdapterError(error, context = {}) {
  if (error instanceof ShardProviderError) return error;

  const rawCode = String(error?.code || error?.name || '').trim().toLowerCase();
  const sqlState = String(error?.sqlState || error?.sqlstate || '').trim().toLowerCase();
  let code = genericCodeMap[rawCode] || genericCodeMap[sqlState] || context.code || DEFAULT_CONFIG.ERROR_CODES.UNKNOWN;

  if (sqlState === '23505') code = DEFAULT_CONFIG.ERROR_CODES.ALREADY_EXISTS;
  if (sqlState === '23503') code = DEFAULT_CONFIG.ERROR_CODES.FAILED_PRECONDITION;
  if (sqlState === '42501') code = DEFAULT_CONFIG.ERROR_CODES.PERMISSION_DENIED;

  return toShardProviderError(error, {
    code,
    collection: context.collection,
    operation: context.operation,
    field: context.field,
    publicMessage: DEFAULT_CONFIG.SAFE_PUBLIC_MESSAGES[code] || DEFAULT_CONFIG.SAFE_PUBLIC_MESSAGES.UNKNOWN,
  });
}
