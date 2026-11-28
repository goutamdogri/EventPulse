// Failure classification moved to packages/retry (Stage 3, S3-F5): ONE classifier for the whole system.
// This file only re-exports it for the sink's existing imports. See packages/retry/src/classifier.ts for the table.
export {
  ERROR_CLASSES,
  backoffMs,
  classifyError as classifyDbError,
  classifyParseCode,
  errorCode,
  isBlocking,
  isRetriable,
  truncate,
} from '@eventpulse/retry';
export type { ErrorClass } from '@eventpulse/retry';
