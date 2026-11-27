// HTTP-facing idempotency outcomes (S3-F1). The exception filter maps them:
//   IdempotencyInProgressError  -> 409 + Retry-After   the original request with this key is still producing
//   IdempotencyKeyReusedError   -> 422                 same key, different payload: a client bug, reported
// (IdempotencyUnavailableError from the package -> 503 + Retry-After, only with IDEM_FAILURE_POLICY=closed.)

export class IdempotencyInProgressError extends Error {
  readonly name = 'IdempotencyInProgressError';
  constructor() {
    super('A request with this idempotency key is still in progress; retry later');
  }
}

export class IdempotencyKeyReusedError extends Error {
  readonly name = 'IdempotencyKeyReusedError';
  constructor() {
    super('This idempotency key was already used with a different payload');
  }
}
