// Error types of the ingest path (S2-F5). The filter maps OverloadedError and KafkaUnavailableError to 503.
// No Nest or Kafka imports, so everything here is unit-testable.

/** Batcher queue is full (by count or bytes). Kafka may be perfectly healthy: the API is just too busy. */
export class OverloadedError extends Error {
  readonly name = 'OverloadedError';
  constructor(message = 'Ingest queue is full') {
    super(message);
  }
}

/** Kafka cannot take writes right now: connection trouble, timeouts, not enough replicas, breaker open... */
export class KafkaUnavailableError extends Error {
  readonly name = 'KafkaUnavailableError';
  constructor(
    message: string,
    readonly cause?: unknown,
  ) {
    super(message);
  }
}

// librdkafka error codes that mean "Kafka can't take this write right now" (retrying later may work).
const UNAVAILABLE_CODES = new Set<number>([
  -195, // ERR__TRANSPORT
  -187, // ERR__ALL_BROKERS_DOWN
  -192, // ERR__MSG_TIMED_OUT (delivery timeout, retries exhausted)
  -185, // ERR__TIMED_OUT
  -193, // ERR__RESOLVE
  -172, // ERR__STATE (producer not connected / disconnected)
  -184, // ERR__QUEUE_FULL (librdkafka's own queue)
  -152, // ERR__PURGE_QUEUE
  -196, // ERR__MSG_TIMED_OUT_QUEUE
  3, // UNKNOWN_TOPIC_OR_PARTITION (metadata not there yet: topic just (re)created / leader election)
  5, // LEADER_NOT_AVAILABLE
  6, // NOT_LEADER_OR_FOLLOWER
  7, // REQUEST_TIMED_OUT
  8, // BROKER_NOT_AVAILABLE
  9, // REPLICA_NOT_AVAILABLE
  13, // NETWORK_EXCEPTION
  15, // COORDINATOR_NOT_AVAILABLE
  19, // NOT_ENOUGH_REPLICAS
  20, // NOT_ENOUGH_REPLICAS_AFTER_APPEND
]);

const UNAVAILABLE_NAMES = new Set([
  'KafkaJSConnectionError',
  'KafkaJSRequestTimeoutError',
  'KafkaJSNumberOfRetriesExceeded',
  'KafkaJSNoBrokerAvailableError',
  'KafkaJSBrokerNotFound',
  'KafkaJSTimeout',
]);

const UNAVAILABLE_TEXT =
  /all brokers down|broker transport failure|message timed out|timed out|not enough (in-sync )?replicas|leader not available|broker not available|connection (refused|reset|closed)|ECONNREFUSED|ECONNRESET|ETIMEDOUT|EHOSTUNREACH|cannot send without awaiting connect/i;

/** True when the error means "Kafka is unavailable right now" (as opposed to a bug or a bad message). */
export function isKafkaUnavailable(err: unknown): boolean {
  if (err instanceof KafkaUnavailableError) return true;
  if (!err || typeof err !== 'object') return false;
  const e = err as { code?: unknown; name?: unknown; message?: unknown };
  if (typeof e.code === 'number' && UNAVAILABLE_CODES.has(e.code)) return true;
  if (typeof e.name === 'string' && UNAVAILABLE_NAMES.has(e.name)) return true;
  if (typeof e.code === 'string' && /^(ECONNREFUSED|ECONNRESET|ETIMEDOUT|EHOSTUNREACH|ENOTFOUND)$/.test(e.code)) return true;
  return typeof e.message === 'string' && UNAVAILABLE_TEXT.test(e.message);
}

/** Wraps a raw client error into KafkaUnavailableError when it is one, otherwise returns it unchanged. */
export function classifySendError(err: unknown): unknown {
  if (err instanceof KafkaUnavailableError) return err;
  if (isKafkaUnavailable(err)) {
    return new KafkaUnavailableError(`Kafka unavailable: ${(err as Error)?.message ?? String(err)}`, err);
  }
  return err;
}

/** Low-cardinality label for metrics/logs (never the raw message). */
export function errorType(err: unknown): string {
  if (err instanceof KafkaUnavailableError) return errorType(err.cause) === 'other' ? 'unavailable' : errorType(err.cause);
  if (err instanceof OverloadedError) return 'overloaded';
  const e = err as { type?: unknown; name?: unknown; code?: unknown } | undefined;
  if (e && typeof e.type === 'string' && e.type !== '-1') return e.type;
  if (e && typeof e.name === 'string' && e.name !== 'Error') return e.name;
  return 'other';
}
