// Failure classification for the sink (S2-F4). The one rule that matters most:
// a database that is DOWN (or broken for every row) must never send records to the DLQ.
// Only a record that fails ON ITS OWN goes there.
//
//   PERMANENT_PARSE  the record can't be decoded (invalid JSON, bad UUID, ...)   -> DLQ at once, no retry
//   PERMANENT_DB     Postgres rejects THIS row: 22xxx data exceptions, 23xxx    -> isolate the row, retry it
//                    integrity violations (the PK conflict never surfaces:         SINK_MAX_RETRIES times, then DLQ
//                    ON CONFLICT DO NOTHING handles it)
//   TRANSIENT_DB     the DB is unreachable / overloaded / restarting / slow:    -> never DLQ; back off, pause the
//                    connection errors, 08xxx, 53xxx, 57P0x, 57014, 40001,         partition, retry until it is back
//                    40P01, pool timeouts, terminated connections
//   SYSTEMIC_DB      every row would fail the same way: SQL/schema errors       -> never DLQ (it would dead-letter
//                    (42xxx, e.g. a missing column), auth (28xxx), unknown         EVERY record); treated like
//                    database (3D000), feature/internal errors (0A, XX)            TRANSIENT_DB, needs a human
//   UNKNOWN          no code we recognize                                       -> like PERMANENT_DB (bounded, then DLQ)
//
// SYSTEMIC_DB is an addition to the plan's table: without it a schema mistake would empty the whole topic
// into the DLQ one row at a time.

export type ErrorClass = 'PERMANENT_PARSE' | 'PERMANENT_DB' | 'TRANSIENT_DB' | 'SYSTEMIC_DB' | 'UNKNOWN';

const TRANSIENT_NODE_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH']);
const TRANSIENT_SQLSTATES = new Set(['57P01', '57P02', '57P03', '57014', '40001', '40P01']);
const TRANSIENT_MESSAGE = /timeout exceeded when trying to connect|Connection terminated|connection timeout|Client has encountered a connection error|Connection ended unexpectedly|the database system is (starting up|shutting down)/i;

/** Error code worth recording (SQLSTATE or Node code), or 'none'. */
export function errorCode(e: unknown): string {
  const code = (e as { code?: unknown })?.code;
  return typeof code === 'string' && /^[0-9A-Z_]{4,20}$/.test(code) ? code : 'none';
}

export function classifyDbError(e: unknown): ErrorClass {
  const code = errorCode(e);
  const msg = String((e as Error)?.message ?? '');
  if (TRANSIENT_NODE_CODES.has(code) || TRANSIENT_SQLSTATES.has(code) || /^(08|53)/.test(code)) return 'TRANSIENT_DB';
  if (code === 'none' && TRANSIENT_MESSAGE.test(msg)) return 'TRANSIENT_DB';
  if (/^(22|23)/.test(code)) return 'PERMANENT_DB';
  if (/^(42|28|0A|XX)/.test(code) || code === '3D000') return 'SYSTEMIC_DB';
  return 'UNKNOWN';
}

/** Classes that block the partition (retry until fixed) instead of dead-lettering a record. */
export const isBlocking = (c: ErrorClass) => c === 'TRANSIENT_DB' || c === 'SYSTEMIC_DB';

/**
 * Exponential backoff with "equal jitter": the cap doubles per attempt (base, 2*base, 4*base ... up to max)
 * and the delay is a random value in [cap/2, cap]. attempt starts at 1.
 */
export function backoffMs(attempt: number, baseMs: number, maxMs: number, rand: () => number = Math.random): number {
  const cap = Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt - 1));
  return Math.round(cap / 2 + rand() * (cap / 2));
}

export function truncate(s: string, maxBytes = 1024): string {
  const b = Buffer.from(s);
  return b.length <= maxBytes ? s : b.subarray(0, maxBytes).toString('utf8') + '…';
}
