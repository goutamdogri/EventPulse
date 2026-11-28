// ONE error classifier for the whole system (S3-F5). Every component (sink main consumer, retry-tier consumer,
// later the DLQ replay tool and the outbox relay) asks this, so a class means the same thing everywhere.
//
//   class               examples                                                  action
//   PERMANENT_PARSE     not Avro / Avro decode error / invalid JSON / bad UUID    dlq            (at once)
//   PERMANENT_SCHEMA    unknown schema id, writer schema not resolvable           dlq            (at once)
//   PERMANENT_DB        SQLSTATE 22xxx data exception, 23xxx integrity, for THIS  isolate+dlq    (bisect to find the
//                       row (retrying bad data never helps)                                       row, then DLQ)
//   TRANSIENT_RECORD    40001 serialization failure, 40P01 deadlock, 57014        retry_tier     (bounded tiers,
//                       statement timeout                                                         then DLQ)
//   TRANSIENT_SYSTEMIC  connection refused/reset, pool timeout, 08xxx, 53xxx,     block          (pause the partition,
//                       57P0x, registry unavailable                                               NEVER retry topic, NEVER DLQ)
//   SYSTEMIC_CONFIG     42xxx (missing column...), 28xxx auth, 3D000, 0A, XX      block          (+ needs a human)
//   UNKNOWN             anything else                                             retry_tier
//
// The rule that matters most (Stage 2, kept): a dependency that is DOWN never sends records to the DLQ, and since
// Stage 3 never floods the retry topics either (C9): an outage is "block", not "retry".

import { AvroDecodeError, IncompatibleWriterError, NotAvroError, RegistryUnavailableError, UnknownSchemaIdError } from '@eventpulse/schema';

export const ERROR_CLASSES = [
  'PERMANENT_PARSE',
  'PERMANENT_SCHEMA',
  'PERMANENT_DB',
  'TRANSIENT_RECORD',
  'TRANSIENT_SYSTEMIC',
  'SYSTEMIC_CONFIG',
  'UNKNOWN',
] as const;
export type ErrorClass = (typeof ERROR_CLASSES)[number];

export type ErrorAction = 'dlq' | 'isolate_then_dlq' | 'retry_tier' | 'block';

export const ACTION: Readonly<Record<ErrorClass, ErrorAction>> = {
  PERMANENT_PARSE: 'dlq',
  PERMANENT_SCHEMA: 'dlq',
  PERMANENT_DB: 'isolate_then_dlq',
  TRANSIENT_RECORD: 'retry_tier',
  TRANSIENT_SYSTEMIC: 'block',
  SYSTEMIC_CONFIG: 'block',
  UNKNOWN: 'retry_tier',
};

const TRANSIENT_NODE_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH']);
const SYSTEMIC_SQLSTATES = new Set(['57P01', '57P02', '57P03']); // admin shutdown, crash shutdown, cannot connect now
const RECORD_SQLSTATES = new Set(['40001', '40P01', '57014']); // serialization failure, deadlock, statement timeout
const TRANSIENT_MESSAGE =
  /timeout exceeded when trying to connect|Connection terminated|connection timeout|Client has encountered a connection error|Connection ended unexpectedly|the database system is (starting up|shutting down|in recovery mode)/i;

/** Error code worth recording (SQLSTATE or Node code), or 'none'. */
export function errorCode(e: unknown): string {
  const code = (e as { code?: unknown })?.code;
  return typeof code === 'string' && /^[0-9A-Z_]{4,20}$/.test(code) ? code : 'none';
}

/** CLASSIFIER_OVERRIDE (debug-only, experiment C9): "<code>=<CLASS>,..." forces a code into a class. */
export function parseClassifierOverride(s: string | undefined): Map<string, ErrorClass> {
  const m = new Map<string, ErrorClass>();
  for (const item of (s ?? '').split(',').map((x) => x.trim()).filter(Boolean)) {
    const [code, cls] = item.split('=').map((x) => x.trim());
    if (!code || !ERROR_CLASSES.includes(cls as ErrorClass)) throw new Error(`bad CLASSIFIER_OVERRIDE entry "${item}" (expected <code>=<${ERROR_CLASSES.join('|')}>)`);
    m.set(code, cls as ErrorClass);
  }
  return m;
}

export class Classifier {
  constructor(private readonly overrides: Map<string, ErrorClass> = new Map()) {}

  classify(e: unknown): ErrorClass {
    const code = errorCode(e);
    const forced = this.overrides.get(code);
    if (forced) return forced;
    if (e instanceof RegistryUnavailableError) return 'TRANSIENT_SYSTEMIC';
    if (e instanceof UnknownSchemaIdError || e instanceof IncompatibleWriterError) return 'PERMANENT_SCHEMA';
    if (e instanceof NotAvroError || e instanceof AvroDecodeError) return 'PERMANENT_PARSE';
    const msg = String((e as Error)?.message ?? '');
    if (TRANSIENT_NODE_CODES.has(code) || SYSTEMIC_SQLSTATES.has(code) || /^(08|53)/.test(code)) return 'TRANSIENT_SYSTEMIC';
    if (code === 'none' && TRANSIENT_MESSAGE.test(msg)) return 'TRANSIENT_SYSTEMIC';
    if (RECORD_SQLSTATES.has(code)) return 'TRANSIENT_RECORD';
    if (/^(22|23)/.test(code)) return 'PERMANENT_DB';
    if (/^(42|28|0A|XX)/.test(code) || code === '3D000') return 'SYSTEMIC_CONFIG';
    return 'UNKNOWN';
  }

  action(e: unknown): ErrorAction {
    return ACTION[this.classify(e)];
  }
}

export const DEFAULT_CLASSIFIER = new Classifier();
export const classifyError = (e: unknown) => DEFAULT_CLASSIFIER.classify(e);

/** A parse error code (consumer-sink parse.ts) -> its class. */
export const classifyParseCode = (code: string): ErrorClass =>
  code === 'unknown_schema_id' || code === 'incompatible_schema' ? 'PERMANENT_SCHEMA' : 'PERMANENT_PARSE';

/** Classes that block the partition (retry until the dependency is back) instead of failing a record. */
export const isBlocking = (c: ErrorClass) => ACTION[c] === 'block';
/** Classes worth a retry tier. */
export const isRetriable = (c: ErrorClass) => ACTION[c] === 'retry_tier';

export function truncate(s: string, maxBytes = 1024): string {
  const b = Buffer.from(s);
  return b.length <= maxBytes ? s : b.subarray(0, maxBytes).toString('utf8') + '…';
}
