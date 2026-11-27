// Decodes one Kafka record of events.raw into a row for the events table.
//
// S3-F7: two value formats.
//   Avro (Confluent wire format: first byte 0x00 + 4-byte schema id)  decoded with the writer schema from the registry,
//        resolved to this sink's reader schema (packages/schema). The writer schemas must be prefetched (the batch
//        handler does it per chunk, so a dead registry pauses the partition instead of failing records).
//   JSON (Stage 2, first byte "{")  still accepted (SINK_ACCEPT_JSON, default true) so replaying old data works.
// Both go through the same validation below.
//
// Returns a result per message instead of throwing, so one bad record never stops the others from being
// parsed. In S2-F2 the batch handler still fails the whole batch on any bad record (that is the
// "poison message blocks a partition" behavior S2-F4 first observes, then fixes with a DLQ).
//
// Rules match the API's CreateEventDto, plus what the API adds (tenant_id, received_at header).

import { HEADER_RECEIVED_AT } from '@eventpulse/kafka-config';
import { AvroDecodeError, IncompatibleWriterError, NotAvroError, UnknownSchemaIdError, looksLikeWire } from '@eventpulse/schema';
import type { EventSerde } from '@eventpulse/schema';
import { eventContentHash } from '@eventpulse/idempotency';

export interface RawMessage {
  partition: number;
  offset: string;
  key: Buffer | string | null;
  value: Buffer | null;
  headers?: Record<string, unknown>;
}

export interface EventRow {
  event_id: string;
  tenant_id: string;
  type: string;
  occurred_at: string;
  schema_version: number;
  /** JSON text, inserted as jsonb. */
  payload: string;
  received_at: string;
  /** sha256 of the canonical event (S3-F2): tells a true duplicate from a reused id with other content. */
  content_hash: Buffer;
  /** S3-F7: registry id of the writer schema (Avro values only). Not stored; for metrics and logs. */
  schema_id?: number;
  partition: number;
  offset: string;
}

export type ParseErrorCode =
  | 'empty_value'
  | 'invalid_json'
  | 'not_an_object'
  | 'bad_event_id'
  | 'bad_tenant_id'
  | 'bad_type'
  | 'bad_occurred_at'
  | 'bad_schema_version'
  | 'bad_payload'
  | 'bad_received_at'
  | 'not_avro'
  | 'unknown_schema_id'
  | 'incompatible_schema'
  | 'avro_decode'
  | 'json_not_accepted';

/** Codes that come from Avro deserialization (ep_schema_deserialize_failures_total{reason}). */
export const DESERIALIZE_CODES: readonly ParseErrorCode[] = ['not_avro', 'unknown_schema_id', 'incompatible_schema', 'avro_decode', 'json_not_accepted'];

export interface ParseOptions {
  /** Decodes Avro values (writer schemas prefetched). Without it, an Avro value is a not_avro parse error. */
  serde?: Pick<EventSerde, 'decode'>;
  /** Stage 2 JSON values (default true). */
  acceptJson?: boolean;
}

function avroErrorCode(e: unknown): ParseErrorCode {
  if (e instanceof UnknownSchemaIdError) return 'unknown_schema_id';
  if (e instanceof IncompatibleWriterError) return 'incompatible_schema';
  if (e instanceof NotAvroError) return 'not_avro';
  if (e instanceof AvroDecodeError) return 'avro_decode';
  throw e; // SerdeNotReadyError (not prefetched) or a bug: never turn it into a dead letter
}

export interface ParseError {
  code: ParseErrorCode;
  message: string;
  partition: number;
  offset: string;
}

export type ParseResult = { ok: true; row: EventRow } | { ok: false; error: ParseError };

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
// ISO 8601 date-time with a time zone (what the API's strict IsISO8601 + Postgres timestamptz both accept).
const ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d{1,9})?)?(Z|[+-]\d{2}:?\d{2})$/;
const MAX_TENANT = 200;

const isIso = (v: unknown): v is string =>
  typeof v === 'string' && ISO_RE.test(v) && !Number.isNaN(Date.parse(v));

function headerString(headers: Record<string, unknown> | undefined, name: string): string | undefined {
  const v = headers?.[name];
  const one = Array.isArray(v) ? v[0] : v;
  if (one === undefined || one === null) return undefined;
  return Buffer.isBuffer(one) ? one.toString('utf8') : String(one);
}

export function parseMessage(m: RawMessage, opts: ParseOptions = {}): ParseResult {
  const fail = (code: ParseErrorCode, message: string): ParseResult => ({
    ok: false,
    error: { code, message, partition: m.partition, offset: m.offset },
  });

  if (!m.value || m.value.length === 0) return fail('empty_value', 'record has no value');

  let v: any;
  let receivedAtFromValue: string | undefined;
  if (looksLikeWire(m.value)) {
    if (!opts.serde) return fail('not_avro', 'Avro value but this sink has no schema registry configured');
    try {
      v = opts.serde.decode(m.value);
      receivedAtFromValue = v.received_at;
    } catch (e) {
      return fail(avroErrorCode(e), (e as Error).message);
    }
  } else {
    if (opts.acceptJson === false) return fail('json_not_accepted', 'Stage 2 JSON value and SINK_ACCEPT_JSON=false');
    try {
      v = JSON.parse(m.value.toString('utf8'));
    } catch (e) {
      return fail('invalid_json', (e as Error).message);
    }
  }
  if (v === null || typeof v !== 'object' || Array.isArray(v)) return fail('not_an_object', 'value is not a JSON object');

  if (typeof v.event_id !== 'string' || !UUID_RE.test(v.event_id)) return fail('bad_event_id', 'event_id must be a UUID');
  if (typeof v.tenant_id !== 'string' || v.tenant_id.length < 1 || v.tenant_id.length > MAX_TENANT) {
    return fail('bad_tenant_id', `tenant_id must be a string of 1-${MAX_TENANT} characters`);
  }
  if (typeof v.type !== 'string' || v.type.length < 1 || v.type.length > 100) {
    return fail('bad_type', 'type must be a string of 1-100 characters');
  }
  if (!isIso(v.occurred_at)) return fail('bad_occurred_at', 'occurred_at must be an ISO 8601 date-time');
  if (!Number.isInteger(v.schema_version) || v.schema_version < 1) {
    return fail('bad_schema_version', 'schema_version must be an integer >= 1');
  }
  if (v.payload === null || typeof v.payload !== 'object' || Array.isArray(v.payload)) {
    return fail('bad_payload', 'payload must be a JSON object');
  }

  // Avro carries received_at in the value; Stage 2 JSON only in the header.
  const receivedAt = receivedAtFromValue ?? headerString(m.headers, HEADER_RECEIVED_AT);
  if (!isIso(receivedAt)) return fail('bad_received_at', `header ${HEADER_RECEIVED_AT} missing or not ISO 8601`);

  let contentHash: Buffer;
  try {
    contentHash = eventContentHash(v);
  } catch (e) {
    return fail('bad_payload', `payload has no canonical form: ${(e as Error).message}`);
  }

  return {
    ok: true,
    row: {
      event_id: v.event_id,
      tenant_id: v.tenant_id,
      type: v.type,
      occurred_at: v.occurred_at,
      schema_version: v.schema_version,
      payload: JSON.stringify(v.payload),
      received_at: receivedAt,
      content_hash: contentHash,
      ...(typeof v.schema_id === 'number' ? { schema_id: v.schema_id } : {}),
      partition: m.partition,
      offset: m.offset,
    },
  };
}
