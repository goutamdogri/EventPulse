// Decodes one Kafka record of events.raw into a row for the events table.
//
// Returns a result per message instead of throwing, so one bad record never stops the others from being
// parsed. In S2-F2 the batch handler still fails the whole batch on any bad record (that is the
// "poison message blocks a partition" behavior S2-F4 first observes, then fixes with a DLQ).
//
// Rules match the API's CreateEventDto, plus what the API adds (tenant_id, received_at header).

import { HEADER_RECEIVED_AT } from '@eventpulse/kafka-config';

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
  | 'bad_received_at';

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

export function parseMessage(m: RawMessage): ParseResult {
  const fail = (code: ParseErrorCode, message: string): ParseResult => ({
    ok: false,
    error: { code, message, partition: m.partition, offset: m.offset },
  });

  if (!m.value || m.value.length === 0) return fail('empty_value', 'record has no value');

  let v: any;
  try {
    v = JSON.parse(m.value.toString('utf8'));
  } catch (e) {
    return fail('invalid_json', (e as Error).message);
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

  const receivedAt = headerString(m.headers, HEADER_RECEIVED_AT);
  if (!isIso(receivedAt)) return fail('bad_received_at', `header ${HEADER_RECEIVED_AT} missing or not ISO 8601`);

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
      partition: m.partition,
      offset: m.offset,
    },
  };
}
