// The message contract on events.raw, shared by the producer (ingest-api) and the consumers.
//
//   key     tenant_id (default) or event_id      KAFKA_KEY_MODE=tenant|event
//   value   JSON: { event_id, tenant_id, type, occurred_at, schema_version, payload }
//   headers received_at (ISO, API receipt time), schema_version, request_id
//
// Decoding (and the poison handling that goes with it) lives in the consumer, not here.

export const KEY_MODES = ['tenant', 'event'] as const;
export type KeyMode = (typeof KEY_MODES)[number];

export const HEADER_RECEIVED_AT = 'received_at';
export const HEADER_SCHEMA_VERSION = 'schema_version';
export const HEADER_REQUEST_ID = 'request_id';

export interface EventEnvelope {
  event_id: string;
  tenant_id: string;
  type: string;
  occurred_at: string;
  schema_version: number;
  payload: Record<string, unknown>;
}

export interface EncodedEvent {
  key: string;
  value: Buffer;
  headers: Record<string, string>;
  /** Approximate size on the wire before compression: key + value + header keys/values. */
  sizeBytes: number;
}

export interface EncodeInput {
  tenantId: string;
  event: Omit<EventEnvelope, 'tenant_id'>;
  receivedAt: Date;
  requestId: string;
  keyMode: KeyMode;
}

export function encodeEvent(i: EncodeInput): EncodedEvent {
  const envelope: EventEnvelope = {
    event_id: i.event.event_id,
    tenant_id: i.tenantId, // always from the API key, never from the body
    type: i.event.type,
    occurred_at: i.event.occurred_at,
    schema_version: i.event.schema_version,
    payload: i.event.payload,
  };
  const key = i.keyMode === 'tenant' ? i.tenantId : i.event.event_id;
  const value = Buffer.from(JSON.stringify(envelope));
  const headers = {
    [HEADER_RECEIVED_AT]: i.receivedAt.toISOString(),
    [HEADER_SCHEMA_VERSION]: String(i.event.schema_version),
    [HEADER_REQUEST_ID]: i.requestId,
  };
  let sizeBytes = Buffer.byteLength(key) + value.length;
  for (const [k, v] of Object.entries(headers)) sizeBytes += Buffer.byteLength(k) + Buffer.byteLength(v);
  return { key, value, headers, sizeBytes };
}
