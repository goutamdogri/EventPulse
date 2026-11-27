import { encodeEvent } from '@eventpulse/kafka-config';
import { eventContentHash } from '@eventpulse/idempotency';
import { EventSerde, encodeWire, readSchemaFile } from '@eventpulse/schema';
import { resolve } from 'node:path';
import { parseMessage } from './parse.js';
import type { RawMessage } from './parse.js';

const event = {
  event_id: '6f1c2b1e-8a43-4c0e-9d2f-0a1b2c3d4e5f',
  type: 'order.created',
  occurred_at: '2026-11-12T10:00:00Z',
  schema_version: 1,
  payload: { a: 1 },
};

/** A record exactly as the API writes it. */
function goodMessage(overrides: Partial<RawMessage> = {}): RawMessage {
  const e = encodeEvent({
    tenantId: 'tenant_a',
    event,
    receivedAt: new Date('2026-11-22T05:00:00.123Z'),
    requestId: 'r1',
    keyMode: 'tenant',
  });
  return { partition: 2, offset: '41', key: e.key, value: e.value, headers: e.headers, ...overrides };
}

const withValue = (v: unknown) => goodMessage({ value: Buffer.from(JSON.stringify(v)) });
const body = { ...event, tenant_id: 'tenant_a' };

describe('parseMessage', () => {
  it('parses a record written by the API', () => {
    const r = parseMessage(goodMessage());
    expect(r).toEqual({
      ok: true,
      row: {
        ...event,
        tenant_id: 'tenant_a',
        payload: '{"a":1}',
        received_at: '2026-11-22T05:00:00.123Z',
        content_hash: eventContentHash(body),
        partition: 2,
        offset: '41',
      },
    });
  });

  it('content_hash ignores key order and formatting of the record, but not its content (S3-F2)', () => {
    const a = parseMessage(goodMessage());
    const reordered = parseMessage(goodMessage({ value: Buffer.from(`{ "payload": {"a": 1.0}, "schema_version": 1, "occurred_at": "${event.occurred_at}", "type": "${event.type}", "tenant_id": "tenant_a", "event_id": "${event.event_id}" }`) }));
    const changed = parseMessage(withValue({ ...body, payload: { a: 2 } }));
    expect(a.ok && reordered.ok && changed.ok).toBe(true);
    if (!a.ok || !reordered.ok || !changed.ok) return;
    expect(reordered.row.content_hash.equals(a.row.content_hash)).toBe(true);
    expect(changed.row.content_hash.equals(a.row.content_hash)).toBe(false);
  });

  it('accepts headers given as Buffers (what the consumer really receives)', () => {
    const r = parseMessage(goodMessage({ headers: { received_at: Buffer.from('2026-11-22T05:00:00Z') } }));
    expect(r.ok && r.row.received_at).toBe('2026-11-22T05:00:00Z');
  });

  it.each([
    ['empty value', goodMessage({ value: null }), 'empty_value'],
    ['invalid JSON', goodMessage({ value: Buffer.from('{not json') }), 'invalid_json'],
    ['JSON array', withValue([1, 2]), 'not_an_object'],
    ['bad UUID', withValue({ ...body, event_id: 'nope' }), 'bad_event_id'],
    ['missing tenant_id', withValue({ ...body, tenant_id: undefined }), 'bad_tenant_id'],
    ['oversized type', withValue({ ...body, type: 'x'.repeat(101) }), 'bad_type'],
    ['bad timestamp', withValue({ ...body, occurred_at: 'yesterday' }), 'bad_occurred_at'],
    ['timestamp without zone', withValue({ ...body, occurred_at: '2026-11-12T10:00:00' }), 'bad_occurred_at'],
    ['schema_version 0', withValue({ ...body, schema_version: 0 }), 'bad_schema_version'],
    ['payload array', withValue({ ...body, payload: [] }), 'bad_payload'],
    ['missing received_at header', goodMessage({ headers: {} }), 'bad_received_at'],
  ])('rejects %s without throwing', (_name, msg, code) => {
    const r = parseMessage(msg);
    expect(r.ok).toBe(false);
    if (!r.ok) {
      expect(r.error.code).toBe(code);
      expect(r.error).toMatchObject({ partition: 2, offset: '41' });
      expect(r.error.message.length).toBeGreaterThan(0);
    }
  });

  it('mixed batch: valid rows and invalid ones are separated', () => {
    const results = [goodMessage(), goodMessage({ value: Buffer.from('x') }), goodMessage()].map((m) => parseMessage(m));
    expect(results.map((r) => r.ok)).toEqual([true, false, true]);
  });
});

describe('S3-F7 Avro values', () => {
  const V2 = readSchemaFile(resolve(import.meta.dirname, '../../../../schemas'), 'events/v2.avsc').parsed;
  const serde = new EventSerde(undefined, { subject: 'events.raw-value', schema: V2 });
  serde.useWriterId(24);
  const avroMessage = (o: Record<string, unknown> = {}) => {
    const value = serde.encode({ ...body, received_at: '2026-11-22T05:00:00.123Z', ...o } as any);
    return { partition: 2, offset: '41', key: 'tenant_a', value, headers: {} };
  };

  it('decodes an Avro record into the same row as its JSON twin (same content_hash)', () => {
    const fromJson = parseMessage(goodMessage());
    const fromAvro = parseMessage(avroMessage(), { serde });
    expect(fromAvro.ok && fromJson.ok).toBe(true);
    if (!fromAvro.ok || !fromJson.ok) return;
    expect(fromAvro.row).toMatchObject({ event_id: event.event_id, tenant_id: 'tenant_a', type: event.type, schema_version: 1, payload: '{"a":1}', schema_id: 24 });
    expect(fromAvro.row.received_at).toBe('2026-11-22T05:00:00.123Z'); // from the value, not a header
    expect(fromAvro.row.content_hash.equals(fromJson.row.content_hash)).toBe(true);
  });

  it('Avro without a serde, an unknown id, corrupt bytes: parse errors with their own codes', () => {
    expect(parseMessage(avroMessage())).toMatchObject({ ok: false, error: { code: 'not_avro' } });
    const unknown = { ...avroMessage(), value: encodeWire(999, Buffer.from([1])) };
    const s2 = new EventSerde(undefined, { subject: 'events.raw-value', schema: V2 });
    return s2.prefetch([unknown.value]).then(() => {
      expect(parseMessage(unknown, { serde: s2 })).toMatchObject({ ok: false, error: { code: 'unknown_schema_id' } });
      const corrupt = { ...avroMessage(), value: Buffer.concat([avroMessage().value.subarray(0, 8)]) };
      expect(parseMessage(corrupt, { serde })).toMatchObject({ ok: false, error: { code: 'avro_decode' } });
    });
  });

  it('a decoded Avro record still goes through the envelope rules', () => {
    expect(parseMessage(avroMessage({ event_id: 'not-a-uuid' }), { serde })).toMatchObject({ ok: false, error: { code: 'bad_event_id' } });
  });

  it('a writer schema that was not prefetched is a bug, not a dead letter: it throws', () => {
    const fresh = new EventSerde(undefined, { subject: 'events.raw-value', schema: V2 });
    expect(() => parseMessage(avroMessage(), { serde: fresh })).toThrow(/not prefetched/);
  });

  it('SINK_ACCEPT_JSON=false: Stage 2 JSON values are json_not_accepted', () => {
    expect(parseMessage(goodMessage(), { serde, acceptJson: false })).toMatchObject({ ok: false, error: { code: 'json_not_accepted' } });
    expect(parseMessage(goodMessage(), { serde }).ok).toBe(true); // default: accepted
  });
});
