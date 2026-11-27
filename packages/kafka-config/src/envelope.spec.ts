import {
  CONTENT_TYPE_JSON,
  HEADER_CONTENT_TYPE,
  HEADER_RECEIVED_AT,
  HEADER_REQUEST_ID,
  HEADER_SCHEMA_VERSION,
  encodeEvent,
} from './envelope.js';

const event = {
  event_id: '6f1c2b1e-8a43-4c0e-9d2f-0a1b2c3d4e5f',
  type: 'order.created',
  occurred_at: '2026-11-12T10:00:00Z',
  schema_version: 2,
  payload: { a: 1 },
};
const receivedAt = new Date('2026-11-22T05:00:00.123Z');

describe('encodeEvent', () => {
  it('keys by tenant_id in tenant mode', () => {
    const e = encodeEvent({ tenantId: 'tenant_a', event, receivedAt, requestId: 'r1', keyMode: 'tenant' });
    expect(e.key).toBe('tenant_a');
  });

  it('keys by event_id in event mode', () => {
    const e = encodeEvent({ tenantId: 'tenant_a', event, receivedAt, requestId: 'r1', keyMode: 'event' });
    expect(e.key).toBe(event.event_id);
  });

  it('writes the envelope with tenant_id from the caller', () => {
    const e = encodeEvent({ tenantId: 'tenant_a', event, receivedAt, requestId: 'r1', keyMode: 'tenant' });
    expect(JSON.parse(e.value.toString())).toEqual({ ...event, tenant_id: 'tenant_a' });
  });

  it('sets the four headers (content-type since Stage 3)', () => {
    const e = encodeEvent({ tenantId: 'tenant_a', event, receivedAt, requestId: 'r1', keyMode: 'tenant' });
    expect(e.headers).toEqual({
      [HEADER_RECEIVED_AT]: '2026-11-22T05:00:00.123Z',
      [HEADER_SCHEMA_VERSION]: '2',
      [HEADER_REQUEST_ID]: 'r1',
      [HEADER_CONTENT_TYPE]: CONTENT_TYPE_JSON,
    });
  });

  it('serialize replaces the value and its content-type; key and size follow', () => {
    const e = encodeEvent({
      tenantId: 'tenant_a',
      event,
      receivedAt,
      requestId: 'r1',
      keyMode: 'tenant',
      serialize: (env, at) => ({ value: Buffer.from(`avro:${env.tenant_id}:${at.getTime()}`), contentType: 'application/x-test' }),
    });
    expect(e.value.toString()).toBe(`avro:tenant_a:${receivedAt.getTime()}`);
    expect(e.headers[HEADER_CONTENT_TYPE]).toBe('application/x-test');
    expect(e.key).toBe('tenant_a');
  });

  it('counts key, value and headers in sizeBytes', () => {
    const e = encodeEvent({ tenantId: 'tenant_a', event, receivedAt, requestId: 'r1', keyMode: 'tenant' });
    const headerBytes = Object.entries(e.headers).reduce((a, [k, v]) => a + k.length + v.length, 0);
    expect(e.sizeBytes).toBe(e.key.length + e.value.length + headerBytes);
  });
});
