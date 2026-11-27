import { encodeEvent, HEADER_RECEIVED_AT, HEADER_REQUEST_ID, HEADER_SCHEMA_VERSION } from './envelope.js';

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

  it('sets all three headers', () => {
    const e = encodeEvent({ tenantId: 'tenant_a', event, receivedAt, requestId: 'r1', keyMode: 'tenant' });
    expect(e.headers).toEqual({
      [HEADER_RECEIVED_AT]: '2026-11-22T05:00:00.123Z',
      [HEADER_SCHEMA_VERSION]: '2',
      [HEADER_REQUEST_ID]: 'r1',
    });
  });

  it('counts key, value and headers in sizeBytes', () => {
    const e = encodeEvent({ tenantId: 'tenant_a', event, receivedAt, requestId: 'r1', keyMode: 'tenant' });
    const headerBytes = Object.entries(e.headers).reduce((a, [k, v]) => a + k.length + v.length, 0);
    expect(e.sizeBytes).toBe(e.key.length + e.value.length + headerBytes);
  });
});
