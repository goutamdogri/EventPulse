import 'reflect-metadata';
import { HEADER_REQUEST_ID } from '@eventpulse/kafka-config';
import { loadConfig } from '../config.js';
import { EventsController } from './events.controller.js';
import { EventsService } from './events.service.js';

const dto = {
  event_id: '6f1c2b1e-8a43-4c0e-9d2f-0a1b2c3d4e5f',
  type: 'order.created',
  occurred_at: '2026-11-12T10:00:00Z',
  schema_version: 1,
  payload: { a: 1 },
};

function setup() {
  let release!: () => void;
  let fail!: (e: unknown) => void;
  const enqueue = vi.fn(
    () =>
      new Promise<void>((res, rej) => {
        release = res;
        fail = rej;
      }),
  );
  const svc = new EventsService({ enqueue } as any, loadConfig({ API_KEYS: 'k:t' }));
  const ctrl = new EventsController(svc);
  return { ctrl, enqueue, release: () => release(), fail: (e: unknown) => fail(e) };
}

describe('EventsController + EventsService', () => {
  it('does not answer until the batch is acknowledged', async () => {
    const s = setup();
    let answered = false;
    const p = s.ctrl.ingest(dto as any, 'tenant_a').then((r) => ((answered = true), r));

    await new Promise((r) => setImmediate(r));
    expect(answered).toBe(false);

    s.release();
    await expect(p).resolves.toEqual({ event_id: dto.event_id });
  });

  it('propagates a failed batch (no 202)', async () => {
    const s = setup();
    const p = s.ctrl.ingest(dto as any, 'tenant_a');
    await new Promise((r) => setImmediate(r));
    s.fail(new Error('kafka down'));
    await expect(p).rejects.toThrow('kafka down');
  });

  it('encodes the tenant from the key and passes a size', async () => {
    const s = setup();
    void s.ctrl.ingest(dto as any, 'tenant_a', 'req-123');
    const arg = (s.enqueue.mock.calls[0] as any)[0];
    expect(arg.message.key).toBe('tenant_a');
    expect(JSON.parse(arg.message.value.toString()).tenant_id).toBe('tenant_a');
    expect(arg.message.headers[HEADER_REQUEST_ID]).toBe('req-123');
    expect(arg.sizeBytes).toBeGreaterThan(arg.message.value.length);
  });

  it('replaces an unsafe x-request-id with a generated one', async () => {
    const s = setup();
    void s.ctrl.ingest(dto as any, 'tenant_a', 'bad id\nwith newline');
    const arg = (s.enqueue.mock.calls[0] as any)[0];
    expect(arg.message.headers[HEADER_REQUEST_ID]).toMatch(/^[0-9a-f-]{36}$/);
  });
});
