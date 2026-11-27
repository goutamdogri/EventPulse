import 'reflect-metadata';
import { HEADER_REQUEST_ID } from '@eventpulse/kafka-config';
import { FaultInjector, loadFaultConfig } from '@eventpulse/fault-injector';
import { Idempotency, IdempotencyUnavailableError, InvalidIdempotencyKeyError, MemoryStore } from '@eventpulse/idempotency';
import type { IdempotencyOptions, IdempotencyStore } from '@eventpulse/idempotency';
import { loadConfig } from '../config.js';
import { IdempotencyInProgressError, IdempotencyKeyReusedError } from '../idempotency/errors.js';
import { EventsController } from './events.controller.js';
import { EventsService } from './events.service.js';

const dto = {
  event_id: '6f1c2b1e-8a43-4c0e-9d2f-0a1b2c3d4e5f',
  type: 'order.created',
  occurred_at: '2026-11-12T10:00:00Z',
  schema_version: 1,
  payload: { a: 1 },
};

const tick = () => new Promise((r) => setImmediate(r));

function setup(o: { store?: IdempotencyStore; idem?: Partial<IdempotencyOptions>; faults?: string; autoAck?: boolean; kill?: () => void } = {}) {
  let release!: () => void;
  let fail!: (e: unknown) => void;
  const enqueue = vi.fn(() =>
    o.autoAck
      ? Promise.resolve()
      : new Promise<void>((res, rej) => {
          release = res;
          fail = rej;
        }),
  );
  const store = o.store ?? new MemoryStore();
  const idem = new Idempotency(store, { enabled: true, ttlS: 3600, inflightTtlS: 30, policy: 'open', ...o.idem });
  const kills: string[] = [];
  const faults = new FaultInjector(loadFaultConfig({ DEBUG_FAULTS: 'true', FAULTS: o.faults }), o.kill ?? (() => void kills.push('SIGKILL')), () => undefined);
  const svc = new EventsService({ enqueue } as any, loadConfig({ API_KEYS: 'k:t' }), idem, faults);
  const ctrl = new EventsController(svc);
  const headers: Record<string, string> = {};
  const res = { setHeader: (k: string, v: string) => void (headers[k] = v) } as any;
  const call = (body: object = dto, key?: string, tenant = 'tenant_a') => ctrl.ingest(body as any, tenant, undefined, key, res);
  return { ctrl, svc, enqueue, store, kills, headers, res, call, release: () => release(), fail: (e: unknown) => fail(e) };
}

describe('EventsController + EventsService (Stage 2 behavior)', () => {
  it('does not answer until the batch is acknowledged', async () => {
    const s = setup();
    let answered = false;
    const p = s.call().then((r) => ((answered = true), r));
    await tick();
    expect(answered).toBe(false);
    s.release();
    await expect(p).resolves.toEqual({ event_id: dto.event_id });
  });

  it('propagates a failed batch (no 202)', async () => {
    const s = setup();
    const p = s.call();
    await tick();
    s.fail(new Error('kafka down'));
    await expect(p).rejects.toThrow('kafka down');
  });

  it('encodes the tenant from the key and passes a size', async () => {
    const s = setup({ autoAck: true });
    await s.ctrl.ingest(dto as any, 'tenant_a', 'req-123');
    const arg = (s.enqueue.mock.calls[0] as any)[0];
    expect(arg.message.key).toBe('tenant_a');
    expect(JSON.parse(arg.message.value.toString()).tenant_id).toBe('tenant_a');
    expect(arg.message.headers[HEADER_REQUEST_ID]).toBe('req-123');
    expect(arg.sizeBytes).toBeGreaterThan(arg.message.value.length);
  });

  it('replaces an unsafe x-request-id with a generated one', async () => {
    const s = setup({ autoAck: true });
    await s.ctrl.ingest(dto as any, 'tenant_a', 'bad id\nwith newline');
    const arg = (s.enqueue.mock.calls[0] as any)[0];
    expect(arg.message.headers[HEADER_REQUEST_ID]).toMatch(/^[0-9a-f-]{36}$/);
  });
});

describe('S3-F1 idempotent ingest', () => {
  it('same event_id twice (no header): produced once, the second gets the original result + Idempotent-Replayed', async () => {
    const s = setup({ autoAck: true });
    await expect(s.call()).resolves.toEqual({ event_id: dto.event_id });
    expect(s.headers['Idempotent-Replayed']).toBeUndefined();
    await expect(s.call()).resolves.toEqual({ event_id: dto.event_id });
    expect(s.headers['Idempotent-Replayed']).toBe('true');
    expect(s.enqueue).toHaveBeenCalledTimes(1);
  });

  it('Idempotency-Key header: the key, not the event_id, identifies the request', async () => {
    const s = setup({ autoAck: true });
    await s.call(dto, 'order-42');
    await s.call(dto, 'order-43'); // other key, same body: a new request
    expect(s.enqueue).toHaveBeenCalledTimes(2);
    await s.call(dto, 'order-42');
    expect(s.enqueue).toHaveBeenCalledTimes(2);
  });

  it('same key while the first is still producing: IdempotencyInProgressError (409)', async () => {
    const s = setup();
    const first = s.call(dto, 'k1');
    await tick();
    await expect(s.call(dto, 'k1')).rejects.toBeInstanceOf(IdempotencyInProgressError);
    s.release();
    await first;
  });

  it('same key, different payload: IdempotencyKeyReusedError (422), nothing produced', async () => {
    const s = setup({ autoAck: true });
    await s.call(dto, 'k1');
    await expect(s.call({ ...dto, payload: { a: 2 } }, 'k1')).rejects.toBeInstanceOf(IdempotencyKeyReusedError);
    expect(s.enqueue).toHaveBeenCalledTimes(1);
  });

  it('key order and formatting of the payload do not make a different request (canonical hash)', async () => {
    const s = setup({ autoAck: true });
    await s.call({ ...dto, payload: { a: 1, b: { y: 2, x: 1 } } }, 'k1');
    await expect(s.call({ ...dto, payload: { b: { x: 1, y: 2 }, a: 1.0 } }, 'k1')).resolves.toEqual({ event_id: dto.event_id });
    expect(s.enqueue).toHaveBeenCalledTimes(1);
  });

  it('a failed produce releases the key, so the retry is a first attempt', async () => {
    const s = setup();
    const p = s.call(dto, 'k1');
    await tick();
    s.fail(new Error('kafka down'));
    await expect(p).rejects.toThrow('kafka down');
    const retry = s.call(dto, 'k1');
    await tick();
    expect(s.enqueue).toHaveBeenCalledTimes(2);
    s.release();
    await expect(retry).resolves.toEqual({ event_id: dto.event_id });
  });

  it('the same key under two tenants is two requests', async () => {
    const s = setup({ autoAck: true });
    await s.call(dto, 'k1', 'tenant_a');
    await s.call(dto, 'k1', 'tenant_b');
    expect(s.enqueue).toHaveBeenCalledTimes(2);
  });

  it('rejects an invalid Idempotency-Key (400) before anything is produced', async () => {
    const s = setup({ autoAck: true });
    await expect(s.call(dto, 'has space')).rejects.toBeInstanceOf(InvalidIdempotencyKeyError);
    expect(s.enqueue).not.toHaveBeenCalled();
  });

  const down: IdempotencyStore = {
    acquire: () => Promise.reject(new Error('ECONNREFUSED')),
    replace: () => Promise.reject(new Error('ECONNREFUSED')),
    remove: () => Promise.reject(new Error('ECONNREFUSED')),
  };

  it('Redis down, policy open: produced anyway (no dedup), still 202', async () => {
    const s = setup({ store: down, idem: { policy: 'open' }, autoAck: true });
    await expect(s.call()).resolves.toEqual({ event_id: dto.event_id });
    await expect(s.call()).resolves.toEqual({ event_id: dto.event_id });
    expect(s.enqueue).toHaveBeenCalledTimes(2); // duplicates go to Kafka; the sink absorbs them
  });

  it('Redis down, policy closed: IdempotencyUnavailableError (503), nothing produced', async () => {
    const s = setup({ store: down, idem: { policy: 'closed' }, autoAck: true });
    await expect(s.call()).rejects.toBeInstanceOf(IdempotencyUnavailableError);
    expect(s.enqueue).not.toHaveBeenCalled();
  });

  it('crash points: what is produced and what the key holds when the process dies there', async () => {
    for (const [point, produced, state] of [
      ['api:before-produce', 0, 'inflight'],
      ['api:after-produce-before-complete', 1, 'inflight'],
      ['api:after-complete-before-response', 1, 'done'],
    ] as const) {
      const store = new MemoryStore();
      const s = setup({ store, faults: point, autoAck: true, kill: () => { throw new Error('SIGKILL'); } });
      await expect(s.call(dto, 'k1'), point).rejects.toThrow('SIGKILL');
      expect(s.enqueue, point).toHaveBeenCalledTimes(produced);
      expect(store.peek('idem:tenant_a:k1')?.split(':')[0], point).toBe(state);
    }
  });
});
