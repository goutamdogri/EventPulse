// S3-F1: the idempotency store's Lua scripts against the real Redis (test database 15, see setup-env.ts).
import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { Idempotency, RedisStore, encodeInflight } from '@eventpulse/idempotency';

describe('RedisStore (real Redis, db 15)', () => {
  const redis = new Redis(process.env.REDIS_URL!, { maxRetriesPerRequest: 1 });
  const store = new RedisStore(redis);
  const k = () => `test:idem:${randomUUID()}`;

  afterAll(() => redis.quit());

  it('acquire is SET NX EX: the first wins, the second gets the current value', async () => {
    const key = k();
    expect(await store.acquire(key, 'v1', 60)).toEqual({ acquired: true });
    expect(await store.acquire(key, 'v2', 60)).toEqual({ acquired: false, current: 'v1' });
    const ttl = await redis.ttl(key);
    expect(ttl).toBeGreaterThan(55);
    expect(ttl).toBeLessThanOrEqual(60);
  });

  it('replace only from the exact expected value (owner check), with the new TTL', async () => {
    const key = k();
    await store.acquire(key, 'inflight-mine', 30);
    expect(await store.replace(key, 'inflight-other', 'done', 3600)).toBe(false);
    expect(await redis.get(key)).toBe('inflight-mine');
    expect(await store.replace(key, 'inflight-mine', 'done', 3600)).toBe(true);
    expect(await redis.get(key)).toBe('done');
    expect(await redis.ttl(key)).toBeGreaterThan(3500);
    expect(await store.replace(k(), 'x', 'y', 10)).toBe(false); // absent key
  });

  it('remove only the exact expected value', async () => {
    const key = k();
    await store.acquire(key, 'mine', 30);
    expect(await store.remove(key, 'theirs')).toBe(false);
    expect(await redis.exists(key)).toBe(1);
    expect(await store.remove(key, 'mine')).toBe(true);
    expect(await redis.exists(key)).toBe(0);
  });

  it('an inflight key expires by itself (the abandoned-request case)', async () => {
    const key = k();
    await store.acquire(key, 'v', 1);
    await new Promise((r) => setTimeout(r, 1_200));
    expect(await store.acquire(key, 'v2', 1)).toEqual({ acquired: true });
  });

  it('works after SCRIPT FLUSH (ioredis falls back from EVALSHA to EVAL)', async () => {
    await redis.script('FLUSH');
    expect(await store.acquire(k(), 'v', 10)).toEqual({ acquired: true });
  });

  it('end to end with the state machine: values written in the documented format', async () => {
    const prefix = `test${randomUUID().slice(0, 8)}`;
    const idem = new Idempotency(store, { enabled: true, ttlS: 600, inflightTtlS: 30, policy: 'closed', prefix, newOwner: () => 'own1', now: () => 42 });
    const h = 'c'.repeat(64);
    const b = await idem.begin('tenant_x', 'key-1', h);
    expect(b.kind).toBe('new');
    expect(await redis.get(`${prefix}:tenant_x:key-1`)).toBe(encodeInflight(h, 'own1', 42));
    if (b.kind === 'new') await idem.complete(b.ticket, 'evt-9');
    expect(await redis.get(`${prefix}:tenant_x:key-1`)).toBe(`done:${h}:evt-9`);
    expect(await idem.begin('tenant_x', 'key-1', h)).toEqual({ kind: 'replay', eventId: 'evt-9' });
  });
});
