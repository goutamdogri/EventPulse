// S3-F1 end to end: the real API (in process) + the 3-broker Kafka cluster + Redis (test db 15).
// "Produced" is measured in the log: the test topic's end offsets before and after (files run one at a time).
import { Test } from '@nestjs/testing';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { randomUUID } from 'node:crypto';
import request from 'supertest';
import { Redis } from 'ioredis';
import { encodeInflight, eventContentHash } from '@eventpulse/idempotency';
import { AppModule } from '../src/app.module.js';
import { configureApp } from '../src/bootstrap.js';
import { endOffsets, readFrom, totalOf } from './kafka-helpers.js';

const ev = (o: any = {}) => ({
  event_id: randomUUID(),
  type: 'order.created',
  occurred_at: new Date().toISOString(),
  schema_version: 1,
  payload: { a: 1, b: { x: 1, y: [1, 2] } },
  ...o,
});

describe('S3-F1 idempotent ingest (API + Kafka + Redis)', () => {
  let app: NestExpressApplication;
  const redis = new Redis(process.env.REDIS_URL!);
  const post = (body: object | string, opts: { key?: string; apiKey?: string } = {}) => {
    const r = request(app.getHttpServer())
      .post('/v1/events')
      .set('Authorization', `Bearer ${opts.apiKey ?? 'key_a'}`)
      .set('Content-Type', 'application/json');
    if (opts.key) r.set('Idempotency-Key', opts.key);
    return r.send(typeof body === 'string' ? body : JSON.stringify(body));
  };
  /** Records appended to the test topic while fn runs. */
  async function produced(fn: () => Promise<unknown>): Promise<bigint> {
    const before = totalOf(await endOffsets());
    await fn();
    return totalOf(await endOffsets()) - before;
  }

  beforeAll(async () => {
    const mod = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = mod.createNestApplication<NestExpressApplication>({ bodyParser: false });
    configureApp(app);
    await app.init();
    await redis.ping();
  }, 30_000);

  afterAll(async () => {
    await app?.close();
    await redis.quit();
  });

  it('[G8.same-key] same Idempotency-Key twice: one record; the second is the original result, marked replayed', async () => {
    const e = ev();
    let first!: request.Response;
    let second!: request.Response;
    expect(
      await produced(async () => {
        first = await post(e, { key: `k-${randomUUID()}` }).expect(202);
      }),
    ).toBe(1n);
    const key = `k-${randomUUID()}`;
    await post(e, { key }).expect(202); // (another key, same event: a separate request)
    expect(
      await produced(async () => {
        second = await post(e, { key }).expect(202);
      }),
    ).toBe(0n);
    expect(first.body).toEqual({ event_id: e.event_id });
    expect(second.body).toEqual({ event_id: e.event_id });
    expect(second.headers['idempotent-replayed']).toBe('true');
    expect(first.headers['idempotent-replayed']).toBeUndefined();
  });

  it('no header: the event_id is the key', async () => {
    const e = ev();
    expect(await produced(() => post(e).expect(202))).toBe(1n);
    expect(await produced(() => post(e).expect(202))).toBe(0n);
  });

  it('[G8.key-reuse] same key, different payload: 422, nothing produced', async () => {
    const key = `k-${randomUUID()}`;
    const e = ev();
    await post(e, { key }).expect(202);
    let res!: request.Response;
    expect(
      await produced(async () => {
        res = await post({ ...e, payload: { a: 2 } }, { key }).expect(422);
      }),
    ).toBe(0n);
    expect(res.body).toMatchObject({ statusCode: 422, reason: 'idempotency_key_reused' });
  });

  it('a body that differs only by key order and whitespace is the SAME request (canonical hash)', async () => {
    const id = randomUUID();
    const at = new Date().toISOString();
    const a = `{"event_id":"${id}","type":"t","occurred_at":"${at}","schema_version":1,"payload":{"a":1,"b":{"x":1,"y":[1,2]}}}`;
    const b = `{ "payload" : { "b": { "y": [1, 2], "x": 1.0 }, "a": 1 },\n  "schema_version": 1, "occurred_at": "${at}", "type": "t", "event_id": "${id}" }`;
    const key = `k-${randomUUID()}`;
    await post(a, { key }).expect(202);
    expect(await produced(() => post(b, { key }).expect(202))).toBe(0n);
  });

  it('[G8.concurrency] 50 parallel identical requests: exactly one record; the others get 409 or the replayed 202', async () => {
    const e = ev();
    const key = `k-${randomUUID()}`;
    const before = await endOffsets();
    const statuses = await Promise.all(Array.from({ length: 50 }, () => post(e, { key }).then((r) => `${r.status}${r.headers['idempotent-replayed'] ? '-replayed' : ''}`)));
    const after = await endOffsets();
    expect(totalOf(after) - totalOf(before)).toBe(1n);
    const counts = statuses.reduce<Record<string, number>>((m, s) => ((m[s] = (m[s] ?? 0) + 1), m), {});
    expect(counts['202']).toBe(1);
    expect((counts['409'] ?? 0) + (counts['202-replayed'] ?? 0)).toBe(49);
    const recs = await readFrom(before, (r) => r.some((x) => x.value.event_id === e.event_id));
    expect(recs.filter((x) => x.value.event_id === e.event_id)).toHaveLength(1);
    console.info(`[G8.concurrency] 50 identical requests -> ${JSON.stringify(counts)}`);
  });

  it('in progress: 409 + Retry-After, nothing produced; it frees itself when the inflight key expires', async () => {
    // Exactly what a request that is still producing (or died mid-way) leaves in Redis.
    const e = ev();
    const key = `k-${randomUUID()}`;
    const hash = eventContentHash({ tenant_id: 'tenant_a', ...e }).toString('hex');
    await redis.set(`idem:tenant_a:${key}`, encodeInflight(hash, 'someone-else', Date.now()), 'EX', 1);
    let res!: request.Response;
    expect(
      await produced(async () => {
        res = await post(e, { key }).expect(409);
      }),
    ).toBe(0n);
    expect(res.headers['retry-after']).toBe('1');
    expect(res.body).toMatchObject({ statusCode: 409, reason: 'idempotency_in_progress' });
    await new Promise((r) => setTimeout(r, 1_200));
    expect(await produced(() => post(e, { key }).expect(202))).toBe(1n);
  });

  it('[G8.tenant-scope] the same key under two tenants is two requests', async () => {
    const key = `k-${randomUUID()}`;
    const e = ev();
    expect(
      await produced(async () => {
        await post(e, { key, apiKey: 'key_a' }).expect(202);
        await post(e, { key, apiKey: 'key_b' }).expect(202);
      }),
    ).toBe(2n);
  });

  it('Redis flushed between two identical requests: the second is produced again (Layer 1 is only an optimization)', async () => {
    const e = ev();
    await post(e).expect(202);
    await redis.flushdb(); // database 15 only
    const r = await post(e).expect(202);
    expect(r.headers['idempotent-replayed']).toBeUndefined();
  });

  it('an invalid Idempotency-Key is a 400 and produces nothing', async () => {
    expect(await produced(() => post(ev(), { key: 'has space' }).expect(400))).toBe(0n);
    expect(await produced(() => post(ev(), { key: 'x'.repeat(256) }).expect(400))).toBe(0n);
  });

  it('metrics: ep_idem_requests_total by result, Redis latency, and /debug/stats idempotency section', async () => {
    const m = await request(app.getHttpServer()).get('/metrics').expect(200);
    expect(m.text).toMatch(/ep_idem_requests_total\{result="new"\} \d+/);
    expect(m.text).toMatch(/ep_idem_requests_total\{result="replayed"\} \d+/);
    expect(m.text).toMatch(/ep_idem_requests_total\{result="payload_mismatch"\} \d+/);
    expect(m.text).toContain('ep_idem_redis_latency_seconds_bucket');
    const s = await request(app.getHttpServer()).get('/debug/stats').expect(200);
    expect(s.body.idempotency).toMatchObject({ redis_status: 'ready', breaker: 'closed' });
    expect(s.body.idempotency.by_result.new).toBeGreaterThan(0);
    expect(s.body.config.idempotency).toMatchObject({ enabled: true, failure_policy: 'open', ttl_s: 3600 });
  });
});
