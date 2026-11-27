// S3-F1 failure policy: Redis cut off (Toxiproxy proxy "redis": localhost:16379 -> redis:6379, `make toxiproxy`).
//   open   (default)  -> 202 without dedup, counted as fail_open; latency stays bounded (timeout, then breaker)
//   closed            -> 503 + Retry-After, reason idempotency_unavailable; nothing produced
// Both recover by themselves when Redis comes back.
import { Test } from '@nestjs/testing';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { randomUUID } from 'node:crypto';
import request from 'supertest';
import { AppModule } from '../src/app.module.js';
import { configureApp } from '../src/bootstrap.js';
import { endOffsets, totalOf } from './kafka-helpers.js';

const TOXI = process.env.TOXI_URL ?? 'http://localhost:8474';
const VIA_PROXY = 'redis://localhost:16379/15';
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function toxi(path: string, method: string, body?: object) {
  const r = await fetch(`${TOXI}${path}`, { method, headers: { 'content-type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
  if (!r.ok && r.status !== 404) throw new Error(`toxiproxy ${method} ${path}: ${r.status} ${await r.text()}`);
}
const setRedisProxy = (enabled: boolean) => toxi('/proxies/redis', 'POST', { enabled });

const ev = () => ({ event_id: randomUUID(), type: 't', occurred_at: new Date().toISOString(), schema_version: 1, payload: {} });

async function startApp(env: Record<string, string>) {
  const saved: Record<string, string | undefined> = {};
  for (const [k, v] of Object.entries(env)) {
    saved[k] = process.env[k];
    process.env[k] = v;
  }
  try {
    const mod = await Test.createTestingModule({ imports: [AppModule] }).compile();
    const app = mod.createNestApplication<NestExpressApplication>({ bodyParser: false });
    configureApp(app);
    await app.init();
    return app;
  } finally {
    for (const [k, v] of Object.entries(saved)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  }
}

describe.each(['open', 'closed'] as const)('S3-F1 Redis unavailable, IDEM_FAILURE_POLICY=%s', (policy) => {
  let app: NestExpressApplication;
  const post = (body: object) =>
    request(app.getHttpServer()).post('/v1/events').set('Authorization', 'Bearer key_a').send(body);
  const stats = async () => (await request(app.getHttpServer()).get('/debug/stats')).body;

  beforeAll(async () => {
    const r = await fetch(`${TOXI}/proxies/redis`);
    if (!r.ok) throw new Error('toxiproxy proxy "redis" missing: run `make toxiproxy`');
    await setRedisProxy(true);
    app = await startApp({
      REDIS_URL: VIA_PROXY,
      IDEM_FAILURE_POLICY: policy,
      REDIS_COMMAND_TIMEOUT_MS: '100',
      IDEM_BREAKER_FAILURES: '3',
      IDEM_BREAKER_COOLDOWN_MS: '1000',
    });
    await post(ev()).expect(202); // Redis reachable: normal path
  }, 30_000);

  afterAll(async () => {
    await setRedisProxy(true);
    await app?.close();
  });

  it(`[G8.redis-down-${policy}] Redis cut off: ${policy === 'open' ? '202 without dedup' : '503 + Retry-After'}, bounded latency, recovers`, async () => {
    try {
      await setRedisProxy(false);
      const before = totalOf(await endOffsets());
      const latencies: number[] = [];
      const statuses: number[] = [];
      let retryAfter: string | undefined;
      let reason: string | undefined;
      for (let i = 0; i < 10; i++) {
        const t0 = performance.now();
        const r = await post(ev());
        latencies.push(performance.now() - t0);
        statuses.push(r.status);
        retryAfter ??= r.headers['retry-after'];
        reason ??= r.body?.reason;
      }
      const producedWhileDown = totalOf(await endOffsets()) - before;
      const st = await stats();
      console.info(`[redis-down ${policy}] statuses ${statuses.join(',')} latency ms ${latencies.map((l) => l.toFixed(0)).join(',')} breaker ${st.idempotency.breaker}`);

      if (policy === 'open') {
        expect(statuses.every((s) => s === 202)).toBe(true);
        expect(producedWhileDown).toBe(10n);
        expect(st.idempotency.by_result.fail_open).toBeGreaterThanOrEqual(10);
      } else {
        expect(statuses.every((s) => s === 503)).toBe(true);
        expect(retryAfter).toBe('1');
        expect(reason).toBe('idempotency_unavailable');
        expect(producedWhileDown).toBe(0n);
        expect(st.idempotency.by_result.fail_closed).toBeGreaterThanOrEqual(10);
      }
      // A dead Redis never costs more than the command timeout (100 ms) plus the produce; once the breaker is open
      // it costs nothing at all.
      expect(Math.max(...latencies.slice(4))).toBeLessThan(policy === 'open' ? 1_000 : 150);
      expect(st.idempotency.breaker).not.toBe('closed');

      await setRedisProxy(true);
      // Back to normal without a restart: reconnect + breaker probe after its cooldown.
      const e = ev();
      for (let i = 0; i < 40; i++) {
        const r = await post(e);
        if (r.status === 202 && (await stats()).idempotency.breaker === 'closed') break;
        await sleep(250);
      }
      expect((await stats()).idempotency.redis_status).toBe('ready');
      const replay = await post(e).expect(202);
      expect(replay.headers['idempotent-replayed']).toBe('true'); // dedup works again
    } finally {
      await setRedisProxy(true);
    }
  }, 60_000);
});
