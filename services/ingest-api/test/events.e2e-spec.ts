// S2-F1 end to end against the real 3-broker cluster (infra/compose/docker-compose.kafka.yml must be up).
import { Test } from '@nestjs/testing';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { randomUUID } from 'node:crypto';
import request from 'supertest';
import {
  HEADER_RECEIVED_AT,
  HEADER_REQUEST_ID,
  HEADER_SCHEMA_VERSION,
} from '@eventpulse/kafka-config';
import { AppModule } from '../src/app.module.js';
import { configureApp } from '../src/bootstrap.js';
import { endOffsets, readFrom, totalOf } from './kafka-helpers.js';

const ev = (o: any = {}) => ({
  event_id: randomUUID(),
  type: 'order.created',
  occurred_at: new Date().toISOString(),
  schema_version: 1,
  payload: { a: 1 },
  ...o,
});

describe('POST /v1/events -> events.raw', () => {
  let app: NestExpressApplication;
  const post = (body: object, key = 'key_a') =>
    request(app.getHttpServer())
      .post('/v1/events')
      .set('Authorization', `Bearer ${key}`)
      .send(body);

  beforeAll(async () => {
    const mod = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = mod.createNestApplication<NestExpressApplication>({ bodyParser: false });
    configureApp(app);
    await app.init(); // connects the producer
  }, 30_000);

  afterAll(() => app?.close());

  it('202 only after the ack: the record is already in the log when the response arrives', async () => {
    const before = await endOffsets();
    const e = ev();
    const res = await post(e).set('x-request-id', 'e2e-req-1').expect(202);
    expect(res.body).toEqual({ event_id: e.event_id });

    // Acknowledged means appended: the end offsets moved by exactly one, without waiting.
    const after = await endOffsets();
    expect(totalOf(after) - totalOf(before)).toBe(1n);

    const recs = await readFrom(before, (r) => r.some((x) => x.value.event_id === e.event_id));
    const rec = recs.find((x) => x.value.event_id === e.event_id)!;
    expect(rec).toBeDefined();
    expect(rec.key).toBe('tenant_a'); // KAFKA_KEY_MODE=tenant (default)
    expect(rec.value).toEqual({
      event_id: e.event_id,
      tenant_id: 'tenant_a',
      type: e.type,
      occurred_at: e.occurred_at,
      schema_version: 1,
      payload: { a: 1 },
    });
    expect(rec.headers[HEADER_REQUEST_ID]).toBe('e2e-req-1');
    expect(rec.headers[HEADER_SCHEMA_VERSION]).toBe('1');
    expect(Number.isNaN(Date.parse(rec.headers[HEADER_RECEIVED_AT]))).toBe(false);
  }, 30_000);

  it('concurrent requests are batched and each one is acknowledged', async () => {
    const before = await endOffsets();
    const events = Array.from({ length: 200 }, () => ev());
    const results = await Promise.all(events.map((e) => post(e)));
    expect(results.every((r) => r.status === 202)).toBe(true);

    const after = await endOffsets();
    expect(totalOf(after) - totalOf(before)).toBe(200n);

    // Same key (one tenant) -> same partition: all 200 records landed in one partition.
    const moved = [...after].filter(([p, o]) => o !== before.get(p));
    expect(moved).toHaveLength(1);
  }, 30_000);

  it('invalid event: 400 and nothing written', async () => {
    const before = totalOf(await endOffsets());
    await post(ev({ occurred_at: 'nope' })).expect(400);
    expect(totalOf(await endOffsets())).toBe(before);
  });

  it('bad key: 401 and nothing written', async () => {
    const before = totalOf(await endOffsets());
    await post(ev(), 'x').expect(401);
    expect(totalOf(await endOffsets())).toBe(before);
  });

  it('oversized body: 413 at the edge, Kafka never sees it', async () => {
    const before = totalOf(await endOffsets());
    await post(ev({ payload: { d: 'x'.repeat(100_000) } })).expect(413);
    expect(totalOf(await endOffsets())).toBe(before);
  });

  it('readyz reports Kafka up', () =>
    request(app.getHttpServer()).get('/readyz').expect(200, { ok: true, kafka: 'up' }));
});
