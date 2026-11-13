import { Test } from '@nestjs/testing';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import request from 'supertest'; 

process.env.DATABASE_URL ??=
  'postgres://eventpulse:eventpulse@localhost:5434/eventpulse';
process.env.API_KEYS = 'key_a:tenant_a,key_b:tenant_b';

import { AppModule } from '../src/app.module.js';
import { configureApp } from '../src/bootstrap.js';
import { PG_POOL } from '../src/db/db.module.js';

const ev = (o: any = {}) => ({
  event_id: randomUUID(),
  type: 'order.created',
  occurred_at: new Date().toISOString(),
  schema_version: 1,
  payload: { a: 1 },
  ...o,
});

describe('POST /v1/events', () => {
  let app: NestExpressApplication;
  let pool: Pool;

  beforeAll(async () => {
    const mod = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = mod.createNestApplication<NestExpressApplication>({
      bodyParser: false,
    });

    configureApp(app);
    await app.init();

    pool = app.get(PG_POOL);
  });

  afterAll(() => app.close());

  const count = async (id: string) =>
    Number(
      (await pool.query('select count(*) from events where event_id=$1', [id]))
        .rows[0].count,
    );

  it('stores a valid event with the tenant from the key', async () => {
    const e = ev();
    await request(app.getHttpServer())
      .post('/v1/events')
      .set('Authorization', 'Bearer key_a')
      .send(e)
      .expect(202);

    const r = await pool.query(
      'select tenant_id, type from events where event_id=$1',
      [e.event_id],
    );

    expect(r.rows[0]).toEqual({ tenant_id: 'tenant_a', type: 'order.created' });
  });

  it('duplicate event_id: both 202, one row', async () => {
    const e = ev();
    const s = () =>
      request(app.getHttpServer())
        .post('/v1/events')
        .set('Authorization', 'Bearer key_a')
        .send(e);
    await s().expect(202);
    await s().expect(202);
    expect(await count(e.event_id)).toBe(1);
  });

  it('invalid event: 400 and zero rows', async () => {
    const e = ev({ occurred_at: 'nope' });
    await request(app.getHttpServer())
      .post('/v1/events')
      .set('Authorization', 'Bearer key_a')
      .send(e)
      .expect(400);
    expect(await count(e.event_id)).toBe(0);
  });

  it('bad key: 401', () =>
    request(app.getHttpServer())
      .post('/v1/events')
      .set('Authorization', 'Bearer x')
      .send(ev())
      .expect(401));

  it('oversized body: 413', () =>
    request(app.getHttpServer())
      .post('/v1/events')
      .set('Authorization', 'Bearer key_a')
      .send(ev({ payload: { d: 'x'.repeat(100_000) } }))
      .expect(413));
});
