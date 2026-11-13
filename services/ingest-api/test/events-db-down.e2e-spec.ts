import { Test } from '@nestjs/testing';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { randomUUID } from 'node:crypto';
import request from 'supertest';

process.env.DATABASE_URL = 'postgres://postgres:x@localhost:1/none'; // nothing listens on port 1
process.env.API_KEYS = 'key_a:tenant_a';
process.env.PG_CONN_TIMEOUT_MS = '500';

import { AppModule } from '../src/app.module.js';
import { configureApp } from '../src/bootstrap.js';

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

  beforeAll(async () => {
    const mod = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = mod.createNestApplication<NestExpressApplication>({
      bodyParser: false,
    });

    configureApp(app);
    await app.init();
  });

  afterAll(() => app.close());

  it('checks retry header to be set when db is down', async () => {
    const e = ev();
    await request(app.getHttpServer())
      .post('/v1/events')
      .set('Authorization', 'Bearer key_a')
      .send(e)
      .expect(503)
      .expect('retry-after', /\d+/);
  });

  it('checks /readyz and /healthz when db is down', async () => {
    await request(app.getHttpServer()).get('/readyz').expect(503);
    await request(app.getHttpServer()).get('/healthz').expect(200);
  });
});
