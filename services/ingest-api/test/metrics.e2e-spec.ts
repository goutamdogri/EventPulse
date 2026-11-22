// S2-F6: /debug/stats and /metrics of the API after real requests (real Kafka, throw-away topic).
import { Test } from '@nestjs/testing';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { randomUUID } from 'node:crypto';
import request from 'supertest';
import { AppModule } from '../src/app.module.js';
import { configureApp } from '../src/bootstrap.js';

const ev = (o: any = {}) => ({ event_id: randomUUID(), type: 'metrics.e2e', occurred_at: new Date().toISOString(), schema_version: 1, payload: {}, ...o });

describe('API metrics (S2-F6)', () => {
  let app: NestExpressApplication;
  beforeAll(async () => {
    process.env.KAFKA_STATS_INTERVAL_MS = '500'; // librdkafka statistics quickly
    const mod = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = mod.createNestApplication<NestExpressApplication>({ bodyParser: false });
    configureApp(app);
    await app.init();
  }, 30_000);
  afterAll(() => app?.close());

  it('/debug/stats has every field, and producer.acked equals the number of 202s', async () => {
    const res = await Promise.all(Array.from({ length: 40 }, () => request(app.getHttpServer()).post('/v1/events').set('Authorization', 'Bearer key_a').send(ev())));
    expect(res.every((r) => r.status === 202)).toBe(true);
    await request(app.getHttpServer()).post('/v1/events').set('Authorization', 'Bearer key_a').send(ev({ type: '' })).expect(400);
    await new Promise((r) => setTimeout(r, 1200)); // at least one client statistics callback

    const s = (await request(app.getHttpServer()).get('/debug/stats').expect(200)).body;
    expect(s.producer.acked).toBe(40); // == 202s: used by the S2-F7 audit (check B)
    expect(s.producer.failed).toBe(0);
    expect(s.http.by_status['202']).toBe(40);
    expect(s.http.by_status['400']).toBe(1);
    expect(s.batcher).toMatchObject({ queue_depth: 0, queue_bytes: 0, in_flight: 0 });
    expect(Object.values<number>(s.batcher.flushes_by_reason).reduce((a, b) => a + b, 0)).toBeGreaterThan(0);
    expect(s.batcher.avg_batch_size).toBeGreaterThan(0);
    expect(s.producer.send_p99_ms).toBeGreaterThan(0);
    expect(s.producer).toMatchObject({ connected: true, breaker: 'closed' });
    expect(Object.keys(s.producer.brokers).length).toBeGreaterThan(0); // librdkafka statistics arrived
    expect(s.kafka_health.healthy).toBe(true);
    expect(s.config).toMatchObject({ acks: -1, idempotent: true, key_mode: 'tenant' });
    expect(s.config.topic).toBe(process.env.KAFKA_TOPIC);
    for (const k of ['uptime_s', 'memory', 'event_loop_delay_ms', 'http', 'batcher', 'producer', 'kafka_health', 'config']) expect(s, k).toHaveProperty(k);
  });

  it('/metrics exposes every series of the plan', async () => {
    const text = (await request(app.getHttpServer()).get('/metrics').expect(200)).text;
    for (const name of [
      'ep_batcher_queue_depth', 'ep_batcher_queue_bytes', 'ep_batcher_in_flight_batches', 'ep_batcher_batch_size_messages',
      'ep_batcher_batch_size_bytes', 'ep_batcher_flush_total', 'ep_batcher_queue_wait_seconds', 'ep_batcher_rejected_total',
      'ep_kafka_send_seconds', 'ep_kafka_send_errors_total', 'ep_kafka_messages_acked_total', 'ep_kafka_messages_failed_total',
      'ep_kafka_breaker_state', 'ep_kafka_producer_connected', 'ep_kafka_health_ok', 'ep_kafka_client_queue_messages',
      'ep_kafka_broker_rtt_seconds', 'ep_kafka_broker_in_flight_requests', 'ep_http_503_total',
      'http_requests_total', 'http_request_duration_seconds', 'eventpulse_event_loop_delay_seconds',
    ]) expect(text, name).toContain(`# TYPE ${name} `);
    expect(text).toMatch(/ep_kafka_messages_acked_total 4\d/);
    expect(text).toContain('ep_kafka_producer_connected 1');
  });
});
