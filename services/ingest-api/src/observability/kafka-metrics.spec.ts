import { Registry } from '@prometheus-io/client';
import { CircuitBreaker } from '../kafka/breaker.js';
import { MicroBatcher } from '../kafka/micro-batcher.js';
import { KafkaMetrics } from './kafka-metrics.js';

const item = (i: number) => ({ message: { key: `k${i}`, value: Buffer.from('x'), headers: {} }, sizeBytes: 10 });
const flush = () => new Promise<void>((r) => setImmediate(r));

function setup(opts: { breaker?: CircuitBreaker } = {}) {
  const registry = new Registry();
  const m = new KafkaMetrics(registry);
  const pending: { resolve: () => void; reject: (e: unknown) => void }[] = [];
  const send = () => new Promise<void>((resolve, reject) => pending.push({ resolve, reject }));
  const breaker = opts.breaker ?? new CircuitBreaker(0, 0);
  const b = new MicroBatcher(send, { maxMessages: 3, maxWaitMs: 10, maxBytes: 1000, maxQueue: 5, maxInFlight: 1 }, { onFlush: (r) => m.onFlush(r), onReject: (r) => m.onReject(r) }, breaker);
  m.bind({ batcher: () => b.stats(), breaker: () => breaker.state(), connected: () => true, healthy: () => true });
  return { registry, m, b, pending };
}

describe('KafkaMetrics (S2-F6)', () => {
  beforeEach(() => vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] }));
  afterEach(() => vi.useRealTimers());

  it('3 flushes (size, time, drain): flush counters, batch-size histogram, acked count, queue gauges', async () => {
    const { registry, m, b, pending } = setup();
    [1, 2, 3].forEach((i) => void b.enqueue(item(i))); // size
    pending[0].resolve();
    await flush();
    void b.enqueue(item(4));
    vi.advanceTimersByTime(10); // time
    void b.enqueue(item(5));
    void b.enqueue(item(6));
    const text1 = await registry.metrics();
    expect(text1).toContain('ep_batcher_queue_depth 2'); // 5 and 6 waiting behind the in-flight batch
    expect(text1).toContain('ep_batcher_in_flight_batches 1');
    pending[1].resolve();
    await flush();
    const d = b.drain(); // drain: 5,6 (they may already have gone by time; force the drain path)
    pending[2]?.resolve();
    await d;

    const snap = await m.snapshot();
    const reasons = snap.batcher.flushes_by_reason;
    expect(reasons.size).toBe(1);
    expect(reasons.time).toBeGreaterThanOrEqual(1);
    expect((reasons.time ?? 0) + (reasons.drain ?? 0)).toBe(2);
    expect(snap.producer.acked).toBe(6);
    const text = await registry.metrics();
    expect(text).toContain('ep_batcher_flush_total{reason="size"} 1');
    expect(text).toContain('ep_batcher_batch_size_messages_count 3');
    expect(text).toContain('ep_kafka_messages_acked_total 6');
    expect(text).toContain('ep_batcher_queue_wait_seconds_count 6'); // one observation per message
    expect(text).toContain('ep_batcher_queue_depth 0');
  });

  it('a failed batch: failed count and error type, never acked', async () => {
    const { registry, m, b, pending } = setup();
    const ps = [1, 2, 3].map((i) => b.enqueue(item(i)).catch(() => undefined));
    pending[0].reject(Object.assign(new Error('Broker: Not enough in-sync replicas'), { code: 19, type: 'ERR_NOT_ENOUGH_REPLICAS' }));
    await Promise.all(ps);
    const snap = await m.snapshot();
    expect(snap.producer).toMatchObject({ acked: 0, failed: 3, errors_by_type: { ERR_NOT_ENOUGH_REPLICAS: 1 } });
    expect(await registry.metrics()).toContain('ep_kafka_send_errors_total{type="ERR_NOT_ENOUGH_REPLICAS"} 1');
  });

  it('overload and breaker rejections are counted by reason (real batcher paths)', async () => {
    const registry = new Registry();
    const m = new KafkaMetrics(registry);
    const breaker = new CircuitBreaker(1, 60_000);
    const hooks = { onFlush: (r: any) => m.onFlush(r), onReject: (r: any) => m.onReject(r) };
    const never = () => new Promise<void>(() => undefined);
    // Batch never full and the timer never fires: everything stays queued, so the bound is reached.
    const full = new MicroBatcher(never, { maxMessages: 100, maxWaitMs: 60_000, maxBytes: 100_000, maxQueue: 5, maxInFlight: 1 }, hooks, breaker);
    for (let i = 0; i < 5; i++) void full.enqueue(item(i)).catch(() => undefined);
    await expect(full.enqueue(item(5))).rejects.toThrow(/queue is full/);
    breaker.forceOpen(); // e.g. the health check saw Kafka down
    const other = new MicroBatcher(never, { maxMessages: 100, maxWaitMs: 60_000, maxBytes: 100_000, maxQueue: 5, maxInFlight: 1 }, hooks, breaker);
    m.bind({ batcher: () => other.stats(), breaker: () => breaker.state(), connected: () => true, healthy: () => false });
    await expect(other.enqueue(item(6))).rejects.toThrow(/circuit breaker open/);
    const text = await registry.metrics();
    expect(text).toContain('ep_batcher_rejected_total{reason="overloaded"} 1');
    expect(text).toContain('ep_batcher_rejected_total{reason="breaker_open"} 1');
    expect(text).toContain('ep_kafka_breaker_state 1');
    expect(text).toContain('ep_kafka_health_ok 0');
    expect((await m.snapshot()).batcher.rejected).toEqual({ overloaded: 1, breaker_open: 1 });
  });

  it('503s by reason and librdkafka client statistics', async () => {
    const { registry, m } = setup();
    m.count503('kafka_unavailable');
    m.count503('shutting_down');
    m.onClientStats(JSON.stringify({ tx_bytes: 1000 }));
    m.onClientStats(JSON.stringify({ tx_bytes: 1500 }));
    m.onClientStats(JSON.stringify({ tx_bytes: 1500, msg_cnt: 42, msg_size: 4200, brokers: {
      'localhost:9092/1': { nodeid: 1, rtt: { p50: 1500, p99: 9000 }, waitresp_cnt: 2 },
      'GroupCoordinator': { nodeid: -1 },
    } }));
    m.onClientStats('not json'); // ignored, keeps the last good values
    const text = await registry.metrics();
    expect(text).toContain('ep_http_503_total{reason="kafka_unavailable"} 1');
    expect(text).toContain('ep_http_503_total{reason="shutting_down"} 1');
    expect(text).toContain('ep_kafka_client_tx_bytes_total 1500');
    expect(text).toContain('ep_kafka_client_queue_messages 42');
    expect(text).toContain('ep_kafka_broker_rtt_seconds{broker="1",quantile="0.99"} 0.009');
    expect(text).toContain('ep_kafka_broker_in_flight_requests{broker="1"} 2');
    expect(text).not.toContain('broker="-1"');
    expect((await m.snapshot()).http_503_by_reason).toEqual({ kafka_unavailable: 1, shutting_down: 1 });
  });
});
