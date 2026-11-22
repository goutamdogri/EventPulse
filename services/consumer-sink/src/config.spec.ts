import { loadConfig } from './config.js';

const base = { DATABASE_URL: 'postgres://x' };

describe('loadConfig', () => {
  it('defaults: group sink-postgres, earliest, port 8001, chunks of 1000', () => {
    const c = loadConfig(base);
    expect(c.consumer).toMatchObject({ groupId: 'sink-postgres', fromBeginning: true, partitionsConcurrently: 1, assignor: 'roundRobin' });
    expect(c.port).toBe(8001);
    expect(c.sink.maxRows).toBe(1000);
    expect(c.kafka.clientId).toBe('consumer-sink');
  });

  it('parses overrides', () => {
    const c = loadConfig({ ...base, CONSUMER_GROUP_ID: 'g', CONSUMER_FROM_BEGINNING: 'false', CONSUMER_ASSIGNOR: 'cooperativeSticky', METRICS_PORT: '8002' });
    expect(c.consumer).toMatchObject({ groupId: 'g', fromBeginning: false, assignor: 'cooperativeSticky' });
    expect(c.port).toBe(8002);
  });

  it('crash points are off by default and need DEBUG_FAULTS', () => {
    expect(loadConfig(base).faults).toEqual({ crashAt: 'none', crashAfterBatches: 1 });
    expect(() => loadConfig({ ...base, CRASH_AT: 'after_db_commit' })).toThrow(/DEBUG_FAULTS/);
    expect(loadConfig({ ...base, CRASH_AT: 'after_db_commit', CRASH_AFTER_BATCHES: '3', DEBUG_FAULTS: 'true' }).faults)
      .toEqual({ crashAt: 'after_db_commit', crashAfterBatches: 3 });
    expect(() => loadConfig({ ...base, CRASH_AT: 'somewhere', DEBUG_FAULTS: 'true' })).toThrow();
  });

  it('SINK_DEDUP=false is debug-only (K5)', () => {
    expect(loadConfig(base).sink).toMatchObject({ dedup: true, dedupTable: 'events_nodedup' });
    expect(() => loadConfig({ ...base, SINK_DEDUP: 'false' })).toThrow(/DEBUG_FAULTS/);
    expect(loadConfig({ ...base, SINK_DEDUP: 'false', DEBUG_FAULTS: 'true' }).sink.dedup).toBe(false);
  });

  it('topic and processing delay overrides', () => {
    expect(loadConfig(base).consumer.topic).toBe('events.raw');
    const c = loadConfig({ ...base, CONSUMER_TOPIC: 't1', SINK_PROCESSING_DELAY_MS: '200' });
    expect(c.consumer.topic).toBe('t1');
    expect(c.sink.processingDelayMs).toBe(200);
  });

  it('poison-handling defaults and validation', () => {
    expect(loadConfig(base).retry).toEqual({
      dlqEnabled: true, dlqTopic: 'events.dlq', maxRetries: 3, baseMs: 100, maxMs: 5000, transientWindowMs: 20000, pauseMs: 5000,
    });
    expect(loadConfig({ ...base, SINK_DLQ_ENABLED: 'false', SINK_DLQ_TOPIC: 'd' }).retry).toMatchObject({ dlqEnabled: false, dlqTopic: 'd' });
    expect(() => loadConfig({ ...base, SINK_TRANSIENT_RETRY_WINDOW_MS: '58000' })).toThrow(/REBALANCE_TIMEOUT/);
    expect(() => loadConfig({ ...base, SINK_RETRY_BASE_MS: '9000' })).toThrow(/SINK_RETRY_BASE_MS/);
  });

  it('requires DATABASE_URL', () => expect(() => loadConfig({})).toThrow(/DATABASE_URL/));

  it('refuses more concurrent partitions than pool connections', () =>
    expect(() => loadConfig({ ...base, CONSUMER_PARTITIONS_CONCURRENTLY: '6', PG_POOL_MAX: '4' })).toThrow(/PG_POOL_MAX/));

  it('refuses heartbeat >= session timeout', () =>
    expect(() => loadConfig({ ...base, CONSUMER_HEARTBEAT_INTERVAL_MS: '50000' })).toThrow(/HEARTBEAT/));
});
