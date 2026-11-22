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

  it('topic and processing delay overrides', () => {
    expect(loadConfig(base).consumer.topic).toBe('events.raw');
    const c = loadConfig({ ...base, CONSUMER_TOPIC: 't1', SINK_PROCESSING_DELAY_MS: '200' });
    expect(c.consumer.topic).toBe('t1');
    expect(c.sink.processingDelayMs).toBe(200);
  });

  it('requires DATABASE_URL', () => expect(() => loadConfig({})).toThrow(/DATABASE_URL/));

  it('refuses more concurrent partitions than pool connections', () =>
    expect(() => loadConfig({ ...base, CONSUMER_PARTITIONS_CONCURRENTLY: '6', PG_POOL_MAX: '4' })).toThrow(/PG_POOL_MAX/));

  it('refuses heartbeat >= session timeout', () =>
    expect(() => loadConfig({ ...base, CONSUMER_HEARTBEAT_INTERVAL_MS: '50000' })).toThrow(/HEARTBEAT/));
});
