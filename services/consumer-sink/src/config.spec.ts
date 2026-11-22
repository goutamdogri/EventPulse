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

  it('requires DATABASE_URL', () => expect(() => loadConfig({})).toThrow(/DATABASE_URL/));

  it('refuses more concurrent partitions than pool connections', () =>
    expect(() => loadConfig({ ...base, CONSUMER_PARTITIONS_CONCURRENTLY: '6', PG_POOL_MAX: '4' })).toThrow(/PG_POOL_MAX/));

  it('refuses heartbeat >= session timeout', () =>
    expect(() => loadConfig({ ...base, CONSUMER_HEARTBEAT_INTERVAL_MS: '50000' })).toThrow(/HEARTBEAT/));
});
