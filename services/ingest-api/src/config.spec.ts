import { loadConfig } from './config.js';

const base = { API_KEYS: 'k1:t1' };

describe('loadConfig', () => {
  it('has durable producer defaults and no database settings', () => {
    const c = loadConfig(base);
    expect(c.producer).toMatchObject({ acks: -1, idempotent: true, compression: 'none', lingerMs: 0 });
    expect(c.breaker).toEqual({ failures: 3, cooldownMs: 5000 });
    expect(c.batcher).toEqual({ maxMessages: 500, maxWaitMs: 5, maxBytes: 900_000, maxQueue: 10_000, maxQueueBytes: 64 * 1024 * 1024, maxInFlight: 1, sendTimeoutMs: 5000 });
    expect(c.keyMode).toBe('tenant');
    expect(c.topic).toBe('events.raw');
    expect(loadConfig({ ...base, KAFKA_TOPIC: 't1' }).topic).toBe('t1');
    expect(c).not.toHaveProperty('databaseUrl');
  });

  it('parses the producer and batcher knobs', () => {
    const c = loadConfig({
      ...base,
      KAFKA_ACKS: '1',
      KAFKA_IDEMPOTENT: 'false',
      KAFKA_COMPRESSION: 'lz4',
      BATCH_MAX_MESSAGES: '50',
      BATCH_MAX_WAIT_MS: '20',
      KAFKA_KEY_MODE: 'event',
    });
    expect(c.producer).toMatchObject({ acks: 1, idempotent: false, compression: 'lz4' });
    expect(c.batcher).toMatchObject({ maxMessages: 50, maxWaitMs: 20 });
    expect(c.keyMode).toBe('event');
  });

  it('refuses idempotence without acks=all', () =>
    expect(() => loadConfig({ ...base, KAFKA_ACKS: '1' })).toThrow(/KAFKA_IDEMPOTENT/));

  it('refuses a batch byte cap above the broker limit', () =>
    expect(() => loadConfig({ ...base, BATCH_MAX_BYTES: '2000000' })).toThrow(/BATCH_MAX_BYTES/));

  it('refuses bad values', () => {
    expect(() => loadConfig({ ...base, KAFKA_ACKS: '2' })).toThrow();
    expect(() => loadConfig({ ...base, KAFKA_COMPRESSION: 'brotli' })).toThrow();
    expect(() => loadConfig({ ...base, BATCH_MAX_QUEUE: '-1' })).toThrow();
    expect(() => loadConfig({})).toThrow(/API_KEYS/);
  });

  it('BATCH_MAX_QUEUE=0 is unbounded mode: no byte bound, breaker off, loud warning', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const c = loadConfig({ ...base, BATCH_MAX_QUEUE: '0' });
    expect(c.batcher.maxQueue).toBe(0);
    expect(c.batcher.maxQueueBytes).toBe(0);
    expect(c.breaker.failures).toBe(0);
    expect(c.batcher.sendTimeoutMs).toBe(0); // the send timeout also expires queued messages: off too (K10)
    expect(loadConfig({ ...base, BATCH_MAX_QUEUE: '0', KAFKA_SEND_TIMEOUT_MS: '5000' }).batcher.sendTimeoutMs).toBe(0);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('no send timeout'));
    warn.mockRestore();
  });

  it('health check defaults follow acks', () => {
    expect(loadConfig(base).health).toEqual({ intervalMs: 2000, timeoutMs: 2000, minIsr: 2 });
    expect(loadConfig({ ...base, KAFKA_ACKS: '1', KAFKA_IDEMPOTENT: 'false' }).health.minIsr).toBe(1);
    expect(loadConfig({ ...base, KAFKA_HEALTH_INTERVAL_MS: '0' }).health.intervalMs).toBe(0);
  });
});
