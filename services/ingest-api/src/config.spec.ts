import { loadConfig } from './config.js';

const base = { API_KEYS: 'k1:t1' };

describe('loadConfig', () => {
  it('has durable producer defaults and no database settings', () => {
    const c = loadConfig(base);
    expect(c.producer).toMatchObject({ acks: -1, idempotent: true, compression: 'none', lingerMs: 0 });
    expect(c.batcher).toEqual({ maxMessages: 500, maxWaitMs: 5, maxBytes: 900_000, maxQueue: 10_000, maxInFlight: 1 });
    expect(c.keyMode).toBe('tenant');
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
    expect(() => loadConfig({ ...base, BATCH_MAX_QUEUE: '0' })).toThrow();
    expect(() => loadConfig({})).toThrow(/API_KEYS/);
  });
});
