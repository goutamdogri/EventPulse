import { loadConfig } from './config.js';

const base = { DATABASE_URL: 'postgres://u:p@localhost:5434/x' };

describe('outbox-relay config', () => {
  it('defaults: single active relay, 100 rows per cycle, 200 ms poll, LISTEN on, 7-day retention', () => {
    const c = loadConfig(base);
    expect(c.enabled).toBe(true);
    expect(c.relay).toMatchObject({ mode: 'single', batchSize: 100, pollMs: 200, listen: true, topics: [], retentionMs: 7 * 24 * 3600 * 1000 });
    expect(c.port).toBe(8021);
  });

  it('reads every knob', () => {
    const c = loadConfig({ ...base, RELAY_MODE: 'multi', RELAY_BATCH_SIZE: '10', RELAY_TOPICS: 'a, b', RELAY_LISTEN: 'false', OUTBOX_RELAY_ENABLED: 'false', RELAY_LOCK_KEY: '7' });
    expect(c.relay).toMatchObject({ mode: 'multi', batchSize: 10, topics: ['a', 'b'], listen: false, lockKey: 7 });
    expect(c.enabled).toBe(false);
  });

  it('rejects a missing database, a bad mode, a backoff max below the initial delay, faults without DEBUG_FAULTS', () => {
    expect(() => loadConfig({})).toThrow(/DATABASE_URL/);
    expect(() => loadConfig({ ...base, RELAY_MODE: 'many' })).toThrow();
    expect(() => loadConfig({ ...base, RELAY_BACKOFF_INITIAL_MS: '500', RELAY_BACKOFF_MAX_MS: '100' })).toThrow(/BACKOFF/);
    expect(() => loadConfig({ ...base, FAULTS: 'relay:before-publish' })).toThrow(/DEBUG_FAULTS/);
    expect(loadConfig({ ...base, DEBUG_FAULTS: 'true', FAULTS: 'relay:after-publish-before-mark@b2' }).faults.crashes).toHaveLength(1);
  });
});
