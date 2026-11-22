import { CircuitBreaker } from './breaker.js';
import { KafkaHealthChecker, evaluateMetadata } from './kafka-health.js';
import type { HealthAdmin } from './kafka-health.js';

const parts = (...p: [number, number][]) => [{ partitions: p.map(([leader, isr]) => ({ leader, isr: Array.from({ length: isr }) })) }];

describe('evaluateMetadata', () => {
  it('healthy when every partition has a leader and enough ISR', () =>
    expect(evaluateMetadata(parts([1, 3], [2, 2], [3, 3]), 2)).toEqual({ ok: true, reason: 'ok' }));
  it('unhealthy without metadata', () => expect(evaluateMetadata([], 2).ok).toBe(false));
  it('unhealthy when a partition has no leader', () => expect(evaluateMetadata(parts([1, 3], [-1, 0]), 2).reason).toMatch(/without a leader/));
  it('unhealthy below min ISR (2 of 3 brokers down with acks=all)', () =>
    expect(evaluateMetadata(parts([1, 1], [1, 1]), 2).reason).toMatch(/below 2 in-sync/));
  it('acks 0/1 only need a leader (minIsr 1)', () => expect(evaluateMetadata(parts([1, 1]), 1).ok).toBe(true));
});

describe('KafkaHealthChecker', () => {
  function setup(answers: (() => Promise<any>)[]) {
    let t = 0;
    const now = () => t;
    const breaker = new CircuitBreaker(3, 5000, now);
    let i = 0;
    const admin: HealthAdmin = { fetchTopicMetadata: () => answers[Math.min(i++, answers.length - 1)]() };
    const c = new KafkaHealthChecker(async () => admin, breaker, { topic: 't', minIsr: 2, timeoutMs: 1000, staleAfterMs: 7000 }, now);
    return { c, breaker, advance: (ms: number) => (t += ms) };
  }
  const ok = () => Promise.resolve(parts([1, 3], [2, 3]));
  const down = () => Promise.reject(Object.assign(new Error('all brokers down'), { code: -187 }));

  it('Kafka down with NO traffic: check fails -> unhealthy and breaker forced open (fast 503s)', async () => {
    const s = setup([down]);
    await s.c.check();
    expect(s.c.healthy()).toBe(false);
    expect(s.breaker.state()).toBe('open');
  });

  it('Kafka back with NO traffic: the next passing check closes the breaker (no user probe needed)', async () => {
    const s = setup([down, ok]);
    await s.c.check();
    expect(s.breaker.state()).toBe('open');
    await s.c.check();
    expect(s.c.healthy()).toBe(true);
    expect(s.breaker.state()).toBe('closed');
  });

  it('breaker opened by real send failures while metadata looks fine: closes only after its cooldown (no flapping)', async () => {
    const s = setup([ok]);
    await s.c.check(); // healthy
    s.breaker.onFailure();
    s.breaker.onFailure();
    s.breaker.onFailure(); // opened by sends
    await s.c.check();
    expect(s.breaker.state()).toBe('open'); // still cooling down
    s.advance(5000); // cooldown over -> half-open
    await s.c.check(); // the check is the probe
    expect(s.breaker.state()).toBe('closed');
  });

  it('a passing result goes stale (no recent check -> not healthy)', async () => {
    const s = setup([ok]);
    await s.c.check();
    expect(s.c.healthy()).toBe(true);
    s.advance(7001);
    expect(s.c.healthy()).toBe(false);
  });

  it('a hanging metadata request counts as a failure after the timeout', async () => {
    vi.useFakeTimers();
    const s = setup([() => new Promise(() => undefined)]);
    const p = s.c.check();
    await vi.advanceTimersByTimeAsync(1000);
    const r = await p;
    vi.useRealTimers();
    expect(r.ok).toBe(false);
    expect(r.reason).toMatch(/timed out/);
  });

  it('never throws, even if the admin cannot be created', async () => {
    const breaker = new CircuitBreaker(3, 5000);
    const c = new KafkaHealthChecker(() => Promise.reject(new Error('connect failed')), breaker, { topic: 't', minIsr: 2, timeoutMs: 100, staleAfterMs: 1000 });
    await expect(c.check()).resolves.toMatchObject({ ok: false });
  });
});
