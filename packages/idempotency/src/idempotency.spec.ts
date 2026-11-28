import fc from 'fast-check';
import {
  Idempotency,
  IdempotencyUnavailableError,
  InvalidIdempotencyKeyError,
  decodeValue,
  encodeDone,
  encodeInflight,
  validateIdempotencyKey,
} from './idempotency.js';
import type { Begin, Guard, IdempotencyOptions, IdempotencyResult } from './idempotency.js';
import { MemoryStore } from './store.js';
import type { IdempotencyStore } from './store.js';

const H1 = 'a'.repeat(64);
const H2 = 'b'.repeat(64);

function setup(o: Partial<IdempotencyOptions> = {}, store?: IdempotencyStore, guard?: Guard) {
  let t = 1_000_000;
  const clock = { now: () => t, advance: (ms: number) => void (t += ms) };
  const mem = new MemoryStore(clock.now);
  const results: IdempotencyResult[] = [];
  const completeFailed: string[] = [];
  let owner = 0;
  const idem = new Idempotency(
    store ?? mem,
    { enabled: true, ttlS: 3600, inflightTtlS: 30, policy: 'open', now: clock.now, newOwner: () => `o${++owner}`, ...o },
    guard,
    { result: (r) => results.push(r), completeFailed: (r) => completeFailed.push(r) },
  );
  return { idem, mem, clock, results, completeFailed };
}

const failing = (): IdempotencyStore => ({
  acquire: async () => {
    throw new Error('ECONNREFUSED');
  },
  replace: async () => {
    throw new Error('ECONNREFUSED');
  },
  remove: async () => {
    throw new Error('ECONNREFUSED');
  },
});

describe('key validation and value encoding', () => {
  it('accepts 1-255 printable ASCII without spaces', () => {
    expect(validateIdempotencyKey('order-123_ABC.x:y')).toBe('order-123_ABC.x:y');
    expect(validateIdempotencyKey('x'.repeat(255))).toHaveLength(255);
    for (const bad of ['', ' ', 'a b', 'x'.repeat(256), 'é', 'tab\t', 'nl\n']) {
      expect(() => validateIdempotencyKey(bad), JSON.stringify(bad)).toThrow(InvalidIdempotencyKeyError);
    }
  });

  it('round-trips both states and rejects foreign values', () => {
    expect(decodeValue(encodeInflight(H1, 'own', 42))).toEqual({ state: 'inflight', hash: H1, owner: 'own', startedMs: 42 });
    expect(decodeValue(encodeDone(H1, 'e-1'))).toEqual({ state: 'done', hash: H1, eventId: 'e-1' });
    for (const bad of ['', 'done', 'done:xyz:e', `inflight:${H1}:o`, `other:${H1}:e`, `done:${H1}:`]) expect(decodeValue(bad)).toBeNull();
  });
});

describe('semantics table (S3-F1)', () => {
  it('first time key seen: new (then 202)', async () => {
    const s = setup();
    const b = await s.idem.begin('t', 'k', H1);
    expect(b.kind).toBe('new');
    expect(s.mem.peek('idem:t:k')).toBe(encodeInflight(H1, 'o1', 1_000_000));
    expect(s.results).toEqual(['new']);
  });

  it('same key + same payload while the original is in progress: in_progress (then 409)', async () => {
    const s = setup();
    await s.idem.begin('t', 'k', H1);
    expect((await s.idem.begin('t', 'k', H1)).kind).toBe('in_progress');
  });

  it('same key + same payload after the original completed: replay with the ORIGINAL event_id, nothing new', async () => {
    const s = setup();
    const b = (await s.idem.begin('t', 'k', H1)) as Extract<Begin, { kind: 'new' }>;
    expect(await s.idem.complete(b.ticket, 'evt-original')).toBe(true);
    expect(await s.idem.begin('t', 'k', H1)).toEqual({ kind: 'replay', eventId: 'evt-original' });
    expect(s.mem.peek('idem:t:k')).toBe(encodeDone(H1, 'evt-original'));
  });

  it('same key + different payload: mismatch (then 422), in progress or done', async () => {
    const s = setup();
    const b = (await s.idem.begin('t', 'k', H1)) as Extract<Begin, { kind: 'new' }>;
    expect((await s.idem.begin('t', 'k', H2)).kind).toBe('mismatch');
    await s.idem.complete(b.ticket, 'e');
    expect((await s.idem.begin('t', 'k', H2)).kind).toBe('mismatch');
    expect(s.results).toEqual(['new', 'payload_mismatch', 'payload_mismatch']);
  });

  it('key older than the window: treated as new', async () => {
    const s = setup({ ttlS: 60 });
    const b = (await s.idem.begin('t', 'k', H1)) as Extract<Begin, { kind: 'new' }>;
    await s.idem.complete(b.ticket, 'e');
    s.clock.advance(60_001);
    expect((await s.idem.begin('t', 'k', H1)).kind).toBe('new');
  });

  it('the key is scoped by tenant: the same key under two tenants is two keys', async () => {
    const s = setup();
    expect((await s.idem.begin('t1', 'k', H1)).kind).toBe('new');
    expect((await s.idem.begin('t2', 'k', H1)).kind).toBe('new');
  });

  it('hash may be given as a Buffer (sha256 digest)', async () => {
    const s = setup();
    expect((await s.idem.begin('t', 'k', Buffer.alloc(32, 0xaa))).kind).toBe('new');
    expect((await s.idem.begin('t', 'k', H1)).kind).toBe('in_progress'); // 32 x 0xaa in hex is 'aa..aa' === H1
  });
});

describe('in-progress lifetime and ownership', () => {
  it('an abandoned inflight (request died) frees itself after the SHORT inflight TTL', async () => {
    const s = setup({ inflightTtlS: 30, ttlS: 3600 });
    await s.idem.begin('t', 'k', H1); // never completed
    s.clock.advance(29_000);
    expect((await s.idem.begin('t', 'k', H1)).kind).toBe('in_progress');
    s.clock.advance(1_001);
    expect((await s.idem.begin('t', 'k', H1)).kind).toBe('new');
  });

  it('release deletes our inflight, so the retry is new', async () => {
    const s = setup();
    const b = (await s.idem.begin('t', 'k', H1)) as Extract<Begin, { kind: 'new' }>;
    await s.idem.release(b.ticket);
    expect((await s.idem.begin('t', 'k', H1)).kind).toBe('new');
  });

  it('a slow request whose inflight expired and was re-acquired cannot complete or release the new owner', async () => {
    const s = setup({ inflightTtlS: 30 });
    const slow = (await s.idem.begin('t', 'k', H1)) as Extract<Begin, { kind: 'new' }>;
    s.clock.advance(31_000);
    const fresh = (await s.idem.begin('t', 'k', H1)) as Extract<Begin, { kind: 'new' }>;
    expect(await s.idem.complete(slow.ticket, 'e-slow')).toBe(false);
    await s.idem.release(slow.ticket);
    expect(s.mem.peek('idem:t:k')).toBe(fresh.ticket.inflight); // untouched
    expect(s.completeFailed).toEqual(['lost_ownership']);
    expect(await s.idem.complete(fresh.ticket, 'e-fresh')).toBe(true);
  });

  it('completing after the store was flushed reports lost ownership, does not throw', async () => {
    const s = setup();
    const b = (await s.idem.begin('t', 'k', H1)) as Extract<Begin, { kind: 'new' }>;
    s.mem.flush();
    expect(await s.idem.complete(b.ticket, 'e')).toBe(false);
  });
});

describe('store failures and policy', () => {
  it('policy open: a failing store gives bypass (produce without dedup) and is counted', async () => {
    const s = setup({ policy: 'open' }, failing());
    expect(await s.idem.begin('t', 'k', H1)).toEqual({ kind: 'bypass', reason: 'fail_open' });
    expect(s.results).toEqual(['fail_open']);
  });

  it('policy closed: a failing store throws IdempotencyUnavailableError (then 503)', async () => {
    const s = setup({ policy: 'closed' }, failing());
    await expect(s.idem.begin('t', 'k', H1)).rejects.toBeInstanceOf(IdempotencyUnavailableError);
    expect(s.results).toEqual(['fail_closed']);
  });

  it('complete and release never throw when the store fails (the event is already durable)', async () => {
    const s = setup({}, failing());
    const ticket = { key: 'idem:t:k', hash: H1, inflight: encodeInflight(H1, 'o', 1) };
    await expect(s.idem.complete(ticket, 'e')).resolves.toBe(false);
    await expect(s.idem.release(ticket)).resolves.toBeUndefined();
    expect(s.completeFailed).toEqual(['error']);
  });

  it('a corrupt value is handled like a store failure, never as a match', async () => {
    const mem = new MemoryStore();
    await mem.acquire('idem:t:k', 'garbage', 60);
    expect(await setup({ policy: 'open' }, mem).idem.begin('t', 'k', H1)).toEqual({ kind: 'bypass', reason: 'fail_open' });
    await expect(setup({ policy: 'closed' }, mem).idem.begin('t', 'k', H1)).rejects.toBeInstanceOf(IdempotencyUnavailableError);
  });

  it('an open guard (breaker) skips the store call entirely', async () => {
    const calls: string[] = [];
    const store: IdempotencyStore = { ...new MemoryStore(), acquire: async () => (calls.push('acquire'), { acquired: true }) } as any;
    const guard: Guard = { tryAcquire: () => false, onSuccess: () => undefined, onFailure: () => undefined };
    expect(await setup({ policy: 'open' }, store, guard).idem.begin('t', 'k', H1)).toEqual({ kind: 'bypass', reason: 'fail_open' });
    expect(calls).toEqual([]);
  });

  it('the guard is told about successes and failures', async () => {
    const events: string[] = [];
    const guard: Guard = { tryAcquire: () => true, onSuccess: () => events.push('ok'), onFailure: () => events.push('fail') };
    await setup({}, undefined, guard).idem.begin('t', 'k', H1);
    await setup({}, failing(), guard).idem.begin('t', 'k', H1);
    expect(events).toEqual(['ok', 'fail']);
  });

  it('disabled: always bypass, store untouched', async () => {
    const s = setup({ enabled: false });
    expect(await s.idem.begin('t', 'k', H1)).toEqual({ kind: 'bypass', reason: 'disabled' });
    expect(s.mem.size).toBe(0);
  });

  it('rejects inflight TTL longer than the window', () => {
    expect(() => setup({ ttlS: 10, inflightTtlS: 30 })).toThrow();
  });
});

// --- Property: the implementation against a tiny reference model of the semantics table ---------------------
type Step =
  | { op: 'request'; key: string; hash: string; outcome: 'ok' | 'produce_fails' | 'crash_before_produce' | 'crash_after_produce' }
  | { op: 'advance'; ms: number };

const stepArb: fc.Arbitrary<Step> = fc.oneof(
  { weight: 5, arbitrary: fc.record({
    op: fc.constant('request' as const),
    key: fc.constantFrom('k1', 'k2', 'k3'),
    hash: fc.constantFrom(H1, H2),
    outcome: fc.constantFrom('ok', 'ok', 'ok', 'produce_fails', 'crash_before_produce', 'crash_after_produce'),
  }) },
  { weight: 1, arbitrary: fc.record({ op: fc.constant('advance' as const), ms: fc.constantFrom(1_000, 10_000, 31_000, 120_000) }) },
);

describe('property: matches the reference model for any request sequence', () => {
  it('same responses and same number of produce calls', async () => {
    await fc.assert(
      fc.asyncProperty(fc.array(stepArb, { maxLength: 60 }), async (steps) => {
        const TTL = 100;
        const INFLIGHT = 30;
        const s = setup({ ttlS: TTL, inflightTtlS: INFLIGHT });
        // Reference model: key -> state with absolute expiry.
        const model = new Map<string, { state: 'inflight' | 'done'; hash: string; eventId?: string; exp: number }>();
        let now = 1_000_000;
        let produced = 0;
        let modelProduced = 0;
        let n = 0;

        for (const st of steps) {
          if (st.op === 'advance') {
            s.clock.advance(st.ms);
            now += st.ms;
            continue;
          }
          const eventId = `e${++n}`;
          // --- model
          let m = model.get(st.key);
          if (m && m.exp <= now) {
            model.delete(st.key);
            m = undefined;
          }
          let expected: Begin['kind'];
          if (!m) expected = 'new';
          else if (m.hash !== st.hash) expected = 'mismatch';
          else expected = m.state === 'inflight' ? 'in_progress' : 'replay';
          if (expected === 'new') {
            if (st.outcome !== 'crash_before_produce' && st.outcome !== 'produce_fails') modelProduced++;
            if (st.outcome === 'ok') model.set(st.key, { state: 'done', hash: st.hash, eventId, exp: now + TTL * 1000 });
            else if (st.outcome === 'produce_fails') model.delete(st.key);
            else model.set(st.key, { state: 'inflight', hash: st.hash, exp: now + INFLIGHT * 1000 });
          }
          // --- implementation
          const b = await s.idem.begin('t', st.key, st.hash);
          if (b.kind !== expected) return false;
          if (b.kind === 'replay' && b.eventId !== m?.eventId) return false;
          if (b.kind === 'new') {
            if (st.outcome === 'produce_fails') await s.idem.release(b.ticket);
            else if (st.outcome !== 'crash_before_produce') {
              produced++;
              if (st.outcome === 'ok') await s.idem.complete(b.ticket, eventId);
            }
          }
        }
        return produced === modelProduced;
      }),
      { numRuns: 2000 },
    );
  });

  it('without crashes or expiry, a key is produced at most once however often it is sent', async () => {
    await fc.assert(
      fc.asyncProperty(fc.array(fc.constantFrom('k1', 'k2', 'k3', 'k4'), { maxLength: 100 }), async (keys) => {
        const s = setup();
        const producedPerKey = new Map<string, number>();
        for (const k of keys) {
          const b = await s.idem.begin('t', k, H1);
          if (b.kind === 'new') {
            producedPerKey.set(k, (producedPerKey.get(k) ?? 0) + 1);
            await s.idem.complete(b.ticket, `e-${k}`);
          } else if (b.kind !== 'replay') return false;
        }
        return [...producedPerKey.values()].every((v) => v === 1);
      }),
      { numRuns: 1000 },
    );
  });
});
