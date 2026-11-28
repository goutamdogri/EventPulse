import fc from 'fast-check';
import { AvroDecodeError, IncompatibleWriterError, NotAvroError, RegistryUnavailableError, UnknownSchemaIdError } from '@eventpulse/schema';
import { backoffCap, backoffMs, jittered } from './backoff.js';
import { ACTION, Classifier, classifyError, classifyParseCode, errorCode, isBlocking, isRetriable, parseClassifierOverride } from './classifier.js';
import { TokenBucket } from './limiter.js';
import { RETRY_HEADERS, originalHeaders, parseRetryHeaders, retryHeaders, route, validateTierPolicy } from './router.js';

const err = (code?: string, message = 'x') => Object.assign(new Error(message), code ? { code } : {});

describe('classifier: one table for the whole system', () => {
  it.each([
    // a dependency is down: block (pause), never a retry topic, never the DLQ
    ['ECONNREFUSED', 'TRANSIENT_SYSTEMIC'],
    ['ECONNRESET', 'TRANSIENT_SYSTEMIC'],
    ['ETIMEDOUT', 'TRANSIENT_SYSTEMIC'],
    ['08006', 'TRANSIENT_SYSTEMIC'], // connection failure
    ['53300', 'TRANSIENT_SYSTEMIC'], // too many connections
    ['57P01', 'TRANSIENT_SYSTEMIC'], // admin shutdown
    ['57P03', 'TRANSIENT_SYSTEMIC'], // cannot connect now
    // this record ran into something transient: retry tiers
    ['40001', 'TRANSIENT_RECORD'], // serialization failure
    ['40P01', 'TRANSIENT_RECORD'], // deadlock
    ['57014', 'TRANSIENT_RECORD'], // statement timeout
    // this row is bad: isolate, then DLQ (no retry: bad data stays bad)
    ['22001', 'PERMANENT_DB'],
    ['22P02', 'PERMANENT_DB'],
    ['22P05', 'PERMANENT_DB'], // \u0000 in jsonb
    ['23502', 'PERMANENT_DB'],
    ['23514', 'PERMANENT_DB'],
    // every row would fail the same way: block + a human
    ['42703', 'SYSTEMIC_CONFIG'],
    ['42P01', 'SYSTEMIC_CONFIG'],
    ['28P01', 'SYSTEMIC_CONFIG'],
    ['3D000', 'SYSTEMIC_CONFIG'],
    ['XX000', 'SYSTEMIC_CONFIG'],
    ['0A000', 'SYSTEMIC_CONFIG'],
  ])('%s -> %s', (code, cls) => expect(classifyError(err(code))).toBe(cls));

  it('pool / connection messages without a code are TRANSIENT_SYSTEMIC', () => {
    expect(classifyError(err(undefined, 'timeout exceeded when trying to connect'))).toBe('TRANSIENT_SYSTEMIC');
    expect(classifyError(err(undefined, 'Connection terminated unexpectedly'))).toBe('TRANSIENT_SYSTEMIC');
    expect(classifyError(err(undefined, 'the database system is starting up'))).toBe('TRANSIENT_SYSTEMIC');
  });

  it('schema errors: registry down blocks; unknown id / unresolvable writer / bad bytes are permanent', () => {
    expect(classifyError(new RegistryUnavailableError('down'))).toBe('TRANSIENT_SYSTEMIC');
    expect(classifyError(new UnknownSchemaIdError(9))).toBe('PERMANENT_SCHEMA');
    expect(classifyError(new IncompatibleWriterError(9, 'x'))).toBe('PERMANENT_SCHEMA');
    expect(classifyError(new NotAvroError('x'))).toBe('PERMANENT_PARSE');
    expect(classifyError(new AvroDecodeError('x'))).toBe('PERMANENT_PARSE');
    expect(classifyParseCode('unknown_schema_id')).toBe('PERMANENT_SCHEMA');
    expect(classifyParseCode('incompatible_schema')).toBe('PERMANENT_SCHEMA');
    expect(classifyParseCode('invalid_json')).toBe('PERMANENT_PARSE');
  });

  it('anything else is UNKNOWN (bounded retry tiers, then DLQ)', () => {
    expect(classifyError(new TypeError('cannot read properties of undefined'))).toBe('UNKNOWN');
    expect(classifyError('a string')).toBe('UNKNOWN');
  });

  it('actions', () => {
    expect(ACTION).toEqual({
      PERMANENT_PARSE: 'dlq',
      PERMANENT_SCHEMA: 'dlq',
      PERMANENT_DB: 'isolate_then_dlq',
      TRANSIENT_RECORD: 'retry_tier',
      TRANSIENT_SYSTEMIC: 'block',
      SYSTEMIC_CONFIG: 'block',
      UNKNOWN: 'retry_tier',
    });
    expect(isBlocking('TRANSIENT_SYSTEMIC') && isBlocking('SYSTEMIC_CONFIG') && !isBlocking('TRANSIENT_RECORD')).toBe(true);
    expect(isRetriable('TRANSIENT_RECORD') && isRetriable('UNKNOWN') && !isRetriable('PERMANENT_DB')).toBe(true);
  });

  it('CLASSIFIER_OVERRIDE forces a code into a class (C9: inject a misclassification)', () => {
    const c = new Classifier(parseClassifierOverride('23502=TRANSIENT_RECORD, 40001=PERMANENT_DB'));
    expect(c.classify(err('23502'))).toBe('TRANSIENT_RECORD');
    expect(c.classify(err('40001'))).toBe('PERMANENT_DB');
    expect(c.classify(err('22001'))).toBe('PERMANENT_DB');
    expect(() => parseClassifierOverride('23502=WRONG')).toThrow(/CLASSIFIER_OVERRIDE/);
  });

  it('errorCode keeps SQLSTATE / Node codes only', () => {
    expect(errorCode(err('22001'))).toBe('22001');
    expect(errorCode(err('some code with spaces'))).toBe('none');
    expect(errorCode(null)).toBe('none');
  });
});

describe('backoff with jitter', () => {
  it('the cap doubles per attempt and is capped', () => {
    expect([1, 2, 3, 4, 5, 6].map((a) => backoffCap(a, 100, 1000))).toEqual([100, 200, 400, 800, 1000, 1000]);
  });

  it('property: equal jitter in [cap/2, cap], full jitter in [0, cap], never above max', () => {
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 40 }), fc.integer({ min: 1, max: 10_000 }), fc.integer({ min: 1, max: 600_000 }), fc.double({ min: 0, max: 1, maxExcluded: true, noNaN: true }), (attempt, base, max, r) => {
        const cap = backoffCap(attempt, base, max);
        const eq = backoffMs(attempt, base, max, () => r, 'equal');
        const full = backoffMs(attempt, base, max, () => r, 'full');
        return eq >= Math.floor(cap / 2) && eq <= cap && full >= 0 && full <= cap && cap <= Math.max(max, base) && cap <= max;
      }),
      { numRuns: 5000 },
    );
  });

  it('property: the cap never decreases with the attempt', () => {
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 39 }), fc.integer({ min: 1, max: 10_000 }), fc.integer({ min: 1, max: 600_000 }), (a, base, max) => backoffCap(a, base, max) <= backoffCap(a + 1, base, max)),
      { numRuns: 2000 },
    );
  });

  it('jittered tier delay', () => {
    expect(jittered(1000, () => 0)).toBe(500);
    expect(jittered(1000, () => 1)).toBe(1000);
    expect(jittered(1000, () => 0, 'full')).toBe(0);
  });
});

describe('router', () => {
  const p = { topics: ['r1', 'r2', 'r3'], delaysMs: [5000, 60000, 600000], jitter: 'equal' as const };

  it('attempt k goes to tier k+1 with not_before = now + jittered delay; exhausted -> DLQ', () => {
    expect(route(0, 'TRANSIENT_RECORD', p, 1000, () => 1)).toEqual({ kind: 'retry', tier: 1, topic: 'r1', notBefore: 6000, attempt: 1 });
    expect(route(1, 'UNKNOWN', p, 1000, () => 0)).toEqual({ kind: 'retry', tier: 2, topic: 'r2', notBefore: 31000, attempt: 2 });
    expect(route(2, 'TRANSIENT_RECORD', p, 0, () => 1)).toMatchObject({ tier: 3, topic: 'r3', notBefore: 600000 });
    expect(route(3, 'TRANSIENT_RECORD', p, 0)).toEqual({ kind: 'dlq', attempts: 3 });
  });

  it('permanent classes go to the DLQ at once; blocking classes never to a retry topic or the DLQ', () => {
    expect(route(0, 'PERMANENT_DB', p, 0)).toEqual({ kind: 'dlq', attempts: 0 });
    expect(route(2, 'PERMANENT_PARSE', p, 0)).toEqual({ kind: 'dlq', attempts: 2 });
    expect(route(0, 'TRANSIENT_SYSTEMIC', p, 0)).toEqual({ kind: 'block' });
    expect(route(1, 'SYSTEMIC_CONFIG', p, 0)).toEqual({ kind: 'block' });
  });

  it('validates the tier policy', () => {
    expect(() => validateTierPolicy({ ...p, delaysMs: [1, 2] })).toThrow(/3 retry topics but 2 delays/);
    expect(() => validateTierPolicy({ ...p, topics: ['a', 'a', 'b'] })).toThrow(/distinct/);
    expect(() => validateTierPolicy({ topics: [], delaysMs: [], jitter: 'equal' })).toThrow();
  });

  it('headers: original headers kept, retry.* replaced (never duplicated), origin survives every tier', () => {
    const origin = { topic: 'events.raw', partition: 4, offset: '1234', firstFailedAt: '2026-11-28T10:00:00.000Z' };
    const r1 = route(0, 'TRANSIENT_RECORD', p, 1000, () => 1) as any;
    const h1 = retryHeaders({ received_at: Buffer.from('t'), request_id: 'r' }, r1, origin, { errorClass: 'TRANSIENT_RECORD', errorCode: '40001', errorMessage: 'm' });
    expect(h1).toMatchObject({ received_at: Buffer.from('t'), request_id: 'r', [RETRY_HEADERS.attempt]: '1', [RETRY_HEADERS.originalOffset]: '1234' });
    const state1 = parseRetryHeaders(h1);
    expect(state1).toEqual({ attempt: 1, notBefore: 6000, origin });
    const r2 = route(state1.attempt, 'TRANSIENT_RECORD', p, 7000, () => 1) as any;
    const h2 = retryHeaders(h1, r2, state1.origin!, { errorClass: 'TRANSIENT_RECORD', errorCode: '40001', errorMessage: 'again' });
    expect(parseRetryHeaders(h2)).toEqual({ attempt: 2, notBefore: 67000, origin });
    expect(Object.keys(h2).filter((k) => k === RETRY_HEADERS.attempt)).toHaveLength(1);
    expect(originalHeaders(h2)).toEqual({ received_at: Buffer.from('t'), request_id: 'r' });
  });

  it('missing or broken retry headers: attempt 1, due now, no origin', () => {
    expect(parseRetryHeaders({})).toEqual({ attempt: 1, notBefore: 0, origin: null });
    expect(parseRetryHeaders({ [RETRY_HEADERS.attempt]: 'x', [RETRY_HEADERS.notBefore]: 'later' })).toEqual({ attempt: 1, notBefore: 0, origin: null });
  });
});

describe('TokenBucket', () => {
  it('allows the burst, then the rate', () => {
    let t = 0;
    const b = new TokenBucket(10, 10, () => t);
    expect(b.tryTake(10)).toBe(true);
    expect(b.tryTake(1)).toBe(false);
    t = 500; // +5 tokens
    expect(b.tryTake(5)).toBe(true);
    expect(b.tryTake(1)).toBe(false);
  });

  it('rate 0 = unlimited', () => {
    const b = new TokenBucket(0);
    for (let i = 0; i < 1000; i++) expect(b.tryTake(100)).toBe(true);
  });
});
