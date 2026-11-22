import { backoffMs, classifyDbError, errorCode, isBlocking, truncate } from './errors.js';

const err = (code?: string, message = 'x') => Object.assign(new Error(message), code ? { code } : {});

describe('classifyDbError', () => {
  it.each([
    // DB unreachable / overloaded / restarting -> TRANSIENT_DB (never DLQ)
    ['ECONNREFUSED', 'TRANSIENT_DB'],
    ['ECONNRESET', 'TRANSIENT_DB'],
    ['ETIMEDOUT', 'TRANSIENT_DB'],
    ['08006', 'TRANSIENT_DB'], // connection_failure
    ['53300', 'TRANSIENT_DB'], // too_many_connections
    ['57P01', 'TRANSIENT_DB'], // admin_shutdown
    ['57P03', 'TRANSIENT_DB'], // cannot_connect_now
    ['57014', 'TRANSIENT_DB'], // statement timeout
    ['40001', 'TRANSIENT_DB'], // serialization_failure
    ['40P01', 'TRANSIENT_DB'], // deadlock
    // this row is bad -> PERMANENT_DB (isolate, retry, DLQ)
    ['22001', 'PERMANENT_DB'], // value too long
    ['22P02', 'PERMANENT_DB'], // invalid text representation
    ['22P05', 'PERMANENT_DB'], // untranslatable character (\u0000 in jsonb)
    ['22008', 'PERMANENT_DB'], // datetime out of range
    ['23502', 'PERMANENT_DB'], // not null
    ['23514', 'PERMANENT_DB'], // check
    // every row would fail -> SYSTEMIC_DB (never DLQ)
    ['42703', 'SYSTEMIC_DB'], // undefined column
    ['42P01', 'SYSTEMIC_DB'], // undefined table
    ['28P01', 'SYSTEMIC_DB'], // bad password
    ['3D000', 'SYSTEMIC_DB'], // database does not exist
    ['XX000', 'SYSTEMIC_DB'],
  ])('%s -> %s', (code, cls) => expect(classifyDbError(err(code))).toBe(cls));

  it('pg pool / connection messages without a code are TRANSIENT_DB', () => {
    expect(classifyDbError(err(undefined, 'timeout exceeded when trying to connect'))).toBe('TRANSIENT_DB');
    expect(classifyDbError(err(undefined, 'Connection terminated unexpectedly'))).toBe('TRANSIENT_DB');
  });

  it('anything else is UNKNOWN', () => {
    expect(classifyDbError(new TypeError('cannot read properties of undefined'))).toBe('UNKNOWN');
    expect(classifyDbError('a string')).toBe('UNKNOWN');
  });

  it('isBlocking: transient and systemic only', () => {
    expect(['TRANSIENT_DB', 'SYSTEMIC_DB'].every((c) => isBlocking(c as any))).toBe(true);
    expect(['PERMANENT_DB', 'PERMANENT_PARSE', 'UNKNOWN'].some((c) => isBlocking(c as any))).toBe(false);
  });

  it('errorCode keeps SQLSTATE / Node codes only', () => {
    expect(errorCode(err('22001'))).toBe('22001');
    expect(errorCode(err('some code with spaces'))).toBe('none');
    expect(errorCode(null)).toBe('none');
  });
});

describe('backoffMs', () => {
  it('doubles per attempt and is capped', () => {
    const max = () => 1; // jitter at its top: delay == cap
    expect([1, 2, 3, 4, 5, 6].map((a) => backoffMs(a, 100, 1000, max))).toEqual([100, 200, 400, 800, 1000, 1000]);
  });

  it('jitter keeps the delay within [cap/2, cap]', () => {
    for (let a = 1; a <= 8; a++) {
      const cap = Math.min(1000, 100 * 2 ** (a - 1));
      expect(backoffMs(a, 100, 1000, () => 0)).toBe(Math.round(cap / 2));
      for (let i = 0; i < 50; i++) {
        const d = backoffMs(a, 100, 1000);
        expect(d).toBeGreaterThanOrEqual(Math.floor(cap / 2));
        expect(d).toBeLessThanOrEqual(cap);
      }
    }
  });
});

describe('truncate', () => {
  it('limits to maxBytes', () => {
    expect(truncate('abc', 10)).toBe('abc');
    expect(Buffer.byteLength(truncate('x'.repeat(5000), 1024))).toBeLessThanOrEqual(1024 + 3);
  });
});
