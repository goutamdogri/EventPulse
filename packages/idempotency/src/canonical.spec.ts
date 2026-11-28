import { CanonicalJsonError, canonicalJson, eventContentHash } from './canonical.js';

const base = {
  tenant_id: 't1',
  event_id: '0b5a2d9e-3c1f-4e7a-9b2d-1f0e8c7a6b5d',
  type: 'order.created',
  occurred_at: '2026-11-12T10:00:00Z',
  schema_version: 1,
  payload: { a: 1, nested: { z: [1, 2, { y: 'x' }], b: true } },
};

describe('canonicalJson', () => {
  it('sorts keys recursively and drops whitespace', () => {
    expect(canonicalJson({ b: 1, a: { d: [3, { f: 1, e: 2 }], c: null } })).toBe('{"a":{"c":null,"d":[3,{"e":2,"f":1}]},"b":1}');
  });

  it('is independent of key order and of the source text formatting', () => {
    const x = JSON.parse('{ "b" : 1,\n "a" : { "y": 2, "x": 1 } }');
    const y = JSON.parse('{"a":{"x":1,"y":2},"b":1}');
    expect(canonicalJson(x)).toBe(canonicalJson(y));
  });

  it('keeps array order (order is data)', () => {
    expect(canonicalJson([1, 2])).not.toBe(canonicalJson([2, 1]));
  });

  it('numbers in shortest form: 1.0 == 1, -0 == 0, exponents normalized', () => {
    expect(canonicalJson(JSON.parse('{"n":1.0}'))).toBe('{"n":1}');
    expect(canonicalJson(-0)).toBe('0');
    expect(canonicalJson(JSON.parse('1e3'))).toBe('1000');
    expect(canonicalJson(0.1 + 0.2)).toBe('0.30000000000000004');
  });

  it('NFC-normalizes strings and keys (é precomposed == e + combining accent)', () => {
    const pre = 'café';
    const dec = 'café';
    expect(canonicalJson({ [dec]: dec })).toBe(canonicalJson({ [pre]: pre }));
  });

  it('escapes like JSON.stringify (quotes, control characters)', () => {
    expect(canonicalJson({ q: 'he said "hi"\n' })).toBe('{"q":"he said \\"hi\\"\\n"}');
  });

  it('rejects values with no JSON form instead of silently dropping them', () => {
    expect(() => canonicalJson({ a: undefined })).toThrow(CanonicalJsonError);
    expect(() => canonicalJson({ a: Number.NaN })).toThrow(/non-finite/);
    expect(() => canonicalJson({ a: Infinity })).toThrow(/non-finite/);
    expect(() => canonicalJson({ a: 1n })).toThrow(CanonicalJsonError);
    expect(() => canonicalJson({ a: new Date() })).toThrow(/plain objects/);
  });

  it('rejects two keys that collide after NFC normalization', () => {
    expect(() => canonicalJson({ 'café': 1, 'café': 2 })).toThrow(/duplicate key/);
  });
});

describe('eventContentHash', () => {
  const h = (o: Partial<typeof base> = {}) => eventContentHash({ ...base, ...o }).toString('hex');

  it('is a 32-byte sha256', () => {
    expect(eventContentHash(base)).toHaveLength(32);
  });

  it('same content, different key order or formatting: same hash', () => {
    expect(h({ payload: JSON.parse('{ "nested": { "b": true, "z": [1,2,{"y":"x"}] }, "a": 1.0 }') })).toBe(h());
  });

  it('event_id case and occurred_at offset form do not matter', () => {
    expect(h({ event_id: base.event_id.toUpperCase() })).toBe(h());
    expect(h({ occurred_at: '2026-11-12T12:00:00+02:00' })).toBe(h());
    expect(h({ occurred_at: '2026-11-12T10:00:00.000Z' })).toBe(h());
  });

  it('every content field changes the hash', () => {
    const ref = h();
    for (const o of [
      { tenant_id: 't2' },
      { event_id: '1b5a2d9e-3c1f-4e7a-9b2d-1f0e8c7a6b5d' },
      { type: 'order.updated' },
      { occurred_at: '2026-11-12T10:00:00.001Z' },
      { schema_version: 2 },
      { payload: { a: 2, nested: base.payload.nested } },
    ]) {
      expect(h(o), JSON.stringify(o)).not.toBe(ref);
    }
  });

  it('rejects an occurred_at that is not a date-time', () => {
    expect(() => h({ occurred_at: 'yesterday' })).toThrow(CanonicalJsonError);
  });
});
