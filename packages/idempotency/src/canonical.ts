// Canonical JSON and content hashing, shared by the API (idempotency fingerprint, Stage 3 step 2) and the sink
// (content_hash, step 1). Both sides MUST hash the same way, so this is the only implementation.
//
// Rules (RFC 8785 "JCS" style, simplified for what EventPulse sends):
//   - object keys sorted by UTF-16 code units (JS default sort), recursively; no whitespace
//   - strings (values AND keys) normalized to Unicode NFC, then JSON-escaped by JSON.stringify
//   - numbers in JS shortest round-trip form (1.0 and 1 are the same number: "1"); NaN/Infinity rejected
//   - arrays keep their order (order is data)
//   - undefined, functions, symbols, bigint rejected (they have no JSON form; silently dropping them would make
//     two different inputs hash the same)
//
// Two bodies that differ only by key order, whitespace or Unicode normalization get the same hash.

import { createHash } from 'node:crypto';

export class CanonicalJsonError extends Error {
  readonly name = 'CanonicalJsonError';
}

export function canonicalJson(value: unknown): string {
  return write(value, '$');
}

function write(v: unknown, path: string): string {
  if (v === null) return 'null';
  switch (typeof v) {
    case 'boolean':
      return v ? 'true' : 'false';
    case 'number':
      if (!Number.isFinite(v)) throw new CanonicalJsonError(`${path}: non-finite number`);
      return JSON.stringify(v); // -0 becomes "0", shortest round-trip form otherwise
    case 'string':
      return JSON.stringify(v.normalize('NFC'));
    case 'object': {
      if (Array.isArray(v)) return `[${v.map((x, i) => write(x, `${path}[${i}]`)).join(',')}]`;
      const proto = Object.getPrototypeOf(v);
      if (proto !== Object.prototype && proto !== null) {
        throw new CanonicalJsonError(`${path}: only plain objects are allowed (got ${proto?.constructor?.name ?? 'unknown'})`);
      }
      const entries = Object.entries(v as Record<string, unknown>).map(([k, x]) => [k.normalize('NFC'), x] as const);
      entries.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
      for (let i = 1; i < entries.length; i++) {
        if (entries[i][0] === entries[i - 1][0]) throw new CanonicalJsonError(`${path}: duplicate key after NFC normalization: ${entries[i][0]}`);
      }
      return `{${entries.map(([k, x]) => `${JSON.stringify(k)}:${write(x, `${path}.${k}`)}`).join(',')}}`;
    }
    default:
      throw new CanonicalJsonError(`${path}: ${typeof v} has no JSON form`);
  }
}

export function sha256(text: string): Buffer {
  return createHash('sha256').update(text, 'utf8').digest();
}

/** What identifies the CONTENT of one event (everything the client controls, plus the tenant). */
export interface EventContent {
  tenant_id: string;
  event_id: string;
  type: string;
  occurred_at: string;
  schema_version: number;
  payload: Record<string, unknown>;
}

/**
 * sha256 of the canonical event. event_id is lower-cased (UUIDs are case-insensitive, Postgres stores them
 * lower-case) and occurred_at is normalized to the UTC instant in ISO form with milliseconds, so
 * "2026-01-01T10:00:00+02:00" and "2026-01-01T08:00:00.000Z" hash the same. Sub-millisecond digits are dropped
 * by this normalization (the precision decision of Stage 3 step 8).
 */
export function eventContentHash(e: EventContent): Buffer {
  const t = Date.parse(e.occurred_at);
  if (Number.isNaN(t)) throw new CanonicalJsonError('occurred_at is not a date-time');
  return sha256(
    canonicalJson({
      tenant_id: e.tenant_id,
      event_id: e.event_id.toLowerCase(),
      type: e.type,
      occurred_at: new Date(t).toISOString(),
      schema_version: e.schema_version,
      payload: e.payload,
    }),
  );
}
