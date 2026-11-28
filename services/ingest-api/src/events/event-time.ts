// Event time rules at the API (S3-F8). Written down in docs/api-contract.md ("Time fields").
//
// occurred_at  client clock. MUST be an RFC 3339 date-time WITH a zone: "Z" or "+hh:mm" / "-hh:mm". A time without a
//              zone is a 400: JavaScript would read it as the API host's LOCAL time, so the same request would mean
//              different instants on different hosts. Normalized to UTC ("...Z").
//              Precision: MILLISECONDS. More fractional digits are accepted and TRUNCATED (not rounded) to ms:
//              JavaScript Date and Avro timestamp-millis cannot hold more. "…:00.123999Z" is stored as "…:00.123Z".
//              The normalized value is what is hashed (idempotency), produced and stored, so "…+02:00" and the same
//              instant in "Z" are the same event content.
// received_at  server clock, set by the API when the request is handled. Never from the client.
//
// Skew bounds (C13 policy), checked ONLY here, against received_at (not "now" later in the pipeline): a replay of old
// data, or a record that waited on a retry tier, must still be stored by the sink.
//   occurred_at - received_at >  OCCURRED_AT_MAX_FUTURE_S (300)       -> 422 occurred_at_in_future
//   received_at - occurred_at >  OCCURRED_AT_MAX_PAST_DAYS (365) days -> 422 occurred_at_too_old
// Both limits are INCLUSIVE: exactly 300 s ahead / exactly 365 days old is accepted. The past bound is generous on
// purpose (late events are stored regardless of lateness, G5); it only stops broken clocks (1970, 2099).

/** RFC 3339 date-time with a mandatory zone; fraction of any length. */
export const OCCURRED_AT_RE = /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})$/i;

export class InvalidOccurredAtError extends Error {
  readonly name = 'InvalidOccurredAtError';
}

export const SKEW_REASONS = ['occurred_at_in_future', 'occurred_at_too_old'] as const;
export type SkewReason = (typeof SKEW_REASONS)[number];

export class OccurredAtOutOfRangeError extends Error {
  readonly name = 'OccurredAtOutOfRangeError';
  constructor(readonly reason: SkewReason, message: string) {
    super(message);
  }
}

/** -> ISO UTC string with exactly 3 fractional digits. Throws InvalidOccurredAtError. */
export function normalizeOccurredAt(s: string): string {
  const m = typeof s === 'string' ? s.match(OCCURRED_AT_RE) : null;
  if (!m) throw new InvalidOccurredAtError('occurred_at must be an RFC 3339 date-time with a time zone (Z or ±hh:mm)');
  const [, date, time, frac = '', zone] = m;
  const ms = (frac + '000').slice(0, 3); // truncate, never round: rounding could move an event into the next second/day
  const zoneUpper = zone.toUpperCase();
  if (zoneUpper !== 'Z') {
    const [hh, mm] = zoneUpper.slice(1).split(':').map(Number);
    if (hh > 23 || mm > 59) throw new InvalidOccurredAtError('occurred_at has an invalid zone offset');
  }
  const t = Date.parse(`${date}T${time}.${ms}${zoneUpper}`);
  if (!Number.isFinite(t)) throw new InvalidOccurredAtError('occurred_at is not a valid date-time');
  // Date.parse accepts some impossible values (e.g. 24:00:00); compare the fields that cannot shift.
  const [y, mo, d] = date.split('-').map(Number);
  const [h, mi, se] = time.split(':').map(Number);
  if (mo < 1 || mo > 12 || d < 1 || d > new Date(Date.UTC(y, mo, 0)).getUTCDate() || h > 23 || mi > 59 || se > 59) {
    throw new InvalidOccurredAtError('occurred_at is not a valid date-time');
  }
  return new Date(t).toISOString();
}

export interface SkewBounds {
  maxFutureS: number;
  maxPastDays: number;
}

/** Throws OccurredAtOutOfRangeError; returns the lateness (received_at - occurred_at) in seconds, negative = ahead. */
export function checkSkew(occurredAtIso: string, receivedAt: Date, b: SkewBounds): number {
  const diffMs = receivedAt.getTime() - Date.parse(occurredAtIso);
  if (-diffMs > b.maxFutureS * 1000) {
    throw new OccurredAtOutOfRangeError('occurred_at_in_future', `occurred_at is more than ${b.maxFutureS} s after the server time (client clock ahead?)`);
  }
  if (diffMs > b.maxPastDays * 86_400_000) {
    throw new OccurredAtOutOfRangeError('occurred_at_too_old', `occurred_at is more than ${b.maxPastDays} days before the server time (client clock broken?)`);
  }
  return diffMs / 1000;
}
