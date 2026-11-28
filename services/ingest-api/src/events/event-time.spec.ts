import { Registry } from '@prometheus-io/client';
import { EventTimeMetrics } from './event-time-metrics.js';
import { InvalidOccurredAtError, OccurredAtOutOfRangeError, checkSkew, normalizeOccurredAt } from './event-time.js';

describe('normalizeOccurredAt (S3-F8)', () => {
  it('UTC with 3 fractional digits is kept exactly', () => {
    expect(normalizeOccurredAt('2026-11-28T09:00:00.123Z')).toBe('2026-11-28T09:00:00.123Z');
  });

  it('zone offsets are normalized to UTC', () => {
    expect(normalizeOccurredAt('2026-11-28T11:30:00.000+02:30')).toBe('2026-11-28T09:00:00.000Z');
    expect(normalizeOccurredAt('2026-11-28T04:00:00-05:00')).toBe('2026-11-28T09:00:00.000Z');
    expect(normalizeOccurredAt('2026-11-28T00:30:00+01:00')).toBe('2026-11-27T23:30:00.000Z'); // crosses a day
    expect(normalizeOccurredAt('2026-11-28t09:00:00z')).toBe('2026-11-28T09:00:00.000Z'); // RFC 3339 allows lower case
  });

  it('precision: more than 3 fractional digits are TRUNCATED to milliseconds, never rounded', () => {
    expect(normalizeOccurredAt('2026-11-28T09:00:00.123456Z')).toBe('2026-11-28T09:00:00.123Z');
    expect(normalizeOccurredAt('2026-11-28T09:00:00.999999999Z')).toBe('2026-11-28T09:00:00.999Z'); // not 09:00:01
    expect(normalizeOccurredAt('2026-12-31T23:59:59.9999Z')).toBe('2026-12-31T23:59:59.999Z'); // not next year
    expect(normalizeOccurredAt('2026-11-28T09:00:00.1Z')).toBe('2026-11-28T09:00:00.100Z');
    expect(normalizeOccurredAt('2026-11-28T09:00:00Z')).toBe('2026-11-28T09:00:00.000Z');
  });

  it.each([
    ['no zone (would be read as the host\'s local time)', '2026-11-28T09:00:00'],
    ['date only', '2026-11-28'],
    ['space instead of T', '2026-11-28 09:00:00Z'],
    ['February 30', '2026-02-30T09:00:00Z'],
    ['hour 24', '2026-11-28T24:00:00Z'],
    ['second 60', '2026-11-28T09:00:60Z'],
    ['offset hour 24', '2026-11-28T09:00:00+24:00'],
    ['offset without colon', '2026-11-28T09:00:00+0200'],
    ['epoch number as string', '1764320400000'],
  ])('rejects %s', (_name, v) => {
    expect(() => normalizeOccurredAt(v)).toThrow(InvalidOccurredAtError);
  });

  it('accepts 29 February of a leap year only', () => {
    expect(normalizeOccurredAt('2028-02-29T00:00:00Z')).toBe('2028-02-29T00:00:00.000Z');
    expect(() => normalizeOccurredAt('2027-02-29T00:00:00Z')).toThrow(InvalidOccurredAtError);
  });
});

describe('checkSkew (S3-F8, C13 policy): limits are inclusive', () => {
  const received = new Date('2026-11-28T12:00:00.000Z');
  const b = { maxFutureS: 300, maxPastDays: 365 };
  const at = (deltaMs: number) => new Date(received.getTime() + deltaMs).toISOString();

  it('exactly 300 s ahead is accepted, 1 ms more is 422 occurred_at_in_future', () => {
    expect(checkSkew(at(300_000), received, b)).toBe(-300);
    expect(() => checkSkew(at(300_001), received, b)).toThrow(expect.objectContaining({ reason: 'occurred_at_in_future' }));
  });

  it('exactly 365 days old is accepted, 1 ms more is 422 occurred_at_too_old', () => {
    expect(checkSkew(at(-365 * 86_400_000), received, b)).toBe(365 * 86_400);
    expect(() => checkSkew(at(-365 * 86_400_000 - 1), received, b)).toThrow(OccurredAtOutOfRangeError);
    expect(() => checkSkew(at(-365 * 86_400_000 - 1), received, b)).toThrow(expect.objectContaining({ reason: 'occurred_at_too_old' }));
  });

  it('1970 and 2099 are rejected; minutes, hours, a day late are fine (late events are stored)', () => {
    expect(() => checkSkew('1970-01-01T00:00:00.000Z', received, b)).toThrow(expect.objectContaining({ reason: 'occurred_at_too_old' }));
    expect(() => checkSkew('2099-01-01T00:00:00.000Z', received, b)).toThrow(expect.objectContaining({ reason: 'occurred_at_in_future' }));
    for (const lateMs of [60_000, 3_600_000, 86_400_000]) expect(checkSkew(at(-lateMs), received, b)).toBe(lateMs / 1000);
  });

  it('compares against received_at (the request), never against the clock later in the pipeline', () => {
    const old = new Date('2025-01-01T00:00:00.000Z'); // a request received long ago, checked with its own time
    expect(checkSkew('2024-12-31T23:59:00.000Z', old, b)).toBe(60);
  });
});

describe('EventTimeMetrics', () => {
  it('lateness histogram, rejections by reason, events ahead of the server counted', async () => {
    const r = new Registry();
    const m = new EventTimeMetrics(r);
    m.observe(3600);
    m.observe(-5);
    m.reject('occurred_at_in_future');
    m.reject('occurred_at_too_old');
    m.reject('occurred_at_too_old');
    const text = await r.metrics();
    expect(text).toMatch(/ep_event_lateness_seconds_count 2/);
    expect(text).toMatch(/ep_event_rejected_total\{reason="future"\} 1/);
    expect(text).toMatch(/ep_event_rejected_total\{reason="too_old"\} 2/);
    expect(text).toMatch(/ep_event_ahead_total 1/);
  });
});
