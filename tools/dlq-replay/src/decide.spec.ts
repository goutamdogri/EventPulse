import { decide, priorReplays, replayHeaders } from './decide.ts';

const base = { found: true, rowReplayCount: 0, maxReplays: 3, includeFailing: false };
const bad = { ok: false as const, errorClass: 'PERMANENT_PARSE', code: 'invalid_json', message: 'Unexpected token' };

describe('loop guard and decisions', () => {
  it('prior replays = replay.count header (earlier rounds) + this row\'s replay_count', () => {
    expect(priorReplays(0, {})).toBe(0);
    expect(priorReplays(0, { 'replay.count': Buffer.from('2') })).toBe(2);
    expect(priorReplays(1, { 'replay.count': '2' })).toBe(3);
    expect(priorReplays(0, { 'replay.count': 'garbage' })).toBe(0);
  });

  it('first replay gets count 1', () => {
    expect(decide({ ...base, check: { ok: true } })).toEqual({ kind: 'replay', count: 1, forced: false });
  });

  it('replay 3 is allowed, replay 4 is skipped at DLQ_MAX_REPLAYS=3', () => {
    expect(decide({ ...base, dlqHeaders: { 'replay.count': '2' } })).toEqual({ kind: 'replay', count: 3, forced: false });
    expect(decide({ ...base, dlqHeaders: { 'replay.count': '3' } })).toMatchObject({ kind: 'skip', reason: 'loop_guard' });
    expect(decide({ ...base, rowReplayCount: 3 })).toMatchObject({ kind: 'skip', reason: 'loop_guard' });
  });

  it('the loop guard wins over --include-failing', () => {
    expect(decide({ ...base, includeFailing: true, check: bad, dlqHeaders: { 'replay.count': '3' } })).toMatchObject({ kind: 'skip', reason: 'loop_guard' });
  });

  it('a record the pre-check rejects is skipped, or replayed (forced) with --include-failing', () => {
    expect(decide({ ...base, check: bad })).toEqual({ kind: 'skip', reason: 'would_fail', detail: 'PERMANENT_PARSE invalid_json: Unexpected token' });
    expect(decide({ ...base, check: bad, includeFailing: true })).toEqual({ kind: 'replay', count: 1, forced: true });
  });

  it('a record missing from the DLQ topic fails', () => {
    expect(decide({ ...base, found: false })).toMatchObject({ kind: 'fail', reason: 'missing_in_dlq' });
  });
});

describe('replayHeaders', () => {
  it('original headers kept, dlq.* / retry.* / old replay.* dropped, new replay.* added', () => {
    const h = replayHeaders(
      {
        received_at: Buffer.from('2026-11-28T10:00:00.000Z'),
        'request-id': 'r1',
        'content-type': ['application/vnd.eventpulse.avro'],
        'dlq.original_topic': 'events.raw',
        'dlq.error_class': 'PERMANENT_DB',
        'retry.attempt': '3',
        'replay.count': '1',
        'replay.id': 'old',
      },
      { replayId: 'r-new', count: 2, dlqPartition: 4, dlqOffset: '99' },
    );
    expect(h).toEqual({
      received_at: Buffer.from('2026-11-28T10:00:00.000Z'),
      'request-id': 'r1',
      'content-type': 'application/vnd.eventpulse.avro',
      'replay.id': 'r-new',
      'replay.count': '2',
      'replay.dlq_partition': '4',
      'replay.dlq_offset': '99',
    });
  });
});
