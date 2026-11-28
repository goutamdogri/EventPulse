import { REPLAY_HEADERS } from '@eventpulse/retry';
import { DLQ_HEADERS, toDlqMessage } from './dlq-publisher.js';

describe('toDlqMessage', () => {
  const raw = {
    partition: 5,
    offset: '2893',
    key: Buffer.from('poison-key'),
    value: Buffer.from('{"event_id": broken'),
    headers: { received_at: Buffer.from('2026-11-22T06:31:07.773Z'), request_id: Buffer.from('r-1') },
  };

  it('keeps the original key, bytes and headers and adds the dlq.* headers', () => {
    const m = toDlqMessage(
      { raw, errorClass: 'PERMANENT_PARSE', errorCode: 'invalid_json', errorMessage: 'Unexpected token b', attempts: 1 },
      { sourceTopic: 'events.raw', groupId: 'sink-postgres' },
      new Date('2026-11-22T07:00:00Z'),
    );
    expect(m.key).toBe(raw.key);
    expect(m.value).toBe(raw.value); // the very same bytes, not re-encoded
    expect(m.headers).toMatchObject({
      received_at: raw.headers.received_at,
      request_id: raw.headers.request_id,
      [DLQ_HEADERS.originalTopic]: 'events.raw',
      [DLQ_HEADERS.originalPartition]: '5',
      [DLQ_HEADERS.originalOffset]: '2893',
      [DLQ_HEADERS.consumerGroup]: 'sink-postgres',
      [DLQ_HEADERS.errorClass]: 'PERMANENT_PARSE',
      [DLQ_HEADERS.errorCode]: 'invalid_json',
      [DLQ_HEADERS.errorMessage]: 'Unexpected token b',
      [DLQ_HEADERS.attempts]: '1',
      [DLQ_HEADERS.failedAt]: '2026-11-22T07:00:00.000Z',
    });
  });

  it('truncates long error messages to 1 KB', () => {
    const m = toDlqMessage(
      { raw, errorClass: 'PERMANENT_DB', errorCode: '22001', errorMessage: 'x'.repeat(5000), attempts: 4 },
      { sourceTopic: 'events.raw', groupId: 'g' },
    );
    expect(Buffer.byteLength(m.headers[DLQ_HEADERS.errorMessage] as string)).toBeLessThanOrEqual(1027);
  });

  it('S3-F6: a replayed record that fails again keeps its replay.* headers (the replay loop guard reads replay.count)', () => {
    const replayed = { ...raw, headers: { ...raw.headers, [REPLAY_HEADERS.id]: 'r-77', [REPLAY_HEADERS.count]: Buffer.from('2'), [REPLAY_HEADERS.dlqOffset]: '9' } };
    const m = toDlqMessage(
      { raw: replayed, errorClass: 'PERMANENT_PARSE', errorCode: 'invalid_json', errorMessage: 'still broken', attempts: 0 },
      { sourceTopic: 'events.raw', groupId: 'sink-postgres' },
    );
    expect(m.headers[REPLAY_HEADERS.count]).toEqual(Buffer.from('2'));
    expect(m.headers[REPLAY_HEADERS.id]).toBe('r-77');
    expect(m.headers[DLQ_HEADERS.originalOffset]).toBe('2893'); // a new dead letter of the replayed record
  });
});
