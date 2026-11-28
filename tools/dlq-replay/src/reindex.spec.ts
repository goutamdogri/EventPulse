import { metaFromRecord } from './reindex.ts';
import { renderTextfile } from './textfile.ts';

const h = (o: Record<string, string>) => Object.fromEntries(Object.entries(o).map(([k, v]) => [k, Buffer.from(v)]));

describe('reindex: dlq_meta row from a DLQ record', () => {
  const headers = {
    'dlq.original_topic': 'events.raw',
    'dlq.original_partition': '3',
    'dlq.original_offset': '1234',
    'dlq.failed_topic': 'events.retry.3',
    'dlq.error_class': 'TRANSIENT_RECORD',
    'dlq.error_code': '40001',
    'dlq.error_message': 'serialization failure',
    'dlq.attempts': '3',
    'dlq.first_failed_at': '2026-11-28T10:00:00.000Z',
    'dlq.failed_at': '2026-11-28T10:11:00.000Z',
  };

  it('every column from the headers + where the record sits', () => {
    const m = metaFromRecord({ topic: 'events.dlq', partition: 1, offset: '77', key: null, value: null, headers: h(headers) }, { tenantId: 't1', eventId: 'e1' });
    expect(m).toEqual({
      sourceTopic: 'events.raw',
      sourcePartition: 3,
      sourceOffset: '1234',
      failedTopic: 'events.retry.3',
      dlqTopic: 'events.dlq',
      dlqPartition: 1,
      dlqOffset: '77',
      tenantId: 't1',
      eventId: 'e1',
      errorClass: 'TRANSIENT_RECORD',
      errorCode: '40001',
      errorMessage: 'serialization failure',
      attempts: 3,
      firstFailedAt: '2026-11-28T10:00:00.000Z',
      lastFailedAt: '2026-11-28T10:11:00.000Z',
    });
  });

  it('Stage 2 dead letters (no first_failed_at / failed_topic): sensible fallbacks; unparsed value: null tenant', () => {
    const { 'dlq.first_failed_at': _a, 'dlq.failed_topic': _b, ...old } = headers;
    const m = metaFromRecord({ topic: 'events.dlq', partition: 0, offset: '5', key: null, value: null, headers: h(old) }, null);
    expect(m).toMatchObject({ failedTopic: 'events.raw', firstFailedAt: '2026-11-28T10:11:00.000Z', tenantId: null, eventId: null });
  });

  it('a record without dlq.* headers is not a dead letter', () => {
    expect(metaFromRecord({ topic: 'events.dlq', partition: 0, offset: '5', key: null, value: null, headers: {} }, null)).toBeNull();
  });
});

describe('textfile', () => {
  it('last executed run as gauges', () => {
    const t = renderTextfile({ status: 'done', counts: { selected: 5, replayed: 3, skipped: 2, failed: 0 } } as any, 1_764_000_000);
    expect(t).toContain('ep_dlq_replay_selected 5\n');
    expect(t).toContain('ep_dlq_replay_replayed 3\n');
    expect(t).toContain('ep_dlq_replay_skipped 2\n');
    expect(t).toContain('ep_dlq_replay_failed 0\n');
    expect(t).toContain('ep_dlq_replay_last_run_success 1\n');
    expect(t).toContain('# TYPE ep_dlq_replay_replayed gauge');
  });
});
