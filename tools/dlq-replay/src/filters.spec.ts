import { UsageError, filtersJson, parseFilters, parseTime, selectSql, whereClause } from './filters.ts';

const NOW = new Date('2026-11-28T12:00:00.000Z');

describe('parseFilters', () => {
  it('parses every filter', () => {
    const f = parseFilters(
      {
        id: '3,4',
        tenant: 't1',
        'event-id': '0b5f4e0e-6c39-4d7e-9a43-2b3c1d6f8a11',
        'error-class': 'permanent_db',
        'error-code': '22P05',
        'failed-topic': 'events.retry.3',
        'dlq-topic': 'events.dlq',
        'source-partition': '2',
        'source-offset-from': '10',
        'source-offset-to': '20',
        since: '2h',
        until: '2026-11-28T11:30:00Z',
        limit: '50',
        'include-replayed': true,
      },
      NOW,
    );
    expect(f).toEqual({
      ids: [3, 4],
      tenant: 't1',
      eventId: '0b5f4e0e-6c39-4d7e-9a43-2b3c1d6f8a11',
      errorClass: 'PERMANENT_DB',
      errorCode: '22P05',
      failedTopic: 'events.retry.3',
      dlqTopic: 'events.dlq',
      sourcePartition: 2,
      sourceOffsetFrom: 10,
      sourceOffsetTo: 20,
      since: new Date('2026-11-28T10:00:00.000Z'),
      until: new Date('2026-11-28T11:30:00.000Z'),
      limit: 50,
      includeReplayed: true,
    });
  });

  it('ignores the non-filter options and defaults to unreplayed rows', () => {
    expect(parseFilters({ execute: true, yes: true, json: true, rate: '5' })).toEqual({});
  });

  it.each([
    [{ id: 'x' }, /--id/],
    [{ 'event-id': 'nope' }, /UUID/],
    [{ 'error-class': 'BAD' }, /--error-class must be one of/],
    [{ 'source-partition': '-1' }, /--source-partition/],
    [{ 'source-offset-from': '9', 'source-offset-to': '3' }, /from must be <=/],
    [{ since: 'yesterday' }, /ISO 8601/],
    [{ since: '1h', until: '2h' }, /--since must be before --until/],
    [{ limit: '0' }, /--limit/],
  ])('rejects %j', (o, msg) => {
    expect(() => parseFilters(o as any, NOW)).toThrow(UsageError);
    expect(() => parseFilters(o as any, NOW)).toThrow(msg);
  });

  it('parseTime: ages in s, m, h, d', () => {
    expect(parseTime('since', '30s', NOW).toISOString()).toBe('2026-11-28T11:59:30.000Z');
    expect(parseTime('since', '15m', NOW).toISOString()).toBe('2026-11-28T11:45:00.000Z');
    expect(parseTime('since', '7d', NOW).toISOString()).toBe('2026-11-21T12:00:00.000Z');
  });
});

describe('SQL builder', () => {
  it('no filters: only unreplayed rows, oldest first, no parameters', () => {
    expect(selectSql({})).toEqual({
      text: expect.stringMatching(/^SELECT id, source_topic, .* FROM dlq_meta WHERE replayed_at IS NULL ORDER BY first_failed_at, id$/),
      values: [],
    });
  });

  it('every filter is a numbered parameter, in order, and the limit comes last', () => {
    const q = selectSql({
      ids: [1, 2],
      tenant: 't',
      eventId: 'e',
      errorClass: 'PERMANENT_PARSE',
      errorCode: 'c',
      failedTopic: 'f',
      dlqTopic: 'd',
      sourcePartition: 0,
      sourceOffsetFrom: 5,
      sourceOffsetTo: 9,
      since: new Date('2026-11-01T00:00:00Z'),
      until: new Date('2026-11-02T00:00:00Z'),
      limit: 7,
      includeReplayed: true,
    });
    expect(q.text).toContain(
      'WHERE id = ANY($1::bigint[]) AND tenant_id = $2 AND event_id = $3::uuid AND error_class = $4 AND error_code = $5 AND failed_topic = $6 AND dlq_topic = $7 ' +
        'AND source_partition = $8 AND source_offset >= $9 AND source_offset <= $10 AND first_failed_at >= $11 AND first_failed_at < $12 ORDER BY first_failed_at, id LIMIT $13',
    );
    expect(q.text).not.toContain('replayed_at IS NULL');
    expect(q.values).toEqual([[1, 2], 't', 'e', 'PERMANENT_PARSE', 'c', 'f', 'd', 0, 5, 9, '2026-11-01T00:00:00.000Z', '2026-11-02T00:00:00.000Z', 7]);
  });

  it('values never reach the SQL text (injection attempt is just a parameter)', () => {
    const evil = "x' OR 1=1 --";
    const q = selectSql({ tenant: evil, errorCode: "'; DROP TABLE dlq_meta; --" });
    expect(q.text).not.toContain(evil);
    expect(q.text).not.toContain('DROP');
    expect(q.values).toEqual([evil, "'; DROP TABLE dlq_meta; --"]);
  });

  it('whereClause numbers from a given offset (stats reuse it)', () => {
    expect(whereClause({ tenant: 't' }, 3)).toEqual({ text: 'WHERE tenant_id = $3 AND replayed_at IS NULL', values: ['t'] });
  });

  it('filtersJson: dates as ISO strings, undefined dropped', () => {
    expect(filtersJson({ tenant: 't', since: new Date('2026-11-01T00:00:00Z'), limit: undefined })).toEqual({ tenant: 't', since: '2026-11-01T00:00:00.000Z' });
  });
});
