// src/events/create-event.dto.spec.ts
import 'reflect-metadata';
import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import { CreateEventDto } from './create-event.dto.js';

const good = {
  event_id: '6f1c2b1e-8a43-4c0e-9d2f-0a1b2c3d4e5f',
  type: 'order.created',
  occurred_at: '2026-11-12T10:00:00Z',
  schema_version: 1,
  payload: { a: 1 },
};
const errs = async (o: object) => validate(plainToInstance(CreateEventDto, o));

describe('CreateEventDto', () => {
  it('accepts a valid event', async () =>
    expect(await errs(good)).toHaveLength(0));

  it.each([
    ['missing field', { ...good, type: undefined }],
    ['bad uuid', { ...good, event_id: 'nope' }],
    ['bad timestamp', { ...good, occurred_at: 'yesterday' }],
    ['oversized type', { ...good, type: 'x'.repeat(101) }],
    ['payload is array', { ...good, payload: [] }],
    ['schema_version 0', { ...good, schema_version: 0 }],
  ])('rejects %s', async (_n, o) =>
    expect((await errs(o)).length).toBeGreaterThan(0),
  );
});
