import { AllExceptionsFilter } from './all-exceptions.filter.js';
import { KafkaUnavailableError, OverloadedError } from '../kafka/errors.js';
import { BatcherClosedError, MessageTooLargeError } from '../kafka/micro-batcher.js';

function run(err: unknown, retryAfter = 7) {
  const headers: Record<string, string> = {};
  let status = 0;
  let body: any;
  const res: any = {
    status: (s: number) => ((status = s), res),
    set: (k: string, v: string) => ((headers[k] = v), res),
    json: (b: unknown) => ((body = b), res),
  };
  const m = { by: {} as Record<string, number>, count503(r: string) { this.by[r] = (this.by[r] ?? 0) + 1; } };
  new AllExceptionsFilter(m as any, retryAfter).catch(err, { switchToHttp: () => ({ getResponse: () => res }) } as any);
  return { status, headers, body, m };
}

describe('AllExceptionsFilter (S2-F5)', () => {
  it('OverloadedError -> 503, Retry-After, reason overloaded', () => {
    const r = run(new OverloadedError());
    expect(r.status).toBe(503);
    expect(r.headers['Retry-After']).toBe('7');
    expect(r.body).toMatchObject({ statusCode: 503, error: 'Service Unavailable', reason: 'overloaded' });
    expect(r.m.by).toEqual({ overloaded: 1 });
  });

  it('KafkaUnavailableError -> 503, Retry-After, reason kafka_unavailable', () => {
    const r = run(new KafkaUnavailableError('down'));
    expect(r.status).toBe(503);
    expect(r.headers['Retry-After']).toBe('7');
    expect(r.body.reason).toBe('kafka_unavailable');
    expect(r.m.by).toEqual({ kafka_unavailable: 1 });
  });

  it('request during the SIGTERM drain (BatcherClosedError) -> 503, Retry-After, reason shutting_down (not 500)', () => {
    const spy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const r = run(new BatcherClosedError());
    expect(spy).not.toHaveBeenCalled(); // not an "[unhandled]" bug
    spy.mockRestore();
    expect(r.status).toBe(503);
    expect(r.headers['Retry-After']).toBe('7');
    expect(r.body).toMatchObject({ statusCode: 503, reason: 'shutting_down' });
    expect(r.m.by).toEqual({ shutting_down: 1 });
  });

  it('MessageTooLargeError -> 413, not retryable', () => {
    const r = run(new MessageTooLargeError(2_000_000, 900_000));
    expect(r.status).toBe(413);
    expect(r.headers['Retry-After']).toBeUndefined();
  });

  it('other errors stay 500 and do not leak details', () => {
    const spy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const r = run(new Error('secret'));
    spy.mockRestore();
    expect(r.status).toBe(500);
    expect(JSON.stringify(r.body)).not.toContain('secret');
    expect(r.headers['Retry-After']).toBeUndefined();
  });
});
