import 'reflect-metadata';
import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import { ValidationPipe } from '@nestjs/common';
import { BadIfMatchError, etagOf, ifMatchOk, parseIfMatch } from './if-match.js';
import { PutConfigDto } from './tenant-config.dto.js';

// The same pipe options as bootstrap.ts: unknown keys are rejected, not stripped.
const pipe = new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true, transform: true });
const check = (body: unknown) => pipe.transform(body, { type: 'body', metatype: PutConfigDto });

describe('PutConfigDto', () => {
  it('accepts a full valid config', async () => {
    const body = {
      config: { rate_limit_per_s: 500, retention_days: 30, allowed_event_types: ['page.view', 'click'], pii_fields: ['email'], features: { beta_ui: true }, paused: false },
    };
    const r = await check(body);
    expect(JSON.parse(JSON.stringify(r.config))).toEqual(body.config);
  });

  it('accepts an empty config (every key optional)', async () => {
    await expect(check({ config: {} })).resolves.toBeDefined();
  });

  it.each([
    ['missing config', {}],
    ['config not an object', { config: 'x' }],
    ['unknown key', { config: { rate_limit: 5 } }],
    ['unknown top-level key', { config: {}, extra: 1 }],
    ['rate limit 0', { config: { rate_limit_per_s: 0 } }],
    ['rate limit not an integer', { config: { rate_limit_per_s: 1.5 } }],
    ['rate limit as a string', { config: { rate_limit_per_s: '5' } }],
    ['retention too long', { config: { retention_days: 4000 } }],
    ['event type with a space', { config: { allowed_event_types: ['a b'] } }],
    ['too many event types', { config: { allowed_event_types: Array.from({ length: 201 }, (_, i) => `t${i}`) } }],
    ['feature value not boolean', { config: { features: { beta: 'yes' } } }],
    ['feature key not snake_case', { config: { features: { BetaUI: true } } }],
    ['paused not boolean', { config: { paused: 1 } }],
  ])('400 on %s', async (_name, body) => {
    await expect(check(body)).rejects.toMatchObject({ status: 400 });
  });

  it('class-validator alone reports the nested property path', async () => {
    const errs = await validate(plainToInstance(PutConfigDto, { config: { retention_days: 0 } }));
    expect(errs[0].property).toBe('config');
    expect(errs[0].children?.[0].property).toBe('retention_days');
  });
});

describe('If-Match', () => {
  it('absent: no check', () => {
    expect(parseIfMatch(undefined)).toEqual({ kind: 'none' });
    expect(ifMatchOk(parseIfMatch(undefined), 0)).toBe(true);
    expect(ifMatchOk(parseIfMatch(''), 7)).toBe(true);
  });

  it('"N" and W/"N": the current version must match, else 412', () => {
    expect(ifMatchOk(parseIfMatch('"3"'), 3)).toBe(true);
    expect(ifMatchOk(parseIfMatch('W/"3"'), 3)).toBe(true);
    expect(ifMatchOk(parseIfMatch('"3"'), 4)).toBe(false);
    expect(ifMatchOk(parseIfMatch('"2", "4"'), 4)).toBe(true);
    expect(ifMatchOk(parseIfMatch('"0"'), 0)).toBe(true); // "create only if it does not exist yet"
  });

  it('*: the config must exist', () => {
    expect(ifMatchOk(parseIfMatch('*'), 0)).toBe(false);
    expect(ifMatchOk(parseIfMatch('*'), 1)).toBe(true);
  });

  it('garbage is a BadIfMatchError (400)', () => {
    for (const h of ['3', '"x"', '"3', 'abc', '"1",garbage']) expect(() => parseIfMatch(h)).toThrow(BadIfMatchError);
  });

  it('ETag is the quoted version', () => {
    expect(etagOf(5)).toBe('"5"');
  });
});
