import { DEFAULT_BROKERS, envBool, envEnum, envInt, loadKafkaClientConfig } from './env.js';

describe('env parsers', () => {
  it('envInt uses default, parses, and validates', () => {
    expect(envInt({}, 'X', 5)).toBe(5);
    expect(envInt({ X: '7' }, 'X', 5)).toBe(7);
    expect(() => envInt({ X: '1.5' }, 'X', 5)).toThrow(/integer/);
    expect(() => envInt({ X: '-1' }, 'X', 5, 0)).toThrow(/>= 0/);
    expect(() => envInt({ X: 'abc' }, 'X', 5)).toThrow(/number/);
  });

  it('envBool accepts true/false/1/0 only', () => {
    expect(envBool({}, 'X', true)).toBe(true);
    expect(envBool({ X: 'false' }, 'X', true)).toBe(false);
    expect(envBool({ X: '1' }, 'X', false)).toBe(true);
    expect(() => envBool({ X: 'yes' }, 'X', false)).toThrow();
  });

  it('envEnum rejects unknown values', () => {
    expect(envEnum({ X: 'b' }, 'X', ['a', 'b'] as const, 'a')).toBe('b');
    expect(() => envEnum({ X: 'c' }, 'X', ['a', 'b'] as const, 'a')).toThrow(/one of/);
  });

  it('loadKafkaClientConfig has host defaults and parses the broker list', () => {
    expect(loadKafkaClientConfig({}).brokers).toEqual(DEFAULT_BROKERS);
    expect(loadKafkaClientConfig({ KAFKA_BROKERS: 'a:1, b:2' }).brokers).toEqual(['a:1', 'b:2']);
    expect(loadKafkaClientConfig({}, 'ingest-api').clientId).toBe('ingest-api');
  });
});
