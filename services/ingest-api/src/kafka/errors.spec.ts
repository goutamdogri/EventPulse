import { KafkaUnavailableError, OverloadedError, classifySendError, isKafkaUnavailable } from './errors.js';

describe('isKafkaUnavailable', () => {
  const unavailable: [string, unknown][] = [
    ['librdkafka transport', { code: -195, message: 'Local: Broker transport failure' }],
    ['all brokers down', { code: -187, message: 'x' }],
    ['message timed out', { code: -192, message: 'Local: Message timed out' }],
    ['producer not connected', { code: -172, message: 'Cannot send without awaiting connect()' }],
    ['leader not available', { code: 5, message: 'x' }],
    ['not enough replicas', { code: 19, message: 'Broker: Not enough in-sync replicas' }],
    ['not enough replicas after append', { code: 20, message: 'x' }],
    ['broker not available', { code: 8, message: 'x' }],
    ['kafkajs connection error by name', { name: 'KafkaJSConnectionError', message: 'x' }],
    ['kafkajs retries exhausted by name', { name: 'KafkaJSNumberOfRetriesExceeded', message: 'x' }],
    ['node ECONNREFUSED', { code: 'ECONNREFUSED', message: 'x' }],
    ['text only', new Error('connect ECONNREFUSED 127.0.0.1:9092')],
    ['own error', new KafkaUnavailableError('x')],
  ];
  it.each(unavailable)('%s -> unavailable', (_n, err) => expect(isKafkaUnavailable(err)).toBe(true));

  const other: [string, unknown][] = [
    ['plain bug', new TypeError('x is undefined')],
    ['message too large (broker rejects the record)', { code: 10, message: 'Broker: Message size too large' }],
    ['invalid arg', { code: -186, message: 'bad' }],
    ['overloaded is not a Kafka problem', new OverloadedError()],
    ['null', null],
    ['string', 'boom'],
  ];
  it.each(other)('%s -> not unavailable', (_n, err) => expect(isKafkaUnavailable(err)).toBe(false));

  it('classifySendError wraps unavailable errors and keeps the rest', () => {
    const raw = { code: -195, message: 'Broker transport failure' };
    const wrapped = classifySendError(raw);
    expect(wrapped).toBeInstanceOf(KafkaUnavailableError);
    expect((wrapped as KafkaUnavailableError).cause).toBe(raw);
    const bug = new TypeError('x');
    expect(classifySendError(bug)).toBe(bug);
  });
});
