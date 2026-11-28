// Publishes records to the retry tiers (S3-F5): original key and value bytes, original headers + retry.* headers
// (packages/retry router). Idempotent producer, acks=all: the source offset only moves after the ack.
import { createKafka } from '@eventpulse/kafka-config';
import type { KafkaClientConfig, Producer } from '@eventpulse/kafka-config';
import { retryHeaders } from '@eventpulse/retry';
import type { Failure, Origin, Route } from '@eventpulse/retry';
import type { RawMessage } from '../sink/parse.js';

export interface RetryLetter {
  raw: RawMessage;
  route: Extract<Route, { kind: 'retry' }>;
  origin: Origin;
  failure: Failure;
}

export interface RetrySink {
  publish(letters: RetryLetter[]): Promise<void>;
}

export function toRetryMessage(l: RetryLetter) {
  return { key: l.raw.key ?? null, value: l.raw.value ?? null, headers: retryHeaders(l.raw.headers, l.route, l.origin, l.failure) };
}

export class RetryPublisher implements RetrySink {
  private readonly producer: Producer;

  constructor(kafka: KafkaClientConfig) {
    this.producer = createKafka({ ...kafka, clientId: `${kafka.clientId}-retry` }).producer({
      kafkaJS: { acks: -1, idempotent: true, allowAutoTopicCreation: false },
      'linger.ms': 0,
      'message.timeout.ms': 15_000,
    } as any);
  }

  connect() {
    return this.producer.connect();
  }

  disconnect() {
    return this.producer.disconnect();
  }

  async publish(letters: RetryLetter[]): Promise<void> {
    if (letters.length === 0) return;
    const byTopic = new Map<string, RetryLetter[]>();
    for (const l of letters) byTopic.set(l.route.topic, [...(byTopic.get(l.route.topic) ?? []), l]);
    await Promise.all([...byTopic].map(([topic, ls]) => this.producer.send({ topic, messages: ls.map(toRetryMessage) as any })));
  }
}
