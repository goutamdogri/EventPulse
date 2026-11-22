// Publishes dead letters to events.dlq (S2-F4).
//
// - Value = the ORIGINAL raw bytes of the record (not re-encoded), key = the original key, so what failed can be
//   inspected exactly (scripts/dlq-peek.sh).
// - Headers = all original headers + dlq.* headers describing where it came from and why it failed.
// - Dedicated idempotent producer with acks=all. The batch handler waits for the ack before committing the
//   source offset; if the publish fails, the source offset is not committed and the chunk is retried
//   (so a DLQ record can be duplicated; noted for Stage 3).

import { createKafka } from '@eventpulse/kafka-config';
import type { KafkaClientConfig, Producer } from '@eventpulse/kafka-config';
import type { ErrorClass } from './errors.js';
import { truncate } from './errors.js';
import type { RawMessage } from './parse.js';

export interface DeadLetter {
  raw: RawMessage;
  errorClass: ErrorClass;
  errorCode: string;
  errorMessage: string;
  attempts: number;
}

export interface DlqContext {
  sourceTopic: string;
  groupId: string;
}

export const DLQ_HEADERS = {
  originalTopic: 'dlq.original_topic',
  originalPartition: 'dlq.original_partition',
  originalOffset: 'dlq.original_offset',
  consumerGroup: 'dlq.consumer_group',
  errorClass: 'dlq.error_class',
  errorCode: 'dlq.error_code',
  errorMessage: 'dlq.error_message',
  attempts: 'dlq.attempts',
  failedAt: 'dlq.failed_at',
} as const;

function originalHeaders(h: Record<string, unknown> | undefined): Record<string, string | Buffer> {
  const out: Record<string, string | Buffer> = {};
  for (const [k, v] of Object.entries(h ?? {})) {
    const one = Array.isArray(v) ? v[0] : v;
    if (Buffer.isBuffer(one) || typeof one === 'string') out[k] = one;
    else if (one !== undefined && one !== null) out[k] = String(one);
  }
  return out;
}

/** Builds the DLQ record for one dead letter (pure, unit-tested). */
export function toDlqMessage(d: DeadLetter, ctx: DlqContext, now = new Date()) {
  return {
    key: d.raw.key ?? null,
    value: d.raw.value ?? null,
    headers: {
      ...originalHeaders(d.raw.headers),
      [DLQ_HEADERS.originalTopic]: ctx.sourceTopic,
      [DLQ_HEADERS.originalPartition]: String(d.raw.partition),
      [DLQ_HEADERS.originalOffset]: d.raw.offset,
      [DLQ_HEADERS.consumerGroup]: ctx.groupId,
      [DLQ_HEADERS.errorClass]: d.errorClass,
      [DLQ_HEADERS.errorCode]: d.errorCode,
      [DLQ_HEADERS.errorMessage]: truncate(d.errorMessage, 1024),
      [DLQ_HEADERS.attempts]: String(d.attempts),
      [DLQ_HEADERS.failedAt]: now.toISOString(),
    },
  };
}

export interface DlqSink {
  publish(letters: DeadLetter[]): Promise<void>;
}

export class DlqPublisher implements DlqSink {
  private readonly producer: Producer;

  constructor(
    kafka: KafkaClientConfig,
    private readonly dlqTopic: string,
    private readonly ctx: DlqContext,
  ) {
    this.producer = createKafka({ ...kafka, clientId: `${kafka.clientId}-dlq` }).producer({
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

  /** Resolves only when every dead letter is acknowledged by Kafka (acks=all). */
  async publish(letters: DeadLetter[]): Promise<void> {
    if (letters.length === 0) return;
    const now = new Date();
    await this.producer.send({
      topic: this.dlqTopic,
      messages: letters.map((d) => toDlqMessage(d, this.ctx, now)) as any,
    });
  }
}
