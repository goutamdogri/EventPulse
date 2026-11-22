// The only module (besides the services' kafka/ folders) that imports the Kafka client library.
// We use the KafkaJS-compatible API of @confluentinc/kafka-javascript (librdkafka underneath).
// See docs/adr/0001-kafka-client-library.md.

import Confluent from '@confluentinc/kafka-javascript';
import type { KafkaJS as KafkaJSTypes } from '@confluentinc/kafka-javascript';
import type { KafkaClientConfig } from './env.js';

export const KafkaJS = Confluent.KafkaJS;
export type Kafka = KafkaJSTypes.Kafka;
export type Producer = KafkaJSTypes.Producer;
export type ProducerConstructorConfig = KafkaJSTypes.ProducerConstructorConfig;
export type KafkaMessage = KafkaJSTypes.Message;
export type IHeaders = KafkaJSTypes.IHeaders;
export type Consumer = KafkaJSTypes.Consumer;
export type ConsumerConstructorConfig = KafkaJSTypes.ConsumerConstructorConfig;
export type EachBatchPayload = KafkaJSTypes.EachBatchPayload;

/** librdkafka error codes, e.g. ERR__ASSIGN_PARTITIONS (-175) / ERR__REVOKE_PARTITIONS (-174) in rebalance_cb. */
export const RdKafkaErrors = Confluent.CODES.ERRORS;

const LOG_LEVELS = {
  nothing: KafkaJS.logLevel.NOTHING,
  error: KafkaJS.logLevel.ERROR,
  warn: KafkaJS.logLevel.WARN,
  info: KafkaJS.logLevel.INFO,
  debug: KafkaJS.logLevel.DEBUG,
} as const;

export function createKafka(cfg: KafkaClientConfig): Kafka {
  return new KafkaJS.Kafka({
    kafkaJS: {
      brokers: cfg.brokers,
      clientId: cfg.clientId,
      connectionTimeout: cfg.connectionTimeoutMs,
      requestTimeout: cfg.requestTimeoutMs,
      retry: {
        retries: cfg.retries,
        initialRetryTime: cfg.initialRetryTimeMs,
        maxRetryTime: cfg.maxRetryTimeMs,
      },
      logLevel: LOG_LEVELS[cfg.logLevel],
    },
  });
}

type AdminLike = { fetchTopicMetadata(o: { topics: string[] }): Promise<any> };

/**
 * Wait until every partition of a just-created topic has a leader and a full ISR, then a short settle time.
 * Producing into a topic in its first moments lost acknowledged records in our tests (see notebook/fix/step-1.md,
 * 1.4); real topics are created by kafka-init long before any client writes to them.
 */
export async function waitTopicReady(admin: AdminLike, topic: string, partitions: number, settleMs = 2000, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      const md = await admin.fetchTopicMetadata({ topics: [topic] });
      const ps: any[] = md?.[0]?.partitions ?? [];
      if (ps.length === partitions && ps.every((p) => p.leader >= 0 && p.isr.length === p.replicas.length)) break;
    } catch {
      /* metadata not there yet */
    }
    if (Date.now() > deadline) throw new Error(`topic ${topic} not ready after ${timeoutMs} ms`);
    await new Promise((r) => setTimeout(r, 100));
  }
  await new Promise((r) => setTimeout(r, settleMs));
}
