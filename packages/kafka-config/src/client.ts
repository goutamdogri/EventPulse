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
