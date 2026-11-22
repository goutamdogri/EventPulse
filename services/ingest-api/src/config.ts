import { Global, Module } from '@nestjs/common';
import {
  KEY_MODES,
  envBool,
  envEnum,
  envInt,
  loadKafkaClientConfig,
} from '@eventpulse/kafka-config';
import type { KafkaClientConfig, KeyMode } from '@eventpulse/kafka-config';

export const CONFIG = Symbol('CONFIG');

export const COMPRESSIONS = ['none', 'gzip', 'snappy', 'lz4', 'zstd'] as const;
export type Compression = (typeof COMPRESSIONS)[number];

/**
 * The broker's default message.max.bytes is 1048588 and applies to one record batch per partition.
 * Keeping our batch byte cap under 1 MB keeps us under it whatever the partition split is.
 */
export const MAX_BATCH_BYTES_LIMIT = 1_000_000;

export interface ProducerConfig {
  /** -1 = all in-sync replicas, 1 = leader only, 0 = no ack. Set per producer, not per send. */
  acks: -1 | 0 | 1;
  compression: Compression;
  idempotent: boolean;
  /** librdkafka's own batching delay. 0 by default so the micro-batcher alone decides the batching. */
  lingerMs: number;
  /** librdkafka message.timeout.ms: total time for a message to be delivered, retries included. */
  deliveryTimeoutMs: number;
  /** Broker-side ack timeout for a produce request. */
  ackTimeoutMs: number;
}

export interface BatcherConfig {
  maxMessages: number;
  maxWaitMs: number;
  maxBytes: number;
  maxQueue: number;
  maxInFlight: number;
}

export interface AppConfig {
  port: number;
  apiKeys: Map<string, string>; // key -> tenant
  bodyLimit: string;
  logEnabled: boolean;
  retryAfterS: number;
  kafka: KafkaClientConfig;
  producer: ProducerConfig;
  batcher: BatcherConfig;
  keyMode: KeyMode;
}

// Called from a provider factory (not at import time) so tests can set env first.
export function loadConfig(env: Record<string, string | undefined> = process.env): AppConfig {
  const apiKeys = new Map<string, string>();
  for (const pair of (env.API_KEYS ?? '').split(',').filter(Boolean)) {
    const [key, tenant] = pair.split(':').map((s) => s.trim());
    if (!key || !tenant) throw new Error(`Bad API_KEYS entry: "${pair}"`);
    apiKeys.set(key, tenant);
  }
  if (apiKeys.size === 0) throw new Error('API_KEYS must define at least one key:tenant');

  const acks = envInt(env, 'KAFKA_ACKS', -1);
  if (acks !== -1 && acks !== 0 && acks !== 1) throw new Error('KAFKA_ACKS must be -1, 0 or 1');
  const idempotent = envBool(env, 'KAFKA_IDEMPOTENT', true);
  if (idempotent && acks !== -1) {
    throw new Error('KAFKA_IDEMPOTENT=true requires KAFKA_ACKS=-1 (set KAFKA_IDEMPOTENT=false to try acks 0/1)');
  }

  const batcher: BatcherConfig = {
    maxMessages: envInt(env, 'BATCH_MAX_MESSAGES', 500, 1),
    maxWaitMs: envInt(env, 'BATCH_MAX_WAIT_MS', 5, 0),
    maxBytes: envInt(env, 'BATCH_MAX_BYTES', 900_000, 1),
    maxQueue: envInt(env, 'BATCH_MAX_QUEUE', 10_000, 1),
    maxInFlight: envInt(env, 'BATCH_MAX_IN_FLIGHT', 1, 1),
  };
  if (batcher.maxBytes > MAX_BATCH_BYTES_LIMIT) {
    throw new Error(`BATCH_MAX_BYTES must be <= ${MAX_BATCH_BYTES_LIMIT} (broker message.max.bytes)`);
  }

  return {
    port: envInt(env, 'PORT', 8000, 0),
    apiKeys,
    bodyLimit: env.BODY_LIMIT || '64kb',
    logEnabled: env.LOG_ENABLED === 'true',
    retryAfterS: envInt(env, 'RETRY_AFTER_S', 1, 0),
    kafka: loadKafkaClientConfig(env, 'ingest-api'),
    producer: {
      acks: acks as -1 | 0 | 1,
      compression: envEnum(env, 'KAFKA_COMPRESSION', COMPRESSIONS, 'none'),
      idempotent,
      lingerMs: envInt(env, 'KAFKA_LINGER_MS', 0, 0),
      deliveryTimeoutMs: envInt(env, 'KAFKA_DELIVERY_TIMEOUT_MS', 30_000, 1),
      ackTimeoutMs: envInt(env, 'KAFKA_ACK_TIMEOUT_MS', 30_000, 1),
    },
    batcher,
    keyMode: envEnum(env, 'KAFKA_KEY_MODE', KEY_MODES, 'tenant'),
  };
}

@Global()
@Module({
  providers: [{ provide: CONFIG, useFactory: () => loadConfig() }],
  exports: [CONFIG],
})
export class ConfigModule {}
