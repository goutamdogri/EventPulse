import { Global, Module } from '@nestjs/common';
import {
  KEY_MODES,
  TOPIC_EVENTS_RAW,
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
  /** 0 = unbounded mode (K10): also no byte bound, no breaker, no send timeout / queue expiry. */
  maxQueue: number;
  /** 0 = unbounded. */
  maxQueueBytes: number;
  maxInFlight: number;
  /** Per-send timeout; also the cap on queue wait. */
  sendTimeoutMs: number;
}

export interface BreakerConfig {
  /** Consecutive failed sends that open the breaker. 0 = breaker disabled. */
  failures: number;
  cooldownMs: number;
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
  breaker: BreakerConfig;
  /** Max wait for the first Kafka connection at startup before the API starts anyway and keeps retrying. */
  startupWaitMs: number;
  reconnectInitialMs: number;
  reconnectMaxMs: number;
  /** On SIGTERM, how long drain() may wait for Kafka before the remaining requests get 503. */
  shutdownDrainTimeoutMs: number;
  /** librdkafka statistics interval for the client metrics (S2-F6). 0 = off. */
  statsIntervalMs: number;
  /** Background Kafka health check (drives readiness and the breaker). intervalMs 0 = off. */
  health: { intervalMs: number; timeoutMs: number; minIsr: number };
  keyMode: KeyMode;
  /** events.raw unless overridden (KAFKA_TOPIC); e2e tests write to a throw-away topic, never to events.raw. */
  topic: string;
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
    maxQueue: envInt(env, 'BATCH_MAX_QUEUE', 10_000, 0),
    maxQueueBytes: envInt(env, 'BATCH_MAX_QUEUE_BYTES', 64 * 1024 * 1024, 0),
    maxInFlight: envInt(env, 'BATCH_MAX_IN_FLIGHT', 1, 1),
    sendTimeoutMs: envInt(env, 'KAFKA_SEND_TIMEOUT_MS', 5_000, 0),
  };
  const unbounded = batcher.maxQueue === 0;
  if (unbounded) {
    // K10 compares a truly unbounded queue with the bounded one. Every bound must go, including the send
    // timeout: it also expires queued messages, which would silently cap the queue at about rate x timeout.
    batcher.maxQueueBytes = 0;
    batcher.sendTimeoutMs = 0;
    console.warn(
      '[config] WARNING: BATCH_MAX_QUEUE=0 = UNBOUNDED mode: no queue limit (count or bytes), no circuit breaker, ' +
        'no send timeout / queue expiry. Memory is NOT bounded while Kafka is down. Use only for experiment K10.',
    );
  }
  const breaker: BreakerConfig = {
    failures: unbounded ? 0 : envInt(env, 'KAFKA_BREAKER_FAILURES', 3, 0),
    cooldownMs: envInt(env, 'KAFKA_BREAKER_COOLDOWN_MS', 5_000, 1),
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
    breaker,
    startupWaitMs: envInt(env, 'KAFKA_STARTUP_WAIT_MS', 10_000, 0),
    reconnectInitialMs: envInt(env, 'KAFKA_RECONNECT_INITIAL_MS', 500, 1),
    reconnectMaxMs: envInt(env, 'KAFKA_RECONNECT_MAX_MS', 10_000, 1),
    shutdownDrainTimeoutMs: envInt(env, 'SHUTDOWN_DRAIN_TIMEOUT_MS', 10_000, 0),
    statsIntervalMs: envInt(env, 'KAFKA_STATS_INTERVAL_MS', 5_000, 0),
    health: {
      intervalMs: envInt(env, 'KAFKA_HEALTH_INTERVAL_MS', 2_000, 0),
      timeoutMs: envInt(env, 'KAFKA_HEALTH_TIMEOUT_MS', 2_000, 1),
      // acks=all needs min.insync.replicas (2) in-sync copies to accept a write; acks 0/1 only need a leader.
      minIsr: envInt(env, 'KAFKA_HEALTH_MIN_ISR', acks === -1 ? 2 : 1, 1),
    },
    keyMode: envEnum(env, 'KAFKA_KEY_MODE', KEY_MODES, 'tenant'),
    topic: env.KAFKA_TOPIC || TOPIC_EVENTS_RAW,
  };
}

@Global()
@Module({
  providers: [{ provide: CONFIG, useFactory: () => loadConfig() }],
  exports: [CONFIG],
})
export class ConfigModule {}
