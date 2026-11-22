import { Global, Module } from '@nestjs/common';
import {
  GROUP_SINK_POSTGRES,
  TOPIC_EVENTS_DLQ,
  TOPIC_EVENTS_RAW,
  envBool,
  envEnum,
  envInt,
  loadKafkaClientConfig,
} from '@eventpulse/kafka-config';
import type { KafkaClientConfig } from '@eventpulse/kafka-config';
import { CRASH_POINTS } from './consumer/fault-injector.js';
import type { FaultConfig } from './consumer/fault-injector.js';

export const CONFIG = Symbol('CONFIG');

export const ASSIGNORS = ['roundRobin', 'range', 'cooperativeSticky'] as const;
export type Assignor = (typeof ASSIGNORS)[number];

export interface ConsumerConfig {
  groupId: string;
  /** events.raw unless overridden (CONSUMER_TOPIC); tests use a throw-away topic per run. */
  topic: string;
  /** true = 'earliest' when the group has no committed offset (a new group does not skip data). */
  fromBeginning: boolean;
  sessionTimeoutMs: number;
  heartbeatIntervalMs: number;
  /** Max time to rejoin on a rebalance; with this client it also sets the max processing time per batch. */
  rebalanceTimeoutMs: number;
  /** Bounded prefetch: bytes fetched per partition / per request. */
  maxBytesPerPartition: number;
  maxBytes: number;
  maxWaitMs: number;
  /** Max messages handed to one eachBatch call (librdkafka-side 'js.consumer.max.batch.size'). */
  maxBatchSize: number;
  partitionsConcurrently: number;
  assignor: Assignor;
}

export interface AppConfig {
  port: number;
  databaseUrl: string;
  pgPoolMax: number;
  pgConnTimeoutMs: number;
  pgStatementTimeoutMs: number;
  kafka: KafkaClientConfig;
  consumer: ConsumerConfig;
  sink: { maxRows: number; processingDelayMs: number };
  /** How often the group lag is polled for /metrics and /debug/stats (S2-F6). 0 = off. */
  lagPollMs: number;
  /** S2-F4 poison handling. */
  retry: {
    dlqEnabled: boolean;
    dlqTopic: string;
    maxRetries: number;
    baseMs: number;
    maxMs: number;
    transientWindowMs: number;
    pauseMs: number;
  };
  faults: FaultConfig;
}

// Called from a provider factory (not at import time) so tests can set env first.
export function loadConfig(env: Record<string, string | undefined> = process.env): AppConfig {
  const databaseUrl = env.DATABASE_URL;
  if (!databaseUrl) throw new Error('DATABASE_URL is required');

  const consumer: ConsumerConfig = {
    groupId: env.CONSUMER_GROUP_ID || GROUP_SINK_POSTGRES,
    topic: env.CONSUMER_TOPIC || TOPIC_EVENTS_RAW,
    fromBeginning: envBool(env, 'CONSUMER_FROM_BEGINNING', true),
    sessionTimeoutMs: envInt(env, 'CONSUMER_SESSION_TIMEOUT_MS', 45_000, 1),
    heartbeatIntervalMs: envInt(env, 'CONSUMER_HEARTBEAT_INTERVAL_MS', 3_000, 1),
    rebalanceTimeoutMs: envInt(env, 'CONSUMER_REBALANCE_TIMEOUT_MS', 60_000, 1),
    maxBytesPerPartition: envInt(env, 'CONSUMER_MAX_BYTES_PER_PARTITION', 1_048_576, 1),
    maxBytes: envInt(env, 'CONSUMER_MAX_BYTES', 10_485_760, 1),
    maxWaitMs: envInt(env, 'CONSUMER_MAX_WAIT_MS', 500, 0),
    maxBatchSize: envInt(env, 'CONSUMER_MAX_BATCH_SIZE', 1000, 1),
    partitionsConcurrently: envInt(env, 'CONSUMER_PARTITIONS_CONCURRENTLY', 1, 1),
    assignor: envEnum(env, 'CONSUMER_ASSIGNOR', ASSIGNORS, 'roundRobin'),
  };
  if (consumer.heartbeatIntervalMs >= consumer.sessionTimeoutMs) {
    throw new Error('CONSUMER_HEARTBEAT_INTERVAL_MS must be lower than CONSUMER_SESSION_TIMEOUT_MS');
  }

  const pgPoolMax = envInt(env, 'PG_POOL_MAX', 5, 1);
  if (consumer.partitionsConcurrently > pgPoolMax) {
    // Each concurrently processed partition holds one connection during its transaction.
    throw new Error('CONSUMER_PARTITIONS_CONCURRENTLY must be <= PG_POOL_MAX');
  }

  const faults: FaultConfig = {
    crashAt: envEnum(env, 'CRASH_AT', CRASH_POINTS, 'none'),
    crashAfterBatches: envInt(env, 'CRASH_AFTER_BATCHES', 1, 1),
  };
  if (faults.crashAt !== 'none' && !envBool(env, 'DEBUG_FAULTS', false)) {
    throw new Error(`CRASH_AT=${faults.crashAt} needs DEBUG_FAULTS=true (crash points are debug-only)`);
  }

  const retry = {
    dlqEnabled: envBool(env, 'SINK_DLQ_ENABLED', true),
    dlqTopic: env.SINK_DLQ_TOPIC || TOPIC_EVENTS_DLQ,
    maxRetries: envInt(env, 'SINK_MAX_RETRIES', 3, 0),
    baseMs: envInt(env, 'SINK_RETRY_BASE_MS', 100, 1),
    maxMs: envInt(env, 'SINK_RETRY_MAX_MS', 5_000, 1),
    transientWindowMs: envInt(env, 'SINK_TRANSIENT_RETRY_WINDOW_MS', 20_000, 0),
    pauseMs: envInt(env, 'SINK_PAUSE_MS', 5_000, 1),
  };
  if (retry.baseMs > retry.maxMs) throw new Error('SINK_RETRY_BASE_MS must be <= SINK_RETRY_MAX_MS');
  if (retry.transientWindowMs + retry.maxMs >= consumer.rebalanceTimeoutMs) {
    // One eachBatch call must finish within the rebalance timeout (it is also the max poll interval).
    throw new Error('SINK_TRANSIENT_RETRY_WINDOW_MS + SINK_RETRY_MAX_MS must be < CONSUMER_REBALANCE_TIMEOUT_MS');
  }

  return {
    port: envInt(env, 'METRICS_PORT', 8001, 0),
    databaseUrl,
    pgPoolMax,
    pgConnTimeoutMs: envInt(env, 'PG_CONN_TIMEOUT_MS', 2000, 0),
    pgStatementTimeoutMs: envInt(env, 'PG_STATEMENT_TIMEOUT_MS', 10_000, 0),
    kafka: loadKafkaClientConfig(env, 'consumer-sink'),
    consumer,
    sink: {
      maxRows: envInt(env, 'SINK_MAX_ROWS', 1000, 1),
      processingDelayMs: envInt(env, 'SINK_PROCESSING_DELAY_MS', 0, 0),
    },
    faults,
    retry,
    lagPollMs: envInt(env, 'LAG_POLL_MS', 2_000, 0),
  };
}

@Global()
@Module({
  providers: [{ provide: CONFIG, useFactory: () => loadConfig() }],
  exports: [CONFIG],
})
export class ConfigModule {}
