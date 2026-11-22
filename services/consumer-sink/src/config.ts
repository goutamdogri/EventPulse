import { Global, Module } from '@nestjs/common';
import {
  GROUP_SINK_POSTGRES,
  envBool,
  envEnum,
  envInt,
  loadKafkaClientConfig,
} from '@eventpulse/kafka-config';
import type { KafkaClientConfig } from '@eventpulse/kafka-config';

export const CONFIG = Symbol('CONFIG');

export const ASSIGNORS = ['roundRobin', 'range', 'cooperativeSticky'] as const;
export type Assignor = (typeof ASSIGNORS)[number];

export interface ConsumerConfig {
  groupId: string;
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
  sink: { maxRows: number };
}

// Called from a provider factory (not at import time) so tests can set env first.
export function loadConfig(env: Record<string, string | undefined> = process.env): AppConfig {
  const databaseUrl = env.DATABASE_URL;
  if (!databaseUrl) throw new Error('DATABASE_URL is required');

  const consumer: ConsumerConfig = {
    groupId: env.CONSUMER_GROUP_ID || GROUP_SINK_POSTGRES,
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

  return {
    port: envInt(env, 'METRICS_PORT', 8001, 0),
    databaseUrl,
    pgPoolMax,
    pgConnTimeoutMs: envInt(env, 'PG_CONN_TIMEOUT_MS', 2000, 0),
    pgStatementTimeoutMs: envInt(env, 'PG_STATEMENT_TIMEOUT_MS', 10_000, 0),
    kafka: loadKafkaClientConfig(env, 'consumer-sink'),
    consumer,
    sink: { maxRows: envInt(env, 'SINK_MAX_ROWS', 1000, 1) },
  };
}

@Global()
@Module({
  providers: [{ provide: CONFIG, useFactory: () => loadConfig() }],
  exports: [CONFIG],
})
export class ConfigModule {}
