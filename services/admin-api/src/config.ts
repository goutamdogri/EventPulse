// admin-api env (S3-F4). Separate service from ingest-api on purpose: the ingest hot path keeps ZERO dependency on
// Postgres, so its failure modes stay the ones measured in Stage 2. See README.md.
import { Global, Module } from '@nestjs/common';
import { envBool, envInt } from '@eventpulse/kafka-config';
import { loadFaultConfig } from '@eventpulse/fault-injector';
import type { FaultConfig } from '@eventpulse/fault-injector';
import { TOPIC_CONFIG_CHANGED } from '@eventpulse/outbox';

export const CONFIG = Symbol('CONFIG');

export interface AppConfig {
  port: number;
  /** Authorization: Bearer <ADMIN_API_KEY>. Separate from the tenant keys of ingest-api. */
  adminApiKey: string;
  databaseUrl: string;
  pgPoolMax: number;
  pgStatementTimeoutMs: number;
  bodyLimit: string;
  logEnabled: boolean;
  /** Topic of the config.changed outbox rows (tests use a throw-away one). */
  configTopic: string;
  /** NOTIFY outbox_new on enqueue so the relay publishes at once. */
  outboxNotify: boolean;
  schema: { registryUrl: string; registryTimeoutMs: number; initRetryMaxMs: number };
  retryAfterS: number;
  faults: FaultConfig;
}

export function loadConfig(env: Record<string, string | undefined> = process.env): AppConfig {
  const adminApiKey = env.ADMIN_API_KEY ?? '';
  if (adminApiKey.length < 16) throw new Error('ADMIN_API_KEY must be set (at least 16 characters)');
  const databaseUrl = env.DATABASE_URL;
  if (!databaseUrl) throw new Error('DATABASE_URL is required');
  return {
    port: envInt(env, 'PORT', 8020, 0),
    adminApiKey,
    databaseUrl,
    pgPoolMax: envInt(env, 'PG_POOL_MAX', 5, 1),
    pgStatementTimeoutMs: envInt(env, 'PG_STATEMENT_TIMEOUT_MS', 5_000, 0),
    bodyLimit: env.BODY_LIMIT || '16kb',
    logEnabled: env.LOG_ENABLED === 'true',
    configTopic: env.CONFIG_TOPIC || TOPIC_CONFIG_CHANGED,
    outboxNotify: envBool(env, 'OUTBOX_NOTIFY', true),
    schema: {
      registryUrl: env.SCHEMA_REGISTRY_URL || 'http://localhost:8081',
      registryTimeoutMs: envInt(env, 'SCHEMA_REGISTRY_TIMEOUT_MS', 2_000, 1),
      initRetryMaxMs: envInt(env, 'SCHEMA_INIT_RETRY_MAX_MS', 5_000, 1),
    },
    retryAfterS: envInt(env, 'RETRY_AFTER_S', 1, 0),
    faults: loadFaultConfig(env),
  };
}

@Global()
@Module({
  providers: [{ provide: CONFIG, useFactory: () => loadConfig() }],
  exports: [CONFIG],
})
export class ConfigModule {}
