// outbox-relay env (S3-F4).
//
//   OUTBOX_RELAY_ENABLED       false = the process serves /healthz and /metrics but publishes nothing (default true)
//   DATABASE_URL               Postgres with the outbox table (same database as admin-api)
//   PORT                       HTTP: /healthz /readyz /metrics /debug/stats /debug/faults (default 8021)
//   RELAY_MODE                 single (default: one active relay, pg advisory lock) | multi (no lock: C8 only)
//   RELAY_LOCK_KEY             advisory lock key of the single active relay
//   RELAY_BATCH_SIZE           rows per cycle (default 100)
//   RELAY_POLL_MS              poll interval when idle (default 200); LISTEN outbox_new wakes it earlier
//   RELAY_LISTEN               LISTEN outbox_new (default true)
//   RELAY_STANDBY_POLL_MS      how often a standby tries the lock (default 1000)
//   RELAY_BACKOFF_INITIAL_MS / RELAY_BACKOFF_MAX_MS   after a failed cycle (Kafka/Postgres down), with jitter
//   RELAY_SEND_TIMEOUT_MS      librdkafka message.timeout.ms (default 10000)
//   RELAY_TOPICS               only rows of these topics (comma list; tests use it). Empty = all
//   RELAY_RETENTION_MS         published rows older than this are deleted (default 7 days, 0 = never)
//   RELAY_RETENTION_INTERVAL_MS / RELAY_RETENTION_BATCH
//   OUTBOX_GAUGE_POLL_MS       how often ep_outbox_unpublished / oldest age are read (default 2000)
//   KAFKA_*                    packages/kafka-config
//   DEBUG_FAULTS, FAULTS, FAULT_ERRORS   crash points relay:* (tests)
import { envBool, envEnum, envInt, envList, loadKafkaClientConfig } from '@eventpulse/kafka-config';
import type { KafkaClientConfig } from '@eventpulse/kafka-config';
import { loadFaultConfig } from '@eventpulse/fault-injector';
import type { FaultConfig } from '@eventpulse/fault-injector';
import { DEFAULT_RELAY_OPTIONS, RELAY_MODES } from '@eventpulse/outbox';
import type { RelayOptions } from '@eventpulse/outbox';

export interface RelayConfig {
  enabled: boolean;
  databaseUrl: string;
  port: number;
  relay: RelayOptions;
  kafka: KafkaClientConfig;
  sendTimeoutMs: number;
  gaugePollMs: number;
  logEnabled: boolean;
  faults: FaultConfig;
}

export function loadConfig(env: Record<string, string | undefined> = process.env): RelayConfig {
  const databaseUrl = env.DATABASE_URL;
  if (!databaseUrl) throw new Error('DATABASE_URL is required');
  const d = DEFAULT_RELAY_OPTIONS;
  const relay: RelayOptions = {
    batchSize: envInt(env, 'RELAY_BATCH_SIZE', d.batchSize, 1),
    pollMs: envInt(env, 'RELAY_POLL_MS', d.pollMs, 1),
    mode: envEnum(env, 'RELAY_MODE', RELAY_MODES, d.mode),
    lockKey: envInt(env, 'RELAY_LOCK_KEY', d.lockKey),
    standbyPollMs: envInt(env, 'RELAY_STANDBY_POLL_MS', d.standbyPollMs, 1),
    topics: envList(env, 'RELAY_TOPICS', []),
    backoffInitialMs: envInt(env, 'RELAY_BACKOFF_INITIAL_MS', d.backoffInitialMs, 1),
    backoffMaxMs: envInt(env, 'RELAY_BACKOFF_MAX_MS', d.backoffMaxMs, 1),
    listen: envBool(env, 'RELAY_LISTEN', d.listen),
    retentionMs: envInt(env, 'RELAY_RETENTION_MS', d.retentionMs, 0),
    retentionIntervalMs: envInt(env, 'RELAY_RETENTION_INTERVAL_MS', d.retentionIntervalMs, 1),
    retentionBatch: envInt(env, 'RELAY_RETENTION_BATCH', d.retentionBatch, 1),
  };
  if (relay.backoffMaxMs < relay.backoffInitialMs) throw new Error('RELAY_BACKOFF_MAX_MS must be >= RELAY_BACKOFF_INITIAL_MS');
  if (relay.mode === 'multi') console.warn('[config] RELAY_MODE=multi: several relays may publish one tenant\'s changes OUT OF ORDER (experiment C8 only)');
  return {
    enabled: envBool(env, 'OUTBOX_RELAY_ENABLED', true),
    databaseUrl,
    port: envInt(env, 'PORT', 8021, 0),
    relay,
    kafka: { ...loadKafkaClientConfig(env, 'outbox-relay'), logLevel: (env.KAFKA_LOG_LEVEL as KafkaClientConfig['logLevel']) || 'error' },
    sendTimeoutMs: envInt(env, 'RELAY_SEND_TIMEOUT_MS', 10_000, 1),
    gaugePollMs: envInt(env, 'OUTBOX_GAUGE_POLL_MS', 2_000, 100),
    logEnabled: env.LOG_ENABLED === 'true',
    faults: loadFaultConfig(env),
  };
}
