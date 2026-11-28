// Env for tools/dlq-replay. Defaults match the dev pipeline (services/consumer-sink/.env for the database).
//
//   DLQ_REPLAY_DATABASE_URL | DATABASE_URL   Postgres with dlq_meta (default: DATABASE_URL of services/consumer-sink/.env)
//   KAFKA_BROKERS ...                        packages/kafka-config
//   DLQ_TOPIC                                topic `reindex` scans (default events.dlq)
//   DLQ_MAX_REPLAYS                          loop guard: max times one event is re-injected (default 3)
//   DLQ_REPLAY_RATE                          default --rate, records/s (default 100, 0 = unlimited)
//   SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_TIMEOUT_MS, SCHEMA_SUBJECT, SINK_ACCEPT_JSON   pre-check, as the sink
//   DLQ_REPLAY_TEXTFILE                      Prometheus textfile of the last executed replay
//                                            (default infra/compose/textfile/dlq_replay.prom, "off" = none)
//   DLQ_REPLAY_OPERATOR                      recorded in dlq_replays.operator (default $USER)
//   DEBUG_FAULTS, FAULTS                     crash point replay:after-produce-before-mark (tests)

import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { TOPIC_EVENTS_DLQ, loadKafkaClientConfig } from '@eventpulse/kafka-config';
import type { KafkaClientConfig } from '@eventpulse/kafka-config';
import type { PreCheckConfig } from './precheck.ts';

export const REPO = resolve(import.meta.dirname, '../../..');

export interface ReplayConfig {
  databaseUrl: string;
  kafka: KafkaClientConfig;
  dlqTopic: string;
  maxReplays: number;
  rate: number;
  precheck: PreCheckConfig;
  textfile: string | null;
  operator: string;
}

function envFileValue(path: string, key: string): string | undefined {
  if (!existsSync(path)) return undefined;
  const line = readFileSync(path, 'utf8').split('\n').find((l) => l.startsWith(`${key}=`));
  return line?.slice(key.length + 1).trim().replace(/^["']|["']$/g, '');
}

function int(env: Record<string, string | undefined>, key: string, def: number, min: number): number {
  const v = env[key];
  if (v === undefined || v === '') return def;
  const n = Number(v);
  if (!Number.isInteger(n) || n < min) throw new Error(`${key} must be an integer >= ${min} (got "${v}")`);
  return n;
}

export function loadReplayConfig(env: Record<string, string | undefined> = process.env): ReplayConfig {
  const databaseUrl = env.DLQ_REPLAY_DATABASE_URL || env.DATABASE_URL || envFileValue(resolve(REPO, 'services/consumer-sink/.env'), 'DATABASE_URL');
  if (!databaseUrl) throw new Error('No database URL: set DLQ_REPLAY_DATABASE_URL or DATABASE_URL in services/consumer-sink/.env');
  const textfile = env.DLQ_REPLAY_TEXTFILE ?? resolve(REPO, 'infra/compose/textfile/dlq_replay.prom');
  return {
    databaseUrl,
    kafka: { ...loadKafkaClientConfig(env, 'dlq-replay'), logLevel: (env.KAFKA_LOG_LEVEL as KafkaClientConfig['logLevel']) || 'error' },
    dlqTopic: env.DLQ_TOPIC || TOPIC_EVENTS_DLQ,
    maxReplays: int(env, 'DLQ_MAX_REPLAYS', 3, 1),
    rate: int(env, 'DLQ_REPLAY_RATE', 100, 0),
    precheck: {
      registryUrl: env.SCHEMA_REGISTRY_URL || 'http://localhost:8081',
      registryTimeoutMs: int(env, 'SCHEMA_REGISTRY_TIMEOUT_MS', 2000, 1),
      subject: env.SCHEMA_SUBJECT || 'events.raw-value',
      acceptJson: !/^(0|false|no|off)$/i.test(env.SINK_ACCEPT_JSON ?? 'true'),
    },
    textfile: textfile === '' || textfile === 'off' ? null : textfile,
    operator: env.DLQ_REPLAY_OPERATOR || env.USER || 'unknown',
  };
}
