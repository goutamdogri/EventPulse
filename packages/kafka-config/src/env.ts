// Small env parsers shared by every service, plus the Kafka client settings both services use.
// All functions take the env object as a parameter (default process.env) so tests can pass their own.

export type Env = Record<string, string | undefined>;

export function envNum(env: Env, key: string, def: number): number {
  const v = env[key];
  if (v === undefined || v === '') return def;
  const n = Number(v);
  if (!Number.isFinite(n)) throw new Error(`${key} must be a number, got "${v}"`);
  return n;
}

export function envInt(env: Env, key: string, def: number, min = Number.MIN_SAFE_INTEGER): number {
  const n = envNum(env, key, def);
  if (!Number.isInteger(n)) throw new Error(`${key} must be an integer, got "${n}"`);
  if (n < min) throw new Error(`${key} must be >= ${min}, got ${n}`);
  return n;
}

export function envBool(env: Env, key: string, def: boolean): boolean {
  const v = env[key];
  if (v === undefined || v === '') return def;
  if (v === 'true' || v === '1') return true;
  if (v === 'false' || v === '0') return false;
  throw new Error(`${key} must be true or false, got "${v}"`);
}

export function envEnum<T extends string>(env: Env, key: string, allowed: readonly T[], def: T): T {
  const v = env[key];
  if (v === undefined || v === '') return def;
  if (!(allowed as readonly string[]).includes(v)) {
    throw new Error(`${key} must be one of ${allowed.join(', ')}, got "${v}"`);
  }
  return v as T;
}

export function envList(env: Env, key: string, def: string[]): string[] {
  const v = env[key];
  if (v === undefined || v === '') return def;
  const list = v.split(',').map((s) => s.trim()).filter(Boolean);
  if (list.length === 0) throw new Error(`${key} must contain at least one entry`);
  return list;
}

export interface KafkaClientConfig {
  brokers: string[];
  clientId: string;
  /** connectionTimeout + authenticationTimeout are enforced together by the Confluent client. */
  connectionTimeoutMs: number;
  /** Network request timeout. */
  requestTimeoutMs: number;
  /** Retries for produce requests and connect() (librdkafka does the retrying). */
  retries: number;
  initialRetryTimeMs: number;
  maxRetryTimeMs: number;
  logLevel: 'nothing' | 'error' | 'warn' | 'info' | 'debug';
}

export const DEFAULT_BROKERS = ['localhost:9092', 'localhost:9094', 'localhost:9096'];

export function loadKafkaClientConfig(env: Env = process.env, defaultClientId = 'eventpulse'): KafkaClientConfig {
  return {
    brokers: envList(env, 'KAFKA_BROKERS', DEFAULT_BROKERS),
    clientId: env.KAFKA_CLIENT_ID || defaultClientId,
    connectionTimeoutMs: envInt(env, 'KAFKA_CONNECTION_TIMEOUT_MS', 3000, 1),
    requestTimeoutMs: envInt(env, 'KAFKA_REQUEST_TIMEOUT_MS', 30000, 1),
    retries: envInt(env, 'KAFKA_RETRIES', 5, 0),
    initialRetryTimeMs: envInt(env, 'KAFKA_RETRY_INITIAL_MS', 100, 1),
    maxRetryTimeMs: envInt(env, 'KAFKA_RETRY_MAX_MS', 1000, 1),
    logLevel: envEnum(env, 'KAFKA_LOG_LEVEL', ['nothing', 'error', 'warn', 'info', 'debug'] as const, 'warn'),
  };
}
