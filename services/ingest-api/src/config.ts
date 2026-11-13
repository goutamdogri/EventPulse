import { Global, Module } from '@nestjs/common';

export const CONFIG = Symbol('CONFIG');

export interface AppConfig {
  port: number;
  databaseUrl: string;
  apiKeys: Map<string, string>; // key -> tenant
  pgPoolMax: number;
  pgConnTimeoutMs: number;      // 0 = unset
  pgStatementTimeoutMs: number; // 0 = unset
  bodyLimit: string;
  logEnabled: boolean;
  retryAfterS: number;
}

const num = (k: string, d: number): number => {
  const v = process.env[k];
  if (v === undefined || v === '') return d;
  const n = Number(v);
  if (!Number.isFinite(n)) throw new Error(`${k} must be a number`);
  return n;
};

// Called from a provider factory (not at import time) so tests can set env first.
export function loadConfig(): AppConfig {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) throw new Error('DATABASE_URL is required');

  const apiKeys = new Map<string, string>();
  for (const pair of (process.env.API_KEYS ?? '').split(',').filter(Boolean)) {
    const [key, tenant] = pair.split(':').map((s) => s.trim());
    if (!key || !tenant) throw new Error(`Bad API_KEYS entry: "${pair}"`);
    apiKeys.set(key, tenant);
  }
  if (apiKeys.size === 0) throw new Error('API_KEYS must define at least one key:tenant');

  return {
    port: num('PORT', 8000),
    databaseUrl,
    apiKeys,
    pgPoolMax: num('PG_POOL_MAX', 10),
    pgConnTimeoutMs: num('PG_CONN_TIMEOUT_MS', 2000),
    pgStatementTimeoutMs: num('PG_STATEMENT_TIMEOUT_MS', 5000),
    bodyLimit: process.env.BODY_LIMIT ?? '64kb',
    logEnabled: process.env.LOG_ENABLED === 'true',
    retryAfterS: num('RETRY_AFTER_S', 1),
  };
}

@Global()
@Module({
  providers: [{ provide: CONFIG, useFactory: loadConfig }],
  exports: [CONFIG],
})
export class ConfigModule {}