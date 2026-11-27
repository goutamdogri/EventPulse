// S3-F1 wiring: one Redis client, one Idempotency (state machine + Redis Lua store + breaker + metrics) and one
// fault injector per process.
//
// The Redis client never queues commands while disconnected (enableOfflineQueue: false) and never retries a
// command (maxRetriesPerRequest: 0): a dead Redis must cost a request at most REDIS_COMMAND_TIMEOUT_MS, and after
// IDEM_BREAKER_FAILURES failures nothing at all (the breaker applies IDEM_FAILURE_POLICY without a call).
// It keeps reconnecting in the background. Redis is NOT part of /readyz: with the default policy (open) the API
// works without it, and the sink's key keeps storage correct.

import { Global, Inject, Injectable, Logger, Module, OnApplicationShutdown } from '@nestjs/common';
import { Redis } from 'ioredis';
import { Registry } from '@prometheus-io/client';
import { FaultInjector, describeCrash } from '@eventpulse/fault-injector';
import { Idempotency, RedisStore } from '@eventpulse/idempotency';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import { CircuitBreaker } from '../kafka/breaker.js';
import { IdempotencyMetrics } from './idempotency-metrics.js';

export const REDIS = Symbol('REDIS');
export const IDEMPOTENCY = Symbol('IDEMPOTENCY');
export const IDEM_BREAKER = Symbol('IDEM_BREAKER');
export const FAULT_INJECTOR = Symbol('FAULT_INJECTOR');

export function createRedis(cfg: AppConfig['idempotency'], log: Logger): Redis {
  const r = new Redis(cfg.redisUrl, {
    enableOfflineQueue: false,
    maxRetriesPerRequest: 0,
    commandTimeout: cfg.commandTimeoutMs,
    connectTimeout: cfg.connectTimeoutMs,
    retryStrategy: (attempt) => Math.min(100 * 2 ** Math.min(attempt, 5), 2_000),
    lazyConnect: !cfg.enabled,
  });
  let up = false;
  r.on('ready', () => {
    up = true;
    log.log(`redis connected (${new URL(cfg.redisUrl).host})`);
  });
  // Without a listener, ioredis prints every reconnect error. Log the transition once.
  r.on('error', (e) => {
    if (up) log.warn(`redis error, idempotency now follows IDEM_FAILURE_POLICY=${cfg.policy}: ${e.message}`);
    up = false;
  });
  return r;
}

@Injectable()
class RedisLifecycle implements OnApplicationShutdown {
  constructor(@Inject(REDIS) private readonly redis: Redis) {}
  async onApplicationShutdown() {
    await this.redis.quit().catch(() => this.redis.disconnect());
  }
}

@Global()
@Module({
  providers: [
    {
      provide: REDIS,
      inject: [CONFIG],
      useFactory: (cfg: AppConfig) => createRedis(cfg.idempotency, new Logger('Idempotency')),
    },
    { provide: IdempotencyMetrics, inject: [Registry], useFactory: (r: Registry) => new IdempotencyMetrics(r) },
    {
      provide: IDEM_BREAKER,
      inject: [CONFIG],
      useFactory: (cfg: AppConfig) => new CircuitBreaker(cfg.idempotency.breakerFailures, cfg.idempotency.breakerCooldownMs),
    },
    {
      provide: IDEMPOTENCY,
      inject: [CONFIG, REDIS, IDEM_BREAKER, IdempotencyMetrics],
      useFactory: (cfg: AppConfig, redis: Redis, breaker: CircuitBreaker, m: IdempotencyMetrics) => {
        const c = cfg.idempotency;
        if (!c.enabled) new Logger('Idempotency').warn('IDEM_ENABLED=false: no API-level deduplication (the sink still dedups)');
        return new Idempotency(
          new RedisStore(redis),
          { enabled: c.enabled, ttlS: c.ttlS, inflightTtlS: c.inflightTtlS, policy: c.policy, prefix: c.keyPrefix },
          breaker.enabled ? breaker : undefined,
          m,
        );
      },
    },
    {
      provide: FAULT_INJECTOR,
      inject: [CONFIG],
      useFactory: (cfg: AppConfig) => {
        if (cfg.faults.crashes.length > 0) new Logger('Faults').warn(`DEBUG FAULTS ARMED: ${cfg.faults.crashes.map(describeCrash).join(', ')}`);
        return new FaultInjector(cfg.faults);
      },
    },
    RedisLifecycle,
  ],
  exports: [REDIS, IDEMPOTENCY, IDEM_BREAKER, IdempotencyMetrics, FAULT_INJECTOR],
})
export class IdempotencyModule {}
