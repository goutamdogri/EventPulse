import { Inject, Injectable, Logger, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { createKafka } from '@eventpulse/kafka-config';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import { BatcherService } from './batcher.service.js';
import { KafkaHealthChecker } from './kafka-health.js';
import type { HealthAdmin } from './kafka-health.js';

const ERR__STATE = -172;

/** Runs KafkaHealthChecker every KAFKA_HEALTH_INTERVAL_MS with its own admin client (see kafka-health.ts). */
@Injectable()
export class KafkaHealthService implements OnModuleInit, OnApplicationShutdown {
  private readonly log = new Logger('KafkaHealth');
  private readonly checker: KafkaHealthChecker;
  private admin?: ReturnType<ReturnType<typeof createKafka>['admin']>;
  private timer?: NodeJS.Timeout;
  private stopped = false;
  private lastOk?: boolean;

  constructor(
    @Inject(CONFIG) private readonly cfg: AppConfig,
    batcher: BatcherService,
  ) {
    const h = cfg.health;
    this.checker = new KafkaHealthChecker(() => this.getAdmin(), batcher.breaker, {
      topic: cfg.topic,
      minIsr: h.minIsr,
      timeoutMs: h.timeoutMs,
      staleAfterMs: 3 * h.intervalMs + h.timeoutMs,
    });
  }

  get enabled() {
    return this.cfg.health.intervalMs > 0;
  }

  onModuleInit() {
    if (!this.enabled) return;
    void this.loop();
  }

  private async loop() {
    while (!this.stopped) {
      const r = await this.checker.check();
      if (r.ok !== this.lastOk) {
        if (r.ok) this.log.log('Kafka healthy');
        else this.log.warn(`Kafka unhealthy: ${r.reason}`);
        this.lastOk = r.ok;
      }
      if (this.stopped) return;
      await new Promise<void>((res) => {
        this.timer = setTimeout(res, this.cfg.health.intervalMs);
        this.timer.unref();
      });
    }
  }

  /** One admin client, rebuilt if it ends up disconnected (a failed connect can't be retried on the same one). */
  private async getAdmin(): Promise<HealthAdmin> {
    if (!this.admin) {
      const admin = createKafka({ ...this.cfg.kafka, clientId: `${this.cfg.kafka.clientId}-health`, logLevel: 'nothing' }).admin();
      try {
        await admin.connect();
      } catch (e) {
        await admin.disconnect().catch(() => undefined);
        throw e;
      }
      this.admin = admin;
    }
    const admin = this.admin;
    const discard = () => {
      if (this.admin !== admin) return;
      this.admin = undefined;
      void admin.disconnect().catch(() => undefined);
    };
    return {
      discard,
      fetchTopicMetadata: async (o) => {
        try {
          return (await admin.fetchTopicMetadata(o as any)) as any;
        } catch (e) {
          if ((e as { code?: number })?.code === ERR__STATE) discard();
          throw e;
        }
      },
    };
  }

  /** Readiness input: true when a recent check passed (always true when the check is off). */
  healthy(): boolean {
    return !this.enabled || this.checker.healthy();
  }

  snapshot() {
    return { enabled: this.enabled, healthy: this.healthy(), last: this.checker.lastResult() ?? null };
  }

  async onApplicationShutdown() {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    const admin = this.admin;
    this.admin = undefined;
    if (admin) await Promise.race([admin.disconnect().catch(() => undefined), new Promise((r) => setTimeout(r, 2000))]);
  }
}
