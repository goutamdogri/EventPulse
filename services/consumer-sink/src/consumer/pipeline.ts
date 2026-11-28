// What the main consumer and the retry-tier consumer share (one per process): the error classifier, the retry-tier
// policy and its rate limiter, the DLQ and retry producers, dlq_meta, and the Avro serde (S3-F5, S3-F7).
// Producers connect before the consumers start (onModuleInit) and disconnect after they left their groups
// (onApplicationShutdown runs after the consumers' beforeApplicationShutdown).

import { Inject, Injectable, Logger, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import type { Pool } from 'pg';
import { Classifier, TokenBucket } from '@eventpulse/retry';
import type { TierPolicy } from '@eventpulse/retry';
import { EventSerde, RegistryClient, latestSchema } from '@eventpulse/schema';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import { PG_POOL } from '../db/db.module.js';
import { SinkStats } from '../observability/sink-stats.js';
import { DlqMetaStore, DlqPublisher } from '../sink/dlq-publisher.js';
import { RetryPublisher } from '../retry/retry-publisher.js';

@Injectable()
export class SinkPipeline implements OnModuleInit, OnApplicationShutdown {
  private readonly log = new Logger('SinkPipeline');
  readonly classifier: Classifier;
  readonly policy: TierPolicy;
  readonly limiter: TokenBucket;
  readonly dlq: DlqPublisher;
  readonly dlqMeta: DlqMetaStore;
  readonly retryOut: RetryPublisher;
  readonly serde: EventSerde;
  readonly readerFile: string;
  private sizeTimer?: NodeJS.Timeout;

  constructor(
    @Inject(CONFIG) private readonly cfg: AppConfig,
    @Inject(PG_POOL) pool: Pool,
    private readonly stats: SinkStats,
  ) {
    this.classifier = new Classifier(cfg.classifierOverride);
    if (cfg.classifierOverride.size > 0) this.log.warn(`DEBUG CLASSIFIER_OVERRIDE: ${[...cfg.classifierOverride].map(([c, k]) => `${c}=${k}`).join(', ')}`);
    const t = cfg.retryTiers;
    this.policy = { topics: t.topics, delaysMs: t.delaysMs, jitter: t.jitter };
    this.limiter = new TokenBucket(t.maxRatePerS);
    this.dlq = new DlqPublisher(cfg.kafka, cfg.retry.dlqTopic, { sourceTopic: cfg.consumer.topic, groupId: cfg.consumer.groupId });
    this.dlqMeta = new DlqMetaStore(pool);
    this.retryOut = new RetryPublisher(cfg.kafka);
    const reader = latestSchema(cfg.schema.subject);
    this.readerFile = reader.file;
    this.serde = new EventSerde(
      new RegistryClient({ url: cfg.schema.registryUrl, timeoutMs: cfg.schema.registryTimeoutMs, observer: { request: (op, ok, s) => stats.observeRegistry(op, ok, s) } }),
      { subject: cfg.schema.subject, schema: reader.parsed },
    );
    if (!t.enabled) this.log.warn('RETRY_TOPICS_ENABLED=false: Stage 2 in-process retries, TRANSIENT_RECORD blocks the partition');
  }

  get tiersEnabled() {
    return this.cfg.retryTiers.enabled;
  }

  async onModuleInit() {
    await this.dlq.connect();
    if (this.tiersEnabled) await this.retryOut.connect();
    if (this.cfg.dlqSizePollMs > 0) {
      const poll = () =>
        this.dlqMeta
          .sizes()
          .then((s) => this.stats.setDlqSize(s))
          .catch(() => undefined); // DB trouble: keep the last value, the next poll tries again
      void poll();
      this.sizeTimer = setInterval(poll, this.cfg.dlqSizePollMs);
      this.sizeTimer.unref();
    }
  }

  async onApplicationShutdown() {
    if (this.sizeTimer) clearInterval(this.sizeTimer);
    await this.dlq.disconnect().catch(() => undefined);
    if (this.tiersEnabled) await this.retryOut.disconnect().catch(() => undefined);
  }
}
