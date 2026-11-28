// S3-F7 producer side: the Avro writer for events.raw.
//
// - The writer schema is the newest version in schemas/subjects.json, compiled into this build.
// - At startup the service LOOKS UP its registry id (never registers: `make schemas` does that at deploy). Startup
//   waits for the first attempt (bounded by SCHEMA_REGISTRY_TIMEOUT_MS); if it fails, the lookup is retried in the
//   background with backoff until it succeeds. Until then /readyz is 503 and POST /v1/events answers 503
//   schema_unavailable, so the API never takes traffic it cannot serialize.
// - Once the id is known, encoding is synchronous and calls nothing: a registry outage after startup changes
//   nothing for the producer (the "hidden dependency" policy, C11).
// - SERIALIZATION=json keeps the Stage 2 JSON value (no registry at all).

import { Inject, Injectable, Logger, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { Counter, Histogram, Registry } from '@prometheus-io/client';
import {
  CONTENT_TYPE_AVRO,
  EventSerde,
  RegistryClient,
  SchemaNotRegisteredError,
  latestSchema,
} from '@eventpulse/schema';
import type { EventInput, RegistryOp } from '@eventpulse/schema';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';

export class SchemaUnavailableError extends Error {
  readonly name = 'SchemaUnavailableError';
  constructor() {
    super('writer schema id not known yet (schema registry unreachable, or the schema is not registered: make schemas)');
  }
}

@Injectable()
export class SchemaService implements OnModuleInit, OnApplicationShutdown {
  private readonly log = new Logger('Schema');
  readonly serde?: EventSerde;
  readonly writerFile?: string;
  private timer?: NodeJS.Timeout;
  private stopped = false;
  private attempts = 0;
  lastError?: { at: string; message: string };
  private readonly requests: Counter<string>;
  private readonly latency: Histogram<string>;

  constructor(@Inject(CONFIG) private readonly cfg: AppConfig, registry: Registry) {
    const registers = [registry];
    this.requests = new Counter({ name: 'ep_schema_registry_requests_total', help: 'Schema Registry requests by operation and result.', labelNames: ['op', 'result'], registers });
    this.latency = new Histogram({ name: 'ep_schema_registry_latency_seconds', help: 'Schema Registry request latency.', labelNames: ['op'], buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5], registers });
    if (cfg.schema.serialization !== 'avro') return;
    const client = new RegistryClient({
      url: cfg.schema.registryUrl,
      timeoutMs: cfg.schema.registryTimeoutMs,
      observer: { request: (op: RegistryOp, ok: boolean, s: number) => this.observe(op, ok, s) },
    });
    const latest = latestSchema(cfg.schema.subject);
    this.writerFile = latest.file;
    this.serde = new EventSerde(client, { subject: cfg.schema.subject, schema: latest.parsed });
  }

  private observe(op: RegistryOp, ok: boolean, seconds: number) {
    this.requests.inc({ op, result: ok ? 'ok' : 'error' });
    this.latency.observe({ op }, seconds);
  }

  get avro(): boolean {
    return this.cfg.schema.serialization === 'avro';
  }

  /** Ready to serialize: always for json; for avro once the writer id is known. */
  get ready(): boolean {
    return !this.avro || (this.serde?.ready ?? false);
  }

  async onModuleInit() {
    if (!this.avro) {
      this.log.warn('SERIALIZATION=json: Stage 2 JSON values, no schema registry');
      return;
    }
    await this.tryInit(); // one bounded attempt; on failure it schedules the background retries itself
  }

  private async tryInit(): Promise<void> {
    if (this.stopped || !this.serde) return;
    this.attempts++;
    try {
      const id = await this.serde.init();
      this.log.log(`writer schema ${this.writerFile} = id ${id} on subject ${this.cfg.schema.subject}`);
      this.lastError = undefined;
    } catch (e) {
      this.lastError = { at: new Date().toISOString(), message: (e as Error).message };
      const wait = Math.min(this.cfg.schema.initRetryMaxMs, 250 * 2 ** Math.min(this.attempts, 6));
      // Not registered is a deploy mistake, not an outage: say so loudly, but keep retrying (someone may run it).
      if (e instanceof SchemaNotRegisteredError || this.attempts === 1) this.log.error(`schema lookup failed, retrying in ${wait} ms: ${(e as Error).message}`);
      this.timer = setTimeout(() => void this.tryInit(), wait);
    }
  }

  onApplicationShutdown() {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
  }

  /** The value for encodeEvent(): Avro wire format, synchronous. Throws SchemaUnavailableError / AvroEncodeError. */
  serialize(e: Omit<EventInput, 'received_at'>, receivedAt: Date): { value: Buffer; contentType: string } {
    if (!this.serde?.ready) throw new SchemaUnavailableError();
    return { value: this.serde.encode({ ...e, received_at: receivedAt }), contentType: CONTENT_TYPE_AVRO };
  }

  snapshot() {
    return {
      serialization: this.cfg.schema.serialization,
      ready: this.ready,
      subject: this.cfg.schema.subject,
      writer_file: this.writerFile ?? null,
      writer_schema_id: this.serde?.writerId ?? null,
      init_attempts: this.attempts,
      last_error: this.lastError ?? null,
    };
  }
}
