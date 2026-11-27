import { Inject, Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { encodeEvent } from '@eventpulse/kafka-config';
import { FaultInjector } from '@eventpulse/fault-injector';
import { Idempotency, eventContentHash, validateIdempotencyKey } from '@eventpulse/idempotency';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import { BatcherService } from '../kafka/batcher.service.js';
import { FAULT_INJECTOR, IDEMPOTENCY } from '../idempotency/idempotency.module.js';
import { IdempotencyInProgressError, IdempotencyKeyReusedError } from '../idempotency/errors.js';
import { CreateEventDto } from './create-event.dto.js';
import { SchemaService } from '../schema/schema.service.js';

const REQUEST_ID_RE = /^[A-Za-z0-9._:-]{1,128}$/;

export interface IngestResult {
  event_id: string;
  /** true = an earlier request with the same key already produced it; nothing new was produced. */
  replayed: boolean;
}

@Injectable()
export class EventsService {
  constructor(
    private readonly batcher: BatcherService,
    @Inject(CONFIG) private readonly cfg: AppConfig,
    @Inject(IDEMPOTENCY) private readonly idem: Idempotency,
    @Inject(FAULT_INJECTOR) private readonly faults: FaultInjector,
    private readonly schema: SchemaService,
  ) {}

  /**
   * S3-F1: idempotent ingest. Key = the Idempotency-Key header, or the body's event_id when there is none, scoped
   * by tenant. Resolves after Kafka acknowledged the event (or with the original result for a completed key);
   * rejects with IdempotencyInProgressError (409), IdempotencyKeyReusedError (422), InvalidIdempotencyKeyError
   * (400), IdempotencyUnavailableError (503, policy closed) or the produce error (503 etc.).
   *
   * Gaps, by design (documented in docs/api-contract.md):
   *  - dies after produce, before complete: the key stays inflight for IDEM_INFLIGHT_TTL_S (retries get 409),
   *    then a retry produces a second copy, which the sink's key absorbs (counted there).
   *  - produce fails ambiguously (send timeout: Kafka may have it): the key is released, the retry may produce a
   *    second copy, absorbed by the sink.
   */
  async ingest(tenantId: string, e: CreateEventDto, receivedAt: Date, opts: { idempotencyKey?: string; requestId?: string } = {}): Promise<IngestResult> {
    const key = opts.idempotencyKey !== undefined ? validateIdempotencyKey(opts.idempotencyKey) : e.event_id.toLowerCase();
    const hash = eventContentHash({
      tenant_id: tenantId,
      event_id: e.event_id,
      type: e.type,
      occurred_at: e.occurred_at,
      schema_version: e.schema_version,
      payload: e.payload,
    });

    const begin = await this.idem.begin(tenantId, key, hash);
    if (begin.kind === 'replay') return { event_id: begin.eventId, replayed: true };
    if (begin.kind === 'in_progress') throw new IdempotencyInProgressError();
    if (begin.kind === 'mismatch') throw new IdempotencyKeyReusedError();

    const ticket = begin.kind === 'new' ? begin.ticket : undefined; // bypass: fail_open / disabled
    this.faults.hit('api:before-produce');
    try {
      await this.publish(tenantId, e, receivedAt, opts.requestId);
    } catch (err) {
      if (ticket) await this.idem.release(ticket); // the client's retry is then a first attempt
      throw err;
    }
    this.faults.hit('api:after-produce-before-complete');
    if (ticket) await this.idem.complete(ticket, e.event_id);
    this.faults.hit('api:after-complete-before-response');
    return { event_id: e.event_id, replayed: false };
  }

  /**
   * Appends the event to events.raw. Resolves only after Kafka acknowledged the batch containing it
   * (under the configured acks), rejects if that batch failed or could not be queued.
   */
  async publish(tenantId: string, e: CreateEventDto, receivedAt: Date, requestId?: string): Promise<void> {
    const encoded = encodeEvent({
      tenantId,
      event: {
        event_id: e.event_id,
        type: e.type,
        occurred_at: e.occurred_at,
        schema_version: e.schema_version,
        payload: e.payload,
      },
      receivedAt,
      // Use the caller's id for tracing only if it is safe to put in a header; otherwise make one.
      requestId: requestId && REQUEST_ID_RE.test(requestId) ? requestId : randomUUID(),
      keyMode: this.cfg.keyMode,
      // S3-F7: Avro (wire format, registered schema id) unless SERIALIZATION=json. Throws SchemaUnavailableError
      // (503) before the writer id is known, AvroEncodeError (400) for a value that does not fit the schema.
      ...(this.schema.avro ? { serialize: (env, at) => this.schema.serialize(env, at) } : {}),
    });
    await this.batcher.enqueue({
      message: { key: encoded.key, value: encoded.value, headers: encoded.headers },
      sizeBytes: encoded.sizeBytes,
    });
  }
}
