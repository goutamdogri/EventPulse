import { Inject, Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { encodeEvent } from '@eventpulse/kafka-config';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import { BatcherService } from '../kafka/batcher.service.js';
import { CreateEventDto } from './create-event.dto.js';

const REQUEST_ID_RE = /^[A-Za-z0-9._:-]{1,128}$/;

@Injectable()
export class EventsService {
  constructor(
    private readonly batcher: BatcherService,
    @Inject(CONFIG) private readonly cfg: AppConfig,
  ) {}

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
    });
    await this.batcher.enqueue({
      message: { key: encoded.key, value: encoded.value, headers: encoded.headers },
      sizeBytes: encoded.sizeBytes,
    });
  }
}
