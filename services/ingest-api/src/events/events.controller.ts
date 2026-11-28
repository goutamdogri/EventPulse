import {
  Body,
  Controller,
  Headers,
  HttpCode,
  Post,
  Res,
  UseGuards,
} from '@nestjs/common';
import type { Response } from 'express';
import { CreateEventDto } from './create-event.dto.js';
import { EventsService } from './events.service.js';
import { ApiKeyGuard, TenantId } from '../auth/api-key.guard.js';

export const IDEMPOTENT_REPLAYED_HEADER = 'Idempotent-Replayed';

@Controller('v1/events')
@UseGuards(ApiKeyGuard)
export class EventsController {
  constructor(private readonly svc: EventsService) {}

  // 202 means "durably appended to events.raw under the configured acks policy" (Stage 2), and from Stage 3 also
  // "exactly once per (tenant, Idempotency-Key or event_id) within the dedup window": a repeat returns the
  // original result with Idempotent-Replayed: true and produces nothing (see docs/api-contract.md).
  @Post()
  @HttpCode(202)
  async ingest(
    @Body() dto: CreateEventDto,
    @TenantId() tenantId: string,
    @Headers('x-request-id') requestId?: string,
    @Headers('idempotency-key') idempotencyKey?: string,
    @Res({ passthrough: true }) res?: Response,
  ) {
    const receivedAt = new Date();
    const r = await this.svc.ingest(tenantId, dto, receivedAt, { idempotencyKey, requestId });
    if (r.replayed) res?.setHeader(IDEMPOTENT_REPLAYED_HEADER, 'true');
    return { event_id: r.event_id };
  }
}
