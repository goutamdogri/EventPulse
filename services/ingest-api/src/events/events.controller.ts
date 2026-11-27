import {
  Body,
  Controller,
  Headers,
  HttpCode,
  Post,
  UseGuards,
} from '@nestjs/common';
import { CreateEventDto } from './create-event.dto.js';
import { EventsService } from './events.service.js';
import { ApiKeyGuard, TenantId } from '../auth/api-key.guard.js';

@Controller('v1/events')
@UseGuards(ApiKeyGuard)
export class EventsController {
  constructor(private readonly svc: EventsService) {}

  // Stage 2: 202 means "durably appended to events.raw under the configured acks policy".
  // It does NOT mean the event is in Postgres yet (see docs/api-contract.md).
  @Post()
  @HttpCode(202)
  async ingest(
    @Body() dto: CreateEventDto,
    @TenantId() tenantId: string,
    @Headers('x-request-id') requestId?: string,
  ) {
    const receivedAt = new Date();
    await this.svc.publish(tenantId, dto, receivedAt, requestId);
    return { event_id: dto.event_id };
  }
}
