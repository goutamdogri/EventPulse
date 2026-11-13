import {
  Body,
  Controller,
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

  // In Stage 1, 202 means "committed to Postgres". 
  @Post()
  @HttpCode(202)
  async ingest(@Body() dto: CreateEventDto, @TenantId() tenantId: string) {
    await this.svc.insert(tenantId, dto);
    return { event_id: dto.event_id };
  }
}
