import { Module } from '@nestjs/common';
import { ApiKeyGuard } from '../auth/api-key.guard.js';
import { EventsService } from './events.service.js';
import { EventsController } from './events.controller.js';

@Module({
  controllers: [EventsController],
  providers: [EventsService, ApiKeyGuard],
})
export class EventsModule {}
