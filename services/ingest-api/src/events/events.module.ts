import { Module } from '@nestjs/common';
import { Registry } from '@prometheus-io/client';
import { EventTimeMetrics } from './event-time-metrics.js';
import { ApiKeyGuard } from '../auth/api-key.guard.js';
import { EventsService } from './events.service.js';
import { EventsController } from './events.controller.js';

@Module({
  controllers: [EventsController],
  providers: [EventsService, ApiKeyGuard, { provide: EventTimeMetrics, inject: [Registry], useFactory: (r: Registry) => new EventTimeMetrics(r) }],
  exports: [EventTimeMetrics],
})
export class EventsModule {}
