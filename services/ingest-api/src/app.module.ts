import { Module } from '@nestjs/common';
import { ConfigModule } from './config.js';
import { DbModule } from './db/db.module.js';
import { EventsModule } from './events/events.module.js';
import { HealthModule } from './health/health.module.js';
import { ObservabilityModule } from './observability/observability.module.js';

@Module({
  imports: [
    ConfigModule,
    DbModule,
    ObservabilityModule,
    HealthModule,
    EventsModule,
  ],
})
export class AppModule {}
