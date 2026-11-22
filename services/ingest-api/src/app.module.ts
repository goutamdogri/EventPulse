import { Module } from '@nestjs/common';
import { ConfigModule } from './config.js';
import { EventsModule } from './events/events.module.js';
import { KafkaModule } from './kafka/kafka.module.js';
import { HealthModule } from './health/health.module.js';
import { ObservabilityModule } from './observability/observability.module.js';

@Module({
  imports: [
    ConfigModule,
    KafkaModule,
    ObservabilityModule,
    HealthModule,
    EventsModule,
  ],
})
export class AppModule {}
