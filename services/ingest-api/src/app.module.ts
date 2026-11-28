import { Module } from '@nestjs/common';
import { ConfigModule } from './config.js';
import { EventsModule } from './events/events.module.js';
import { KafkaModule } from './kafka/kafka.module.js';
import { MetricsCoreModule } from './observability/metrics-core.module.js';
import { HealthModule } from './health/health.module.js';
import { IdempotencyModule } from './idempotency/idempotency.module.js';
import { SchemaModule } from './schema/schema.module.js';
import { ObservabilityModule } from './observability/observability.module.js';

@Module({
  imports: [
    ConfigModule,
    MetricsCoreModule,
    KafkaModule,
    IdempotencyModule,
    SchemaModule,
    ObservabilityModule,
    HealthModule,
    EventsModule,
  ],
})
export class AppModule {}
