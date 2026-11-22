import { Module } from '@nestjs/common';
import { ConfigModule } from './config.js';
import { ConsumerModule } from './consumer/consumer.module.js';
import { DbModule } from './db/db.module.js';
import { HealthModule } from './health/health.module.js';
import { ObservabilityModule } from './observability/observability.module.js';

@Module({
  imports: [ConfigModule, DbModule, ObservabilityModule, ConsumerModule, HealthModule],
})
export class AppModule {}
