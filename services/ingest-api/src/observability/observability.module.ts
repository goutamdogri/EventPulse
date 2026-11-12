import { Global, Module } from '@nestjs/common';
import { MetricsService } from './metrics.service.js';
import { ObservabilityController } from './observability.controller.js';

@Global()
@Module({
  providers: [MetricsService],
  controllers: [ObservabilityController],
  exports: [MetricsService],
})
export class ObservabilityModule {}
