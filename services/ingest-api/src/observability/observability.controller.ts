import { Controller, Get, Header } from '@nestjs/common';
import { MetricsService } from './metrics.service.js';

@Controller()
export class ObservabilityController {
  constructor(private readonly m: MetricsService) {}

  @Get('metrics')
  @Header('Content-Type', 'text/plain; version=0.0.4')
  metrics() {
    return this.m.registry.metrics();
  }

  @Get('debug/stats')
  stats() {
    return this.m.snapshot();
  }
}
