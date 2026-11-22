import {
  Controller,
  Get,
  Module,
  ServiceUnavailableException,
} from '@nestjs/common';
import { ProducerService } from '../kafka/producer.service.js';

@Controller()
export class HealthController {
  constructor(private readonly producer: ProducerService) {}

  @Get('healthz') // liveness: shallow, never touches dependencies
  healthz() {
    return { ok: true };
  }

  // Readiness: the API no longer depends on Postgres, only on Kafka.
  // (S2-F5 makes this follow broker outages and reconnects at runtime.)
  @Get('readyz')
  readyz() {
    if (this.producer.isConnected()) return { ok: true, kafka: 'up' };
    throw new ServiceUnavailableException({ ok: false, kafka: 'down' });
  }
}

@Module({ controllers: [HealthController] })
export class HealthModule {}
