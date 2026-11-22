import {
  Controller,
  Get,
  Module,
  ServiceUnavailableException,
} from '@nestjs/common';
import { BatcherService } from '../kafka/batcher.service.js';
import { KafkaHealthService } from '../kafka/kafka-health.service.js';
import { ProducerService } from '../kafka/producer.service.js';

@Controller()
export class HealthController {
  constructor(
    private readonly producer: ProducerService,
    private readonly batcher: BatcherService,
    private readonly health: KafkaHealthService,
  ) {}

  @Get('healthz') // liveness: shallow, never touches dependencies
  healthz() {
    return { ok: true };
  }

  // Readiness: ready = producer connected AND a recent background health check passed AND breaker closed.
  // The health check runs without traffic, so readiness follows Kafka both ways (down -> 503, back -> 200) even
  // when no request arrives. Kafka outages never touch liveness: an orchestrator stops routing traffic without
  // killing a process that will recover by itself.
  @Get('readyz')
  readyz() {
    const breaker = this.batcher.breaker.state();
    const connected = this.producer.isConnected();
    const healthy = this.health.healthy();
    if (connected && healthy && breaker === 'closed') return { ok: true, kafka: 'up' };
    throw new ServiceUnavailableException({
      ok: false,
      kafka: 'down',
      breaker,
      connected,
      health: this.health.snapshot().last?.reason ?? 'no check yet',
    });
  }
}

@Module({ controllers: [HealthController] })
export class HealthModule {}
