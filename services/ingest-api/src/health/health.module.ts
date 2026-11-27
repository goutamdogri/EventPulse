import {
  Controller,
  Get,
  Module,
  ServiceUnavailableException,
} from '@nestjs/common';
import { BatcherService } from '../kafka/batcher.service.js';
import { KafkaHealthService } from '../kafka/kafka-health.service.js';
import { ProducerService } from '../kafka/producer.service.js';
import { SchemaService } from '../schema/schema.service.js';

@Controller()
export class HealthController {
  constructor(
    private readonly producer: ProducerService,
    private readonly batcher: BatcherService,
    private readonly health: KafkaHealthService,
    private readonly schema: SchemaService,
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
    // S3-F7: with SERIALIZATION=avro, not ready until the writer schema id is known (never take traffic we can't
    // serialize). After that a registry outage does not matter to the producer, so it is not part of readiness.
    const schema = this.schema.ready ? 'ready' : 'unavailable';
    if (connected && healthy && breaker === 'closed' && this.schema.ready) return { ok: true, kafka: 'up', schema };
    throw new ServiceUnavailableException({
      ok: false,
      kafka: connected && healthy && breaker === 'closed' ? 'up' : 'down',
      schema,
      breaker,
      connected,
      health: this.health.snapshot().last?.reason ?? 'no check yet',
    });
  }
}

@Module({ controllers: [HealthController] })
export class HealthModule {}
