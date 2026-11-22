import { Global, Module } from '@nestjs/common';
import { BatcherService } from './batcher.service.js';
import { KafkaHealthService } from './kafka-health.service.js';
import { ProducerService } from './producer.service.js';

@Global()
@Module({
  providers: [ProducerService, BatcherService, KafkaHealthService],
  exports: [ProducerService, BatcherService, KafkaHealthService],
})
export class KafkaModule {}
