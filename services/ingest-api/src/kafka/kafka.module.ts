import { Global, Module } from '@nestjs/common';
import { BatcherService } from './batcher.service.js';
import { ProducerService } from './producer.service.js';

@Global()
@Module({
  providers: [ProducerService, BatcherService],
  exports: [ProducerService, BatcherService],
})
export class KafkaModule {}
