import { Module } from '@nestjs/common';
import { FaultInjector } from '@eventpulse/fault-injector';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import { FAULT_INJECTOR, FaultsController } from './faults.controller.js';
import { SinkConsumer } from './sink-consumer.js';

@Module({
  providers: [
    // One injector per process, shared by the batch handler, the batch writer and /debug/faults.
    { provide: FAULT_INJECTOR, inject: [CONFIG], useFactory: (cfg: AppConfig) => new FaultInjector(cfg.faults) },
    SinkConsumer,
  ],
  controllers: [FaultsController],
  exports: [SinkConsumer],
})
export class ConsumerModule {}
