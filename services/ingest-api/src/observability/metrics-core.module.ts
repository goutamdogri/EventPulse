import { Global, Module } from '@nestjs/common';
import { Registry } from '@prometheus-io/client';
import { KafkaMetrics } from './kafka-metrics.js';

/** The one Prometheus registry of the process, plus the Kafka/batcher metrics that feed it (S2-F6). */
@Global()
@Module({
  providers: [
    { provide: Registry, useFactory: () => new Registry() },
    { provide: KafkaMetrics, inject: [Registry], useFactory: (r: Registry) => new KafkaMetrics(r) },
  ],
  exports: [Registry, KafkaMetrics],
})
export class MetricsCoreModule {}
