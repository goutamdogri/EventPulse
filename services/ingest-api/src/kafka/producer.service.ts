import { Inject, Injectable, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { KafkaJS, createKafka } from '@eventpulse/kafka-config';
import type { Producer } from '@eventpulse/kafka-config';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import type { BatchMessage } from './micro-batcher.js';

const COMPRESSION = {
  none: KafkaJS.CompressionTypes.None,
  gzip: KafkaJS.CompressionTypes.GZIP,
  snappy: KafkaJS.CompressionTypes.Snappy,
  lz4: KafkaJS.CompressionTypes.LZ4,
  zstd: KafkaJS.CompressionTypes.ZSTD,
} as const;

/**
 * Owns the one Kafka producer of the process. Everything about durability (acks, idempotence) and the
 * client's own batching (linger.ms) is configured here, per producer: the Confluent client does not allow
 * acks/compression/timeout per send().
 */
@Injectable()
export class ProducerService implements OnModuleInit, OnApplicationShutdown {
  private readonly producer: Producer;
  private connected = false;

  constructor(@Inject(CONFIG) private readonly cfg: AppConfig) {
    const p = cfg.producer;
    this.producer = createKafka(cfg.kafka).producer({
      kafkaJS: {
        acks: p.acks,
        compression: COMPRESSION[p.compression],
        idempotent: p.idempotent,
        timeout: p.ackTimeoutMs,
        allowAutoTopicCreation: false,
      },
      // librdkafka properties (outside the kafkaJS block):
      'linger.ms': p.lingerMs,
      'message.timeout.ms': p.deliveryTimeoutMs,
    });
  }

  async onModuleInit() {
    // S2-F1: the API needs Kafka to start. Starting without Kafka and reconnecting is S2-F5.
    await this.producer.connect();
    this.connected = true;
  }

  /** Called by the batcher. Resolves only when every message is acknowledged under the configured acks. */
  send = async (messages: BatchMessage[]): Promise<void> => {
    await this.producer.send({ topic: this.cfg.topic, messages });
  };

  isConnected() {
    return this.connected;
  }

  /** Effective producer settings, for /debug/stats and experiment records. */
  settings() {
    return { topic: this.cfg.topic, ...this.cfg.producer, brokers: this.cfg.kafka.brokers };
  }

  // Runs after BatcherService.beforeApplicationShutdown has drained the queue.
  async onApplicationShutdown() {
    if (!this.connected) return;
    this.connected = false;
    await this.producer.disconnect();
  }
}
