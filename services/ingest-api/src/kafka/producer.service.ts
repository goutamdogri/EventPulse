import { Inject, Injectable, Logger, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { KafkaJS, createKafka } from '@eventpulse/kafka-config';
import type { Producer } from '@eventpulse/kafka-config';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import { classifySendError, errorType } from './errors.js';
import type { BatchMessage } from './micro-batcher.js';

const COMPRESSION = {
  none: KafkaJS.CompressionTypes.None,
  gzip: KafkaJS.CompressionTypes.GZIP,
  snappy: KafkaJS.CompressionTypes.Snappy,
  lz4: KafkaJS.CompressionTypes.LZ4,
  zstd: KafkaJS.CompressionTypes.ZSTD,
} as const;

const ERR__STATE = -172;
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * Owns the one Kafka producer of the process. Everything about durability (acks, idempotence) and the
 * client's own batching (linger.ms) is configured here, per producer: the Confluent client does not allow
 * acks/compression/timeout per send().
 *
 * S2-F5: the API starts even when Kafka is down. connect() failures are retried in the background with backoff
 * (a failed Confluent producer cannot connect() again, so every attempt builds a new one). Once connected,
 * librdkafka reconnects to restarted brokers by itself; sends fail meanwhile and the breaker turns that into 503s.
 */
@Injectable()
export class ProducerService implements OnModuleInit, OnApplicationShutdown {
  private readonly log = new Logger('Producer');
  private producer?: Producer;
  private connected = false;
  private shuttingDown = false;
  private connecting?: Promise<void>;
  readonly errorsByType: Record<string, number> = {};

  constructor(@Inject(CONFIG) private readonly cfg: AppConfig) {}

  private build(): Producer {
    const p = this.cfg.producer;
    return createKafka(this.cfg.kafka).producer({
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
    this.startConnecting();
    // Give Kafka a bounded time to be there at boot, so the first requests are not 503 for no reason.
    // If it is not, the API starts anyway and keeps trying in the background.
    await Promise.race([this.connecting, sleep(this.cfg.startupWaitMs)]);
    if (!this.connected) this.log.warn('Kafka not reachable yet: starting anyway, retrying in the background');
  }

  private startConnecting() {
    if (this.connecting || this.shuttingDown) return;
    this.connecting = this.connectLoop().finally(() => (this.connecting = undefined));
  }

  private async connectLoop() {
    let delay = this.cfg.reconnectInitialMs;
    while (!this.shuttingDown) {
      const producer = this.build();
      try {
        await producer.connect();
        if (this.shuttingDown) {
          await producer.disconnect().catch(() => undefined);
          return;
        }
        this.producer = producer;
        this.connected = true;
        this.log.log('Kafka producer connected');
        return;
      } catch (e) {
        this.count(e);
        await producer.disconnect().catch(() => undefined);
        this.log.warn(`Kafka connect failed (${(e as Error).message}); retry in ${delay} ms`);
        await sleep(delay);
        delay = Math.min(delay * 2, this.cfg.reconnectMaxMs);
      }
    }
  }

  /** Called by the batcher. Resolves only when every message is acknowledged under the configured acks. */
  send = async (messages: BatchMessage[]): Promise<void> => {
    const producer = this.producer;
    if (!this.connected || !producer) {
      this.startConnecting();
      throw classifySendError(Object.assign(new Error('Cannot send without awaiting connect()'), { code: ERR__STATE }));
    }
    try {
      await producer.send({ topic: this.cfg.topic, messages });
    } catch (e) {
      this.count(e);
      if ((e as { code?: number })?.code === ERR__STATE && !this.shuttingDown) {
        // The client says it is disconnected: build a new one in the background.
        this.connected = false;
        this.producer = undefined;
        void producer.disconnect().catch(() => undefined);
        this.startConnecting();
      }
      throw classifySendError(e);
    }
  };

  private count(e: unknown) {
    const t = errorType(e);
    this.errorsByType[t] = (this.errorsByType[t] ?? 0) + 1;
  }

  isConnected() {
    return this.connected;
  }

  /** Effective producer settings, for /debug/stats and experiment records. */
  settings() {
    return { topic: this.cfg.topic, ...this.cfg.producer, brokers: this.cfg.kafka.brokers };
  }

  // Runs after BatcherService.beforeApplicationShutdown has drained the queue.
  async onApplicationShutdown() {
    this.shuttingDown = true;
    const producer = this.producer;
    this.connected = false;
    this.producer = undefined;
    if (!producer) return;
    // disconnect() flushes librdkafka's queue; with Kafka down it must not hang the exit.
    await Promise.race([producer.disconnect().catch(() => undefined), sleep(5000)]);
  }
}
