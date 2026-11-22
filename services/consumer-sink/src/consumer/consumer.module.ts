import { Module } from '@nestjs/common';
import { SinkConsumer } from './sink-consumer.js';

@Module({ providers: [SinkConsumer], exports: [SinkConsumer] })
export class ConsumerModule {}
