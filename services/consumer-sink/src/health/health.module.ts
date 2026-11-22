import { Controller, Get, Module, ServiceUnavailableException } from '@nestjs/common';
import { SinkConsumer } from '../consumer/sink-consumer.js';
import { ConsumerModule } from '../consumer/consumer.module.js';

@Controller()
export class HealthController {
  constructor(private readonly consumer: SinkConsumer) {}

  @Get('healthz') // liveness: the process is up
  healthz() {
    return { ok: true };
  }

  // Readiness: joined the group. A member with zero partitions is still ready (it is a standby).
  @Get('readyz')
  readyz() {
    const st = this.consumer.stats;
    if (st.rebalances > 0) return { ok: true, assigned_partitions: [...st.assigned].sort() };
    throw new ServiceUnavailableException({ ok: false, reason: 'not yet joined the group' });
  }
}

@Module({ imports: [ConsumerModule], controllers: [HealthController] })
export class HealthModule {}
