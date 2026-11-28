import { Controller, Get, Module, ServiceUnavailableException } from '@nestjs/common';
import { SinkConsumer } from '../consumer/sink-consumer.js';
import { RetryConsumer } from '../retry/retry-consumer.js';
import { ConsumerModule } from '../consumer/consumer.module.js';

@Controller()
export class HealthController {
  constructor(
    private readonly consumer: SinkConsumer,
    private readonly retry: RetryConsumer,
  ) {}

  @Get('healthz') // liveness: the process is up
  healthz() {
    return { ok: true };
  }

  // Readiness: joined the group. A member with zero partitions is still ready (it is a standby).
  @Get('readyz')
  readyz() {
    const st = this.consumer.stats;
    // SINK_ROLE=retry: ready once the retry-tier consumer joined its group.
    if (st.rebalances > 0 || (this.retry.enabled && this.retry.joined && !this.consumer.mainEnabled)) {
      return { ok: true, assigned_partitions: [...st.assigned].sort(), retry_tiers: this.retry.enabled };
    }
    throw new ServiceUnavailableException({ ok: false, reason: 'not yet joined the group' });
  }
}

@Module({ imports: [ConsumerModule], controllers: [HealthController] })
export class HealthModule {}
