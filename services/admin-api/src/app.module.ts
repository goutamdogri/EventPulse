import { Module } from '@nestjs/common';
import { FaultInjector } from '@eventpulse/fault-injector';
import { CONFIG, ConfigModule } from './config.js';
import type { AppConfig } from './config.js';
import { DbModule } from './db.module.js';
import { AdminMetrics } from './observability/metrics.js';
import { FaultsController } from './observability/faults.controller.js';
import { ObservabilityController } from './observability/observability.controller.js';
import { ConfigSerdeService } from './tenant-config/config-serde.service.js';
import { TenantConfigController } from './tenant-config/tenant-config.controller.js';
import { FAULT_INJECTOR, TenantConfigService } from './tenant-config/tenant-config.service.js';

@Module({
  imports: [ConfigModule, DbModule],
  controllers: [TenantConfigController, ObservabilityController, FaultsController],
  providers: [
    AdminMetrics,
    ConfigSerdeService,
    TenantConfigService,
    { provide: FAULT_INJECTOR, inject: [CONFIG], useFactory: (c: AppConfig) => new FaultInjector(c.faults) },
  ],
})
export class AppModule {}
