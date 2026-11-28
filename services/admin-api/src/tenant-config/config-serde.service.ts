// The config.changed-value writer schema id, looked up once at startup (never registered here: `make schemas`).
// Until it is known, PUT answers 503 and /readyz is false; lookups keep retrying with backoff in the background.
import { Inject, Injectable, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { ConfigChangedSerde } from '@eventpulse/outbox';
import { RegistryClient } from '@eventpulse/schema';
import { backoffMs } from '@eventpulse/retry';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';

@Injectable()
export class ConfigSerdeService implements OnModuleInit, OnApplicationShutdown {
  readonly serde: ConfigChangedSerde;
  lastError: string | null = null;
  private stopped = false;
  private timer?: NodeJS.Timeout;

  constructor(@Inject(CONFIG) private readonly cfg: AppConfig) {
    this.serde = new ConfigChangedSerde(new RegistryClient({ url: cfg.schema.registryUrl, timeoutMs: cfg.schema.registryTimeoutMs }));
  }

  async onModuleInit() {
    await this.attempt(1); // the first try is awaited, so a healthy start is ready at once
  }

  private async attempt(n: number): Promise<void> {
    try {
      await this.serde.init();
      this.lastError = null;
    } catch (e) {
      this.lastError = (e as Error).message;
      console.error(`[schema] config.changed-value lookup failed (attempt ${n}): ${this.lastError}`);
      if (!this.stopped) this.timer = setTimeout(() => void this.attempt(n + 1), backoffMs(n, 200, this.cfg.schema.initRetryMaxMs));
    }
  }

  onApplicationShutdown() {
    this.stopped = true;
    clearTimeout(this.timer);
  }
}
