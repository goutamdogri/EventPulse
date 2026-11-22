import { NestFactory } from '@nestjs/core';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { AppModule } from './app.module.js';
import { CONFIG } from './config.js';
import type { AppConfig } from './config.js';

// consumer-sink: HTTP only for /healthz, /readyz, /metrics and /debug/stats. The real work is the consumer.
async function main() {
  const logEnabled = process.env.LOG_ENABLED !== 'false'; // assignment logs are useful, on by default
  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
    logger: logEnabled ? ['log', 'warn', 'error'] : ['error'],
  });
  app.disable('x-powered-by');
  // SIGTERM/SIGINT -> leave the group (the current chunk finishes and commits), close the DLQ producer and the
  // pool, then exit 0. Handled here instead of enableShutdownHooks(), which re-raises the signal (exit 143).
  let closing = false;
  const shutdown = (signal: string) => {
    if (closing) return;
    closing = true;
    console.log(`consumer-sink: ${signal} received, shutting down`);
    app.close().then(
      () => process.exit(0),
      (e: unknown) => {
        console.error('consumer-sink: shutdown failed', e);
        process.exit(1);
      },
    );
  };
  process.once('SIGTERM', () => shutdown('SIGTERM'));
  process.once('SIGINT', () => shutdown('SIGINT'));
  const cfg = app.get<AppConfig>(CONFIG);
  await app.listen(cfg.port, '0.0.0.0');
  console.log(`consumer-sink group=${cfg.consumer.groupId} http=:${cfg.port} pid=${process.pid}`);
}
void main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
