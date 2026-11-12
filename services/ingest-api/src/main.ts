import { NestFactory } from '@nestjs/core';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { AppModule } from './app.module.js';
import { configureApp } from './bootstrap.js';

async function main() {
  const logEnabled = process.env.LOG_ENABLED === 'true';
  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
    bodyParser: false,
    logger: logEnabled ? ['log', 'warn', 'error'] : ['error'],
  });
  const cfg = configureApp(app);
  await app.listen(cfg.port, '0.0.0.0');
  console.log(`ingest-api listening on :${cfg.port} pid=${process.pid}`);
}
main();
