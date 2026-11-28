import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { AppModule } from './app.module.js';
import { configureApp } from './bootstrap.js';

async function main() {
  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
    bodyParser: false,
    logger: process.env.LOG_ENABLED === 'true' ? ['log', 'warn', 'error'] : ['error'],
  });
  const cfg = configureApp(app);
  await app.listen(cfg.port, '0.0.0.0');
  console.log(`admin-api listening on :${cfg.port} pid=${process.pid} topic=${cfg.configTopic}`);
}
void main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
