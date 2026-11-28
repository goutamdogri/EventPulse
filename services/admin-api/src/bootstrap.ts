import { ValidationPipe } from '@nestjs/common';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { CONFIG } from './config.js';
import type { AppConfig } from './config.js';
import { AllExceptionsFilter } from './common/all-exceptions.filter.js';
import { AdminMetrics } from './observability/metrics.js';

export function configureApp(app: NestExpressApplication): AppConfig {
  const cfg = app.get<AppConfig>(CONFIG);
  app.disable('x-powered-by');
  app.useBodyParser('json', { limit: cfg.bodyLimit }); // needs { bodyParser: false } at create()
  // Unknown config keys are a 400, not silently dropped: a typo must not become a config nobody reads.
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true, transform: true }));
  app.useGlobalFilters(new AllExceptionsFilter(app.get(AdminMetrics), cfg.retryAfterS));
  app.enableShutdownHooks();
  return cfg;
}
