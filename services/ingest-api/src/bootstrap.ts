import { ValidationPipe } from '@nestjs/common';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { AppConfig, CONFIG } from './config.js';
import { AllExceptionsFilter } from './common/all-exceptions.filter.js';
import { MetricsService } from './observability/metrics.service.js';

export function configureApp(app: NestExpressApplication) {
  const cfg = app.get<AppConfig>(CONFIG);
  const metrics = app.get(MetricsService);

  app.disable('x-powered-by');
  app.use(metrics.middleware()); // first, so it also sees 413s
  app.useBodyParser('json', { limit: cfg.bodyLimit }); // needs { bodyParser: false } at create()
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  app.useGlobalFilters(new AllExceptionsFilter(metrics, cfg.retryAfterS));
  app.enableShutdownHooks();
  metrics.trackConnections(app.getHttpServer());
  return cfg;
}
