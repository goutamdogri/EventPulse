import { ValidationPipe } from '@nestjs/common';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { AppConfig, CONFIG } from './config.js';
import { AllExceptionsFilter } from './common/all-exceptions.filter.js';
import { MetricsService } from './observability/metrics.service.js';

export function configureApp(app: NestExpressApplication) {
  const cfg = app.get<AppConfig>(CONFIG);
  const metrics = app.get(MetricsService);

  app.disable('x-powered-by');
  // Shutdown: server.close() only drops connections that are idle AT THAT MOMENT. A response still in flight
  // (since Stage 3 a request does one more Redis round trip after its Kafka ack) leaves a keep-alive socket that
  // would hold the process open until the client's keep-alive timeout. Once the server is closing, drop each
  // connection as soon as its response is done.
  const server = app.getHttpServer();
  app.use((_req: unknown, res: import('node:http').ServerResponse, next: () => void) => {
    res.once('finish', () => {
      if (!server.listening) setImmediate(() => server.closeIdleConnections());
    });
    next();
  });
  app.use(metrics.middleware()); // first, so it also sees 413s
  app.useBodyParser('json', { limit: cfg.bodyLimit }); // needs { bodyParser: false } at create()
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  app.useGlobalFilters(new AllExceptionsFilter(metrics, cfg.retryAfterS));
  app.enableShutdownHooks();
  metrics.trackConnections(app.getHttpServer());
  return cfg;
}
