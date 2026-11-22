import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
} from '@nestjs/common';
import type { Response } from 'express';
import { MetricsService } from '../observability/metrics.service.js';

@Catch()
export class AllExceptionsFilter implements ExceptionFilter {
  constructor(
    private readonly m: MetricsService,
    private readonly retryAfterS = 1,
  ) {}

  catch(err: any, host: ArgumentsHost) {
    const res = host.switchToHttp().getResponse<Response>();

    if (err instanceof HttpException) {
      return res.status(err.getStatus()).json(err.getResponse());
    }
    // body-parser errors (413 too large, 400 bad JSON) are http-errors, not HttpException
    const status = err?.status ?? err?.statusCode;
    if (typeof status === 'number' && status >= 400 && status < 500) {
      return res.status(status).json({
        statusCode: status,
        error: err.type ?? 'Bad Request',
        message: err.message,
      });
    }
    // Kafka failures (and a full batcher queue) are still 500 here: mapping them to 503 + Retry-After is S2-F5.
    console.error('[unhandled]', err);
    return res
      .status(500)
      .json({ statusCode: 500, error: 'Internal Server Error' });
  }
}
