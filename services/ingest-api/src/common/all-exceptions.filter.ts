import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
} from '@nestjs/common';
import type { Response } from 'express';
import { MetricsService } from '../observability/metrics.service.js';

const TRANSIENT_CODES = new Set([
  'ECONNREFUSED',
  'ECONNRESET',
  'ETIMEDOUT',
  'EPIPE',
  'ENOTFOUND',
]);

function isTransientDbError(e: any): boolean {
  const code: string | undefined = e?.code;
  if (code && (TRANSIENT_CODES.has(code) || /^(08|53|57)/.test(code)))
    return true; // conn / resources / shutdown / query_canceled (57014)
  const msg = String(e?.message ?? '');
  return /timeout exceeded when trying to connect|Connection terminated|connection timeout/i.test(
    msg,
  );
}

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
    if (isTransientDbError(err)) {
      this.m.dbErrors.inc();
      res.setHeader('Retry-After', String(this.retryAfterS));
      return res.status(503).json({
        statusCode: 503,
        error: 'Service Unavailable',
        message: 'Database unavailable',
      });
    }
    console.error('[unhandled]', err);
    return res
      .status(500)
      .json({ statusCode: 500, error: 'Internal Server Error' });
  }
}
