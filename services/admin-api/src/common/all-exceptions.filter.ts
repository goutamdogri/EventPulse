// Errors -> responses, and the result label of ep_admin_config_puts_total for PUTs.
//   HttpException as is (400 validation, 401, 404, 412, 413...)
//   Postgres / connection trouble the classifier calls TRANSIENT_* -> 503 + Retry-After (the change did NOT happen:
//   its transaction rolled back), everything else 500.
import { ArgumentsHost, Catch, HttpException, HttpStatus } from '@nestjs/common';
import type { ExceptionFilter } from '@nestjs/common';
import type { Request, Response } from 'express';
import { classifyError } from '@eventpulse/retry';
import type { AdminMetrics, PutResult } from '../observability/metrics.js';

function resultOf(status: number): PutResult {
  if (status === 404) return 'not_found';
  if (status === 412) return 'precondition_failed';
  if (status === 503) return 'unavailable';
  if (status >= 400 && status < 500) return 'invalid';
  return 'error';
}

@Catch()
export class AllExceptionsFilter implements ExceptionFilter {
  constructor(private readonly metrics: AdminMetrics, private readonly retryAfterS: number) {}

  catch(e: unknown, host: ArgumentsHost) {
    const http = host.switchToHttp();
    const req = http.getRequest<Request>();
    const res = http.getResponse<Response>();
    let status: number;
    let body: unknown;
    if (e instanceof HttpException) {
      status = e.getStatus();
      const r = e.getResponse();
      body = typeof r === 'string' ? { statusCode: status, message: r } : { statusCode: status, ...(r as object) };
    } else {
      const cls = classifyError(e);
      status = cls === 'TRANSIENT_SYSTEMIC' || cls === 'TRANSIENT_RECORD' ? HttpStatus.SERVICE_UNAVAILABLE : HttpStatus.INTERNAL_SERVER_ERROR;
      body = { statusCode: status, error: status === 503 ? 'unavailable' : 'internal_error', message: (e as Error)?.message ?? String(e) };
      if (status === 500) console.error('[admin-api] unhandled', e);
    }
    if (status === 503) res.setHeader('Retry-After', String(this.retryAfterS));
    if (req.method === 'PUT' && req.path.startsWith('/v1/admin/')) this.metrics.puts.labels(resultOf(status)).inc();
    if (!res.headersSent) res.status(status).json(body);
  }
}
