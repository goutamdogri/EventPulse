import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
} from '@nestjs/common';
import type { Response } from 'express';
import { CanonicalJsonError, IdempotencyUnavailableError, InvalidIdempotencyKeyError } from '@eventpulse/idempotency';
import { IdempotencyInProgressError, IdempotencyKeyReusedError } from '../idempotency/errors.js';
import { KafkaUnavailableError, OverloadedError } from '../kafka/errors.js';
import { BatcherClosedError, MessageTooLargeError } from '../kafka/micro-batcher.js';
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
    // S3-F1 idempotency outcomes.
    if (err instanceof IdempotencyInProgressError) {
      return res
        .status(409)
        .set('Retry-After', String(this.retryAfterS))
        .json({ statusCode: 409, error: 'Conflict', message: err.message, reason: 'idempotency_in_progress' });
    }
    if (err instanceof IdempotencyKeyReusedError) {
      return res.status(422).json({ statusCode: 422, error: 'Unprocessable Entity', message: err.message, reason: 'idempotency_key_reused' });
    }
    if (err instanceof InvalidIdempotencyKeyError || err instanceof CanonicalJsonError) {
      return res.status(400).json({ statusCode: 400, error: 'Bad Request', message: err.message });
    }
    // Event larger than the batch byte cap (only reachable if BODY_LIMIT is raised above it): the client's
    // fault, never retryable, so 413 like the edge limit, not 500.
    if (err instanceof MessageTooLargeError) {
      return res.status(413).json({ statusCode: 413, error: 'Payload Too Large', message: err.message });
    }
    // Kafka unavailable, ingest queue full, or the instance is shutting down (SIGTERM drain in progress):
    // 503 + Retry-After. The client retries with the same event_id (another instance, or this one restarted).
    // Redis unreachable with IDEM_FAILURE_POLICY=closed: same 503 contract, reason idempotency_unavailable.
    if (
      err instanceof OverloadedError ||
      err instanceof KafkaUnavailableError ||
      err instanceof BatcherClosedError ||
      err instanceof IdempotencyUnavailableError
    ) {
      const reason =
        err instanceof OverloadedError
          ? 'overloaded'
          : err instanceof BatcherClosedError
            ? 'shutting_down'
            : err instanceof IdempotencyUnavailableError
              ? 'idempotency_unavailable'
              : 'kafka_unavailable';
      this.m.count503(reason);
      return res
        .status(503)
        .set('Retry-After', String(this.retryAfterS))
        .json({
          statusCode: 503,
          error: 'Service Unavailable',
          message: 'Ingest temporarily unavailable',
          reason,
        });
    }
    console.error('[unhandled]', err);
    return res
      .status(500)
      .json({ statusCode: 500, error: 'Internal Server Error' });
  }
}
