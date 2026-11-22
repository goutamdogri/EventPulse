import { Injectable } from '@nestjs/common';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import type { NextFunction, Request, Response } from 'express';
import {
  Counter,
  Gauge,
  Histogram,
  Registry,
  collectDefaultMetrics,
} from '@prometheus-io/client';
import { BatcherService } from '../kafka/batcher.service.js';
import { ProducerService } from '../kafka/producer.service.js';

@Injectable()
export class MetricsService {
  readonly registry = new Registry();
  private readonly startedAt = Date.now();
  private readonly loop = monitorEventLoopDelay({ resolution: 10 });

  readonly httpRequests: Counter<string>;
  readonly httpDuration: Histogram<string>;
  readonly inFlight: Gauge<string>;
  readonly connOpen: Gauge<string>;
  readonly connTotal: Counter<string>;

  constructor(
    private readonly batcher: BatcherService,
    private readonly producer: ProducerService,
  ) {
    const registers = [this.registry];
    collectDefaultMetrics({ register: this.registry });
    this.loop.enable();

    this.httpRequests = new Counter({
      name: 'http_requests_total',
      help: 'Total number of HTTP requests.',
      labelNames: ['method', 'route', 'status'],
      registers,
    });

    this.httpDuration = new Histogram({
      name: 'http_request_duration_seconds',
      help: 'HTTP request duration.',
      labelNames: ['method', 'route', 'status'],
      buckets: [
        0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
      ],
      registers,
    });

    this.inFlight = new Gauge({
      name: 'http_requests_in_flight',
      help: "Requests currently being handled (Little's Law: rate x latency).",
      registers,
    });

    this.connOpen = new Gauge({
      name: 'http_connections_open',
      help: 'Open TCP connections',
      registers,
    });

    this.connTotal = new Counter({
      name: 'http_connections_total',
      help: 'Accepted connections',
      registers,
    });

    const loop = this.loop;
    new Gauge({
      name: 'eventpulse_event_loop_delay_seconds',
      help: 'Event loop delay (10 ms resolution histogram) since last reset.',
      labelNames: ['quantile'],
      registers,
      collect() {
        this.set({ quantile: '0.5' }, loop.percentile(50) / 1e9);
        this.set({ quantile: '0.99' }, loop.percentile(99) / 1e9);
        this.set({ quantile: 'max' }, loop.max / 1e9);
        loop.reset();
      },
    });
  }

  middleware() {
    return (req: Request, res: Response, next: NextFunction) => {
      const start = process.hrtime.bigint();
      this.inFlight.inc();
      res.once('close', () => {
        this.inFlight.dec();
        const labels = {
          method: req.method,
          route: req.route?.path ?? 'unmatched',
          status: String(res.statusCode),
        };
        this.httpRequests.inc(labels);
        this.httpDuration.observe(
          labels,
          Number(process.hrtime.bigint() - start) / 1e9,
        );
      });
      next();
    };
  }

  trackConnections(server: import('node:http').Server) {
    server.on('connection', (socket) => {
      this.connTotal.inc();
      this.connOpen.inc();
      socket.once('close', () => this.connOpen.dec());
    });
  }

  async snapshot() {
    const byClass: Record<string, number> = {};

    for (const v of (await this.httpRequests.get()).values) {
      const k = `${String(v.labels.status)[0]}xx`;
      byClass[k] = (byClass[k] ?? 0) + v.value;
    }

    const inFlight = (await this.inFlight.get()).values[0]?.value ?? 0;
    return {
      uptime_s: Math.round((Date.now() - this.startedAt) / 1000),
      // Stage 2: the request path ends at Kafka. Full producer/batcher metrics arrive with S2-F6.
      batcher: this.batcher.stats(),
      producer: { connected: this.producer.isConnected(), ...this.producer.settings() },
      // Note: this histogram is reset on every Prometheus scrape, so these are "since last scrape".
      event_loop_delay_ms: {
        p50: this.loop.percentile(50) / 1e6,
        p99: this.loop.percentile(99) / 1e6,
        max: this.loop.max / 1e6,
      },
      in_flight: inFlight,
      requests_by_status_class: byClass,
      memory: process.memoryUsage(),
    };
  }
}
