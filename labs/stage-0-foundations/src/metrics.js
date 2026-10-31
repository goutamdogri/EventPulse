// Prometheus registry and all metrics. Gauges that read live state take it via arguments.
import client from "@prometheus-io/client";

export function createMetrics({ pool, loopDelay, state }) {
  const register = new client.Registry();
  register.setDefaultLabels({ app: "eventpulse", service: "stage-0-server" });
  client.collectDefaultMetrics({ register });

  const httpRequestsTotal = new client.Counter({
    name: "http_requests_total",
    help: "Total number of HTTP requests.",
    labelNames: ["method", "route", "status_code"],
    registers: [register],
  });

  const httpRequestDuration = new client.Histogram({
    name: "http_request_duration_seconds",
    help: "HTTP request duration in seconds.",
    labelNames: ["method", "route", "status_code"],
    buckets: [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1],
    registers: [register],
  });

  new client.Gauge({
    name: "http_requests_in_flight",
    help: "Requests currently being handled (Little's Law: rate x latency).",
    registers: [register],
    collect() {
      this.set(state.inFlight);
    },
  });

  new client.Gauge({
    name: "eventpulse_event_loop_delay_seconds",
    help: "Event loop delay (10 ms resolution histogram) since last reset.",
    labelNames: ["quantile"],
    registers: [register],
    collect() {
      this.set({ quantile: "0.5" }, loopDelay.percentile(50) / 1e9);
      this.set({ quantile: "0.99" }, loopDelay.percentile(99) / 1e9);
      this.set({ quantile: "max" }, loopDelay.max / 1e9);

      /* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
          Note:-
          loopDelay comes from monitorEventLoopDelay().
          It keeps every sample since we called loopDelay.enable(),
          or since the last loopDelay.reset().
          reset() empties the histogram, so the next measurements start from zero.
          Otherwise loopdelay.max is maximum of all collected data, not the max of last second.
      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */
      loopDelay.reset();
    },
  });

  new client.Gauge({
    name: "eventpulse_cpu_worker_queue_depth",
    help: "Jobs waiting for a free worker thread.",
    registers: [register],
    collect() {
      this.set(pool.queue.length);
    },
  });

  new client.Gauge({
    name: "eventpulse_cpu_worker_busy",
    help: "Worker threads currently computing.",
    registers: [register],
    collect() {
      this.set(pool.stats().busy);
    },
  });

  const echoBodyBytes = new client.Histogram({
    name: "eventpulse_echo_body_bytes",
    help: "Request body size on POST /echo.",
    buckets: [100, 1_000, 10_000, 100_000, 1_000_000],
    registers: [register],
  });

  const stallsTotal = new client.Counter({
    name: "eventpulse_stalls_total",
    help: "Number of /stall calls (synchronous event-loop blocks).",
    registers: [register],
  });

  new client.Gauge({
    name: "eventpulse_cpu_worker_pool_size",
    help: "Configured number of worker threads in the pool.",
    registers: [register],
    collect() {
      this.set(pool.stats().size);
    },
  });

  // Buckets reach 480 s because the wait grows to ~150 s at the end of C1
  const WAIT_BUCKETS = [
    0.001, 0.01, 0.05, 0.1, 0.35, 0.5, 1, 2, 5, 10, 30, 60, 120, 240, 480,
  ];

  const workerWait = new client.Histogram({
    name: "eventpulse_cpu_worker_wait_seconds",
    help: "Time a job spent queued before a worker picked it up.",
    buckets: WAIT_BUCKETS,
    registers: [register],
  });
  const workerRun = new client.Histogram({
    name: "eventpulse_cpu_worker_run_seconds",
    help: "Time a job spent executing in a worker.",
    buckets: WAIT_BUCKETS,
    registers: [register],
  });
  const workerJobs = new client.Counter({
    name: "eventpulse_cpu_worker_jobs_total",
    help: "Worker jobs by outcome.",
    labelNames: ["outcome"],
    registers: [register],
  });
  for (const o of ["completed", "rejected", "failed"])
    workerJobs.labels(o).inc(0); // series exist from the start

  pool.hooks = {
    wait: (s) => workerWait.observe(s),
    run: (s) => workerRun.observe(s),
    outcome: (o) => workerJobs.inc({ outcome: o }),
  };

  const connOpen = new client.Gauge({
    name: "http_connections_open",
    help: "Open TCP connections",
    registers: [register],
  });
  const connTotal = new client.Counter({
    name: "http_connections_total",
    help: "Accepted connections",
    registers: [register],
  });
  const serverErrors = new client.Counter({
    name: "http_server_errors_total",
    help: "Server-level errors",
    labelNames: ["code"],
    registers: [register],
  });

  return {
    register,
    httpRequestsTotal,
    httpRequestDuration,
    echoBodyBytes,
    stallsTotal,
    connOpen,
    connTotal,
    serverErrors,
  };
}
