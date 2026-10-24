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

  return {
    register,
    httpRequestsTotal,
    httpRequestDuration,
    echoBodyBytes,
    stallsTotal,
  };
}
