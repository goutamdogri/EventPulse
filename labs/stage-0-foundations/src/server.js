// EventPulse Stage 0 server: node:http only (plus prom-client for metrics).
// Run from this folder: node src/server.js
import http from "node:http";
import { monitorEventLoopDelay } from "node:perf_hooks";
import { config } from "./config.js";
import { WorkerPool } from "./worker-pool.js";
import { createMetrics } from "./metrics.js";
import { createRoutes } from "./routes/index.js";
import { sendJson } from "./http-utils.js";

const pool = new WorkerPool(config.WORKERS, config.MAX_QUEUE);
const loopDelay = monitorEventLoopDelay({ resolution: 10 });
loopDelay.enable();

const state = { inFlight: 0 };
const metrics = createMetrics({ pool, loopDelay, state });
const ctx = { config, pool, loopDelay, state, metrics };
const routes = createRoutes(ctx);

const server = http.createServer(async (req, res) => {
  const start = process.hrtime.bigint();
  const qi = req.url.indexOf("?");
  const pathname = qi === -1 ? req.url : req.url.slice(0, qi);
  const search = qi === -1 ? "" : req.url.slice(qi + 1);

  // Normalized route label keeps Prometheus label cardinality bounded.
  const route = Object.hasOwn(routes, pathname) ? pathname : "unmatched";

  state.inFlight++;

  // Record when the response is fully written, not before the handler runs.
  res.on("finish", () => {
    const labels = { method: req.method, route, status_code: String(res.statusCode) };
    metrics.httpRequestsTotal.inc(labels);
    metrics.httpRequestDuration.observe(labels, Number(process.hrtime.bigint() - start) / 1e9);
  });
  // 'close' fires for completed and aborted requests alike.
  res.on("close", () => {
    state.inFlight--;
  });

  try {
    if (route === "unmatched") return sendJson(res, 404, { error: "not found" });
    const handler = routes[pathname][req.method];
    if (!handler) {
      return sendJson(res, 405, { error: "method not allowed" }, { allow: Object.keys(routes[pathname]).join(", ") });
    }
    await handler(req, res, search);
  } catch (err) {
    if (!res.headersSent) sendJson(res, 500, { error: "internal error" });
    else res.destroy();
    console.error(err);
  }
});

server.listen(config.PORT, "0.0.0.0", () => {
  console.log(
    `stage-0 server on :${config.PORT} | workers=${config.WORKERS} maxQueue=${config.MAX_QUEUE} maxBody=${config.MAX_BODY_BYTES}B`,
  );
});

const shutdown = async () => {
  server.close();
  await pool.close();
  process.exit(0);
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
