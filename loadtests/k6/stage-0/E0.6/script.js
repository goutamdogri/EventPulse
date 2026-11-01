// E0.6: keep-alive vs new connection per request.
// ONE script for both modes. The only difference between runs is KEEPALIVE.
//
//   KEEPALIVE=1 (default)  reuse TCP connections
//   KEEPALIVE=0            new TCP connection for every request
//   RATE                   target requests per second (open model)
//   DURATION               e.g. 60s
//   BASE_URL, ROUTE        target (default http://localhost:8000/health, same as runner.sh)

import http from "k6/http";

const KEEPALIVE = __ENV.KEEPALIVE !== "0";

export const options = {
  // true = k6 opens a new connection per request and sends "Connection: close"
  noConnectionReuse: !KEEPALIVE,

  // This tag becomes a label on every k6 metric, so Grafana can split on/off.
  tags: { keepalive: KEEPALIVE ? "on" : "off" },

  scenarios: {
    steady: {
      executor: "constant-arrival-rate", // open model: rate does not slow down with the server
      rate: Number(__ENV.RATE || 1000),
      timeUnit: "1s",
      duration: __ENV.DURATION || "60s",
      preAllocatedVUs: 300,
      // No-reuse makes each request slower (connect + close), so Little's Law
      // needs many more VUs. Watch dropped_iterations: if > 0, discard the run.
      maxVUs: 5000,
    },
  },
};

export default function () {
  http.get(
    `${__ENV.BASE_URL || "http://localhost:8000"}${__ENV.ROUTE || "/health"}`,
  );
}
