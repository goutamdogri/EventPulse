// E0.8: Accept queue and backlog
// ONE k6 script for every cell. Only env vars change between runs.
//
// What I am doing here (in my own words):
//   1. At t=0 I send ONE request to /stall?ms=STALL_MS. The Node event loop blocks for that long,
//      so Node stops calling accept() and the kernel has to park new connections in the accept queue.
//   2. At t=BURST_DELAY_MS (default 200 ms) BURST virtual users each open ONE brand new connection and
//      send ONE GET /health. If the queue (size = backlog) is smaller than BURST, the kernel has to drop
//      some of them.
//   3. I count what happened to each connection: ok / timeout / reset / other, and how many needed a
//      SYN retry (connect time >= 900 ms, because the first SYN retry is sent after 1 s).
//
// Env vars (runner.sh forwards every exported variable to k6 as -e NAME=value):
//   CELL            name of the matrix cell, becomes a tag (for example A_b16_n200)
//   BURST           number of connections in the burst (= number of VUs, each VU does exactly 1 iteration)
//   STALL_MS        how long /stall blocks the loop; 0 = no stall at all (control runs)
//   BURST_DELAY_MS  how long after t=0 the burst starts (default 200, so the stall has really begun)
//   TIMEOUT         k6 per-request timeout, includes the connect time (default 10s)
//   BACKLOG, SOMAXCONN, ABORT   only used as tags so Grafana can split the cells (the server and the
//                   sysctls are set by step.sh, not by this script)
//   PHASE           measured | probe
//   BASE_URL, ROUTE target (same defaults as runner.sh)
//
// Why not constant-arrival-rate like the other experiments? Here I do not measure a steady rate. I look at
// what happens to N specific connections that arrive together. Each VU runs exactly one iteration, so
// dropped_iterations does not apply; I check that k6_iterations_total == BURST (+1 for the stall request).
import http from "k6/http";
import { Counter } from "k6/metrics";

const BASE = __ENV.BASE_URL || "http://localhost:8000";
const HEALTH = `${BASE}${__ENV.ROUTE || "/health"}`;
const CELL = __ENV.CELL || "manual";
const BURST = Number(__ENV.BURST || 200);
const STALL_MS = __ENV.STALL_MS === undefined ? 1500 : Number(__ENV.STALL_MS);
const BURST_DELAY_MS = Number(__ENV.BURST_DELAY_MS || 200);
const TIMEOUT = __ENV.TIMEOUT || "10s";

// Custom counters (appear in Prometheus as k6_e08_*_total, see missing-metrics.txt)
const okC = new Counter("e08_conn_ok");
const timeoutC = new Counter("e08_conn_timeout");
const resetC = new Counter("e08_conn_reset");
const otherC = new Counter("e08_conn_other_error");
const slowC = new Counter("e08_connect_slow"); // connect took >= 900 ms: the first SYN was dropped and retried

const scenarios = {
  burst: {
    executor: "per-vu-iterations", // BURST VUs, 1 iteration each: all of them start together
    vus: BURST,
    iterations: 1,
    startTime: `${BURST_DELAY_MS}ms`,
    maxDuration: "60s",
    exec: "burst",
  },
};

if (STALL_MS > 0) {
  scenarios.stall = {
    executor: "per-vu-iterations",
    vus: 1,
    iterations: 1,
    startTime: "0s",
    maxDuration: "60s",
    exec: "stall",
  };
}

export const options = {
  // A new TCP connection for every request. Without this a VU would reuse its connection
  // and I would not be testing the accept queue at all.
  noConnectionReuse: true,

  // These tags become labels on every k6 metric so Grafana can split the cells.
  tags: {
    exp: "e08",
    cell: CELL,
    backlog: String(__ENV.BACKLOG || "na"),
    somaxconn: String(__ENV.SOMAXCONN || "na"),
    abort: String(__ENV.ABORT || "na"),
    burst: String(BURST),
    stall_ms: String(STALL_MS),
    phase: __ENV.PHASE || "measured",
  },
  scenarios,
};

// The one request that blocks the Node event loop.
export function stall() {
  http.get(`${BASE}/stall?ms=${STALL_MS}`, {
    timeout: "30s",
    tags: { kind: "stall" },
  });
}

// One new connection + one GET. I classify the outcome by the error TEXT, because I am not sure
// which numeric k6 error codes map to "reset" and "timeout" (I will check the 'unclassified' warnings in the trial run).
export function burst() {
  const startMs = Date.now(); // wall clock (same machine as the server), used to line requests up on a timeline
  const res = http.get(HEALTH, { timeout: TIMEOUT, tags: { kind: "burst" } });
  let outcome = "ok";

  if (res.status === 200) {
    okC.add(1);
  } else {
    const err = String(res.error || "").toLowerCase();
    if (err.includes("reset")) {
      resetC.add(1);
      outcome = "reset";
    } else if (
      err.includes("timeout") ||
      err.includes("deadline") ||
      err.includes("timed out")
    ) {
      timeoutC.add(1);
      outcome = "timeout";
    } else {
      otherC.add(1);
      outcome = "other";
      console.warn(
        `unclassified failure: status=${res.status} error="${res.error}" code=${res.error_code}`,
      );
    }
  }

  // timings.connecting is the TCP connect time in ms. The first SYN retry fires after 1 s,
  // so a connect time >= 900 ms means "my first SYN was dropped".
  const t = res.timings || {};
  if (t.connecting >= 900) slowC.add(1);

  // ONE LINE PER CONNECTION. Prometheus scrapes every 1 s, far too coarse for a 1.5 s event, so this is the
  // real per-connection record. step.sh pulls these lines out of k6's output into captures/<testid>.req.csv
  // (no spaces, commas or quotes inside a field, so it is easy to parse).
  const f = (x) => (x === undefined ? 0 : Number(x).toFixed(1));
  console.log(
    `E08REQ,${CELL},${startMs},${f(t.blocked)},${f(t.connecting)},${f(t.sending)},${f(t.waiting)},${f(t.receiving)},${f(t.duration)},${res.status},${outcome}`,
  );
}
