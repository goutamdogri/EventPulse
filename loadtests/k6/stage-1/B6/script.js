// B6: Durability knob - k6 script for POST /v1/events
// One script for every run. Only env vars change between runs. The thing I change (synchronous_commit on or off)
// is NOT done here: run.sh sets it in Postgres (ALTER ROLE) before it starts the server. This script only sends
// events at a fixed rate and counts how many of them the server answered with 202, so run.sh can compare
// "202s the client saw" with "rows that Postgres really holds" (the durability audit).
//
//   RATE        target requests per second. Open model (constant-arrival-rate): k6 keeps sending at this rate even
//               when the server gets slow. A closed model would slow itself down and hide the saturation.
//   DURATION    e.g. 45s
//   SYNC        on | off. Label only (tag "sync"): the value of synchronous_commit that run.sh set.
//   TENANTS     how many tenants (api keys) share the load, uniform random pick per request. Default 100.
//   KEY_PREFIX  api key i is KEY_PREFIX + i (i = 0..TENANTS-1). run.sh gives the server the same keys.
//               Named KEY_PREFIX on purpose: it is not a secret, and it does not match runner.sh's redaction.
//   PAD         size of the filler string in the payload, default 300 -> about 500 B per event (same as B3 to B5)
//   PRE_VUS / MAX_VUS   virtual users. If k6 runs out of VUs it reports dropped_iterations.
//   REQ_TIMEOUT per-request timeout, default 10s (so a hung request cannot hold a VU forever)
//   BASE_URL    default http://localhost:8000
//   PHASE       at | over | probe_crash (tag). at = just below the capacity of "on" (compare latency),
//               over = far above it (max accepted rps), probe_crash = the run where Postgres is killed.
//   POOL_MAX    the pg pool size the SERVER was started with (tag pool_max)
//
// I do NOT use the ROUTE env var here. runner.sh uses ROUTE only for its preflight curl
// (I set it to /healthz), so the event route has its own variable.
import http from "k6/http";
import { Counter } from "k6/metrics";

const BASE = __ENV.BASE_URL || "http://localhost:8000";
const EVENTS_ROUTE = __ENV.EVENTS_ROUTE || "/v1/events";
const KEY_PREFIX = __ENV.KEY_PREFIX || "epk_b6_";
const TENANTS = Number(__ENV.TENANTS || 100);
const RATE = Number(__ENV.RATE || 4000);
const PAD = "x".repeat(Number(__ENV.PAD || 300));

// 10 event types, picked uniformly (same list as B5, so the rows look the same as in the earlier experiments).
const TYPES = [
  "order.created",
  "order.paid",
  "order.shipped",
  "order.cancelled",
  "user.signup",
  "user.login",
  "cart.updated",
  "page.viewed",
  "payment.failed",
  "refund.issued",
];

// Custom counters, so my audit can compare "202s the client saw" with "rows in Postgres".
// (k6 http_reqs does not split by status in the summary export.) The 202 count is the number of events the
// service PROMISED to have stored: that promise is exactly what B6 tests.
const resp202 = new Counter("b6_resp_202");
const resp503 = new Counter("b6_resp_503");
const respOther = new Counter("b6_resp_other"); // 4xx, 500, or status 0 (timeout / connection refused)

export const options = {
  // The default summary does not include p99; I want p99 and p99.9 in the exported json.
  summaryTrendStats: [
    "avg",
    "min",
    "med",
    "max",
    "p(50)",
    "p(90)",
    "p(95)",
    "p(99)",
    "p(99.9)",
  ],

  scenarios: {
    ingest: {
      executor: "constant-arrival-rate",
      rate: RATE,
      timeUnit: "1s",
      duration: __ENV.DURATION || "45s",
      preAllocatedVUs: Number(__ENV.PRE_VUS || 500),
      // If dropped_iterations > 0 either the generator is the bottleneck (check k6 CPU) or the server is so slow
      // that all VUs are waiting. In an "over" run the second one is expected (that is how I find capacity).
      maxVUs: Number(__ENV.MAX_VUS || 3000),
      // Scenario tags become labels on every k6 metric of this scenario. They are NOT options.tags on purpose:
      // runner.sh starts k6 with --tag testid=..., and a command-line tag REPLACES the script's global options.tags.
      tags: {
        exp: "B6",
        sync: String(__ENV.SYNC || "unset"),
        tenants: String(TENANTS),
        pool_max: String(__ENV.POOL_MAX || "unset"),
        offered_rate: String(RATE),
        phase: __ENV.PHASE || "over",
      },
    },
  },
};

const HEX = "0123456789abcdef";
function hexDigits(n) {
  let s = "";
  for (let i = 0; i < n; i++) s += HEX[Math.floor(Math.random() * 16)];
  return s;
}

// RFC 4122 version 4 uuid without any import (no internet needed). IsUUID() on the server wants the
// version and variant nibbles to be right, so they are set explicitly.
function uuidv4() {
  return (
    hexDigits(8) +
    "-" +
    hexDigits(4) +
    "-4" +
    hexDigits(3) +
    "-" +
    HEX[8 + Math.floor(Math.random() * 4)] +
    hexDigits(3) +
    "-" +
    hexDigits(12)
  );
}

// One header object per tenant, built once (not per request).
const headersByTenant = [];
for (let i = 0; i < TENANTS; i++) {
  headersByTenant.push({
    "Content-Type": "application/json",
    Authorization: "Bearer " + KEY_PREFIX + i,
  });
}

export default function () {
  const body = JSON.stringify({
    event_id: uuidv4(), // unique per request, so every 202 must become one row
    type: TYPES[Math.floor(Math.random() * TYPES.length)],
    occurred_at: new Date().toISOString(),
    schema_version: 1,
    payload: { user: "u" + Math.floor(Math.random() * 1e6), data: PAD },
  });

  const res = http.post(BASE + EVENTS_ROUTE, body, {
    headers: headersByTenant[Math.floor(Math.random() * TENANTS)],
    timeout: __ENV.REQ_TIMEOUT || "10s",
    // Only 202 counts as success, so http_req_failed = "not accepted".
    responseCallback: http.expectedStatuses(202),
  });

  if (res.status === 202) resp202.add(1);
  else if (res.status === 503) resp503.add(1);
  else respOther.add(1);
}
