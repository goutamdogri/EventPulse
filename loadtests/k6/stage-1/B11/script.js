// B11: Table growth soak - k6 script for POST /v1/events
// One script for every run (the two capacity probes, the soak, the fresh-table control). Only env vars change.
// It sends events at a FIXED rate and counts what the server answered. Nothing is broken on purpose here: the
// experiment is time. run.sh samples Postgres once a minute from outside while this script keeps sending, and
// compares "events the client was promised (202)" with "rows Postgres really holds" after the run.
//
//   RATE        target requests per second. Open model (constant-arrival-rate): k6 keeps sending at this rate even if
//               the server gets slower, so a slower server shows up as higher latency (or as dropped iterations), not as a
//               quietly lower rate. Soak: 3000 (about 70 % of the capacity). Probes: 6000 (above it on purpose).
//   DURATION    e.g. 60m for the soak, 60s for a probe
//   PHASE       tag "phase": cap_start | soak | cap_end | fresh (Grafana splits the panels by it)
//   TENANTS     how many tenants (api keys) share the load, uniform random pick per request. Default 100.
//   KEY_PREFIX  api key i is KEY_PREFIX + i. run.sh gives the server the same keys.
//   PAD         size of the filler string in the payload, default 300 -> about 500 B per event (same as B3 to B8)
//   PRE_VUS / MAX_VUS   virtual users. At 3000 req/s with a 6 ms answer only about 20 are busy; in the probes above
//               the capacity requests wait in the pool queue and many more are busy. If k6 runs out of VUs it reports
//               dropped_iterations (expected in the probes, not in the soak).
//   REQ_TIMEOUT per-request timeout, default 10s.
//   BASE_URL    default http://localhost:8000
//   POOL_MAX    the pg pool size the SERVER was started with (tag pool_max)
//
// ROUTE (runner.sh preflight) is /healthz; the event route has its own variable EVENTS_ROUTE.
import http from "k6/http";
import { Counter } from "k6/metrics";

const BASE = __ENV.BASE_URL || "http://localhost:8000";
const EVENTS_ROUTE = __ENV.EVENTS_ROUTE || "/v1/events";
const KEY_PREFIX = __ENV.KEY_PREFIX || "epk_b11_";
const TENANTS = Number(__ENV.TENANTS || 100);
const RATE = Number(__ENV.RATE || 3000);
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

// Custom counters, so my audit can compare "202s the client saw" with "rows in Postgres" (k6 http_reqs does not split
// by status in the summary export). Only 202 is a success. 503 is the server saying "pool is full / slow".
const resp202 = new Counter("b11_resp_202");
const resp503 = new Counter("b11_resp_503");
const respOther = new Counter("b11_resp_other"); // 4xx, 500, or status 0 (timeout / connection refused)

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
      duration: __ENV.DURATION || "60m",
      preAllocatedVUs: Number(__ENV.PRE_VUS || 1000),
      // If dropped_iterations > 0 either the generator is the bottleneck (check k6 CPU) or the server is so slow that every
      // VU is waiting. In the soak that would mean the capacity fell below the rate, which is part of the finding.
      maxVUs: Number(__ENV.MAX_VUS || 8000),
      // Scenario tags become labels on every k6 metric of this scenario. They are NOT options.tags on purpose:
      // runner.sh starts k6 with --tag testid=..., and a command-line tag REPLACES the script's global options.tags.
      tags: {
        exp: "B11",
        tenants: String(TENANTS),
        pool_max: String(__ENV.POOL_MAX || "unset"),
        offered_rate: String(RATE),
        phase: __ENV.PHASE || "soak",
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
