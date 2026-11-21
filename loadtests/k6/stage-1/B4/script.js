// B4: Lock contention with a hot row - k6 script for POST /v1/events
// One script for every cell. Only env vars change between runs. The variable (how many tenants share the load)
// is made here: every request picks one of TENANTS api keys at random, and the server maps key -> tenant.
// Whether the server also increments the tenant counter (COUNTER_MODE=hot|off) is decided by run.sh when it
// starts the server, not here. This script only tags every metric with both values.
//
//   RATE        target requests per second. Open model (constant-arrival-rate): k6 keeps sending at this rate even
//               when the server gets slow. A closed model would slow itself down and hide the collapse.
//   DURATION    e.g. 45s
//   TENANTS     how many different tenants (api keys) share the load, evenly (uniform random pick per request)
//   KEY_PREFIX  api key i is KEY_PREFIX + i (i = 0..TENANTS-1). run.sh gives the server the same keys.
//               Named KEY_PREFIX on purpose: it is not a secret, and it does not match runner.sh's redaction.
//   PAD         size of the filler string in the payload, default 300 -> about 500 B per event
//   PRE_VUS / MAX_VUS   virtual users. If k6 runs out of VUs it reports dropped_iterations.
//   REQ_TIMEOUT per-request timeout, default 10s (so a hung request cannot hold a VU forever)
//   BASE_URL    default http://localhost:8000
//   PHASE       under | over | probe_*  (tag). under = below what 1 hot tenant can do (latency),
//               over = far above it (capacity). Probes are filtered out of headline panels.
//   COUNTER     hot | off (tag counter), the COUNTER_MODE the SERVER was started with
//   POOL_MAX    the pg pool size the SERVER was started with (tag pool_max)
//
// I do NOT use the ROUTE env var here. runner.sh uses ROUTE only for its preflight curl
// (I set it to /healthz), so the event route has its own variable.
import http from "k6/http";
import { Counter } from "k6/metrics";

const BASE = __ENV.BASE_URL || "http://localhost:8000";
const EVENTS_ROUTE = __ENV.EVENTS_ROUTE || "/v1/events";
const KEY_PREFIX = __ENV.KEY_PREFIX || "epk_b4_";
const TENANTS = Number(__ENV.TENANTS || 1);
const RATE = Number(__ENV.RATE || 500);
const PAD = "x".repeat(Number(__ENV.PAD || 300));

// Custom counters, so my audit can compare "202s the client saw" with "rows in Postgres".
// (k6 http_reqs does not split by status in the summary export.)
const resp202 = new Counter("b4_resp_202");
const resp503 = new Counter("b4_resp_503");
const respOther = new Counter("b4_resp_other"); // 4xx, 500, or status 0 (timeout / connection error)

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
      // If dropped_iterations > 0 either the generator is the bottleneck (check k6 CPU)
      // or the server is so slow that all VUs are waiting (that one is a finding, not a bug).
      maxVUs: Number(__ENV.MAX_VUS || 3000),
      // Scenario tags become labels on every k6 metric of this scenario. They are NOT options.tags on purpose:
      // runner.sh starts k6 with --tag testid=..., and a command-line tag REPLACES the script's global options.tags.
      tags: {
        exp: "B4",
        tenants: String(TENANTS),
        counter: __ENV.COUNTER || "unset",
        pool_max: String(__ENV.POOL_MAX || "unset"),
        offered_rate: String(RATE),
        phase: __ENV.PHASE || "over",
      },
    },
  },
};

// RFC 4122 version 4 uuid without any import (no internet needed). IsUUID() on the server wants the
// version and variant nibbles to be right, so they are set explicitly.
function uuidv4() {
  const hex = "0123456789abcdef";
  let s = "";
  for (let i = 0; i < 36; i++) {
    if (i === 8 || i === 13 || i === 18 || i === 23) s += "-";
    else if (i === 14) s += "4";
    else if (i === 19) s += hex[8 + Math.floor(Math.random() * 4)];
    else s += hex[Math.floor(Math.random() * 16)];
  }
  return s;
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
    type: "order.created",
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
