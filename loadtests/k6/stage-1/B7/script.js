// B7: Payload size - k6 script for POST /v1/events
// One script for every run. Only env vars change between runs. The thing I change is the SIZE of the request body.
// Everything else (route, 100 tenants, event types, open model) is the same as B6, so the 500 B case can be compared
// with B6 "sync=on".
//
//   SIZE        target size of the whole JSON request body in bytes (200, 500, 2048, 10240, 102400).
//               The filler string in payload.data is made longer or shorter until the body has about this size
//               (type and user id change the length by a few bytes, so "about").
//   RATE        target requests per second. Open model (constant-arrival-rate): k6 keeps sending at this rate even
//               when the server gets slow. A closed model would slow itself down and hide the saturation.
//   DURATION    e.g. 45s
//   TENANTS     how many tenants (api keys) share the load, uniform random pick per request. Default 100.
//   KEY_PREFIX  api key i is KEY_PREFIX + i (i = 0..TENANTS-1). run.sh gives the server the same keys.
//   PRE_VUS / MAX_VUS   virtual users. If k6 runs out of VUs it reports dropped_iterations.
//   REQ_TIMEOUT per-request timeout, default 10s
//   BASE_URL    default http://localhost:8000
//   PHASE       at | over (tag). at = just below the predicted capacity of this size (compare latency),
//               over = far above it (max accepted rps).
//   POOL_MAX    the pg pool size the SERVER was started with (tag pool_max)
//
// I do NOT use the ROUTE env var here. runner.sh uses ROUTE only for its preflight curl
// (run.sh sets it to /healthz), so the event route has its own variable.
import http from "k6/http";
import { Counter } from "k6/metrics";

const BASE = __ENV.BASE_URL || "http://localhost:8000";
const EVENTS_ROUTE = __ENV.EVENTS_ROUTE || "/v1/events";
const KEY_PREFIX = __ENV.KEY_PREFIX || "epk_b7_";
const TENANTS = Number(__ENV.TENANTS || 100);
const RATE = Number(__ENV.RATE || 4000);
const SIZE = Number(__ENV.SIZE || 500);

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

// Response-class counters, same idea as B6 (k6 http_reqs does not split by status in the summary export).
const resp202 = new Counter("b7_resp_202");
const resp503 = new Counter("b7_resp_503");
const respOther = new Counter("b7_resp_other"); // 4xx (a 413 would show here), 500, or status 0 (timeout)

export const options = {
  summaryTrendStats: ["avg", "min", "med", "max", "p(50)", "p(90)", "p(95)", "p(99)", "p(99.9)"],
  scenarios: {
    ingest: {
      executor: "constant-arrival-rate",
      rate: RATE,
      timeUnit: "1s",
      duration: __ENV.DURATION || "45s",
      preAllocatedVUs: Number(__ENV.PRE_VUS || 500),
      maxVUs: Number(__ENV.MAX_VUS || 3000),
      // Scenario tags become labels on every k6 metric of this scenario (not options.tags: runner.sh passes
      // --tag testid=..., and a command-line tag REPLACES the script's global options.tags).
      tags: {
        exp: "B7",
        size: String(SIZE),
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

// RFC 4122 version 4 uuid without any import (no internet needed). The server wants the version and variant nibbles right.
function uuidv4() {
  return (
    hexDigits(8) + "-" + hexDigits(4) + "-4" + hexDigits(3) + "-" +
    HEX[8 + Math.floor(Math.random() * 4)] + hexDigits(3) + "-" + hexDigits(12)
  );
}

// The filler is RANDOM hex, not "xxxx". Postgres tries to compress big values (pglz); a string of one repeated
// character would shrink to almost nothing and hide the real WAL/disk cost of a big payload. Random hex does not
// compress well, so the bytes I send are the bytes that get stored. Built once per VU (init code), not per request.
// Size of the body without filler: build one body with an empty filler and measure it.
const envelope = JSON.stringify({
  event_id: uuidv4(),
  type: "order.cancelled", // one of the longer types, so the body is not bigger than SIZE
  occurred_at: new Date().toISOString(),
  schema_version: 1,
  payload: { user: "u000000", data: "" },
}).length;
const PAD = hexDigits(Math.max(0, SIZE - envelope));

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
    responseCallback: http.expectedStatuses(202), // only 202 counts as success
  });

  if (res.status === 202) resp202.add(1);
  else if (res.status === 503) resp503.add(1);
  else respOther.add(1);
}
