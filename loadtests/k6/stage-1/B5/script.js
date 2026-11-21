// B5: Index write amplification - k6 script for POST /v1/events
// One script for every cell. Only env vars change between runs. The thing I change (how many secondary indexes
// the events table has) is NOT done here: run.sh creates the indexes in Postgres before every run. This script only
// chooses how the event_id is made (random v4 or time-ordered v7) and tags every metric with the cell.
//
//   RATE        target requests per second. Open model (constant-arrival-rate): k6 keeps sending at this rate even
//               when the server gets slow. A closed model would slow itself down and hide the saturation.
//   DURATION    e.g. 45s
//   KEY_KIND    v4 | v7. v4 = random uuid (every new key lands in a random place of the primary key index).
//               v7 = time-ordered uuid (first 48 bits = milliseconds since 1970, so new keys always go to the right end).
//   IDX         label only (0, 1, 3, 5, 1g): how many secondary indexes run.sh created. Becomes the tag "idx".
//   TENANTS     how many tenants (api keys) share the load, uniform random pick per request. Default 100.
//   KEY_PREFIX  api key i is KEY_PREFIX + i (i = 0..TENANTS-1). run.sh gives the server the same keys.
//               Named KEY_PREFIX on purpose: it is not a secret, and it does not match runner.sh's redaction.
//   PAD         size of the filler string in the payload, default 300 -> about 500 B per event (same as B3 and B4)
//   PRE_VUS / MAX_VUS   virtual users. If k6 runs out of VUs it reports dropped_iterations.
//   REQ_TIMEOUT per-request timeout, default 10s (so a hung request cannot hold a VU forever)
//   BASE_URL    default http://localhost:8000
//   PHASE       under | over | probe_*  (tag). under = fixed comfortable rate (cost per row),
//               over = far above capacity (max accepted rps). Probes are filtered out of headline panels.
//   PRELOAD     0 | 1 (tag): 1 = the table already had millions of rows before the run (probe only)
//   POOL_MAX    the pg pool size the SERVER was started with (tag pool_max)
//
// I do NOT use the ROUTE env var here. runner.sh uses ROUTE only for its preflight curl
// (I set it to /healthz), so the event route has its own variable.
import http from "k6/http";
import { Counter } from "k6/metrics";

const BASE = __ENV.BASE_URL || "http://localhost:8000";
const EVENTS_ROUTE = __ENV.EVENTS_ROUTE || "/v1/events";
const KEY_PREFIX = __ENV.KEY_PREFIX || "epk_b5_";
const KEY_KIND = __ENV.KEY_KIND || "v4";
const TENANTS = Number(__ENV.TENANTS || 100);
const RATE = Number(__ENV.RATE || 1500);
const PAD = "x".repeat(Number(__ENV.PAD || 300));

// 10 event types, picked uniformly. B3 and B4 always sent "order.created"; with one single value an index on
// "type" would be almost free (all entries equal, Postgres stores them very compactly), which would hide its cost.
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
// (k6 http_reqs does not split by status in the summary export.)
const resp202 = new Counter("b5_resp_202");
const resp503 = new Counter("b5_resp_503");
const respOther = new Counter("b5_resp_other"); // 4xx, 500, or status 0 (timeout / connection error)

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
        exp: "B5",
        key: KEY_KIND,
        idx: String(__ENV.IDX || "unset"),
        tenants: String(TENANTS),
        preload: String(__ENV.PRELOAD || "0"),
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

// Version 7 uuid (RFC 9562): 48 bits of milliseconds since 1970, then version 7, then random bits.
// Two ids made in the same millisecond are in random order, but ids from different milliseconds sort by time,
// so new rows go to the right end of the primary key index (like a serial number would).
function uuidv7() {
  const ms = Date.now().toString(16).padStart(12, "0");
  return (
    ms.slice(0, 8) +
    "-" +
    ms.slice(8, 12) +
    "-7" +
    hexDigits(3) +
    "-" +
    HEX[8 + Math.floor(Math.random() * 4)] +
    hexDigits(3) +
    "-" +
    hexDigits(12)
  );
}
const newId = KEY_KIND === "v7" ? uuidv7 : uuidv4;

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
    event_id: newId(), // unique per request, so every 202 must become one row
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
