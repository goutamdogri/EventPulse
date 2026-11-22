// B10: Missing timeouts - k6 script for POST /v1/events
// One script for every run. Only env vars change. The variable of the experiment (are the server's
// deadlines set or not) is NOT set here: run.sh starts the server with PG_CONN_TIMEOUT_MS and
// PG_STATEMENT_TIMEOUT_MS, and run.sh creates the fault (latency toxic or a table lock).
// This script only sends load and tags every metric with the variant and the fault.
//
//   RATE        target requests per second. Open model (constant-arrival-rate): k6 keeps sending at this
//               rate even when the server hangs. A closed model would slow down by itself and hide the hang.
//   DURATION    e.g. 80s
//   API_KEY     tenant key (Authorization: Bearer ...). Named API_KEY so runner.sh redacts it in the manifest.
//   PAD         size of the filler string in the payload. A big payload makes every request that waits in the
//               server's memory heavier, so memory growth from queued requests becomes visible.
//   REQ_TIMEOUT how long k6 waits for an answer, default 15s. A request that gets no answer in that time
//               is counted as "hung" (status 0). It is also the deadline of the CLIENT: it does not cancel
//               anything on the server.
//   PRE_VUS / MAX_VUS   virtual users. While requests hang, every request holds one VU for REQ_TIMEOUT,
//               so VUs needed = RATE x REQ_TIMEOUT. If k6 runs out, it reports dropped_iterations.
//   VARIANT     unset | set   (tag variant)
//   FAULT       lat | stall   (tag fault)
//   PHASE       measured | probe_*  (tag phase). Probes are filtered out of headline panels.
//   POOL_MAX    the pool size the SERVER was started with (tag pool_max).
//   BASE_URL    default http://localhost:8000
//
// ROUTE is used only by runner.sh for its preflight curl (run.sh sets it to /healthz), so the event
// route has its own variable.
import http from "k6/http";
import { Counter } from "k6/metrics";

const BASE = __ENV.BASE_URL || "http://localhost:8000";
const EVENTS_ROUTE = __ENV.EVENTS_ROUTE || "/v1/events";
const API_KEY = __ENV.API_KEY;
const RATE = Number(__ENV.RATE || 100);
const PAD = "x".repeat(Number(__ENV.PAD || 300));

// Response classes. analyze.py reads b10_resp_202 / _503 / _other (run.sh passes --prefix b10).
// b10_resp_hung is the part of _other that is status 0: no answer before REQ_TIMEOUT.
const resp202 = new Counter("b10_resp_202");
const resp503 = new Counter("b10_resp_503");
const respOther = new Counter("b10_resp_other"); // 4xx, 500, or status 0
const respHung = new Counter("b10_resp_hung"); // status 0 only

export const options = {
  summaryTrendStats: ["avg", "min", "med", "max", "p(50)", "p(90)", "p(95)", "p(99)", "p(99.9)"],
  scenarios: {
    ingest: {
      executor: "constant-arrival-rate",
      rate: RATE,
      timeUnit: "1s",
      duration: __ENV.DURATION || "60s",
      preAllocatedVUs: Number(__ENV.PRE_VUS || 1000),
      // Dropped iterations here mean k6 ran out of VUs (RATE x REQ_TIMEOUT too big), not that the server is slow.
      maxVUs: Number(__ENV.MAX_VUS || 4000),
      // Scenario tags, not options.tags: a command-line --tag (runner.sh sets testid) replaces the
      // global options.tags, scenario tags survive (learned in B3).
      tags: {
        exp: "B10",
        variant: __ENV.VARIANT || "unset",
        fault: __ENV.FAULT || "none",
        phase: __ENV.PHASE || "measured",
        pool_max: String(__ENV.POOL_MAX || "unset"),
        offered_rate: String(RATE),
      },
    },
  },
};

export function setup() {
  if (!API_KEY) throw new Error("API_KEY env var is missing (run.sh reads it from services/ingest-api/.env)");
}

// RFC 4122 v4 uuid without any import (no internet needed). The server checks the version and variant nibbles.
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

const headers = { "Content-Type": "application/json", Authorization: "Bearer " + API_KEY };

export default function () {
  const body = JSON.stringify({
    event_id: uuidv4(), // unique per request, so every row in Postgres belongs to exactly one request
    type: "order.created",
    occurred_at: new Date().toISOString(),
    schema_version: 1,
    payload: { user: "u" + Math.floor(Math.random() * 1e6), data: PAD },
  });

  const res = http.post(BASE + EVENTS_ROUTE, body, {
    headers,
    timeout: __ENV.REQ_TIMEOUT || "15s",
    responseCallback: http.expectedStatuses(202), // only 202 counts as success
  });

  if (res.status === 202) resp202.add(1);
  else if (res.status === 503) resp503.add(1);
  else {
    respOther.add(1);
    if (res.status === 0) respHung.add(1);
  }
}
