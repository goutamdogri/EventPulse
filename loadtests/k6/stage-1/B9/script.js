// B9: Retry storm - k6 script for POST /v1/events
// One script for every retry policy. Only env vars change between runs.
// The database slowdown (the trigger) is NOT done by this script: run.sh adds and removes a Toxiproxy
// latency toxic between ingest-api and Postgres while this script keeps sending at the same rate.
//
//   VARIANT     retry policy (also a tag):
//                 none              no retries (control)
//                 immediate         up to MAX_RETRIES retries, no waiting between attempts
//                 backoff           exponential wait 100 ms x 2^(n-1) with FULL jitter (random between 0 and that wait)
//                 backoff_nojitter  same wait but without the random part
//   RATE        NEW events per second (the "original rate"). Open model (constant-arrival-rate): k6 starts this many
//               iterations per second whatever the server does. A retry belongs to the iteration that started it.
//               So the extra load from retries is NOT counted in RATE: it shows up in b9_attempts.
//   DURATION    total length, e.g. 170s (run.sh computes it: healthy + slow + after)
//   RELIEF_AT   probe only: second (from the start) where the rate drops to RELIEF_RATE (load shedding by the
//               generator). 0 = never.   RELIEF_RATE  new events per second after that.
//   MAX_RETRIES default 3.   REQ_TIMEOUT per-attempt client timeout, default 1s. It is SHORTER than the
//               server's pool wait (PG_CONN_TIMEOUT_MS = 2 s) on purpose: a client that gives up while its request
//               still waits in the server's queue is how "wasted work" appears.
//   API_KEY     tenant key (named API_KEY so runner.sh redacts it in the manifest)
//   PAD         filler size in the payload, default 300 -> about 500 B per event
//   PRE_VUS / MAX_VUS   virtual users. A retrying iteration holds its VU for several seconds, so MAX_VUS must be
//               large. If k6 runs out of VUs it reports dropped_iterations: the run is then discarded.
//   BASE_URL    default http://localhost:8000
//   PHASE       measured | probe_relief (tag)
import http from "k6/http";
import { sleep } from "k6";
import { Counter } from "k6/metrics";

const BASE = __ENV.BASE_URL || "http://localhost:8000";
const EVENTS_ROUTE = __ENV.EVENTS_ROUTE || "/v1/events";
const API_KEY = __ENV.API_KEY;
const RATE = Number(__ENV.RATE || 600);
const VARIANT = __ENV.VARIANT || "immediate";
const MAX_RETRIES = VARIANT === "none" ? 0 : Number(__ENV.MAX_RETRIES || 3);
const TIMEOUT = __ENV.REQ_TIMEOUT || "1s";
const PAD = "x".repeat(Number(__ENV.PAD || 300));
const RELIEF_AT = Number(__ENV.RELIEF_AT || 0);
const RELIEF_RATE = Number(__ENV.RELIEF_RATE || Math.round(RATE / 10));
const TOTAL_S = Number(String(__ENV.DURATION || "170s").replace("s", ""));

// Counters (Prometheus names get k6_ in front and _total behind).
//   attempts      every HTTP request sent, first tries and retries. attempts/s is the OFFERED load.
//   retries       only the retries
//   unique_ok     iterations that ended with a 202 (the event really got accepted, after any number of tries)
//   giveups       iterations that used all tries and never got a 202
// resp_* count every answer (analyze.py reads them with --prefix b9).
const attempts = new Counter("b9_attempts");
const retries = new Counter("b9_retries");
const uniqueOk = new Counter("b9_unique_ok");
const giveups = new Counter("b9_giveups");
const resp202 = new Counter("b9_resp_202");
const resp503 = new Counter("b9_resp_503");
const respOther = new Counter("b9_resp_other"); // 4xx, 500, or status 0 (client timeout / connection error)

// Scenario tags (NOT options.tags: runner.sh passes --tag testid=..., which replaces global tags in k6 2.x,
// see B3). These become labels on every k6 metric.
const tags = {
  exp: "B9",
  variant: VARIANT,
  base_rate: String(RATE),
  phase: __ENV.PHASE || "measured",
};
const common = {
  executor: "constant-arrival-rate",
  timeUnit: "1s",
  preAllocatedVUs: Number(__ENV.PRE_VUS || 1000),
  maxVUs: Number(__ENV.MAX_VUS || 8000),
  // iterations still retrying when the scenario ends get 30 s to finish, so nothing is cut in the middle
  gracefulStop: "30s",
  tags,
};

const scenarios = RELIEF_AT > 0
  ? {
      main: { ...common, rate: RATE, duration: `${RELIEF_AT}s` },
      relief: {
        ...common,
        rate: RELIEF_RATE,
        startTime: `${RELIEF_AT}s`,
        duration: `${Math.max(1, TOTAL_S - RELIEF_AT)}s`,
      },
    }
  : { main: { ...common, rate: RATE, duration: `${TOTAL_S}s` } };

export const options = {
  summaryTrendStats: ["avg", "min", "med", "max", "p(50)", "p(90)", "p(95)", "p(99)", "p(99.9)"],
  scenarios,
};

export function setup() {
  if (!API_KEY) throw new Error("API_KEY env var is missing (run.sh reads it from services/ingest-api/.env)");
}

// RFC 4122 v4 uuid without imports (no internet needed). The server checks version and variant nibbles.
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

// seconds to wait before retry number n (n = 1, 2, 3)
function waitBeforeRetry(n) {
  if (VARIANT === "backoff") return Math.random() * 0.1 * Math.pow(2, n - 1);
  if (VARIANT === "backoff_nojitter") return 0.1 * Math.pow(2, n - 1);
  return 0; // none / immediate
}

export default function () {
  // ONE event_id per iteration, reused by every retry: the server does INSERT ... ON CONFLICT DO NOTHING,
  // so a retry of a request that already committed does not create a second row.
  const body = JSON.stringify({
    event_id: uuidv4(),
    type: "order.created",
    occurred_at: new Date().toISOString(),
    schema_version: 1,
    payload: { user: "u" + Math.floor(Math.random() * 1e6), data: PAD },
  });

  for (let n = 0; n <= MAX_RETRIES; n++) {
    if (n > 0) {
      const w = waitBeforeRetry(n);
      if (w > 0) sleep(w);
      retries.add(1);
    }
    attempts.add(1);
    const res = http.post(BASE + EVENTS_ROUTE, body, {
      headers,
      timeout: TIMEOUT,
      responseCallback: http.expectedStatuses(202), // only 202 is success, so http_req_failed = "not accepted"
    });
    if (res.status === 202) {
      resp202.add(1);
      uniqueOk.add(1);
      return;
    }
    if (res.status === 503) resp503.add(1);
    else respOther.add(1);
    // 4xx would be a bug in the test (bad key / body): retrying cannot fix it
    if (res.status >= 400 && res.status < 500) break;
  }
  giveups.add(1);
}
