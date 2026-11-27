// K2: Producer settings sweep - k6 script for POST /v1/events
// Same request as K1 (about 500 B), except the filler can be random so compression is tested on realistic bytes.
// Only env vars change between runs; the producer settings are on the server side (run.sh).
//
//   RATE        target requests per second. Open model (constant-arrival-rate):
//               k6 keeps sending at this rate even when the server gets slow.
//               A closed model would slow itself down and hide the collapse (Stage 0 lesson).
//   DURATION    e.g. 60s
//   API_KEY     the tenant key (Authorization: Bearer ...). Named API_KEY on purpose:
//               runner.sh redacts names that contain API_KEY in its manifest.
//   PAD         size of the filler string in the payload, default 300 -> about 500 B per event (same as K1)
//   PAYLOAD_MODE  random (default) = filler picked from a pool of 1024 random base64 strings, hardly compressible;
//                 x = the K1 filler "xxxx..." (compresses about 50:1, the unrealistic best case)
//   PRE_VUS / MAX_VUS   virtual users. If k6 runs out of VUs it reports dropped_iterations.
//   REQ_TIMEOUT per-request timeout, default 10s (so a hung request cannot hold a VU forever)
//   BASE_URL    default http://localhost:8000
//   PHASE       measured (tag, so a probe run could be filtered out in Grafana)
//
// One difference from B1: discardResponseBodies is on. At 10k+ requests/s k6 would otherwise spend CPU copying
// response bodies that I never read. It does not change what is sent, only how much work the generator does.
//
// I do NOT use the ROUTE env var here. runner.sh uses ROUTE only for its preflight curl
// (I set it to /healthz), so the event route has its own variable.
import http from "k6/http";
import { Counter } from "k6/metrics";
import { SharedArray } from "k6/data";

const BASE = __ENV.BASE_URL || "http://localhost:8000";
const EVENTS_ROUTE = __ENV.EVENTS_ROUTE || "/v1/events";
const API_KEY = __ENV.API_KEY;
const RATE = Number(__ENV.RATE || 1000);
const PAD_LEN = Number(__ENV.PAD || 300);
const PAYLOAD_MODE = __ENV.PAYLOAD_MODE || "random";

// A pool of random strings, built once (SharedArray: one copy for all VUs). The seed is fixed, so every run and
// every config sends the same bytes. Picking from 1024 strings keeps the generator cheap (no random string per request).
const B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const PADS = new SharedArray("pads", function () {
  if (PAYLOAD_MODE === "x") return ["x".repeat(PAD_LEN)];
  let seed = 12345;
  const rnd = () => (seed = (seed * 1664525 + 1013904223) % 4294967296) / 4294967296;
  const out = [];
  for (let i = 0; i < 1024; i++) {
    let t = "";
    for (let j = 0; j < PAD_LEN; j++) t += B64[Math.floor(rnd() * 64)];
    out.push(t);
  }
  return out;
});

// Three response-class counters; analyze.py reads them with --prefix k2 (k2_resp_202 / k2_resp_503 / k2_resp_other).
// In Stage 2, 202 = "appended to Kafka under the acks policy", 503 = the API shed the request (queue full, breaker open...).
const resp202 = new Counter("k2_resp_202");
const resp503 = new Counter("k2_resp_503");
const respOther = new Counter("k2_resp_other");   // 4xx, 500, or status 0 (timeout / connection error)

export const options = {
  discardResponseBodies: true,

  // These tags become labels on every k6 metric in Prometheus.
  // offered_rate lets a dashboard split the runs by step.
  tags: { exp: "K2", offered_rate: String(RATE), phase: __ENV.PHASE || "measured" },

  // The default summary does not include p99; I want p99 and p99.9 in the exported json.
  summaryTrendStats: ["avg", "min", "med", "max", "p(50)", "p(90)", "p(95)", "p(99)", "p(99.9)"],

  scenarios: {
    ingest: {
      executor: "constant-arrival-rate",
      rate: RATE,
      timeUnit: "1s",
      duration: __ENV.DURATION || "60s",
      preAllocatedVUs: Number(__ENV.PRE_VUS || 500),
      // If dropped_iterations > 0 either the generator is the bottleneck (check k6 CPU)
      // or the server is so slow that all VUs are waiting (that one is a finding, not a bug).
      maxVUs: Number(__ENV.MAX_VUS || 3000),
    },
  },
};

export function setup() {
  if (!API_KEY) {
    throw new Error("API_KEY env var is missing (run.sh reads it from services/ingest-api/.env)");
  }
}

// RFC 4122 version 4 uuid without any import. I avoid the jslib uuid helper so the
// test does not need internet access. The server checks IsUUID(), so the version and
// variant nibbles are set explicitly.
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
    event_id: uuidv4(),                          // unique per request, so every 202 must be exactly one record in the topic
    type: "order.created",
    occurred_at: new Date().toISOString(),
    schema_version: 1,
    payload: { user: "u" + Math.floor(Math.random() * 1e6), data: PADS[Math.floor(Math.random() * PADS.length)] },
  });

  const res = http.post(BASE + EVENTS_ROUTE, body, {
    headers,
    timeout: __ENV.REQ_TIMEOUT || "10s",
    // Only 202 counts as success, so http_req_failed = "not accepted".
    responseCallback: http.expectedStatuses(202),
  });

  if (res.status === 202) resp202.add(1);
  else if (res.status === 503) resp503.add(1);
  else respOther.add(1);
}
