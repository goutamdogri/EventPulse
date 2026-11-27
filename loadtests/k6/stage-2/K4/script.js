// K4: Kill a broker under load
// ONE k6 script for every variant (acks all / acks 1). The variant lives in the API's settings, not here: this script
// only sends a steady stream of valid events and counts what the API answered.
//
//   RATE         requests per second (open model: the rate does NOT slow down when the API slows down)
//   RUN_ID       8 hex chars. Every event_id starts with it, so the audit can count exactly this run's rows in Postgres
//   ACKS         label only (all | 1), becomes a tag so Grafana can split the variants
//   T_ONE_DOWN   seconds after the start when the first broker is killed
//   T_TWO_DOWN   seconds after the start when the second broker is killed
//   T_RESTART    seconds after the start when both brokers are started again
//   DURATION     total length, e.g. 210s
//   API_KEY, BASE_URL, PHASE
//
// The 4 time segments (tag "seg"): steady -> one_down -> two_down -> recovery. The run.sh controller does the real
// kills with docker, this script only knows the planned times, so the segment edges are good to about a second.
// The three response-class counters are the ones the shared analyze.py reads (resp_202, resp_503, resp_other).
import http from "k6/http";
import { Counter } from "k6/metrics";
import exec from "k6/execution";

const API_KEY = __ENV.API_KEY;
const RUN_ID = (__ENV.RUN_ID || "").toLowerCase();
const ACKS = __ENV.ACKS || "all";
const T1 = Number(__ENV.T_ONE_DOWN || 30);
const T2 = Number(__ENV.T_TWO_DOWN || 90);
const T3 = Number(__ENV.T_RESTART || 135);
const PAYLOAD_BYTES = Number(__ENV.PAYLOAD_BYTES || 100);
if (!API_KEY) throw new Error("API_KEY is required");
if (!/^[0-9a-f]{8}$/.test(RUN_ID)) throw new Error("RUN_ID must be 8 hex characters");

const respOk = new Counter("resp_202");
const respFail = new Counter("resp_503");
const respOther = new Counter("resp_other");   // 4xx, 5xx other than 503, and status 0 (timeout / connection error)

export const options = {
  discardResponseBodies: true,
  tags: { acks: ACKS, phase: __ENV.PHASE || "measured" },
  scenarios: {
    steady: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.RATE || 2000),
      timeUnit: "1s",
      duration: __ENV.DURATION || "210s",
      preAllocatedVUs: Number(__ENV.PRE_VUS || 200),
      // While two brokers are down a request can wait for the API's send timeout (5 s). 2000 req/s x 5 s = 10,000
      // requests in flight is the worst case, so the VU pool must be bigger than that or k6 drops iterations.
      maxVUs: Number(__ENV.MAX_VUS || 12000),
    },
  },
  // Empty thresholds only make k6 keep a sub-metric per segment in the summary export (resp_202{seg:steady} ...),
  // which run.sh reads to print how many 202/503 each segment had.
  thresholds: Object.fromEntries(
    ["resp_202", "resp_503", "resp_other"].flatMap((m) =>
      ["steady", "one_down", "two_down", "recovery"].map((s) => [`${m}{seg:${s}}`, ["count>=0"]]),
    ),
  ),
};

const hex = (n) => {
  let s = "";
  for (let i = 0; i < n; i++) s += ((Math.random() * 16) | 0).toString(16);
  return s;
};
// A valid UUID v4 whose first 8 hex chars are the run id.
const eventId = () => `${RUN_ID}-${hex(4)}-4${hex(3)}-${"89ab"[(Math.random() * 4) | 0]}${hex(3)}-${hex(12)}`;
const filler = "x".repeat(Math.max(0, PAYLOAD_BYTES - 20));

export default function () {
  // exec.scenario.startTime is the wall-clock start of the scenario (ms), the same for every VU. (Date.now() at init
  // time would differ per VU, because k6 creates the extra VUs only when it needs them.)
  const el = (Date.now() - exec.scenario.startTime) / 1000;
  const seg = el < T1 ? "steady" : el < T2 ? "one_down" : el < T3 ? "two_down" : "recovery";
  const body = JSON.stringify({
    event_id: eventId(),
    type: "k4.load",
    occurred_at: new Date().toISOString(),
    schema_version: 1,
    payload: { f: filler },
  });
  const res = http.post(`${__ENV.BASE_URL || "http://localhost:8000"}/v1/events`, body, {
    headers: { "content-type": "application/json", authorization: `Bearer ${API_KEY}` },
    timeout: "15s",
    tags: { seg },
  });
  if (res.status === 202) respOk.add(1, { seg });
  else if (res.status === 503) respFail.add(1, { seg });
  else respOther.add(1, { seg });
}
