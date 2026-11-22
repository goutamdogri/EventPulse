// K10: Kafka unavailable and API backpressure
// ONE k6 script for both variants (unbounded queue / bounded queue). The variant lives in the API's settings, not here:
// this script only sends a steady stream of valid events and counts what the API answered.
//
//   RATE         requests per second (open model: the rate does NOT slow down when the API slows down)
//   VARIANT      label only (unbounded | bounded), becomes a tag so Grafana can split the variants
//   T_DOWN       seconds after the start when run.sh kills all 3 brokers
//   T_RESTART    seconds after the start when run.sh starts them again
//   DURATION     total length, e.g. 240s
//   CLIENT_TIMEOUT  how long k6 waits for an answer before it gives up (default 2s)
//   API_KEY, BASE_URL, PHASE
//
// The 3 time segments (tag "seg"): steady -> down -> recovery. run.sh does the real kills with docker, this script only
// knows the planned times, so the segment edges are good to about a second.
// The three response-class counters are the ones the shared analyze.py reads (resp_202, resp_503, resp_other).
// resp_other is where an unbounded API shows up: a request that never gets an answer ends as status 0 (timeout), and so
// does a connection refused after the process died.
import http from "k6/http";
import { Counter } from "k6/metrics";
import exec from "k6/execution";

const API_KEY = __ENV.API_KEY;
const VARIANT = __ENV.VARIANT || "bounded";
const T1 = Number(__ENV.T_DOWN || 30);
const T2 = Number(__ENV.T_RESTART || 150);
const PAYLOAD_BYTES = Number(__ENV.PAYLOAD_BYTES || 100);
const CLIENT_TIMEOUT = __ENV.CLIENT_TIMEOUT || "2s";
if (!API_KEY) throw new Error("API_KEY is required");

const respOk = new Counter("resp_202");
const respFail = new Counter("resp_503");
const respOther = new Counter("resp_other");   // 4xx, 5xx other than 503, and status 0 (timeout / connection error)

export const options = {
  discardResponseBodies: true,
  tags: { variant: VARIANT, phase: __ENV.PHASE || "measured" },
  scenarios: {
    steady: {
      // constant-arrival-rate = open model: new requests keep coming even if the API stops answering. That is the point:
      // a real client population does not slow down because Kafka is down.
      executor: "constant-arrival-rate",
      rate: Number(__ENV.RATE || 2000),
      timeUnit: "1s",
      duration: __ENV.DURATION || "240s",
      preAllocatedVUs: Number(__ENV.PRE_VUS || 200),
      // An unanswered request holds a VU for CLIENT_TIMEOUT (2 s). 2000 req/s x 2 s = 4,000 VUs in the worst case, so
      // the pool must be bigger than that or k6 drops iterations. I kept the timeout short on purpose: the machine has
      // little free RAM and every VU costs memory (a 5 s timeout would need 10,000 VUs).
      maxVUs: Number(__ENV.MAX_VUS || 6000),
    },
  },
  // Empty thresholds only make k6 keep a sub-metric per segment in the summary export (resp_202{seg:steady} ...),
  // which run.sh reads to print how many 202/503/other each segment had.
  thresholds: Object.fromEntries(
    ["resp_202", "resp_503", "resp_other"].flatMap((m) =>
      ["steady", "down", "recovery"].map((s) => [`${m}{seg:${s}}`, ["count>=0"]]),
    ),
  ),
};

const hex = (n) => {
  let s = "";
  for (let i = 0; i < n; i++) s += ((Math.random() * 16) | 0).toString(16);
  return s;
};
// A valid random UUID v4 (the API validates event_id).
const eventId = () => `${hex(8)}-${hex(4)}-4${hex(3)}-${"89ab"[(Math.random() * 4) | 0]}${hex(3)}-${hex(12)}`;
const filler = "x".repeat(Math.max(0, PAYLOAD_BYTES - 20));

export default function () {
  // exec.scenario.startTime is the wall-clock start of the scenario (ms), the same for every VU. (Date.now() at init
  // time would differ per VU, because k6 creates the extra VUs only when it needs them.)
  const el = (Date.now() - exec.scenario.startTime) / 1000;
  const seg = el < T1 ? "steady" : el < T2 ? "down" : "recovery";
  const body = JSON.stringify({
    event_id: eventId(),
    type: "k10.load",
    occurred_at: new Date().toISOString(),
    schema_version: 1,
    payload: { f: filler },
  });
  const res = http.post(`${__ENV.BASE_URL || "http://localhost:8000"}/v1/events`, body, {
    headers: { "content-type": "application/json", authorization: `Bearer ${API_KEY}` },
    timeout: CLIENT_TIMEOUT,
    tags: { seg },
  });
  if (res.status === 202) respOk.add(1, { seg });
  else if (res.status === 503) respFail.add(1, { seg });
  else respOther.add(1, { seg });
}
