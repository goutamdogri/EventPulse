// K12: Message size and batch limits
// ONE k6 script for every variant and every size. The variant (edge / batcher / broker / client / acks0) lives in the
// settings of the ingest-api and of the topic, not here. run.sh starts one k6 run per cell (variant x size), so every
// run of this script sends ONE size.
//
// What it sends: valid events (they pass the DTO validation), only the payload is padded with "x" so the size is what I want.
//
//   SIZE_KIND=body   SIZE = bytes of the HTTP request body. This is what the edge (BODY_LIMIT) judges.
//   SIZE_KIND=wire   SIZE = bytes the batcher counts for one event (key + value + header names and values).
//                    This is what the batcher cap (BATCH_MAX_BYTES) and librdkafka's message.max.bytes judge.
//                    The wire size is a little bigger than the body: the API adds tenant_id to the value, and the
//                    message has a key and 3 headers. Here the key is the event_id (KAFKA_KEY_MODE=event, 36 bytes),
//                    the headers are received_at (11 + 24), schema_version (14 + 1) and request_id (10 + 36) = 96 bytes,
//                    and the value has the extra  "tenant_id":"<tenant>",  = 15 + length of the tenant name.
//                    So wire = body + 36 + 96 + 15 + TENANT.length.
//   SIZE_KIND=mix    "poison in a batch": MIX_RATE req/s of small events (about 1,000 bytes), and every MIX_BIG_EVERY-th
//                    request is a big one (MIX_BIG_BYTES). Why: when a batch has one bad message, I want to see how many
//                    good neighbours fail with it.
//
//   RATE, DURATION_S   open model, constant arrival rate (5 req/s for 10 s = 50 requests per cell by default)
//   TENANT             tenant name of the API key (run.sh reads it from services/ingest-api/.env). Needed for the wire size
//   CELL               variant name, becomes a tag so Grafana can split the variants
//   LEVEL              label of the size (e.g. b65000, w999900, mix), becomes a tag
//   API_KEY, BASE_URL, PHASE
//
// The class counters resp_202 / resp_503 / resp_other are the ones the shared analyze.py reads. resp_other is everything
// that is not 202 or 503. resp_413, resp_500 and resp_status0 split it, because for this experiment the exact failure matters.
// status 0 = k6 got no HTTP answer (connection reset or timeout): the server closed the connection while k6 was still sending.
import http from "k6/http";
import exec from "k6/execution";
import { Counter, Gauge } from "k6/metrics";

const API_KEY = __ENV.API_KEY;
const TENANT = __ENV.TENANT || "";
const KIND = __ENV.SIZE_KIND || "body";
const SIZE = Number(__ENV.SIZE || 1000);
const RATE = Number(__ENV.RATE || 5);
const DURATION_S = Number(__ENV.DURATION_S || 10);
const MIX_BIG_BYTES = Number(__ENV.MIX_BIG_BYTES || 400000);
const MIX_BIG_EVERY = Number(__ENV.MIX_BIG_EVERY || 100);
const SMALL_BYTES = 1000;
if (!API_KEY) throw new Error("API_KEY is required");
if (KIND === "wire" && !TENANT) throw new Error("TENANT is required for SIZE_KIND=wire");
const WIRE_EXTRA = 36 + 96 + 15 + TENANT.length;   // see the header comment

const respOk = new Counter("resp_202");
const respFail = new Counter("resp_503");
const respOther = new Counter("resp_other");
const resp413 = new Counter("resp_413");
const resp500 = new Counter("resp_500");
const respStatus0 = new Counter("resp_status0");
const smallOk = new Counter("small_202");
const smallFail = new Counter("small_fail");
const bigOk = new Counter("big_202");
const bigFail = new Counter("big_fail");
const bodyGauge = new Gauge("body_bytes");   // the body size really sent (the main size of the cell)
const wireGauge = new Gauge("wire_bytes");   // the size the batcher counts for it

const isMix = KIND === "mix";
const options_scenario = isMix
  ? { rate: Number(__ENV.MIX_RATE || 1000), preAllocatedVUs: 100, maxVUs: 500 }
  : { rate: RATE, preAllocatedVUs: 10, maxVUs: 50 };

export const options = {
  discardResponseBodies: true,
  tags: { cell: __ENV.CELL || "none", level: __ENV.LEVEL || "none", phase: __ENV.PHASE || "measured" },
  scenarios: {
    sizes: { executor: "constant-arrival-rate", timeUnit: "1s", duration: `${DURATION_S}s`, ...options_scenario },
  },
};

const hex = (n) => {
  let s = "";
  for (let i = 0; i < n; i++) s += ((Math.random() * 16) | 0).toString(16);
  return s;
};
const eventId = () => `${hex(8)}-${hex(4)}-4${hex(3)}-${"89ab"[(Math.random() * 4) | 0]}${hex(3)}-${hex(12)}`;

// Length of the body when the filler is empty. The real body is this plus the filler length.
const FIXED_ID = "00000000-0000-4000-8000-000000000000";
const FIXED_TS = "2026-01-01T00:00:00.000Z";   // the real occurred_at (toISOString) has the same length: 24
const baseBody = (id, ts, filler) =>
  JSON.stringify({ event_id: id, type: "k12.size", occurred_at: ts, schema_version: 1, payload: { f: filler } });
const BASE_LEN = baseBody(FIXED_ID, FIXED_TS, "").length;

function fillerFor(bodyBytes) {
  const n = bodyBytes - BASE_LEN;
  if (n < 0) throw new Error(`body of ${bodyBytes} bytes is smaller than the smallest event (${BASE_LEN})`);
  return "x".repeat(n);
}

let mainBody = 0;
if (isMix) mainBody = MIX_BIG_BYTES;
else mainBody = KIND === "wire" ? SIZE - WIRE_EXTRA : SIZE;
// The filler strings are built once per VU (module scope), not once per request: a 5 MB string per request would cost k6 CPU.
const FILLER_MAIN = fillerFor(mainBody);
const FILLER_SMALL = isMix ? fillerFor(SMALL_BYTES) : "";

export default function () {
  const big = !isMix || exec.scenario.iterationInTest % MIX_BIG_EVERY === 0;
  const id = eventId();
  const body = baseBody(id, new Date().toISOString(), big ? FILLER_MAIN : FILLER_SMALL);
  // x-request-id = the event id (36 bytes, allowed by the API), so the request_id header has a known length
  const res = http.post(`${__ENV.BASE_URL || "http://localhost:8000"}/v1/events`, body, {
    headers: { "content-type": "application/json", authorization: `Bearer ${API_KEY}`, "x-request-id": id },
    timeout: "30s",
  });
  if (res.status === 202) respOk.add(1);
  else if (res.status === 503) respFail.add(1);
  else respOther.add(1);
  if (res.status === 413) resp413.add(1);
  if (res.status === 500) resp500.add(1);
  if (res.status === 0) respStatus0.add(1);
  if (isMix) {
    if (big) (res.status === 202 ? bigOk : bigFail).add(1);
    else (res.status === 202 ? smallOk : smallFail).add(1);
  }
  if (exec.scenario.iterationInTest === 0) {
    bodyGauge.add(body.length);
    wireGauge.add(body.length + WIRE_EXTRA);
  }
}
