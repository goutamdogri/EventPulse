// K11: Retention and lag
// ONE k6 script for every variant. The variant (earliest / latest / control) lives in the consumer-sink's settings and
// in the timetable of run.sh, not here. This script sends two streams of valid events through the ingest-api:
//
//   main      RATE req/s for LOAD_S seconds, starting at t=0. This is the bulk of the data. The consumer is stopped
//             a few seconds into it (by run.sh), so most of these events are written while nobody reads.
//   trickle   TRICKLE_RATE req/s for TRICKLE_S seconds, starting when main ends. Why: Kafka only deletes CLOSED
//             segments. A segment closes (rolls) when a new record arrives after segment.ms. If the writes stopped
//             at the end of main, the last segment of every partition would stay open forever and never be deleted.
//             The trickle keeps every partition rolling until the consumer is restarted.
//
// The number of 202 answers (both streams together) is the number of events that were acknowledged. The audit
// compares it with the rows in Postgres.
//
//   RATE, LOAD_S              the main stream
//   TRICKLE_RATE, TRICKLE_S   the trickle stream (TRICKLE_S = 0 turns it off)
//   RUN_ID     8 hex chars. Every event_id starts with it, so the audit counts exactly this run's rows in Postgres
//   CELL       label only (earliest, latest, control), becomes a tag so Grafana can split the variants
//   API_KEY, BASE_URL, PHASE, PAYLOAD_BYTES, PRE_VUS, MAX_VUS
//
// The three response-class counters are the ones the shared analyze.py reads (resp_202, resp_503, resp_other).
import http from "k6/http";
import { Counter } from "k6/metrics";

const API_KEY = __ENV.API_KEY;
const RUN_ID = (__ENV.RUN_ID || "").toLowerCase();
const CELL = __ENV.CELL || "none";
const PAYLOAD_BYTES = Number(__ENV.PAYLOAD_BYTES || 100);
const LOAD_S = Number(__ENV.LOAD_S || 60);
const TRICKLE_S = Number(__ENV.TRICKLE_S || 0);
if (!API_KEY) throw new Error("API_KEY is required");
if (!/^[0-9a-f]{8}$/.test(RUN_ID)) throw new Error("RUN_ID must be 8 hex characters");

const respOk = new Counter("resp_202");
const respFail = new Counter("resp_503");
const respOther = new Counter("resp_other");   // 4xx, 5xx other than 503, and status 0 (timeout / connection error)

const scenarios = {
  main: {
    executor: "constant-arrival-rate",
    rate: Number(__ENV.RATE || 1000),
    timeUnit: "1s",
    duration: `${LOAD_S}s`,
    preAllocatedVUs: Number(__ENV.PRE_VUS || 100),
    maxVUs: Number(__ENV.MAX_VUS || 1000),
    tags: { seg: "main" },
  },
};
if (TRICKLE_S > 0) {
  scenarios.trickle = {
    executor: "constant-arrival-rate",
    rate: Number(__ENV.TRICKLE_RATE || 20),
    timeUnit: "1s",
    duration: `${TRICKLE_S}s`,
    startTime: `${LOAD_S}s`,
    preAllocatedVUs: 10,
    maxVUs: 100,
    tags: { seg: "trickle" },
  };
}

export const options = {
  discardResponseBodies: true,
  tags: { cell: CELL, phase: __ENV.PHASE || "measured" },
  scenarios,
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
  const body = JSON.stringify({
    event_id: eventId(),
    type: "k11.load",
    occurred_at: new Date().toISOString(),
    schema_version: 1,
    payload: { f: filler },
  });
  const res = http.post(`${__ENV.BASE_URL || "http://localhost:8000"}/v1/events`, body, {
    headers: { "content-type": "application/json", authorization: `Bearer ${API_KEY}` },
    timeout: "15s",
  });
  if (res.status === 202) respOk.add(1);
  else if (res.status === 503) respFail.add(1);
  else respOther.add(1);
}
