// K8: Rebalances and group stability
// ONE k6 script for every cell (scenario x assignor). The cell lives in what run.sh does to the consumers, not here:
// this script only sends a steady stream of valid events through the ingest-api and counts what the API answered.
// The number of 202 answers is the number of events that MUST end up in Postgres (the audit compares against it).
//
//   RATE       requests per second (open model: the rate does not slow down when the API slows down)
//   DURATION   length of the load, e.g. 30s  (RATE x DURATION = about how many events the run sends)
//   RUN_ID     8 hex chars. Every event_id starts with it, so the audit counts exactly this run's rows in Postgres
//   CELL       label only (e.g. s1-coop), becomes a tag so Grafana can split the cells
//   API_KEY, BASE_URL, PHASE, PAYLOAD_BYTES, PRE_VUS, MAX_VUS
//
// The three response-class counters are the ones the shared analyze.py reads (resp_202, resp_503, resp_other).
import http from "k6/http";
import { Counter } from "k6/metrics";

const API_KEY = __ENV.API_KEY;
const RUN_ID = (__ENV.RUN_ID || "").toLowerCase();
const CELL = __ENV.CELL || "none";
const PAYLOAD_BYTES = Number(__ENV.PAYLOAD_BYTES || 100);
if (!API_KEY) throw new Error("API_KEY is required");
if (!/^[0-9a-f]{8}$/.test(RUN_ID)) throw new Error("RUN_ID must be 8 hex characters");

const respOk = new Counter("resp_202");
const respFail = new Counter("resp_503");
const respOther = new Counter("resp_other");   // 4xx, 5xx other than 503, and status 0 (timeout / connection error)

export const options = {
  discardResponseBodies: true,
  tags: { cell: CELL, phase: __ENV.PHASE || "measured" },
  scenarios: {
    steady: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.RATE || 2000),
      timeUnit: "1s",
      duration: __ENV.DURATION || "30s",
      preAllocatedVUs: Number(__ENV.PRE_VUS || 100),
      maxVUs: Number(__ENV.MAX_VUS || 1000),
    },
  },
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
    type: "k8.load",
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
