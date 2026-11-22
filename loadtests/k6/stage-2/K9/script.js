// K9: Hot key and skew
// ONE k6 script for every cell (key mode x rate). The key mode lives in how run.sh starts the ingest-api, not here:
// this script sends a steady stream of valid events where ONE tenant sends HOT_SHARE % of the requests.
// The ingest-api takes the tenant from the API key (never from the body), so "which tenant" = "which API key".
//
//   RATE        requests per second (open model: the rate does not slow down when the API slows down)
//   DURATION    length of the load, e.g. 60s
//   RUN_ID      8 hex chars. Every event_id starts with it, so the audit counts exactly this run's rows in Postgres
//   K9_KEYS     comma separated API keys. The FIRST one is the hot tenant, the others share the remaining requests evenly
//   HOT_SHARE   percent of the requests that use the first key (default 80)
//   CELL        label only (e.g. tenant-r1500), becomes a tag so Grafana can split the cells
//   BASE_URL, PHASE, PAYLOAD_BYTES, PRE_VUS, MAX_VUS
//
// resp_202 / resp_503 / resp_other are the response-class counters the shared analyze.py reads.
// k9_hot_sent / k9_other_sent count the accepted (202) requests of the hot tenant and of all the others, so the real share
// of the hot tenant can be checked after the run instead of trusted.
import http from "k6/http";
import { Counter } from "k6/metrics";

const KEYS = (__ENV.K9_KEYS || "").split(",").filter(Boolean);
const HOT_SHARE = Number(__ENV.HOT_SHARE || 80);
const RUN_ID = (__ENV.RUN_ID || "").toLowerCase();
const CELL = __ENV.CELL || "none";
const PAYLOAD_BYTES = Number(__ENV.PAYLOAD_BYTES || 100);
if (KEYS.length < 2) throw new Error("K9_KEYS needs at least 2 keys (the hot tenant and one more)");
if (!/^[0-9a-f]{8}$/.test(RUN_ID)) throw new Error("RUN_ID must be 8 hex characters");

const respOk = new Counter("resp_202");
const respFail = new Counter("resp_503");
const respOther = new Counter("resp_other");   // 4xx, 5xx other than 503, and status 0 (timeout / connection error)
const hotSent = new Counter("k9_hot_sent");
const otherSent = new Counter("k9_other_sent");

export const options = {
  discardResponseBodies: true,
  tags: { cell: CELL, phase: __ENV.PHASE || "measured" },
  scenarios: {
    steady: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.RATE || 1000),
      timeUnit: "1s",
      duration: __ENV.DURATION || "60s",
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
  // Each request picks its tenant on its own, so the share is HOT_SHARE % on average over the run (not exact per second).
  const hot = Math.random() * 100 < HOT_SHARE;
  const key = hot ? KEYS[0] : KEYS[1 + ((Math.random() * (KEYS.length - 1)) | 0)];
  const body = JSON.stringify({
    event_id: eventId(),
    type: "k9.load",
    occurred_at: new Date().toISOString(),
    schema_version: 1,
    payload: { f: filler },
  });
  const res = http.post(`${__ENV.BASE_URL || "http://localhost:8000"}/v1/events`, body, {
    headers: { "content-type": "application/json", authorization: `Bearer ${key}` },
    timeout: "15s",
  });
  if (res.status === 202) { respOk.add(1); (hot ? hotSent : otherSent).add(1); }
  else if (res.status === 503) respFail.add(1);
  else respOther.add(1);
}
