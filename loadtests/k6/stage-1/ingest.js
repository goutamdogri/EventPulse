import http from "k6/http";
import { Counter } from "k6/metrics";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

export const accepted = new Counter("ep_accepted"); // 202s, for the audit

export const options = {
  scenarios: {
    ingest: {
      executor: "constant-arrival-rate", // open model
      rate: Number(__ENV.RATE || 500),
      timeUnit: "1s",
      duration: __ENV.DURATION || "60s",
      preAllocatedVUs: 200,
      maxVUs: 3000,
    },
  },
};

const pad = "x".repeat(Number(__ENV.PAD || 300)); // ~500 B events
const url = `${__ENV.BASE_URL || "http://localhost:8000"}/v1/events`;
const params = {
  headers: {
    "Content-Type": "application/json",
    Authorization: "Bearer " + __ENV.API_KEY,
  },
};

export default function () {
  const body = JSON.stringify({
    event_id: uuidv4(),
    type: "order.created",
    occurred_at: new Date().toISOString(),
    schema_version: 1,
    payload: { user: "u" + Math.floor(Math.random() * 1e6), data: pad },
  });
	
  const res = http.post(url, body, params);
  if (res.status === 202) accepted.add(1);
}
