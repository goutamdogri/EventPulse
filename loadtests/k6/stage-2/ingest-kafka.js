// Stage 2 load for the S2-F7 audit: constant arrival rate of POST /v1/events with RUN-SCOPED event ids.
//
// event_id = <RUN_ID (8 hex)>-xxxx-4xxx-yxxx-xxxxxxxxxxxx : a valid UUID whose first 8 hex chars are the run id,
// so the audit can count exactly this run's rows:  WHERE event_id::text LIKE '<RUN_ID>-%'.
//
// Env: BASE_URL (http://localhost:8000), API_KEY (required), RUN_ID (8 hex, required), RATE (req/s, 200),
//      DURATION (30s), PRE_VUS (50), MAX_VUS (500), TENANTS (unused by the API: the tenant comes from the key),
//      PAYLOAD_BYTES (approx payload size, 100), RETRY_503_FRACTION (0..1, default 0: fraction of 503s retried
//      once with the SAME event_id, to measure client-retry duplicates later), SUMMARY_PATH (k6-summary.json).
import http from 'k6/http';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
const API_KEY = __ENV.API_KEY;
const RUN_ID = (__ENV.RUN_ID || '').toLowerCase();
const RATE = Number(__ENV.RATE || 200);
const DURATION = __ENV.DURATION || '30s';
const PAYLOAD_BYTES = Number(__ENV.PAYLOAD_BYTES || 100);
const RETRY_503_FRACTION = Number(__ENV.RETRY_503_FRACTION || 0);
const SUMMARY_PATH = __ENV.SUMMARY_PATH || 'k6-summary.json';

if (!API_KEY) throw new Error('API_KEY is required');
if (!/^[0-9a-f]{8}$/.test(RUN_ID)) throw new Error('RUN_ID must be 8 hex characters');

export const options = {
  discardResponseBodies: true,
  scenarios: {
    ingest: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Number(__ENV.PRE_VUS || 50),
      maxVUs: Number(__ENV.MAX_VUS || 500),
    },
  },
  summaryTrendStats: ['p(50)', 'p(90)', 'p(99)', 'max'],
};

const sent = new Counter('audit_sent');
const s202 = new Counter('audit_status_202');
const s4xx = new Counter('audit_status_4xx');
const s503 = new Counter('audit_status_503');
const sOther = new Counter('audit_status_other');
const netErr = new Counter('audit_network_errors');
const retries = new Counter('audit_retries_sent');

const hex = (n) => {
  let s = '';
  for (let i = 0; i < n; i++) s += ((Math.random() * 16) | 0).toString(16);
  return s;
};
const eventId = () => `${RUN_ID}-${hex(4)}-4${hex(3)}-${'89ab'[(Math.random() * 4) | 0]}${hex(3)}-${hex(12)}`;
const filler = 'x'.repeat(Math.max(0, PAYLOAD_BYTES - 20));
const params = { headers: { 'content-type': 'application/json', authorization: `Bearer ${API_KEY}` }, timeout: '30s' };

function record(res) {
  if (res.status === 0) netErr.add(1);
  else if (res.status === 202) s202.add(1);
  else if (res.status === 503) s503.add(1);
  else if (res.status >= 400 && res.status < 500) s4xx.add(1);
  else sOther.add(1);
}

export default function () {
  const body = JSON.stringify({
    event_id: eventId(),
    type: 'audit.load',
    occurred_at: new Date().toISOString(),
    schema_version: 1,
    payload: { f: filler },
  });
  sent.add(1);
  const res = http.post(`${BASE_URL}/v1/events`, body, params);
  record(res);
  if (res.status === 503 && RETRY_503_FRACTION > 0 && Math.random() < RETRY_503_FRACTION) {
    retries.add(1); // the retry is a separate request with the SAME event_id
    sent.add(1);
    record(http.post(`${BASE_URL}/v1/events`, body, params));
  }
}

export function handleSummary(data) {
  const c = (name) => (data.metrics[name] ? data.metrics[name].values.count : 0);
  const d = data.metrics.http_req_duration ? data.metrics.http_req_duration.values : {};
  const summary = {
    run_id: RUN_ID,
    rate: RATE,
    duration: DURATION,
    sent: c('audit_sent'),
    by_status: { '202': c('audit_status_202'), '4xx': c('audit_status_4xx'), '503': c('audit_status_503'), other: c('audit_status_other') },
    network_errors: c('audit_network_errors'),
    retries_sent: c('audit_retries_sent'),
    dropped_iterations: c('dropped_iterations'),
    latency_ms: { p50: d['p(50)'] ?? null, p99: d['p(99)'] ?? null, max: d.max ?? null },
  };
  return {
    [SUMMARY_PATH]: JSON.stringify(summary, null, 2),
    stdout: `\nk6 summary: sent ${summary.sent}, 202 ${summary.by_status['202']}, 503 ${summary.by_status['503']}, 4xx ${summary.by_status['4xx']}, other ${summary.by_status.other}, network errors ${summary.network_errors}, dropped ${summary.dropped_iterations}\n`,
  };
}
