// E0.4 message-overhead sub-test: find the throughput ceiling of a route whose compute is negligible.
// Run it three times and compare ceilings:
//   TARGET_ROUTE=/health      -> generator + HTTP floor
//   TARGET_ROUTE=/cpu         -> in-thread path (fib(10) ~ free)
//   TARGET_ROUTE=/cpu-worker  -> same work through the worker thread (adds the thread hop)
// Per-request hop cost = 1/ceiling_worker - 1/ceiling_cpu.
//
// Optional env: TARGET_ROUTE=/cpu-worker  N=10  RATES=500,1000,2000,3000,4000,6000,8000
//               STEP=30  GAP=10  BASE=http://localhost:8000
import http from 'k6/http';

const BASE = __ENV.BASE || 'http://localhost:8000';
const TARGET_ROUTE = __ENV.TARGET_ROUTE || '/cpu-worker';
const N = Number(__ENV.N || 10);
const RATES = (__ENV.RATES || '500,1000,2000,3000,4000,6000,8000').split(',').map(Number);
const STEP = Number(__ENV.STEP || 30);
const GAP = Number(__ENV.GAP || 10);

const URL = TARGET_ROUTE === '/health' ? `${BASE}/health` : `${BASE}${TARGET_ROUTE}?n=${N}`;

const scenarios = {};
const thresholds = {};
RATES.forEach((rate, i) => {
  scenarios[`r_${rate}`] = {
    executor: 'constant-arrival-rate',
    exec: 'hit',
    rate,
    timeUnit: '1s',
    startTime: `${i * (STEP + GAP)}s`,
    duration: `${STEP}s`,
    preAllocatedVUs: 200,
    maxVUs: 4000,
    gracefulStop: '30s',
    tags: { offered_rps: String(rate) },
  };
  thresholds[`http_req_duration{offered_rps:${rate}}`] = ['p(50)>=0', 'p(99)>=0', 'max>=0'];
  thresholds[`http_reqs{offered_rps:${rate}}`] = ['rate>=0'];
  thresholds[`dropped_iterations{offered_rps:${rate}}`] = ['count>=0'];
});

export const options = { scenarios, thresholds };

export function setup() {
  console.log(`target=${URL}  steps=${RATES.join(',')} rps  ${STEP}s each, ${GAP}s gap`);
}

export function hit() {
  http.get(URL);
}
