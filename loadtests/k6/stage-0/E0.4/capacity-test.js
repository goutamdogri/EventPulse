// E0.4 Experiment B: capacity of /cpu-worker vs worker count.
// Step test. Each step offers a fraction of the THEORETICAL capacity C = WORKERS / JOB_TIME,
// so the step where reality departs from the prediction is your measured knee.
// A light /health probe runs the whole time (isolation check).
//
// Required env:  JOB_TIME (seconds, from capacity-calc.sh)   WORKERS (must match the server)
// Optional env:  FRACTIONS=0.5,0.8,0.95,1.1,1.5  STEP=60  GAP=15  N=38  HEALTH_RATE=50
//                PAD_KB=0 (see reference/ example)  TIMEOUT=300s  BASE=http://localhost:8000
import http from 'k6/http';

const BASE = __ENV.BASE || 'http://localhost:8000';
const N = Number(__ENV.N || 38);
const WORKERS = Number(__ENV.WORKERS || 1);
const JOB_TIME = Number(__ENV.JOB_TIME);
const FRACTIONS = (__ENV.FRACTIONS || '0.5,0.8,0.95,1.1,1.5').split(',').map(Number);
const STEP = Number(__ENV.STEP || 60);       // seconds per step
const GAP = Number(__ENV.GAP || 15);         // idle seconds between steps so the backlog drains
const HEALTH_RATE = Number(__ENV.HEALTH_RATE || 50);
const PAD_KB = Number(__ENV.PAD_KB || 0);
const TIMEOUT = __ENV.TIMEOUT || '300s';

if (!JOB_TIME) throw new Error('Set JOB_TIME (seconds). Run ./E0.4/capacity-calc.sh first.');

const C = WORKERS / JOB_TIME;                // theoretical capacity, requests/second
const SCALE = 100;                           // k6 rate must be an integer: rate/100s gives 0.01 rps resolution
const TOTAL = FRACTIONS.length * (STEP + GAP);

const scenarios = {};
const thresholds = {
  'http_req_duration{route:health}': ['p(50)>=0', 'p(99)>=0', 'max>=0'],
};

FRACTIONS.forEach((f, i) => {
  const pct = String(Math.round(f * 100));
  scenarios[`step_${pct}`] = {
    executor: 'constant-arrival-rate',
    exec: 'work',
    rate: Math.max(1, Math.round(f * C * SCALE)),
    timeUnit: `${SCALE}s`,
    startTime: `${i * (STEP + GAP)}s`,
    duration: `${STEP}s`,
    preAllocatedVUs: 50,
    maxVUs: 1000,
    gracefulStop: '180s',                    // let the overload backlog finish instead of killing it
    tags: { route: 'cpu_worker', load_pct: pct },
  };
  // thresholds only exist to force per-step numbers into --summary-export
  thresholds[`http_req_duration{load_pct:${pct}}`] = ['p(50)>=0', 'p(99)>=0', 'max>=0'];
  thresholds[`http_reqs{load_pct:${pct}}`] = ['rate>=0'];
  thresholds[`dropped_iterations{load_pct:${pct}}`] = ['count>=0'];
});

scenarios.health = {
  executor: 'constant-arrival-rate',
  exec: 'health',
  rate: HEALTH_RATE,
  timeUnit: '1s',
  duration: `${TOTAL}s`,
  preAllocatedVUs: 20,
  maxVUs: 300,
  tags: { route: 'health' },
};

export const options = { scenarios, thresholds };

export function setup() {
  console.log(`WORKERS=${WORKERS} JOB_TIME=${JOB_TIME}s  C_theoretical=${C.toFixed(3)} rps  total=${TOTAL}s`);
  FRACTIONS.forEach((f) => console.log(`  ${Math.round(f * 100)}% of C -> ${(f * C).toFixed(3)} rps`));
}

export function work() {
  const pad = PAD_KB > 0 ? `&pad=${PAD_KB}` : '';
  http.get(`${BASE}/cpu-worker?n=${N}${pad}`, { timeout: TIMEOUT });
}

export function health() {
  http.get(`${BASE}/health`);
}
