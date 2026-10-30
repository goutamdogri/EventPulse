// E0.4 Experiment C: unbounded worker queue under sustained overload.
// Phases: BASELINE (health only) -> OVERLOAD at LOAD x C for DURATION -> RECOVER (load stopped, queue drains).
// A light /health probe runs the whole time.
//
// Required env: JOB_TIME (seconds, from capacity-calc.sh)   WORKERS (must match the server)
// Optional env: LOAD=1.5  DURATION=300  BASELINE=30  RECOVER=200  N=38  HEALTH_RATE=50
//               PAD_KB=0   (bytes each queued job carries; makes memory growth visible)
//               TIMEOUT=600s  (use TIMEOUT=10s for the wasted-work run)   MAX_VUS=1500
import http from 'k6/http';

const BASE = __ENV.BASE || 'http://localhost:8000';
const N = Number(__ENV.N || 38);
const WORKERS = Number(__ENV.WORKERS || 1);
const JOB_TIME = Number(__ENV.JOB_TIME);
const LOAD = Number(__ENV.LOAD || 1.5);
const DURATION = Number(__ENV.DURATION || 300);
const BASELINE = Number(__ENV.BASELINE || 30);
const RECOVER = Number(__ENV.RECOVER || 200);
const HEALTH_RATE = Number(__ENV.HEALTH_RATE || 50);
const PAD_KB = Number(__ENV.PAD_KB || 0);
const TIMEOUT = __ENV.TIMEOUT || '600s';
const MAX_VUS = Number(__ENV.MAX_VUS || 1500);

if (!JOB_TIME) throw new Error('Set JOB_TIME (seconds). Run ./E0.4/capacity-calc.sh first.');

const C = WORKERS / JOB_TIME;
const SCALE = 100;
const TOTAL = BASELINE + DURATION + RECOVER;

export const options = {
  scenarios: {
    health: {
      executor: 'constant-arrival-rate',
      exec: 'health',
      rate: HEALTH_RATE,
      timeUnit: '1s',
      duration: `${TOTAL}s`,
      preAllocatedVUs: 20,
      maxVUs: 300,
      tags: { route: 'health' },
    },
    overload: {
      executor: 'constant-arrival-rate',
      exec: 'work',
      rate: Math.max(1, Math.round(LOAD * C * SCALE)),
      timeUnit: `${SCALE}s`,
      startTime: `${BASELINE}s`,
      duration: `${DURATION}s`,
      preAllocatedVUs: 100,
      maxVUs: MAX_VUS,
      gracefulStop: '600s',                   // wait for the backlog to drain
      tags: { route: 'cpu_worker' },
    },
  },
  thresholds: {                               // force numbers into --summary-export
    'http_req_duration{route:health}': ['p(50)>=0', 'p(99)>=0', 'max>=0'],
    'http_req_duration{route:cpu_worker,status:200}': ['p(50)>=0', 'p(99)>=0', 'max>=0'],
    'http_reqs{route:cpu_worker,status:200}': ['count>=0'],
    'http_reqs{route:cpu_worker,status:503}': ['count>=0'],   // shed by a bounded queue
    'http_reqs{route:cpu_worker,status:0}': ['count>=0'],     // client timeouts
    'dropped_iterations{route:cpu_worker}': ['count>=0'],
  },
};

export function setup() {
  const lambda = LOAD * C;
  console.log(`WORKERS=${WORKERS} JOB_TIME=${JOB_TIME}s C=${C.toFixed(3)} rps  offered=${lambda.toFixed(3)} rps (${LOAD}x)`);
  console.log(`predicted: queue(t)=${(lambda - C).toFixed(3)}*t jobs, wait(t)=${((lambda - C) / C).toFixed(3)}*t s`);
  console.log(`predicted at end of overload (${DURATION}s): queue~${Math.round((lambda - C) * DURATION)} jobs, wait~${(((lambda - C) / C) * DURATION).toFixed(0)}s, drain~${(((lambda - C) / C) * DURATION).toFixed(0)}s`);
}

export function work() {
  const pad = PAD_KB > 0 ? `&pad=${PAD_KB}` : '';
  http.get(`${BASE}/cpu-worker?n=${N}${pad}`, { timeout: TIMEOUT });
}

export function health() {
  http.get(`${BASE}/health`);
}
