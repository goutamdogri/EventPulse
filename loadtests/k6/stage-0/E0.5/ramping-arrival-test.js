import http from 'k6/http';
import { sleep } from 'k6';
import { Counter } from 'k6/metrics';

const TARGET_URL = __ENV.TARGET_URL || 'http://localhost:8000';
const IO_ROUTE   = __ENV.IO_ROUTE   || '/io';
const HOLD_MS    = Number(__ENV.HOLD_MS || 5000);    // how long server holds each request
const STEP_RPS   = (__ENV.STEP_VUS || '10,20,30,40,50,60')
  .split(',').map((s) => Number(s.trim()));
const RAMP       = __ENV.RAMP       || '10s';        // time to reach each step
const STEP_HOLD  = __ENV.STEP_HOLD  || '20s';       // time spent at each step
const RAMP_DOWN  = __ENV.RAMP_DOWN  || '10s';

const stages = [];
for (const target of STEP_RPS) {
  stages.push({ duration: RAMP, target });
  stages.push({ duration: STEP_HOLD, target });
}
stages.push({ duration: RAMP_DOWN, target: 0 });

export const options = {
  discardResponseBodies: true,
  scenarios: {
    fd_exhaustion: {
      executor: 'ramping-arrival-rate',
      startRate: 0,
      stages,
      gracefulStop: '15s',
      preAllocatedVUs: 2,
			maxVUs: 400
    },
  },
};

const connErrors = new Counter('e05_conn_errors'); // tagged by k6 error_code
const connOk     = new Counter('e05_conn_ok');

export default function () {
  const res = http.get(`${TARGET_URL}${IO_ROUTE}?ms=${HOLD_MS}`, {
    tags: { name: 'io-hold' },                 // fixed name: no URL cardinality
    timeout: `${HOLD_MS + 15000}ms`,
  });

  if (res.status === 0) {
    // status 0 = no HTTP response (reset / EOF / refused / timeout)
    connErrors.add(1, { error_code: String(res.error_code) });
    sleep(0.5); // do not spin a tight reconnect loop against a saturated server
  } else {
    connOk.add(1);
  }
}