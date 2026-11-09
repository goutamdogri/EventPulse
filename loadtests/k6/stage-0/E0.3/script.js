import http from "k6/http";

const BASE = __ENV.BASE || "http://localhost:8000";
const N = __ENV.N || 38;
const CPU_ROUTE = __ENV.CPU_ROUTE || "/cpu"; // switch to /cpu-worker for E0.4
const HEALTH_RATE = Number(__ENV.HEALTH_RATE || 200);
const CPU_RATE = Number(__ENV.CPU_RATE || 2);

export const options = {
  scenarios: {
    health: {
      // runs the whole time
      executor: "constant-arrival-rate",
      exec: "health", // function with same name should present
      rate: HEALTH_RATE,
      timeUnit: "1s",
      duration: "120s",
      preAllocatedVUs: 300,
      maxVUs: 2000,
    },
    cpu: {
      // only active 30s-90s
      executor: "constant-arrival-rate",
      exec: "cpu",
      rate: CPU_RATE,
      timeUnit: "1s",
      startTime: "30s",
      duration: "60s",
      preAllocatedVUs: 20,
      maxVUs: 100,
    },
  },
  thresholds: {
    // forces per-scenario numbers into the summary
    "http_req_duration{scenario:health}": ["p(50)>=0", "p(99)>=0", "max>=0"],
    "http_req_duration{scenario:cpu}": ["p(50)>=0", "max>=0"],
  },
};

export function health() {
  http.get(`${BASE}/health`);
}
export function cpu() {
  http.get(`${BASE}${CPU_ROUTE}?n=${N}`, { timeout: "30s" });
}
