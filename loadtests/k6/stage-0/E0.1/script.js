import http from "k6/http";

export const options = {
  scenarios: {
    steady: {
      executor: "ramping-arrival-rate",

      startRate: 50,
      timeUnit: "1s",

      stages: [
        { target: 5000, duration: "10s" },
        { target: 15000, duration: "10s" },
        { target: 20000, duration: "10s" },
        { target: 100000, duration: "40m" },
      ],

      preAllocatedVUs: 10000,
      maxVUs: 10000,
      gracefulStop: "10s",
    },
  },

  // Applied to built-in metrics as well as custom metrics.
  tags: {
    testid: __ENV.TESTID || "run-001",
    role: "load-generator",
  },

  discardResponseBodies: true,
  summaryTrendStats: ["avg", "p(50)", "p(90)", "p(99)", "p(99.9)", "max"],
};

export default function () {
  http.get(
    `${__ENV.BASE || "http://localhost:8000"}${__ENV.ROUTE || "/health"}`,
    {
      timeout: "10s",
    },
  );
}
