import http from "k6/http";

export const options = {
  scenarios: {
    steady: {
      executor: "constant-arrival-rate",
      rate: 20000,
      duration: "90s",
      timeUnit: "1s",

      preAllocatedVUs: 500,
      maxVUs: 500,
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
