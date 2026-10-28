import http from "k6/http";

export const options = {
  scenarios: {
    closed: {
      executor: "constant-vus",
      vus: 6,
      duration: "90s",
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
