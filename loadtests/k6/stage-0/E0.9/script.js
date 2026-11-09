// E0.9: Payload size scaling on POST /echo
// ONE k6 script for every payload size. The only thing that changes between runs is an env var.
//
//   RATE           target requests per second (open model: the rate does NOT slow down when the server slows down)
//   DURATION       length of the measured part, e.g. 60s
//   WARMUP         length of a low-rate warm-up before the measured part, e.g. 10s (0s = no warm-up).
//                  It runs at 25% of RATE so V8 can JIT the handler. It is tagged stage=warmup so I can leave it out.
//   PAYLOAD_BYTES  size of the request body in bytes (variant being compared): 100 | 1000 | 10000 | 100000
//   SHAPE          obj = many small key/value fields (realistic, expensive to parse)
//                  str = ONE big string field (cheap to parse). Used for the second pass to see how much the
//                  *shape* of the JSON matters, not only its size.
//   PHASE          measured | probe  (tag, so I can filter probe runs out in Grafana)
//   BASE_URL, ECHO_ROUTE   target. NOTE: runner.sh sends a GET to $ROUTE as a preflight and /echo is POST-only,
//                  so step.sh gives runner.sh ROUTE=/health and this script uses ECHO_ROUTE (default /echo).
import http from "k6/http";

const SIZE = Number(__ENV.PAYLOAD_BYTES || 1000);
const SHAPE = __ENV.SHAPE || "obj";
const RATE = Number(__ENV.RATE || 1000);
const DURATION = __ENV.DURATION || "60s";
const WARMUP = __ENV.WARMUP || "10s";

const toSeconds = (s) => {                       // "60s" -> 60, "2m" -> 120
  const m = /^(\d+)(s|m)$/.exec(s);
  if (!m) throw new Error(`bad duration '${s}' (use e.g. 10s or 2m)`);
  return Number(m[1]) * (m[2] === "m" ? 60 : 1);
};
const WARMUP_S = toSeconds(WARMUP);

// ---- Build the body ONCE in the init context (not per request) ---------------
// If I built it inside default(), the generator would spend CPU making strings and I would be measuring k6.
// The body is ASCII only, so string length == number of bytes on the wire.
function makeBody(size, shape) {
  if (shape === "str") {
    const head = '{"data":"', tail = '"}';
    return head + "x".repeat(size - head.length - tail.length) + tail;
  }
  // "obj": fields like "k12":"val12abcd" and "k13":13091, until I am close to the size,
  // then ONE last string field pads the body to exactly `size` bytes.
  let s = "{";
  let i = 0;
  while (true) {
    const entry = (i % 2 === 0 ? `"k${i}":"val${i}abcdef"` : `"k${i}":${i * 7919}`);
    const next = s + (s.length > 1 ? "," : "") + entry;
    if (next.length + 40 > size) break;          // keep room for the pad field and the closing brace
    s = next;
    i++;
  }
  const sep = s.length > 1 ? "," : "";
  const padHead = `${sep}"p":"`, padTail = '"}';
  const padLen = size - s.length - padHead.length - padTail.length;
  if (padLen < 0) throw new Error(`payload ${size} B is too small for shape ${shape}`);
  return s + padHead + "x".repeat(padLen) + padTail;
}

const BODY = makeBody(SIZE, SHAPE);
if (BODY.length !== SIZE) throw new Error(`body is ${BODY.length} B, wanted ${SIZE} B`);
JSON.parse(BODY);                                // fail early if my own JSON is invalid

const scenarios = {
  steady: {
    executor: "constant-arrival-rate",
    startTime: WARMUP_S > 0 ? `${WARMUP_S + 2}s` : "0s",   // 2 s gap after warm-up
    rate: RATE,
    timeUnit: "1s",
    duration: DURATION,
    preAllocatedVUs: 300,
    maxVUs: 5000,        // if dropped_iterations > 0 the generator is the bottleneck: discard the run
    tags: { stage: "steady" },
  },
};
if (WARMUP_S > 0) {
  scenarios.warmup = {
    executor: "constant-arrival-rate",
    rate: Math.max(1, Math.round(RATE * 0.25)),
    timeUnit: "1s",
    duration: WARMUP,
    preAllocatedVUs: 100,
    maxVUs: 1000,
    tags: { stage: "warmup" },
  };
}

export const options = {
  // These tags become labels on every k6 metric, so Grafana can split by size.
  tags: { size: String(SIZE), shape: SHAPE, phase: __ENV.PHASE || "measured" },

  scenarios,

  // I do not need the echoed body in k6: reading and storing 100 KB x thousands/s would load the generator.
  // (step.sh checks once with curl that the server really echoes the body back.)
  discardResponseBodies: true,

  thresholds: {
    // Threshold on dropped_iterations makes k6 exit non-zero if the generator could not keep the schedule.
    // step.sh then marks the run FAILED, so I notice it instead of trusting the numbers.
    dropped_iterations: ["count<1"],
    // These always pass. They exist only so the summary file also contains numbers for the steady part alone
    // (warm-up requests excluded). A threshold on a tagged sub-metric is what makes k6 export it.
    "http_req_duration{stage:steady}": ["p(99)<1000000"],
    "http_req_failed{stage:steady}": ["rate<=1"],
    "http_reqs{stage:steady}": ["count>=0"],
  },
};

const URL = `${__ENV.BASE_URL || "http://localhost:8000"}${__ENV.ECHO_ROUTE || "/echo"}`;
const PARAMS = { headers: { "Content-Type": "application/json" } };

export default function () {
  http.post(URL, BODY, PARAMS);
}
