"use strict";
// REFERENCE ONLY. Merge the ideas into labs/stage-0-foundations/src/worker-pool.js, do not blindly replace it.
//
// Adds what Experiments B and C need:
//   - worker_pool_queue_length / worker_pool_busy_workers   (gauges)
//   - worker_job_wait_seconds / worker_job_run_seconds      (histograms: queue wait vs execution)
//   - worker_pool_jobs_total{outcome=completed|rejected|failed}
//   - optional bounded queue: MAX_QUEUE=0 (default) is UNBOUNDED, MAX_QUEUE=10 sheds with QUEUE_FULL
//   - WORKERS env var for the pool size
//
// Assumed worker.js contract (one job at a time per worker):
//   parentPort.on('message', ({ n }) => parentPort.postMessage({ result: fib(n) }));
//
// Assumed prom-client registry: the default one (the one your /metrics route already serves).
const { Worker } = require("node:worker_threads");
const path = require("node:path");
const client = require("prom-client");

const WORKERS = Number(process.env.WORKERS || 1);
const MAX_QUEUE = Number(process.env.MAX_QUEUE || 0); // 0 = unbounded (Experiment C1/C2)

const queueLength = new client.Gauge({
  name: "worker_pool_queue_length",
  help: "Jobs waiting for a free worker",
});
const busyWorkers = new client.Gauge({
  name: "worker_pool_busy_workers",
  help: "Workers currently running a job",
});
const jobsTotal = new client.Counter({
  name: "worker_pool_jobs_total",
  help: "Jobs by outcome",
  labelNames: ["outcome"],
});
// buckets must reach the longest wait you expect (about 150 s at the end of C1)
const BUCKETS = [
  0.001, 0.01, 0.05, 0.1, 0.35, 0.5, 1, 2, 5, 10, 30, 60, 120, 240, 480,
];
const waitSeconds = new client.Histogram({
  name: "worker_job_wait_seconds",
  help: "Time in queue",
  buckets: BUCKETS,
});
const runSeconds = new client.Histogram({
  name: "worker_job_run_seconds",
  help: "Time executing in worker",
  buckets: BUCKETS,
});

class WorkerPool {
  constructor(size, file) {
    this.file = file;
    this.idle = [];
    this.queue = []; // NOTE: Array.shift() is O(n); fine for thousands of jobs, not millions
    for (let i = 0; i < size; i++) this._spawn();
  }

  _spawn() {
    const w = new Worker(this.file);
    w.job = null;
    w.on("message", (msg) => {
      const job = w.job;
      w.job = null;
      runSeconds.observe((performance.now() - job.startedAt) / 1000);
      jobsTotal.inc({ outcome: "completed" });
      busyWorkers.dec();
      job.resolve(msg.result);
      this.idle.push(w);
      this._dispatch();
    });
    w.on("error", (err) => {
      const job = w.job;
      if (job) {
        busyWorkers.dec();
        jobsTotal.inc({ outcome: "failed" });
        job.reject(err);
      }
      this.idle = this.idle.filter((x) => x !== w);
      this._spawn(); // replace the dead worker
      this._dispatch();
    });
    this.idle.push(w);
  }

  run(data) {
    if (MAX_QUEUE > 0 && this.queue.length >= MAX_QUEUE) {
      jobsTotal.inc({ outcome: "rejected" });
      const e = new Error("worker queue full");
      e.code = "QUEUE_FULL";
      return Promise.reject(e);
    }
    return new Promise((resolve, reject) => {
      this.queue.push({
        data,
        resolve,
        reject,
        enqueuedAt: performance.now(),
        startedAt: 0,
      });
      queueLength.set(this.queue.length);
      this._dispatch();
    });
  }

  _dispatch() {
    while (this.idle.length && this.queue.length) {
      const w = this.idle.pop();
      const job = this.queue.shift();
      job.startedAt = performance.now();
      waitSeconds.observe((job.startedAt - job.enqueuedAt) / 1000);
      w.job = job;
      busyWorkers.inc();
      w.postMessage(job.data); // job.data (incl. pad) is copied to the worker
    }
    queueLength.set(this.queue.length);
  }
}

const pool = new WorkerPool(WORKERS, path.join(__dirname, "worker.js"));
module.exports = { pool };

/* ---------------------------------------------------------------------------------------------
   Route sketch for your plain `http` server (adapt to your routes/cpu-worker.js):

   const { pool } = require('../worker-pool');

   async function cpuWorker(req, res, url) {
     const n = Number(url.searchParams.get('n') ?? 30);
     const padKb = Number(url.searchParams.get('pad') ?? 0);
     // pad simulates a request body held in memory while the job waits in the queue
     const pad = padKb > 0 ? Buffer.alloc(padKb * 1024, 1) : undefined;
     try {
       const result = await pool.run({ n, pad });
       res.writeHead(200, { 'content-type': 'application/json' });
       res.end(JSON.stringify({ result }));
     } catch (e) {
       if (e.code === 'QUEUE_FULL') {
         res.writeHead(503, { 'content-type': 'application/json', 'retry-after': '1' });
         return res.end(JSON.stringify({ error: 'queue_full' }));
       }
       res.writeHead(500); res.end();
     }
   }
   --------------------------------------------------------------------------------------------- */
