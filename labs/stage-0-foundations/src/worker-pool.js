// Fixed-size worker pool with a bounded queue; respawns crashed workers.
import { Worker } from "node:worker_threads";
import { performance } from "node:perf_hooks";

export class WorkerPool {
  constructor(size, maxQueue) {
    this.size = size;
    this.maxQueue = maxQueue > 0 ? maxQueue : Infinity; // 0 = unbounded (Experiment C1/C2)
    this.queue = [];
    this.idle = [];
    this.workers = new Set();
    this.closing = false;
    this.hooks = {}; // set by createMetrics: { wait(s), run(s), outcome(name) }
    for (let i = 0; i < size; i++) this.spawn();
  }

  spawn() {
    const w = new Worker(new URL("./worker.js", import.meta.url));
    w.current = null;
    w.on("message", ({ result }) => {
      const job = w.current;
      w.current = null;
      this.idle.push(w);
      if (job) {
        this.hooks.run?.((performance.now() - job.startedAt) / 1000);
        this.hooks.outcome?.("completed");
        job.resolve(result);
      }
      this.drain();
    });
    w.on("error", () => {});
    w.on("exit", () => {
      this.workers.delete(w);
      this.idle = this.idle.filter((x) => x !== w);
      if (w.current) {
        this.hooks.outcome?.("failed");
        w.current.reject(new Error("worker crashed"));
      }
      if (!this.closing) this.spawn();
    });
    this.workers.add(w);
    this.idle.push(w);
    this.drain();
  }

  submit(n, pad) {
    return new Promise((resolve, reject) => {
      if (this.queue.length >= this.maxQueue) {
        this.hooks.outcome?.("rejected");
        return reject(
          Object.assign(new Error("queue full"), { code: "QUEUE_FULL" }),
        );
      }
      // pad (optional Buffer) is held here while the job waits, simulating a queued request body
      this.queue.push({
        n,
        pad,
        resolve,
        reject,
        enqueuedAt: performance.now(),
        startedAt: 0,
      });
      this.drain();
    });
  }

  drain() {
    while (this.idle.length && this.queue.length) {
      const w = this.idle.pop();
      const job = this.queue.shift();
      job.startedAt = performance.now();
      this.hooks.wait?.((job.startedAt - job.enqueuedAt) / 1000);
      w.current = job;
      w.postMessage({ n: job.n, pad: job.pad }); // worker.js ignores pad
    }
  }

  stats() {
    return {
      size: this.size,
      busy: this.size - this.idle.length,
      queueDepth: this.queue.length,
      maxQueue: this.maxQueue,
    };
  }

  async close() {
    this.closing = true;
    await Promise.all([...this.workers].map((w) => w.terminate()));
  }
}
