// Fixed-size worker pool with a bounded queue; respawns crashed workers.
import { Worker } from "node:worker_threads";

export class WorkerPool {
  constructor(size, maxQueue) {
    this.size = size; // Configured number of workers
    this.maxQueue = maxQueue; // Maximum number of waiting jobs
    this.queue = []; // Jobs waiting for a worker
    this.idle = []; // Workers available to accept a job
    this.workers = new Set(); // Set of all currently tracked workers
    this.closing = false; // Prevents replacement workers from being created during shutdown
    for (let i = 0; i < size; i++) this.spawn();
  }

  spawn() {
    const w = new Worker(new URL("./worker.js", import.meta.url));
    w.current = null;
    w.on("message", ({ result }) => {
      const job = w.current;
      w.current = null;
      this.idle.push(w);
      job?.resolve(result);
      this.drain();
    });
    w.on("error", () => {}); // 'exit' follows and handles cleanup
    w.on("exit", () => {
      this.workers.delete(w);
      this.idle = this.idle.filter((x) => x !== w);
      if (w.current) w.current.reject(new Error("worker crashed"));
      if (!this.closing) this.spawn();
    });
    this.workers.add(w);
    this.idle.push(w);
    this.drain();
  }

  submit(n) {
    return new Promise((resolve, reject) => {
      if (this.queue.length >= this.maxQueue) {
        return reject(
          Object.assign(new Error("queue full"), { code: "QUEUE_FULL" }),
        );
      }
      this.queue.push({ n, resolve, reject });
      this.drain();
    });
  }

  drain() {
    while (this.idle.length && this.queue.length) {
      const w = this.idle.pop();
      const job = this.queue.shift();
      w.current = job;
      w.postMessage({ n: job.n });
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
