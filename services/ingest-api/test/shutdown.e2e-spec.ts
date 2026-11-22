// S2-F1 "graceful shutdown": SIGTERM with a non-empty batcher queue flushes it before the process exits.
// Runs the built API (dist/main.js) as a child process so a real SIGTERM can be sent.
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { endOffsets, totalOf } from './kafka-helpers.js';

const PORT = 18900 + Math.floor(Math.random() * 90);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('SIGTERM with queued requests', () => {
  it('every queued request is answered 202 and is in the log before the process exits', async () => {
    const proc = spawn(process.execPath, ['dist/main.js'], {
      cwd: resolve(import.meta.dirname, '..'),
      // A 2 s batch wait keeps all 50 requests queued in the batcher when SIGTERM arrives.
      env: { ...process.env, PORT: String(PORT), BATCH_MAX_WAIT_MS: '2000', BATCH_MAX_MESSAGES: '1000', LOG_ENABLED: 'false' },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const log: string[] = [];
    proc.stdout.on('data', (d) => log.push(String(d)));
    proc.stderr.on('data', (d) => log.push(String(d)));
    const exited = new Promise<{ code: number | null; signal: string | null }>((r) => proc.once('exit', (code, signal) => r({ code, signal })));

    try {
      for (let i = 0; i < 60; i++) {
        try {
          if ((await fetch(`http://localhost:${PORT}/readyz`)).ok) break;
        } catch {
          /* not listening yet */
        }
        await sleep(250);
      }
      const before = totalOf(await endOffsets());

      const requests = Array.from({ length: 50 }, () =>
        fetch(`http://localhost:${PORT}/v1/events`, {
          method: 'POST',
          headers: { authorization: 'Bearer key_a', 'content-type': 'application/json' },
          body: JSON.stringify({ event_id: randomUUID(), type: 'shutdown.test', occurred_at: new Date().toISOString(), schema_version: 1, payload: {} }),
        }).then((r) => r.status),
      );
      await sleep(400); // requests are queued, the 2 s flush timer has not fired
      const t0 = Date.now();
      proc.kill('SIGTERM');

      const statuses = await Promise.all(requests);
      const exit = await exited;
      expect(statuses.every((s) => s === 202), `statuses ${statuses.join(',')}\n${log.join('')}`).toBe(true);
      expect(Date.now() - t0).toBeLessThan(1500); // drained at once, did not wait for the 2 s timer
      expect(exit.code === 0 || exit.signal === 'SIGTERM').toBe(true);
      expect(totalOf(await endOffsets()) - before).toBe(50n); // all 50 are in the log
    } finally {
      if (proc.exitCode === null && proc.signalCode === null) proc.kill('SIGKILL');
    }
  }, 60_000);
});
