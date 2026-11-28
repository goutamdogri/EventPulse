// S3-F4: Kafka down for 30 s while config changes keep committing. The relay backs off, nothing is lost, the backlog
// is visible (ep_outbox_unpublished, oldest age), and after recovery everything is published in version order.
// This file STOPS AND STARTS the shared broker containers (ep-kafka1..3): don't run it during a benchmark.
import { execFileSync } from 'node:child_process';
import { createKafka, loadKafkaClientConfig, waitTopicReady } from '@eventpulse/kafka-config';
import { sleep, waitFor } from '../src/harness.js';
import { OutboxLane, metricValue, orderViolations, recordMeasurement, waitHttpReady } from '../src/outbox.js';

const BROKERS = ['ep-kafka1', 'ep-kafka2', 'ep-kafka3'];
const docker = (...args: string[]) => execFileSync('docker', args, { stdio: ['ignore', 'pipe', 'pipe'] }).toString().trim();
const health = (n: string) => {
  try {
    return docker('inspect', '-f', '{{.State.Health.Status}}', n);
  } catch {
    return 'missing';
  }
};

async function restoreCluster(topic: string) {
  docker('start', ...BROKERS);
  await waitFor(() => BROKERS.every((b) => health(b) === 'healthy'), 120_000, 'brokers healthy');
  const admin = createKafka({ ...loadKafkaClientConfig(process.env, 'outbox-kafka-down'), logLevel: 'error' }).admin();
  await admin.connect();
  try {
    await waitTopicReady(admin, topic, 3, 3000, 90_000);
  } finally {
    await admin.disconnect();
  }
}

describe('S3-F4 outbox relay with Kafka down', () => {
  it('[G7.kafka-down] 30 s outage while changes keep coming: backlog and oldest age grow, nothing lost, all published in order afterwards', async () => {
    const lane = await OutboxLane.create();
    let stopped = false;
    try {
      const relay = lane.relay({ RELAY_SEND_TIMEOUT_MS: '3000', RELAY_BACKOFF_MAX_MS: '2000', RELAY_BATCH_SIZE: '50' });
      await waitHttpReady(relay);
      await lane.writeChanges(50);
      await waitFor(async () => (await lane.counts()).published === 50, 20_000, 'first 50 published');

      docker('stop', '-t', '1', ...BROKERS);
      stopped = true;
      const t0 = Date.now();
      let written = 50;
      let maxAge = 0;
      let maxBacklog = 0;
      while (Date.now() - t0 < 30_000) {
        written += (await lane.writeChanges(5)).length; // admin-api does not need Kafka: changes keep committing
        maxBacklog = Math.max(maxBacklog, await metricValue(relay, 'ep_outbox_unpublished'));
        maxAge = Math.max(maxAge, await metricValue(relay, 'ep_outbox_oldest_unpublished_age_seconds'));
        await sleep(200);
      }
      const failures = await metricValue(relay, 'ep_outbox_relay_cycle_failures_total');
      expect((await lane.counts()).published).toBeLessThan(written);
      expect(maxBacklog).toBeGreaterThan(100);
      expect(maxAge).toBeGreaterThan(15); // the oldest unpublished change ages with the outage
      expect(failures).toBeGreaterThan(0);
      expect(relay.alive()).toBe(true); // backs off, does not crash

      await restoreCluster(lane.topic);
      stopped = false;
      const recovery0 = Date.now();
      await waitFor(async () => (await lane.counts()).published === written, 120_000, 'backlog drained after recovery');
      const recoveryMs = Date.now() - recovery0;
      await waitFor(async () => (await metricValue(relay, 'ep_outbox_oldest_unpublished_age_seconds')) === 0, 10_000, 'age back to 0');

      const got = await lane.read(30_000, (rs) => rs.length >= written);
      expect(new Set(got.map((c) => c.eventId)).size).toBe(written); // nothing lost
      expect(orderViolations(got)).toBe(0);
      const view = OutboxLane.apply(got);
      expect(view.get(lane.tenant)!.version).toBe(written);
      recordMeasurement('kafka_down_30s', { changes: written, backlog_max: maxBacklog, oldest_age_max_s: Number(maxAge.toFixed(1)), failed_cycles: failures, drain_ms_after_brokers_healthy: recoveryMs, republished: got.length - written, lost: 0, order_violations: 0 });
      await relay.stop();
    } finally {
      if (stopped) await restoreCluster(lane.topic).catch(() => undefined);
      await lane.cleanup();
    }
  });
});
