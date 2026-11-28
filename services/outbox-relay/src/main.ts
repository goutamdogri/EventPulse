// services/outbox-relay (S3-F4): a small process around packages/outbox PollingRelay. Plain node:http (no Nest):
// four read-only endpoints and one debug one do not need a framework.
//
//   GET  /healthz        process alive
//   GET  /readyz         Postgres answers and the producer is connected (a standby is ready too: it can take over)
//   GET  /metrics        ep_outbox_* + process metrics
//   GET  /debug/stats    relay counters, leader, backlog, config, faults
//   GET|POST|DELETE /debug/faults   fault injector (only with DEBUG_FAULTS=true)
//
// Shutdown (SIGTERM/SIGINT): stop polling, finish the in-flight batch, release the advisory lock (a standby takes
// over at once), disconnect.
import { createServer } from 'node:http';
import type { IncomingMessage, ServerResponse } from 'node:http';
import pg from 'pg';
import { FaultConfigError, FaultInjector } from '@eventpulse/fault-injector';
import { createKafka } from '@eventpulse/kafka-config';
import { PollingRelay } from '@eventpulse/outbox';
import type { RelayProducer } from '@eventpulse/outbox';
import { backoffMs } from '@eventpulse/retry';
import { loadConfig } from './config.js';
import { RelayMetrics } from './metrics.js';

const cfg = loadConfig();
const log = (s: string) => {
  if (cfg.logEnabled) console.log(s);
};
const faults = new FaultInjector(cfg.faults);
const metrics = new RelayMetrics();
const pool = new pg.Pool({ connectionString: cfg.databaseUrl, max: 4, connectionTimeoutMillis: 3_000, idleTimeoutMillis: 30_000, application_name: 'outbox-relay' });
pool.on('error', (e) => console.error('[pg idle client error]', e.message));

const kafkaProducer = createKafka(cfg.kafka).producer({
  kafkaJS: { acks: -1, idempotent: true, allowAutoTopicCreation: false },
  'linger.ms': 5,
  'message.timeout.ms': cfg.sendTimeoutMs,
} as any);
let producerConnected = false;
const producer: RelayProducer = {
  async send(topic, messages) {
    await kafkaProducer.send({ topic, messages: messages as any });
  },
};

const relay = new PollingRelay(pool, producer, cfg.relay, faults, metrics, (l) => console.error(l));
metrics.leadership(relay.isLeader);

let lastBacklog: { unpublished: number; oldestAgeS: number | null } | null = null;
async function pollBacklog() {
  try {
    lastBacklog = await relay.backlog();
    metrics.unpublished.set(lastBacklog.unpublished);
    metrics.oldestAge.set(lastBacklog.oldestAgeS ?? 0);
  } catch {
    /* Postgres down: keep the last values */
  }
}
const gaugeTimer = setInterval(() => void pollBacklog(), cfg.gaugePollMs);

function json(res: ServerResponse, status: number, body: unknown) {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify(body));
}

async function readBody(req: IncomingMessage): Promise<any> {
  let s = '';
  for await (const c of req) {
    s += c;
    if (s.length > 64 * 1024) throw new Error('body too large');
  }
  return s ? JSON.parse(s) : {};
}

async function handle(req: IncomingMessage, res: ServerResponse) {
  const path = (req.url ?? '/').split('?')[0];
  if (req.method === 'GET' && path === '/healthz') return json(res, 200, { status: 'ok' });
  if (req.method === 'GET' && path === '/readyz') {
    let postgres = false;
    try {
      await pool.query('SELECT 1');
      postgres = true;
    } catch {
      /* not ready */
    }
    const checks = { postgres, kafka_producer: producerConnected || !cfg.enabled };
    return json(res, postgres && checks.kafka_producer ? 200 : 503, { status: postgres && checks.kafka_producer ? 'ready' : 'not_ready', checks });
  }
  if (req.method === 'GET' && path === '/metrics') {
    res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4' });
    return res.end(await metrics.registry.metrics());
  }
  if (req.method === 'GET' && path === '/debug/stats') {
    await pollBacklog();
    return json(res, 200, { enabled: cfg.enabled, relay: relay.stats, is_leader: relay.isLeader, backlog: lastBacklog, config: { relay: cfg.relay, brokers: cfg.kafka.brokers, send_timeout_ms: cfg.sendTimeoutMs }, faults: faults.describe() });
  }
  if (path === '/debug/faults') {
    if (!faults.enabled) return json(res, 404, { message: 'fault injection is disabled' });
    try {
      if (req.method === 'POST') {
        const b = await readBody(req);
        faults.arm({ faults: b.faults, errors: b.errors });
      } else if (req.method === 'DELETE') faults.disarm();
      return json(res, 200, faults.describe());
    } catch (e) {
      return json(res, e instanceof FaultConfigError ? 400 : 500, { message: (e as Error).message });
    }
  }
  json(res, 404, { message: 'not found' });
}

const server = createServer((req, res) => {
  handle(req, res).catch((e) => json(res, 500, { message: (e as Error).message }));
});

async function connectProducer() {
  for (let attempt = 1; !stopping; attempt++) {
    try {
      await kafkaProducer.connect();
      producerConnected = true;
      return;
    } catch (e) {
      console.error(`[relay] Kafka producer connect failed (attempt ${attempt}): ${(e as Error).message}`);
      await new Promise((r) => setTimeout(r, backoffMs(attempt, 500, 10_000)));
    }
  }
}

let stopping = false;
async function shutdown(signal: string) {
  if (stopping) return;
  stopping = true;
  console.log(`[relay] ${signal}: finishing the in-flight batch`);
  clearInterval(gaugeTimer);
  server.close();
  await relay.stop();
  if (producerConnected) await kafkaProducer.disconnect().catch(() => undefined);
  await pool.end().catch(() => undefined);
  process.exit(0);
}
process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

server.listen(cfg.port, '0.0.0.0', () => {
  console.log(`outbox-relay listening on :${cfg.port} pid=${process.pid} mode=${cfg.relay.mode} enabled=${cfg.enabled} topics=${cfg.relay.topics.join(',') || '(all)'}`);
});
void pollBacklog();
if (cfg.enabled) {
  void connectProducer().then(() => {
    if (stopping) return;
    relay.start();
    log('[relay] started');
  });
}
