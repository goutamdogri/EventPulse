// PollingRelay (S3-F4): publishes committed outbox rows to Kafka, at least once.
//
// One cycle:
//   1. CLAIM (own short transaction, committed):  UPDATE outbox SET attempts = attempts + 1 WHERE id IN (the oldest
//      `batch` unpublished ids, FOR UPDATE SKIP LOCKED) RETURNING id. Committed BEFORE producing, so a crash later in
//      the cycle still leaves attempts > 1 on the rows: durable evidence that they may have been published twice.
//   2. LOCK (the publish transaction):  BEGIN; SELECT the claimed rows still unpublished ... FOR UPDATE SKIP LOCKED.
//                                       relay:before-publish
//   3. PRODUCE every row (idempotent producer, acks=all, one send per topic in id order, headers + outbox_id) and
//      wait for every ack.             relay:after-publish-before-mark
//   4. MARK:  UPDATE outbox SET published_at = now() WHERE id = ANY(ids)     relay:after-mark-before-commit
//      COMMIT.
// Publish-then-mark = at-least-once: a crash between 3 and the COMMIT republishes the batch. Consumers dedup on the
// event's own id (event_id header) and apply by version (applyIfNewer).
//
// attempts > 1 means "needed more than one try": a crash, a Kafka error, a timeout. Some of those reached Kafka
// (crash after the ack, a timed-out send that was delivered anyway), some did not (crash before producing), so
// ep_outbox_republished_total is an UPPER BOUND on duplicates; the exact count is at the consumer (event_id).
//
// Ordering. With several relays, SKIP LOCKED lets relay B publish row 11 while relay A is still publishing row 10 of
// the same tenant. Default RELAY_MODE=single: one active relay holds pg_try_advisory_lock(RELAY_LOCK_KEY) on a
// dedicated session; the others stand by and take over when that session dies (process killed, connection lost).
// RELAY_MODE=multi (no lock) exists for experiment C8 only. Consumers are protected by the version either way.
//
// Kafka down: produce throws, the publish transaction rolls back, rows stay unpublished, the relay backs off with
// jitter and retries. Nothing is ever dropped.

import type { Pool, PoolClient } from 'pg';
import type { CrashPointName } from '@eventpulse/fault-injector';
import { backoffMs } from '@eventpulse/retry';
import { HEADER_OUTBOX_ID } from './config-events.js';
import { OUTBOX_CHANNEL } from './enqueue.js';

export const RELAY_MODES = ['single', 'multi'] as const;
export type RelayMode = (typeof RELAY_MODES)[number];

export interface RelayOptions {
  batchSize: number;
  pollMs: number;
  mode: RelayMode;
  /** pg advisory lock key of the single active relay. */
  lockKey: number;
  /** How often a standby tries to take the lock. */
  standbyPollMs: number;
  /** Only rows of these topics (tests use it to stay on their throw-away topics). Empty = all. */
  topics: string[];
  backoffInitialMs: number;
  backoffMaxMs: number;
  /** LISTEN outbox_new: wake up on a commit instead of waiting for the next poll. */
  listen: boolean;
  /** Published rows older than this are deleted (0 = never). */
  retentionMs: number;
  retentionIntervalMs: number;
  retentionBatch: number;
}

export const DEFAULT_RELAY_OPTIONS: RelayOptions = {
  batchSize: 100,
  pollMs: 200,
  mode: 'single',
  lockKey: 4_281_004, // arbitrary constant: "outbox relay"
  standbyPollMs: 1000,
  topics: [],
  backoffInitialMs: 200,
  backoffMaxMs: 10_000,
  listen: true,
  retentionMs: 7 * 24 * 3600 * 1000,
  retentionIntervalMs: 60_000,
  retentionBatch: 1000,
};

export interface RelayMessage {
  key: string;
  value: Buffer;
  headers: Record<string, string>;
}

export interface RelayProducer {
  /** Resolves when every message is acknowledged (acks=all). */
  send(topic: string, messages: RelayMessage[]): Promise<void>;
}

export interface PublishedRow {
  id: string;
  topic: string;
  attempts: number;
  createdAt: Date;
}

export interface RelayObserver {
  published?(rows: PublishedRow[], publishedAt: Date): void;
  cycleFailed?(err: unknown, consecutive: number): void;
  leadership?(isLeader: boolean): void;
  retention?(deleted: number): void;
}

export interface RelayFaults {
  startBatch(): void;
  hit(p: CrashPointName, ctx?: { rows?: number }): void;
  maybeFail(p: CrashPointName): void;
}

export interface OutboxRow {
  id: string;
  topic: string;
  msg_key: string;
  payload: Buffer;
  headers: Record<string, string>;
  created_at: Date;
  attempts: number;
}

const TOPIC_FILTER = `($2::text[] = '{}' OR topic = ANY($2::text[]))`;

export const CLAIM_SQL = `
  UPDATE outbox SET attempts = attempts + 1
  WHERE id IN (SELECT id FROM outbox WHERE published_at IS NULL AND ${TOPIC_FILTER} ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED)
  RETURNING id`;
export const LOCK_SQL = `
  SELECT id, topic, msg_key, payload, headers, created_at, attempts FROM outbox
  WHERE id = ANY($1::bigint[]) AND published_at IS NULL ORDER BY id FOR UPDATE SKIP LOCKED`;
export const MARK_SQL = `UPDATE outbox SET published_at = now() WHERE id = ANY($1::bigint[]) RETURNING published_at`;
export const BACKLOG_SQL = `
  SELECT count(*)::int AS n, extract(epoch FROM now() - min(created_at))::float8 AS age
  FROM outbox WHERE published_at IS NULL AND ($1::text[] = '{}' OR topic = ANY($1::text[]))`;
export const RETENTION_SQL = `
  DELETE FROM outbox WHERE id IN (
    SELECT id FROM outbox WHERE published_at IS NOT NULL AND published_at < now() - ($1::bigint * interval '1 millisecond') ORDER BY id LIMIT $2)`;

/** The Kafka messages of locked rows, grouped by topic, each group in id order. Pure. */
export function toMessages(rows: OutboxRow[]): Map<string, RelayMessage[]> {
  const out = new Map<string, RelayMessage[]>();
  for (const r of [...rows].sort((a, b) => (BigInt(a.id) < BigInt(b.id) ? -1 : 1))) {
    const headers: Record<string, string> = {};
    for (const [k, v] of Object.entries(r.headers ?? {})) headers[k] = String(v);
    headers[HEADER_OUTBOX_ID] = String(r.id);
    (out.get(r.topic) ?? out.set(r.topic, []).get(r.topic)!).push({ key: r.msg_key, value: r.payload, headers });
  }
  return out;
}

export interface RelayStats {
  mode: RelayMode;
  leader: boolean;
  cycles: number;
  published: number;
  republished: number;
  failedCycles: number;
  consecutiveFailures: number;
  lastError: string | null;
  retentionDeleted: number;
}

export class PollingRelay {
  private stopping = false;
  private leader = false;
  private lockClient?: PoolClient;
  private listenClient?: PoolClient;
  private wake?: () => void;
  /** A NOTIFY arrived while a cycle was running: the next sleep returns at once instead of losing it. */
  private notified = false;
  private loop?: Promise<void>;
  private lastRetention = 0;
  readonly stats: RelayStats;

  constructor(
    private readonly pool: Pool,
    private readonly producer: RelayProducer,
    readonly opts: RelayOptions,
    private readonly faults: RelayFaults,
    private readonly observer: RelayObserver = {},
    private readonly log: (line: string) => void = () => undefined,
  ) {
    this.stats = { mode: opts.mode, leader: false, cycles: 0, published: 0, republished: 0, failedCycles: 0, consecutiveFailures: 0, lastError: null, retentionDeleted: 0 };
  }

  get isLeader() {
    return this.opts.mode === 'multi' || this.leader;
  }

  start(): void {
    if (!this.loop) this.loop = this.run();
  }

  /** Finish the in-flight batch, release the lock and the clients. */
  async stop(): Promise<void> {
    this.stopping = true;
    this.wake?.();
    await this.loop;
    await this.releaseLock();
    if (this.listenClient) {
      this.listenClient.release();
      this.listenClient = undefined;
    }
  }

  /** Unpublished rows (of our topics) and the age of the oldest, for the gauges. */
  async backlog(): Promise<{ unpublished: number; oldestAgeS: number | null }> {
    const r = await this.pool.query(BACKLOG_SQL, [this.opts.topics]);
    return { unpublished: r.rows[0].n, oldestAgeS: r.rows[0].age === null ? null : Number(r.rows[0].age) };
  }

  private async run(): Promise<void> {
    if (this.opts.listen) await this.startListening();
    while (!this.stopping) {
      if (this.opts.mode === 'single' && !(await this.ensureLeader())) {
        await this.sleep(this.opts.standbyPollMs);
        continue;
      }
      let n = 0;
      try {
        n = await this.cycle();
        this.stats.consecutiveFailures = 0;
        await this.maybeRetention();
      } catch (e) {
        this.stats.failedCycles++;
        this.stats.consecutiveFailures++;
        this.stats.lastError = (e as Error).message;
        this.observer.cycleFailed?.(e, this.stats.consecutiveFailures);
        this.log(`[relay] cycle failed (${this.stats.consecutiveFailures} in a row): ${(e as Error).message}`);
        await this.sleep(backoffMs(this.stats.consecutiveFailures, this.opts.backoffInitialMs, this.opts.backoffMaxMs));
        continue;
      }
      if (n < this.opts.batchSize) await this.sleep(this.opts.pollMs); // a full batch: more is waiting, go on at once
    }
  }

  /** One claim + publish + mark. Returns the number of rows claimed. */
  async cycle(): Promise<number> {
    this.stats.cycles++;
    const claimed = (await this.pool.query(CLAIM_SQL, [this.opts.batchSize, this.opts.topics])).rows.map((r) => String(r.id));
    if (claimed.length === 0) return 0;
    this.faults.startBatch();
    const c = await this.pool.connect();
    let broken = false;
    try {
      await c.query('BEGIN');
      const rows: OutboxRow[] = (await c.query(LOCK_SQL, [claimed])).rows.map((r) => ({ ...r, id: String(r.id) }));
      if (rows.length === 0) {
        await c.query('COMMIT'); // another relay (multi mode) took them
        return claimed.length;
      }
      this.faults.hit('relay:before-publish', { rows: rows.length });
      this.faults.maybeFail('relay:before-publish');
      for (const [topic, messages] of toMessages(rows)) await this.producer.send(topic, messages);
      this.faults.hit('relay:after-publish-before-mark', { rows: rows.length });
      const mark = await c.query(MARK_SQL, [rows.map((r) => r.id)]);
      this.faults.hit('relay:after-mark-before-commit', { rows: rows.length });
      await c.query('COMMIT');
      const publishedAt: Date = mark.rows[0]?.published_at ?? new Date();
      this.stats.published += rows.length;
      this.stats.republished += rows.filter((r) => r.attempts > 1).length;
      this.observer.published?.(rows.map((r) => ({ id: r.id, topic: r.topic, attempts: r.attempts, createdAt: r.created_at })), publishedAt);
      return claimed.length;
    } catch (e) {
      try {
        await c.query('ROLLBACK');
      } catch {
        broken = true; // connection is gone: do not return it to the pool
      }
      throw e;
    } finally {
      c.release(broken);
    }
  }

  private async ensureLeader(): Promise<boolean> {
    if (this.leader && this.lockClient) {
      try {
        await this.lockClient.query('SELECT 1'); // the lock lives as long as this session: is it still there?
        return true;
      } catch {
        await this.lostLeadership('lock session failed');
        return false;
      }
    }
    try {
      const c = await this.pool.connect();
      const got = (await c.query('SELECT pg_try_advisory_lock($1) AS ok', [this.opts.lockKey])).rows[0].ok === true;
      if (!got) {
        c.release();
        return false;
      }
      c.on('error', () => void this.lostLeadership('lock session error'));
      this.lockClient = c;
      this.setLeader(true);
      this.log(`[relay] leader (advisory lock ${this.opts.lockKey})`);
      return true;
    } catch (e) {
      this.log(`[relay] cannot try the leader lock: ${(e as Error).message}`);
      return false;
    }
  }

  private setLeader(v: boolean) {
    if (this.leader === v) return;
    this.leader = v;
    this.stats.leader = v;
    this.observer.leadership?.(v);
  }

  private async lostLeadership(why: string) {
    if (!this.lockClient) return;
    this.log(`[relay] lost leadership: ${why}`);
    const c = this.lockClient;
    this.lockClient = undefined;
    this.setLeader(false);
    c.release(true);
  }

  private async releaseLock() {
    if (!this.lockClient) return;
    const c = this.lockClient;
    this.lockClient = undefined;
    try {
      await c.query('SELECT pg_advisory_unlock($1)', [this.opts.lockKey]);
      c.release();
    } catch {
      c.release(true);
    }
    this.setLeader(false);
  }

  private async startListening() {
    try {
      const c = await this.pool.connect();
      c.on('notification', () => {
        this.notified = true;
        this.wake?.();
      });
      c.on('error', () => {
        if (this.listenClient === c) this.listenClient = undefined; // polling still works; no LISTEN until restart
      });
      await c.query(`LISTEN ${OUTBOX_CHANNEL}`);
      this.listenClient = c;
    } catch (e) {
      this.log(`[relay] LISTEN ${OUTBOX_CHANNEL} failed, polling only: ${(e as Error).message}`);
    }
  }

  private async maybeRetention() {
    if (this.opts.retentionMs <= 0 || Date.now() - this.lastRetention < this.opts.retentionIntervalMs) return;
    this.lastRetention = Date.now();
    let total = 0;
    for (;;) {
      const r = await this.pool.query(RETENTION_SQL, [this.opts.retentionMs, this.opts.retentionBatch]);
      total += r.rowCount ?? 0;
      if ((r.rowCount ?? 0) < this.opts.retentionBatch || this.stopping) break;
    }
    if (total > 0) {
      this.stats.retentionDeleted += total;
      this.observer.retention?.(total);
    }
  }

  /** Waits ms, or less: stop() and a NOTIFY (also one that came in during the last cycle) end it early. */
  private sleep(ms: number): Promise<void> {
    if (this.stopping) return Promise.resolve();
    if (this.notified) {
      this.notified = false;
      return Promise.resolve();
    }
    return new Promise((res) => {
      const done = () => {
        clearTimeout(t);
        this.wake = undefined;
        this.notified = false;
        res();
      };
      const t = setTimeout(done, ms);
      this.wake = done;
    });
  }
}
