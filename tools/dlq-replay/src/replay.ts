// `replay` (S3-F6). Plan first, act only with --execute.
//
//   1. select dlq_meta rows with the filters (unreplayed unless --include-replayed)
//   2. read each one's exact record from the DLQ topic at (dlq_partition, dlq_offset)
//   3. decide per row: replay | skip (loop guard, or pre-check says it would fail again) | fail (record missing)
//   4. dry run (default): print the plan, write a dlq_replays row (dry_run=true), stop. Nothing produced, dlq_meta
//      unchanged.
//   5. --execute: confirm (--yes skips), then for each "replay" row: produce to source_topic with the original key,
//      value and headers + replay.*, wait for the ack (acks=all, idempotent), THEN mark the dlq_meta row. Paced at
//      --rate records/s.
//
// Why this is safe: the sink dedups on (tenant_id, event_id), so replaying a record that was in fact stored, or
// replaying twice, changes nothing in storage (it is a counted sink duplicate). A crash between produce and mark
// leaves that row unmarked: re-running the same command replays it again, as a duplicate. Nothing is lost because
// a row is only marked after the ack.

import { randomUUID } from 'node:crypto';
import type { CrashPointName } from '@eventpulse/fault-injector';
import { decide, replayHeaders } from './decide.ts';
import type { CheckResult, Decision } from './decide.ts';
import { filtersJson } from './filters.ts';
import type { Filters } from './filters.ts';
import type { MetaRow, ReplayCounts, ReplayStatus } from './store.ts';

export interface DlqRecord {
  key: Buffer | null;
  value: Buffer | null;
  headers: Record<string, Buffer>;
}

export interface ReplayDeps {
  store: {
    select(f: Filters): Promise<MetaRow[]>;
    markReplayed(id: string, replayId: string): Promise<void>;
    startReplay(replayId: string, operator: string, filters: Record<string, unknown>, dryRun: boolean, status: ReplayStatus, c: ReplayCounts): Promise<void>;
    finishReplay(replayId: string, status: ReplayStatus, c: ReplayCounts): Promise<void>;
  };
  /** Records keyed "partition:offset", per DLQ topic. */
  read(topic: string, at: { partition: number; offset: number }[]): Promise<Map<string, DlqRecord>>;
  /** Resolves when Kafka acknowledged the record. */
  produce(topic: string, m: { key: Buffer | null; value: Buffer | null; headers: Record<string, string | Buffer> }): Promise<void>;
  /** Pre-check; undefined = --no-check. prepare() may throw (registry down): then the run stops before acting. */
  check?: { prepare(values: (Buffer | null)[]): Promise<void>; check(m: { partition: number; offset: string; key: Buffer | null; value: Buffer | null; headers: Record<string, unknown> }): CheckResult };
  confirm(question: string): Promise<boolean>;
  hit(point: CrashPointName): void;
  log(line: string): void;
  sleep(ms: number): Promise<void>;
  now(): number;
}

export interface ReplayOptions {
  execute: boolean;
  yes: boolean;
  includeFailing: boolean;
  rate: number;
  maxReplays: number;
  operator: string;
  replayId?: string;
}

export interface PlannedRow {
  row: MetaRow;
  decision: Decision;
}

export interface ReplayResult {
  replayId: string;
  dryRun: boolean;
  status: ReplayStatus;
  counts: ReplayCounts;
  /** Dry run: how the selection would go. */
  plan: { replay: number; forced: number; wouldFail: number; loopGuard: number; missing: number };
  byClass: Record<string, number>;
  byTenant: Record<string, number>;
  targets: string[];
  samples: { id: string; tenant: string | null; event_id: string | null; error_class: string; error_code: string | null; decision: string }[];
  rows: { id: string; decision: string; detail?: string; count?: number }[];
  error?: string;
}

const tally = (xs: string[]) => xs.reduce<Record<string, number>>((m, k) => ((m[k] = (m[k] ?? 0) + 1), m), {});
const label = (d: Decision) => (d.kind === 'replay' ? (d.forced ? 'replay (forced)' : 'replay') : `${d.kind}: ${d.reason}`);

export async function runReplay(deps: ReplayDeps, filters: Filters, o: ReplayOptions): Promise<ReplayResult> {
  const replayId = o.replayId ?? randomUUID();
  const rows = await deps.store.select(filters);

  // Read every selected dead letter (needed for the pre-check and the loop guard, in a dry run too).
  const records = new Map<string, DlqRecord>();
  const byTopic = new Map<string, MetaRow[]>();
  for (const r of rows) (byTopic.get(r.dlq_topic) ?? byTopic.set(r.dlq_topic, []).get(r.dlq_topic)!).push(r);
  for (const [topic, rs] of byTopic) {
    const got = await deps.read(topic, rs.map((r) => ({ partition: r.dlq_partition, offset: Number(r.dlq_offset) })));
    for (const r of rs) {
      const rec = got.get(`${r.dlq_partition}:${r.dlq_offset}`);
      if (rec) records.set(r.id, rec);
    }
  }
  if (deps.check) await deps.check.prepare([...records.values()].map((r) => r.value));

  const planned: PlannedRow[] = rows.map((row) => {
    const rec = records.get(row.id);
    const check = rec && deps.check ? deps.check.check({ partition: row.source_partition, offset: row.source_offset, key: rec.key, value: rec.value, headers: rec.headers }) : undefined;
    return { row, decision: decide({ found: !!rec, rowReplayCount: row.replay_count, dlqHeaders: rec?.headers, check, maxReplays: o.maxReplays, includeFailing: o.includeFailing }) };
  });

  const n = (pred: (d: Decision) => boolean) => planned.filter((p) => pred(p.decision)).length;
  const plan = {
    replay: n((d) => d.kind === 'replay'),
    forced: n((d) => d.kind === 'replay' && d.forced),
    wouldFail: n((d) => (d.kind === 'skip' && d.reason === 'would_fail') || (d.kind === 'replay' && d.forced)),
    loopGuard: n((d) => d.kind === 'skip' && d.reason === 'loop_guard'),
    missing: n((d) => d.kind === 'fail'),
  };
  const skipped = n((d) => d.kind === 'skip');
  const result: ReplayResult = {
    replayId,
    dryRun: !o.execute,
    status: o.execute ? 'running' : 'dry_run',
    counts: { selected: rows.length, replayed: 0, skipped, failed: plan.missing },
    plan,
    byClass: tally(rows.map((r) => r.error_class)),
    byTenant: tally(rows.map((r) => r.tenant_id ?? '(unparsed)')),
    targets: [...new Set(rows.map((r) => r.source_topic))],
    samples: planned.slice(0, 5).map((p) => ({ id: p.row.id, tenant: p.row.tenant_id, event_id: p.row.event_id, error_class: p.row.error_class, error_code: p.row.error_code, decision: label(p.decision) })),
    rows: planned.map((p) => ({ id: p.row.id, decision: label(p.decision), ...(p.decision.kind === 'replay' ? { count: p.decision.count } : { detail: p.decision.detail }) })),
  };
  const fj = filtersJson(filters);

  if (!o.execute) {
    await deps.store.startReplay(replayId, o.operator, fj, true, 'dry_run', result.counts);
    return result;
  }
  if (plan.replay === 0) {
    await deps.store.startReplay(replayId, o.operator, fj, false, 'done', result.counts);
    return { ...result, status: 'done' };
  }
  if (!o.yes && !(await deps.confirm(`Re-inject ${plan.replay} record(s) into ${result.targets.join(', ')}?`))) {
    await deps.store.startReplay(replayId, o.operator, fj, false, 'aborted', result.counts);
    return { ...result, status: 'aborted' };
  }

  await deps.store.startReplay(replayId, o.operator, fj, false, 'running', result.counts);
  const interval = o.rate > 0 ? 1000 / o.rate : 0;
  let next = deps.now();
  try {
    for (const p of planned) {
      if (p.decision.kind !== 'replay') continue;
      if (interval > 0) {
        const wait = next - deps.now();
        if (wait > 0) await deps.sleep(wait);
        next = Math.max(next, deps.now()) + interval;
      }
      const rec = records.get(p.row.id)!;
      await deps.produce(p.row.source_topic, {
        key: rec.key,
        value: rec.value,
        headers: replayHeaders(rec.headers, { replayId, count: p.decision.count, dlqPartition: p.row.dlq_partition, dlqOffset: p.row.dlq_offset }),
      });
      deps.hit('replay:after-produce-before-mark');
      await deps.store.markReplayed(p.row.id, replayId);
      result.counts.replayed++;
    }
  } catch (e) {
    // Kafka or Postgres trouble mid-run: stop (do not hammer a dead dependency). Re-running the same command is
    // safe: marked rows are not selected again, a produced-but-unmarked row becomes a sink duplicate.
    result.counts.failed += plan.replay - result.counts.replayed;
    result.status = 'failed';
    result.error = (e as Error).message;
    await deps.store.finishReplay(replayId, 'failed', result.counts).catch(() => undefined);
    return result;
  }
  result.status = 'done';
  await deps.store.finishReplay(replayId, 'done', result.counts);
  return result;
}
