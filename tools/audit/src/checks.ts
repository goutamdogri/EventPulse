// Reconciliation rules (S2-F7, extended for Stage 3 S3-F3). Pure: unit-tested with fixtures, no I/O.
//
// Stage 3 vocabulary: "accepted" = DISTINCT events that got a 202 (k6 unique_202; a resend with the same key that
// gets the replayed 202 is not a second event). Falls back to the 202 count for a Stage 2 k6 summary.
//
//   A  sent >= 202s (k6)                                               harness bug          CRITICAL (numbers invalid)
//   B  202s == delta API acked counter + delta replayed 202s           lost responses/bug   CRITICAL
//   C  delta Kafka end offsets >= accepted                             acked but NOT in log CRITICAL (loss)
//   D  delta Kafka end offsets - accepted = extra copies in the log    reported (resends past API dedup, failed batches,
//                                                                      injected records)
//   E  distinct rows (run) == accepted - dlq_run                       below: acked but missing from Postgres CRITICAL
//                                                                      (only once drained; before that it is "not final")
//                                                                      above: 503 but stored (expected), reported
//   F  per consumer process: consumed - inserted - dlq == duplicates   metrics bug
//      and reprocessed + redelivered + unknown == duplicates
//   G  duplicates absorbed by the sink, by cause                       reported, never hidden
//   H  log start offset moved during the run (retention deleted data)  reported
//   I  group lag at the end == 0                                       otherwise NOT DRAINED (exit 2)
//   J  every record the group committed is accounted for:              unexplained != 0 with complete counters, drained:
//      committed - new rows - new DLQ records - redelivered - unknown  CRITICAL (a record vanished or a counter lies)
//      == 0 ("unexplained difference", must be zero)                   counters incomplete (restart): reported
//   K  API idempotency (replayed, in progress, key reused, fail open)  reported; fail_open / fail_closed are findings
//
// Exit code: 1 if a critical check failed, else 2 if not drained, else 0.

import type { AuditResult, Check, ConsumerStats, IdemCounts, K6Summary, RunFacts, Snapshot } from './types.ts';

const sumHigh = (s: Snapshot['raw']) => s.reduce((a, p) => a + p.high, 0);

export function evaluate(start: Snapshot, end: Snapshot, k6: K6Summary | null, facts: RunFacts): AuditResult {
  const checks: Check[] = [];
  const findings: string[] = [];
  const add = (c: Check) => {
    checks.push(c);
    if (c.status === 'finding' || c.status === 'fail' || c.status === 'not_drained') findings.push(`${c.id}: ${c.detail}`);
  };
  const elapsedS = (Date.parse(end.taken_at) - Date.parse(start.taken_at)) / 1000;

  // --- Kafka deltas ----------------------------------------------------------------------------------------------
  const startHigh = sumHigh(start.raw);
  const endHigh = sumHigh(end.raw);
  const topicReset = endHigh < startHigh || end.raw.length !== start.raw.length;
  const deltaRaw = topicReset ? null : endHigh - startHigh;
  const acked = k6?.by_status['202'] ?? null;
  const accepted = k6 ? (k6.unique_202 ?? k6.by_status['202']) : null;
  const idem = idemDelta(start, end);

  // A
  if (!k6) add({ id: 'A', title: 'sent >= acknowledged (k6)', status: 'skipped', critical: true, detail: 'no k6 summary for this run' });
  else if (k6.sent >= k6.by_status['202'])
    add({ id: 'A', title: 'sent >= acknowledged (k6)', status: 'ok', critical: true, detail: `${k6.sent} >= ${k6.by_status['202']}` });
  else add({ id: 'A', title: 'sent >= acknowledged (k6)', status: 'fail', critical: true, detail: `harness bug: sent ${k6.sent} < 202s ${k6.by_status['202']}` });

  // B
  let deltaApiAcked: number | null = null;
  if (acked === null) add({ id: 'B', title: '202s == API acked counter', status: 'skipped', critical: true, detail: 'no k6 summary' });
  else if (!start.api || !end.api) add({ id: 'B', title: '202s == API acked counter', status: 'skipped', critical: true, detail: 'API /debug/stats not reachable at start or end' });
  else if (end.api.uptime_s + 2 < elapsedS)
    add({ id: 'B', title: '202s == API acked counter', status: 'skipped', critical: true, detail: `API restarted during the run (uptime ${end.api.uptime_s}s < ${elapsedS.toFixed(0)}s): its counters reset` });
  else {
    deltaApiAcked = end.api.acked - start.api.acked;
    // A replayed 202 (same key, already done) produces nothing, so it is not in the acked counter.
    const replayed = idem?.replayed ?? 0;
    const rhs = replayed ? `${deltaApiAcked} acked + ${replayed} replayed` : `${deltaApiAcked}`;
    if (deltaApiAcked + replayed === acked) add({ id: 'B', title: '202s == API acked counter (+ replayed)', status: 'ok', critical: true, detail: `${acked} == ${rhs}` });
    else
      add({
        id: 'B',
        title: '202s == API acked counter (+ replayed)',
        status: 'fail',
        critical: true,
        detail: `k6 saw ${acked} x 202 but the API counted ${rhs} (lost responses, other traffic during the run, or a counting bug)`,
      });
  }

  // C, D
  if (deltaRaw === null) {
    add({ id: 'C', title: 'Kafka records >= acknowledged', status: 'fail', critical: true, detail: `${start.topic} end offsets went backwards or partitions changed (topic recreated during the run?)` });
  } else if (accepted === null) {
    add({ id: 'C', title: 'Kafka records >= accepted events', status: 'skipped', critical: true, detail: 'no k6 summary' });
  } else if (deltaRaw >= accepted) {
    add({ id: 'C', title: 'Kafka records >= accepted events', status: 'ok', critical: true, detail: `${deltaRaw} >= ${accepted}` });
    const extra = deltaRaw - accepted;
    add({
      id: 'D',
      title: 'extra copies in the log',
      status: extra > 0 ? 'finding' : 'ok',
      critical: false,
      detail:
        extra > 0
          ? `${extra} record(s) beyond one per accepted event (resends that passed API dedup, failed/timed-out batches, or records written by other producers such as injected poison); the sink absorbs them (G, J)`
          : '0',
    });
  } else {
    add({ id: 'C', title: 'Kafka records >= accepted events', status: 'fail', critical: true, detail: `ACKNOWLEDGED BUT NOT IN THE LOG: ${accepted - deltaRaw} (accepted ${accepted}, new records ${deltaRaw})` });
  }

  // E
  if (accepted === null) add({ id: 'E', title: 'rows == acknowledged - DLQ', status: 'skipped', critical: true, detail: 'no k6 summary' });
  else {
    const expected = accepted - facts.dlq_run;
    const rows = facts.rows_distinct;
    if (rows === expected) add({ id: 'E', title: 'rows == acknowledged - DLQ', status: 'ok', critical: true, detail: `${rows} == ${accepted} - ${facts.dlq_run}` });
    else if (rows < expected && !facts.drained)
      // Not drained: the missing rows may still be on their way. Not a false "loss": I reports NOT DRAINED (exit 2).
      add({ id: 'E', title: 'rows == acknowledged - DLQ', status: 'finding', critical: false, detail: `not final: ${expected - rows} acknowledged event(s) not stored YET (consumer not drained, lag ${facts.lag_end}); rerun "audit end" later` });
    else if (rows < expected)
      add({ id: 'E', title: 'rows == acknowledged - DLQ', status: 'fail', critical: true, detail: `ACKNOWLEDGED BUT MISSING FROM POSTGRES: ${expected - rows} (expected ${expected}, rows ${rows})` });
    else
      add({ id: 'E', title: 'rows == acknowledged - DLQ', status: 'finding', critical: false, detail: `${rows - expected} extra row(s): events answered 503 that were written anyway (expected, a client retry with the same event_id stays one row)` });
  }

  // F, G
  const { perProcess, notes } = consumerDeltas(start.consumers, end.consumers, elapsedS);
  let deltaConsumed = 0;
  let deltaInserted = 0;
  let deltaDup = 0;
  let deltaDlq = 0;
  const causes = { reprocessed: 0, redelivered: 0, unknown: 0, conflicts: 0 };
  let causesComplete = perProcess.length > 0;
  const broken: string[] = [];
  for (const c of perProcess) {
    deltaConsumed += c.consumed;
    deltaInserted += c.inserted;
    deltaDup += c.duplicates;
    deltaDlq += c.dlq;
    if (c.consumed - c.inserted - c.dlq !== c.duplicates) broken.push(`${c.url}: ${c.consumed} - ${c.inserted} - ${c.dlq} != ${c.duplicates}`);
    if (c.dup_reprocessed === undefined) {
      causesComplete = false; // a Stage 2 sink: no split
      continue;
    }
    causes.reprocessed += c.dup_reprocessed;
    causes.redelivered += c.dup_redelivered ?? 0;
    causes.unknown += c.dup_unknown ?? 0;
    causes.conflicts += c.conflicts ?? 0;
    const sum = c.dup_reprocessed + (c.dup_redelivered ?? 0) + (c.dup_unknown ?? 0);
    if (sum !== c.duplicates) broken.push(`${c.url}: reprocessed + redelivered + unknown ${sum} != duplicates ${c.duplicates}`);
  }
  if (perProcess.length === 0) add({ id: 'F', title: 'consumed - inserted - dlq == duplicates', status: 'skipped', critical: false, detail: 'no consumer /debug/stats reachable at the end' });
  else if (broken.length) add({ id: 'F', title: 'consumed - inserted - dlq == duplicates', status: 'fail', critical: false, detail: `metrics bug: ${broken.join('; ')}` });
  else
    add({
      id: 'F',
      title: 'consumed - inserted - dlq == duplicates',
      status: 'ok',
      critical: false,
      detail: `${deltaConsumed} - ${deltaInserted} - ${deltaDlq} == ${deltaDup}${notes.length ? ` (${notes.join('; ')})` : ''}`,
    });
  const split = causesComplete
    ? ` (reprocessed ${causes.reprocessed}: crash/rebalance/offset reset; redelivered ${causes.redelivered}: new offset, key already stored; unknown ${causes.unknown}; conflicting content ${causes.conflicts})`
    : '';
  add({
    id: 'G',
    title: 'duplicates absorbed by the sink',
    status: deltaDup > 0 || causes.conflicts > 0 ? 'finding' : 'ok',
    critical: false,
    detail: `${deltaDup}${split}${notes.length ? '; best-effort: ' + notes.join('; ') : ''}`,
  });

  // H
  const moved = start.raw
    .map((p) => ({ p: p.partition, d: (end.raw.find((e) => e.partition === p.partition)?.low ?? p.low) - p.low }))
    .filter((x) => x.d > 0);
  add({
    id: 'H',
    title: 'log start offset unchanged (no retention deletes)',
    status: moved.length ? 'finding' : 'ok',
    critical: false,
    detail: moved.length ? `retention deleted ${moved.reduce((a, x) => a + x.d, 0)} record(s) on partition(s) ${moved.map((x) => x.p).join(', ')} during the run` : 'unchanged',
  });

  // I
  add({
    id: 'I',
    title: 'group lag at the end == 0',
    status: facts.drained ? 'ok' : 'not_drained',
    critical: false,
    detail: facts.drained ? `0 (waited ${facts.drain_waited_s}s)` : `lag ${facts.lag_end} after waiting ${facts.drain_waited_s}s: rows may still arrive, E is not final`,
  });

  // J: every record the group committed during the run is accounted for.
  const committedDelta = committedSum(end) - committedSum(start);
  const rowsNew = end.db.rows_total - start.db.rows_total;
  let unexplained: number | null = null;
  if (!causesComplete) {
    add({ id: 'J', title: 'every committed record accounted for', status: 'skipped', critical: true, detail: 'no duplicate split from the consumers (Stage 2 sink, or no /debug/stats)' });
  } else {
    unexplained = committedDelta - rowsNew - facts.dlq_new_total - causes.redelivered - causes.unknown;
    const formula = `committed ${committedDelta} - new rows ${rowsNew} - new DLQ ${facts.dlq_new_total} - redelivered ${causes.redelivered} - unknown ${causes.unknown} = ${unexplained}`;
    if (unexplained === 0) add({ id: 'J', title: 'every committed record accounted for', status: 'ok', critical: true, detail: formula });
    else if (!facts.drained || notes.length > 0)
      add({ id: 'J', title: 'every committed record accounted for', status: 'finding', critical: false, detail: `not final / best-effort (${!facts.drained ? 'not drained' : notes.join('; ')}): ${formula}` });
    else
      add({ id: 'J', title: 'every committed record accounted for', status: 'fail', critical: true, detail: `UNEXPLAINED DIFFERENCE: ${formula} (a record committed but neither stored, dead-lettered nor counted as a duplicate)` });
  }

  // K: API-side idempotency (layer 1). Its duplicates never reach Kafka, so they are only visible here.
  if (!idem) add({ id: 'K', title: 'API idempotency', status: 'skipped', critical: false, detail: 'no idempotency counters from the API (Stage 2 API or restarted)' });
  else {
    const degraded = idem.fail_open + idem.fail_closed;
    add({
      id: 'K',
      title: 'API idempotency',
      status: degraded > 0 || idem.payload_mismatch > 0 ? 'finding' : 'ok',
      critical: false,
      detail:
        `new ${idem.new}, replayed ${idem.replayed} (duplicates stopped at the API), in progress ${idem.in_progress} (409), key reused ${idem.payload_mismatch} (422)` +
        (degraded > 0 ? `; REDIS DEGRADED: fail_open ${idem.fail_open} (accepted without dedup), fail_closed ${idem.fail_closed} (503)` : '') +
        (idem.disabled > 0 ? `; disabled ${idem.disabled}` : ''),
    });
  }

  if (facts.dlq_new_total > facts.dlq_run) findings.push(`DLQ: ${facts.dlq_new_total - facts.dlq_run} new record(s) not from this run (e.g. injected poison)`);

  const criticalFail = checks.some((c) => c.critical && c.status === 'fail');
  const exit_code: 0 | 1 | 2 = criticalFail ? 1 : facts.drained ? 0 : 2;
  return {
    run_id: facts.run_id,
    result: criticalFail ? 'NOT RECONCILED' : facts.drained ? 'RECONCILED' : 'NOT DRAINED',
    exit_code,
    numbers: {
      elapsed_s: Math.round(elapsedS),
      sent: k6?.sent ?? null,
      acked_202: acked,
      accepted_unique: accepted,
      replayed_202_k6: k6?.replayed_202 ?? null,
      resends_sent: k6?.resends_sent ?? null,
      status_503: k6?.by_status['503'] ?? null,
      status_4xx: k6?.by_status['4xx'] ?? null,
      status_other: k6 ? k6.by_status.other + k6.network_errors : null,
      api_acked_delta: deltaApiAcked,
      kafka_records_delta: deltaRaw,
      written_not_acked: deltaRaw !== null && accepted !== null ? Math.max(0, deltaRaw - accepted) : null,
      api_new_delta: idem?.new ?? null,
      api_replayed_delta: idem?.replayed ?? null,
      api_in_progress_delta: idem?.in_progress ?? null,
      api_key_reused_delta: idem?.payload_mismatch ?? null,
      api_fail_open_delta: idem?.fail_open ?? null,
      api_fail_closed_delta: idem?.fail_closed ?? null,
      rows_distinct_run: facts.rows_distinct,
      dlq_run: facts.dlq_run,
      dlq_new_total: facts.dlq_new_total,
      consumed_delta: deltaConsumed,
      inserted_delta: deltaInserted,
      duplicates_delta: deltaDup,
      dup_reprocessed: causesComplete ? causes.reprocessed : null,
      dup_redelivered: causesComplete ? causes.redelivered : null,
      dup_unknown: causesComplete ? causes.unknown : null,
      conflicts_delta: causesComplete ? causes.conflicts : null,
      committed_delta: committedDelta,
      rows_new_total: rowsNew,
      unexplained,
      consumer_dlq_delta: deltaDlq,
      lag_end: facts.lag_end,
      freshness_p50_s: facts.freshness_p50_s,
      freshness_p99_s: facts.freshness_p99_s,
    },
    checks,
    findings,
  };
}

/**
 * Per consumer process, the counters for this run. A process present at both ends with enough uptime gives a
 * delta. A process that (re)started during the run gives its counters since it started: complete for itself (its
 * invariant still holds), but whatever the process before it counted is lost, so sums are best-effort.
 */
export function consumerDeltas(start: ConsumerStats[], end: ConsumerStats[], elapsedS: number) {
  const perProcess: ConsumerStats[] = [];
  const notes: string[] = [];
  for (const e of end) {
    const s = start.find((x) => x.url === e.url);
    const restarted = !s || e.uptime_s + 2 < elapsedS;
    if (restarted) {
      notes.push(`${e.url} ${s ? 'restarted' : 'started'} during the run: its counters cover only its own lifetime`);
      perProcess.push(e);
    } else {
      perProcess.push({
        url: e.url,
        uptime_s: e.uptime_s,
        consumed: e.consumed - s.consumed,
        attempted: e.attempted - s.attempted,
        inserted: e.inserted - s.inserted,
        duplicates: e.duplicates - s.duplicates,
        dlq: e.dlq - s.dlq,
        ...(e.dup_reprocessed !== undefined
          ? {
              dup_reprocessed: e.dup_reprocessed - (s.dup_reprocessed ?? 0),
              dup_redelivered: (e.dup_redelivered ?? 0) - (s.dup_redelivered ?? 0),
              dup_unknown: (e.dup_unknown ?? 0) - (s.dup_unknown ?? 0),
              conflicts: (e.conflicts ?? 0) - (s.conflicts ?? 0),
            }
          : {}),
      });
    }
  }
  for (const s of start) if (!end.find((e) => e.url === s.url)) notes.push(`${s.url} was gone at the end: its counters are lost`);
  return { perProcess, notes };
}

/** API idempotency counters for the run; null without them or when the API restarted (counters reset). */
export function idemDelta(start: Snapshot, end: Snapshot): IdemCounts | null {
  const s = start.api?.idem;
  const e = end.api?.idem;
  if (!s || !e || !start.api || !end.api) return null;
  const elapsedS = (Date.parse(end.taken_at) - Date.parse(start.taken_at)) / 1000;
  if (end.api.uptime_s + 2 < elapsedS) return null;
  const out = {} as IdemCounts;
  for (const k of Object.keys(e) as (keyof IdemCounts)[]) out[k] = e[k] - (s[k] ?? 0);
  return out;
}

/** Records the group has processed: committed offset per partition (never committed = the log start). */
export function committedSum(s: Snapshot): number {
  return s.raw.reduce((a, p) => {
    const c = s.committed.find((x) => x.partition === p.partition)?.offset ?? -1;
    return a + (c >= 0 ? c : p.low);
  }, 0);
}

/** Lag summed over partitions: end offset - committed (never committed = everything in the log). */
export function groupLag(raw: Snapshot['raw'], committed: Snapshot['committed']): number {
  return raw.reduce((a, p) => {
    const c = committed.find((x) => x.partition === p.partition)?.offset ?? -1;
    return a + Math.max(0, p.high - (c >= 0 ? c : p.low));
  }, 0);
}
