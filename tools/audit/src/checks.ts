// S2-F7 reconciliation rules (pure: unit-tested with fixtures, no I/O).
//
//   A  sent >= acked_202 (k6)                                          harness bug          CRITICAL (numbers invalid)
//   B  acked_202 == delta API acked counter (no API restart)           lost responses/bug   CRITICAL
//   C  delta Kafka end offsets >= acked_202                            acked but NOT in log CRITICAL (loss)
//   D  delta Kafka end offsets - acked_202 = written but not acked     reported (failed/timed-out batches, injected records)
//   E  distinct rows (run) == acked_202 - dlq_run                      below: acked but missing from Postgres CRITICAL
//                                                                      (only once drained; before that it is "not final")
//                                                                      above: 503 but stored (expected), reported
//   F  per consumer process: consumed - inserted - dlq == duplicates   metrics bug
//   G  duplicates (reprocessed messages absorbed by ON CONFLICT)       reported, never hidden
//   H  log start offset moved during the run (retention deleted data)  reported
//   I  group lag at the end == 0                                       otherwise NOT DRAINED (exit 2)
//
// Exit code: 1 if a critical check failed, else 2 if not drained, else 0.

import type { AuditResult, Check, ConsumerStats, K6Summary, RunFacts, Snapshot } from './types.ts';

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
    if (deltaApiAcked === acked) add({ id: 'B', title: '202s == API acked counter', status: 'ok', critical: true, detail: `${acked} == ${deltaApiAcked}` });
    else
      add({
        id: 'B',
        title: '202s == API acked counter',
        status: 'fail',
        critical: true,
        detail: `k6 saw ${acked} x 202 but the API counted ${deltaApiAcked} acknowledged (lost responses, other traffic during the run, or a counting bug)`,
      });
  }

  // C, D
  if (deltaRaw === null) {
    add({ id: 'C', title: 'Kafka records >= acknowledged', status: 'fail', critical: true, detail: `${start.topic} end offsets went backwards or partitions changed (topic recreated during the run?)` });
  } else if (acked === null) {
    add({ id: 'C', title: 'Kafka records >= acknowledged', status: 'skipped', critical: true, detail: 'no k6 summary' });
  } else if (deltaRaw >= acked) {
    add({ id: 'C', title: 'Kafka records >= acknowledged', status: 'ok', critical: true, detail: `${deltaRaw} >= ${acked}` });
    const extra = deltaRaw - acked;
    add({
      id: 'D',
      title: 'written but not acknowledged',
      status: extra > 0 ? 'finding' : 'ok',
      critical: false,
      detail: extra > 0 ? `${extra} record(s) in the log without a 202 (failed/timed-out batches, client retries, or records written by other producers such as injected poison)` : '0',
    });
  } else {
    add({ id: 'C', title: 'Kafka records >= acknowledged', status: 'fail', critical: true, detail: `ACKNOWLEDGED BUT NOT IN THE LOG: ${acked - deltaRaw} (202s ${acked}, new records ${deltaRaw})` });
  }

  // E
  if (acked === null) add({ id: 'E', title: 'rows == acknowledged - DLQ', status: 'skipped', critical: true, detail: 'no k6 summary' });
  else {
    const expected = acked - facts.dlq_run;
    const rows = facts.rows_distinct;
    if (rows === expected) add({ id: 'E', title: 'rows == acknowledged - DLQ', status: 'ok', critical: true, detail: `${rows} == ${acked} - ${facts.dlq_run}` });
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
  const broken: string[] = [];
  for (const c of perProcess) {
    deltaConsumed += c.consumed;
    deltaInserted += c.inserted;
    deltaDup += c.duplicates;
    deltaDlq += c.dlq;
    if (c.consumed - c.inserted - c.dlq !== c.duplicates) broken.push(`${c.url}: ${c.consumed} - ${c.inserted} - ${c.dlq} != ${c.duplicates}`);
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
  add({
    id: 'G',
    title: 'duplicates absorbed by ON CONFLICT',
    status: deltaDup > 0 ? 'finding' : 'ok',
    critical: false,
    detail: deltaDup > 0 ? `${deltaDup} reprocessed message(s) absorbed (consumer crash, rebalance, client retry)${notes.length ? '; best-effort: ' + notes.join('; ') : ''}` : `0${notes.length ? ' (best-effort: ' + notes.join('; ') + ')' : ''}`,
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
      status_503: k6?.by_status['503'] ?? null,
      status_4xx: k6?.by_status['4xx'] ?? null,
      status_other: k6 ? k6.by_status.other + k6.network_errors : null,
      api_acked_delta: deltaApiAcked,
      kafka_records_delta: deltaRaw,
      written_not_acked: deltaRaw !== null && acked !== null ? Math.max(0, deltaRaw - acked) : null,
      rows_distinct_run: facts.rows_distinct,
      dlq_run: facts.dlq_run,
      dlq_new_total: facts.dlq_new_total,
      consumed_delta: deltaConsumed,
      inserted_delta: deltaInserted,
      duplicates_delta: deltaDup,
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
      });
    }
  }
  for (const s of start) if (!end.find((e) => e.url === s.url)) notes.push(`${s.url} was gone at the end: its counters are lost`);
  return { perProcess, notes };
}

/** Lag summed over partitions: end offset - committed (never committed = everything in the log). */
export function groupLag(raw: Snapshot['raw'], committed: Snapshot['committed']): number {
  return raw.reduce((a, p) => {
    const c = committed.find((x) => x.partition === p.partition)?.offset ?? -1;
    return a + Math.max(0, p.high - (c >= 0 ? c : p.low));
  }, 0);
}
