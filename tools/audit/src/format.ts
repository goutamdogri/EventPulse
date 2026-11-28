// Human-readable audit report (the table printed at the end of a run).

import type { AuditResult, Check } from './types.ts';

const mark: Record<Check['status'], string> = { ok: 'OK', fail: 'FAIL', finding: 'NOTE', skipped: 'SKIP', not_drained: 'NOT DRAINED' };
const v = (x: number | null | undefined, unit = '') => (x === null || x === undefined ? 'n/a' : `${x}${unit}`);

export function formatReport(r: AuditResult, header: string): string {
  const n = r.numbers;
  const tag = (id: string) => {
    const c = r.checks.find((x) => x.id === id);
    return c ? `[${id} ${mark[c.status]}]` : '';
  };
  const lines = [
    `RUN ${r.run_id}  ${header}`,
    `sent                       ${v(n.sent)}   ${tag('A')}`,
    `acknowledged (202)         ${v(n.acked_202)}   503: ${v(n.status_503)}   4xx: ${v(n.status_4xx)}   other/network: ${v(n.status_other)}`,
    `accepted (distinct events) ${v(n.accepted_unique)}   resends: ${v(n.resends_sent)}   replayed 202 (k6): ${v(n.replayed_202_k6)}`,
    `API acked counter (delta)  ${v(n.api_acked_delta)}   ${tag('B')}`,
    `API idempotency (delta)    new ${v(n.api_new_delta)}  replayed ${v(n.api_replayed_delta)}  409 ${v(n.api_in_progress_delta)}  422 ${v(n.api_key_reused_delta)}  fail_open ${v(n.api_fail_open_delta)}  fail_closed ${v(n.api_fail_closed_delta)}   ${tag('K')}`,
    `kafka records (delta)      ${v(n.kafka_records_delta)}   ${tag('C')}  written-not-acked: ${v(n.written_not_acked)}`,
    `rows (distinct, this run)  ${v(n.rows_distinct_run)}   ${tag('E')}  dlq (this run): ${v(n.dlq_run)}   dlq new (any): ${v(n.dlq_new_total)}`,
    `consumed / inserted        ${v(n.consumed_delta)} / ${v(n.inserted_delta)}   duplicates: ${v(n.duplicates_delta)}   dlq: ${v(n.consumer_dlq_delta)}   ${tag('F')} ${tag('G')}`,
    `duplicates by cause        reprocessed ${v(n.dup_reprocessed)}  redelivered ${v(n.dup_redelivered)}  unknown ${v(n.dup_unknown)}  conflicting content ${v(n.conflicts_delta)}`,
    `retry tiers / dead letters routed to tiers ${v(n.retried_delta)}  tier lag at end ${v(n.retry_lag_end)}  dead letters (dlq_meta) ${v(n.dead_letters_new)}`,
    `unexplained difference     ${v(n.unexplained)}   (committed ${v(n.committed_delta)} - new rows ${v(n.rows_new_total)} - dead letters ${v(n.dead_letters_new)} - redelivered - unknown)   ${tag('J')}`,
    `retention                  ${tag('H')}`,
    `lag at end                 ${v(n.lag_end)}   ${tag('I')}`,
    `freshness p50 / p99        ${v(n.freshness_p50_s, 's')} / ${v(n.freshness_p99_s, 's')}`,
    '',
    ...r.checks.map((c) => `  ${c.id} ${mark[c.status].padEnd(11)} ${c.title}: ${c.detail}`),
    '',
    `RESULT: ${r.result}   (exit ${r.exit_code})`,
    ...(r.findings.length ? ['findings (copy into the notebook / failure ledger):', ...r.findings.map((f) => `  - ${f}`)] : ['findings: none']),
  ];
  return lines.join('\n');
}
