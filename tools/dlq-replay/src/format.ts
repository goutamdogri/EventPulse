// Human output of tools/dlq-replay (--json prints the raw objects instead).
import type { ReplayResult } from './replay.ts';
import type { MetaRow } from './store.ts';

export function table(head: string[], rows: (string | number | null | undefined)[][]): string {
  const cells = [head, ...rows.map((r) => r.map((c) => (c === null || c === undefined ? '' : String(c))))];
  const w = head.map((_, i) => Math.min(60, Math.max(...cells.map((r) => (r[i] ?? '').length))));
  const line = (r: string[]) => r.map((c, i) => (c.length > w[i] ? c.slice(0, w[i] - 1) + '…' : c.padEnd(w[i]))).join('  ').trimEnd();
  return [line(cells[0]), w.map((n) => '-'.repeat(n)).join('  '), ...cells.slice(1).map(line)].join('\n');
}

export function age(d: Date, now = Date.now()): string {
  const s = Math.max(0, Math.round((now - d.getTime()) / 1000));
  if (s < 120) return `${s}s`;
  if (s < 7200) return `${Math.round(s / 60)}m`;
  if (s < 172_800) return `${Math.round(s / 3600)}h`;
  return `${Math.round(s / 86_400)}d`;
}

export const listTable = (rows: MetaRow[]) =>
  table(
    ['id', 'tenant', 'event_id', 'class', 'code', 'message', 'attempts', 'age', 'replays'],
    rows.map((r) => [r.id, r.tenant_id ?? '(unparsed)', r.event_id, r.error_class, r.error_code, r.error_message, r.attempts, age(r.first_failed_at), r.replay_count]),
  );

const kv = (o: Record<string, number>) => Object.entries(o).map(([k, v]) => `${k}=${v}`).join(', ') || '-';

export function planText(r: ReplayResult, rate: number): string {
  const p = r.plan;
  const out = [
    `${r.dryRun ? 'DRY RUN (nothing produced, dlq_meta unchanged; add --execute to replay)' : `replay ${r.status}`}  replay_id ${r.replayId}`,
    `selected ${r.counts.selected}: ${p.replay} to replay${p.forced ? ` (${p.forced} forced by --include-failing)` : ''}, ${p.wouldFail} would fail again (pre-check), ${p.loopGuard} over the loop guard, ${p.missing} missing in the DLQ topic`,
    `by class:  ${kv(r.byClass)}`,
    `by tenant: ${kv(r.byTenant)}`,
    `target:    ${r.targets.join(', ') || '-'} at ${rate > 0 ? `${rate}/s` : 'unlimited rate'}`,
  ];
  if (r.samples.length) out.push('samples:', table(['id', 'tenant', 'event_id', 'class', 'code', 'decision'], r.samples.map((s) => [s.id, s.tenant ?? '(unparsed)', s.event_id, s.error_class, s.error_code, s.decision])));
  if (!r.dryRun) out.push(`replayed ${r.counts.replayed}, skipped ${r.counts.skipped}, failed ${r.counts.failed}${r.error ? `: ${r.error}` : ''}`);
  return out.join('\n');
}
