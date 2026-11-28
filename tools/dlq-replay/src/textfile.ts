// Prometheus textfile of the last EXECUTED replay (node-exporter's textfile collector reads
// infra/compose/textfile/*.prom), so "replay success rate" is on the Stage 3 dashboard. Written atomically
// (temp file + rename) so the collector never reads half a file. Dry runs do not write it: they change nothing.
import { mkdirSync, renameSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import type { ReplayResult } from './replay.ts';

export function renderTextfile(r: ReplayResult, nowS: number = Date.now() / 1000): string {
  const g = (name: string, help: string, v: number) => `# HELP ${name} ${help}\n# TYPE ${name} gauge\n${name} ${v}\n`;
  return (
    g('ep_dlq_replay_selected', 'Dead letters selected by the last executed dlq-replay run.', r.counts.selected) +
    g('ep_dlq_replay_replayed', 'Dead letters re-injected (acked) by the last executed dlq-replay run.', r.counts.replayed) +
    g('ep_dlq_replay_skipped', 'Dead letters skipped (pre-check or loop guard) by the last executed dlq-replay run.', r.counts.skipped) +
    g('ep_dlq_replay_failed', 'Dead letters that could not be replayed (missing record, produce error) in the last executed run.', r.counts.failed) +
    g('ep_dlq_replay_last_run_timestamp_seconds', 'When the last executed dlq-replay run finished.', Math.round(nowS)) +
    g('ep_dlq_replay_last_run_success', '1 if the last executed dlq-replay run finished with status done.', r.status === 'done' ? 1 : 0)
  );
}

export function writeTextfile(path: string, r: ReplayResult): void {
  mkdirSync(dirname(path), { recursive: true });
  const tmp = `${path}.${process.pid}.tmp`;
  writeFileSync(tmp, renderTextfile(r));
  renameSync(tmp, path);
}
