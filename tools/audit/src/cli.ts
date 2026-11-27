// S2-F7 one-command audit.
//
//   audit start [RUN_ID]          snapshot before a run  -> loadtests/results/<run>/audit-start.json
//   audit end   <RUN_ID>          wait for the drain, snapshot, reconcile -> audit-result.json, exit 0/1/2
//   audit run   [-- <command>]    start, run the command (default: the Stage 2 k6 script), end   <- THE one command
//
// `run` exports RUN_ID, RESULT_DIR, SUMMARY_PATH and API_KEY to the command, so k6 writes its summary where `end`
// reads it. Env for the default k6 command: RATE (200), DURATION (30s), BASE_URL, PAYLOAD_BYTES, RETRY_503_FRACTION.
// Exit code: 0 reconciled, 1 a critical check failed (possible loss), 2 not drained.

import { spawnSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { evaluate } from './checks.ts';
import { formatReport } from './format.ts';
import { REPO, defaultApiKey, loadAuditConfig, runFacts, takeSnapshot, waitDrained } from './snapshot.ts';
import type { K6Summary, Snapshot } from './types.ts';

const log = (s: string) => process.stderr.write(s + '\n');
const cfg = loadAuditConfig();
const dirOf = (runId: string) => resolve(cfg.resultsDir, runId);
const newRunId = () => randomBytes(4).toString('hex');

async function start(runId = newRunId()): Promise<string> {
  if (!/^[0-9a-f]{8}$/.test(runId)) throw new Error('RUN_ID must be 8 hex characters (it becomes the event_id prefix)');
  const dir = dirOf(runId);
  mkdirSync(dir, { recursive: true });
  const s = await takeSnapshot(cfg);
  writeFileSync(resolve(dir, 'audit-start.json'), JSON.stringify(s, null, 2));
  log(`audit start: run ${runId}, snapshot in ${dir}/audit-start.json`);
  for (const n of s.notes) log(`  note: ${n}`);
  return runId;
}

async function end(runId: string, header = ''): Promise<number> {
  const dir = dirOf(runId);
  const startPath = resolve(dir, 'audit-start.json');
  if (!existsSync(startPath)) throw new Error(`no ${startPath}: run "audit start ${runId}" first`);
  const startSnap = JSON.parse(readFileSync(startPath, 'utf8')) as Snapshot;
  log(`audit end: run ${runId}, waiting up to ${cfg.drainTimeoutS}s for the consumer group to drain`);
  const drain = await waitDrained(cfg, log);
  const endSnap = await takeSnapshot(cfg);
  writeFileSync(resolve(dir, 'audit-end.json'), JSON.stringify(endSnap, null, 2));
  const k6Path = resolve(dir, 'k6-summary.json');
  const k6 = existsSync(k6Path) ? (JSON.parse(readFileSync(k6Path, 'utf8')) as K6Summary) : null;
  if (!k6) log(`  note: no ${k6Path}: the k6-based checks are skipped`);
  const facts = await runFacts(cfg, startSnap, endSnap, runId, drain);
  const result = evaluate(startSnap, endSnap, k6, facts);
  writeFileSync(resolve(dir, 'audit-result.json'), JSON.stringify({ ...result, facts, notes: [...startSnap.notes, ...endSnap.notes] }, null, 2));
  const k = k6 as (K6Summary & { duration?: string; rate?: number }) | null;
  const head = header || (k ? `(rate ${k.rate ?? '?'}/s, duration ${k.duration ?? '?'})` : '');
  const report = formatReport(result, head);
  writeFileSync(resolve(dir, 'audit-report.txt'), report + '\n');
  console.log(report);
  log(`files: ${dir}/{audit-start,audit-end,audit-result}.json, k6-summary.json, audit-report.txt`);
  return result.exit_code;
}

async function run(cmd: string[]): Promise<number> {
  const runId = await start(process.env.RUN_ID || newRunId());
  const dir = dirOf(runId);
  const apiKey = defaultApiKey();
  const env: NodeJS.ProcessEnv = { ...process.env, RUN_ID: runId, RESULT_DIR: dir, SUMMARY_PATH: resolve(dir, 'k6-summary.json'), ...(apiKey ? { API_KEY: apiKey } : {}) };
  const command = cmd.length ? cmd : ['k6', 'run', '--quiet', resolve(REPO, 'loadtests/k6/stage-2/ingest-kafka.js')];
  log(`audit run: ${command.join(' ')}  (RUN_ID=${runId}, RATE=${env.RATE ?? 200}, DURATION=${env.DURATION ?? '30s'})`);
  const r = spawnSync(command[0], command.slice(1), { stdio: 'inherit', env });
  if (r.status !== 0) log(`  note: the command exited with ${r.status ?? r.signal}; auditing anyway`);
  return end(runId, `(command: ${command[0]}, rate ${env.RATE ?? 200}/s, duration ${env.DURATION ?? '30s'})`);
}

async function main() {
  const [command, ...rest] = process.argv.slice(2);
  if (command === 'start') {
    console.log(await start(rest[0]));
    return 0;
  }
  if (command === 'end') {
    if (!rest[0]) throw new Error('usage: audit end <RUN_ID>');
    return end(rest[0]);
  }
  if (command === 'run') {
    const i = rest.indexOf('--');
    return run(i >= 0 ? rest.slice(i + 1) : []);
  }
  console.error('usage: audit start [RUN_ID] | audit end <RUN_ID> | audit run [-- <command...>]');
  return 64;
}

main().then(
  (code) => process.exit(code),
  (e: unknown) => {
    console.error(`audit error: ${(e as Error)?.message ?? e}`);
    process.exit(70);
  },
);
