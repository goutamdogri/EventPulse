// tools/dlq-replay (S3-F6): DLQ triage and safe replay. See docs/runbooks/dlq-triage-and-replay.md.
//
//   pnpm dlq-replay stats   [filters]               counts by class, code, tenant, failed topic, age
//   pnpm dlq-replay list    [filters]               matching dlq_meta rows
//   pnpm dlq-replay show    <id>                    one dead letter: metadata, headers, decoded value
//   pnpm dlq-replay replay  [filters] [options]     plan (DRY RUN by default); --execute re-injects
//   pnpm dlq-replay reindex [--topic events.dlq]    rebuild missing dlq_meta rows from the DLQ topic
//
// Filters: --id --tenant --event-id --error-class --error-code --failed-topic --dlq-topic --source-partition
//          --source-offset-from --source-offset-to --since --until --limit --include-replayed  (src/filters.ts)
// Replay:  --execute  --yes (no prompt)  --no-check (skip the pre-check)  --include-failing (replay records the
//          pre-check says will fail)  --rate N (records/s, default DLQ_REPLAY_RATE or 100, 0 = unlimited)
// Any:     --json (machine-readable output)
//
// Exit codes: 0 ok, 1 something failed (replay error, record missing), 2 usage error or aborted.

import { createInterface } from 'node:readline/promises';
import { parseArgs } from 'node:util';
import { Pool } from 'pg';
import { FaultInjector, loadFaultConfig } from '@eventpulse/fault-injector';
import { createKafka, readRecordsAt, scanTopic } from '@eventpulse/kafka-config';
import { loadReplayConfig } from './config.ts';
import { UsageError, parseFilters } from './filters.ts';
import { listTable, planText, table } from './format.ts';
import { PreCheck } from './precheck.ts';
import { metaFromRecord } from './reindex.ts';
import { runReplay } from './replay.ts';
import { Store } from './store.ts';
import { writeTextfile } from './textfile.ts';

const FILTER_OPTS = ['id', 'tenant', 'event-id', 'error-class', 'error-code', 'failed-topic', 'dlq-topic', 'source-partition', 'source-offset-from', 'source-offset-to', 'since', 'until', 'limit'] as const;

const USAGE = `usage: dlq-replay <stats|list|show|replay|reindex> [options]
  filters  --id N[,N] --tenant T --event-id UUID --error-class C --error-code X --failed-topic T --dlq-topic T
           --source-partition N --source-offset-from N --source-offset-to N --since T --until T --limit N --include-replayed
  replay   --execute --yes --no-check --include-failing --rate N
  reindex  --topic T (default DLQ_TOPIC or events.dlq)
  any      --json`;

const out = (s: string) => process.stdout.write(s + '\n');
const err = (s: string) => process.stderr.write(s + '\n');

async function main(argv: string[]): Promise<number> {
  const { values, positionals } = parseArgs({
    args: argv,
    allowPositionals: true,
    allowNegative: true,
    options: {
      ...Object.fromEntries(FILTER_OPTS.map((k) => [k, { type: 'string' as const }])),
      'include-replayed': { type: 'boolean' },
      execute: { type: 'boolean' },
      yes: { type: 'boolean', short: 'y' },
      check: { type: 'boolean', default: true },
      'include-failing': { type: 'boolean' },
      rate: { type: 'string' },
      topic: { type: 'string' },
      json: { type: 'boolean' },
      help: { type: 'boolean', short: 'h' },
    },
  });
  const [cmd, ...rest] = positionals;
  if (values.help || !cmd) {
    out(USAGE);
    return cmd || values.help ? 0 : 2;
  }
  const json = values.json === true;
  const cfg = loadReplayConfig();
  const pool = new Pool({ connectionString: cfg.databaseUrl, max: 2 });
  pool.on('error', () => undefined);
  const store = new Store(pool);
  try {
    switch (cmd) {
      case 'stats': {
        const s = await store.stats(parseFilters(values));
        if (json) out(JSON.stringify(s, null, 2));
        else {
          out(`${s.total} dead letter(s)${values['include-replayed'] ? '' : ' not replayed'}, oldest failed ${s.oldest ?? '-'}`);
          for (const [title, rows] of [['error class', s.byClass], ['class + code', s.byCode], ['tenant', s.byTenant], ['failed topic', s.byFailedTopic], ['age', s.byAge]] as const) {
            out(`\n${table([title, 'count'], rows.map((r) => [r.k, r.n]))}`);
          }
        }
        return 0;
      }
      case 'list': {
        const rows = await store.select(parseFilters(values));
        out(json ? JSON.stringify(rows, null, 2) : rows.length ? listTable(rows) : 'no matching dead letters');
        return 0;
      }
      case 'show': {
        if (!rest[0] || !/^\d+$/.test(rest[0])) throw new UsageError('usage: dlq-replay show <id>');
        const row = await store.get(Number(rest[0]));
        if (!row) {
          err(`no dlq_meta row ${rest[0]}`);
          return 1;
        }
        const rec = (await readRecordsAt(cfg.kafka, row.dlq_topic, [{ partition: row.dlq_partition, offset: Number(row.dlq_offset) }])).get(`${row.dlq_partition}:${row.dlq_offset}`);
        const pc = new PreCheck(cfg.precheck);
        let decoded: unknown = null;
        if (rec) {
          await pc.prepare([rec.value]).catch((e) => err(`note: schema registry: ${(e as Error).message}`));
          decoded = pc.decode(rec.value);
        }
        const shown = {
          meta: row,
          record: rec
            ? {
                key: rec.key?.toString('utf8') ?? null,
                headers: Object.fromEntries(Object.entries(rec.headers).map(([k, v]) => [k, v.toString('utf8')])),
                value: decoded ?? { format: 'hex', value: rec.value?.toString('hex') ?? null },
                precheck: pc.check({ partition: row.source_partition, offset: row.source_offset, key: rec.key, value: rec.value, headers: rec.headers }),
              }
            : null,
        };
        out(JSON.stringify(shown, null, 2));
        if (!rec) err(`record not found in ${row.dlq_topic} at ${row.dlq_partition}:${row.dlq_offset} (retention?)`);
        return rec ? 0 : 1;
      }
      case 'replay': {
        const filters = parseFilters(values);
        const rate = values.rate === undefined ? cfg.rate : Number(values.rate);
        if (!Number.isFinite(rate) || rate < 0) throw new UsageError('--rate must be a number >= 0');
        const execute = values.execute === true;
        const faults = new FaultInjector(loadFaultConfig(process.env));
        const producer = execute
          ? createKafka(cfg.kafka).producer({ kafkaJS: { acks: -1, idempotent: true, allowAutoTopicCreation: false }, 'linger.ms': 0, 'message.timeout.ms': 15_000 } as any)
          : null;
        if (producer) await producer.connect();
        try {
          const r = await runReplay(
            {
              store,
              read: (topic, at) => readRecordsAt(cfg.kafka, topic, at),
              produce: async (topic, m) => {
                await producer!.send({ topic, messages: [m] as any });
              },
              check: values.check === false ? undefined : new PreCheck(cfg.precheck),
              confirm: async (q) => {
                if (!process.stdin.isTTY) {
                  err('not a terminal: pass --yes to confirm');
                  return false;
                }
                const rl = createInterface({ input: process.stdin, output: process.stderr });
                try {
                  return /^y(es)?$/i.test((await rl.question(`${q} [y/N] `)).trim());
                } finally {
                  rl.close();
                }
              },
              hit: (p) => faults.hit(p),
              log: err,
              sleep: (ms) => new Promise((res) => setTimeout(res, ms)),
              now: Date.now,
            },
            filters,
            { execute, yes: values.yes === true, includeFailing: values['include-failing'] === true, rate, maxReplays: cfg.maxReplays, operator: cfg.operator },
          );
          if (execute && r.status !== 'aborted' && cfg.textfile) writeTextfile(cfg.textfile, r);
          out(json ? JSON.stringify(r, null, 2) : planText(r, rate));
          if (r.status === 'aborted') return 2;
          return r.status === 'failed' || r.counts.failed > 0 ? 1 : 0;
        } finally {
          if (producer) await producer.disconnect().catch(() => undefined);
        }
      }
      case 'reindex': {
        const topic = values.topic ?? cfg.dlqTopic;
        const pc = new PreCheck(cfg.precheck);
        const recs: Parameters<typeof metaFromRecord>[0][] = [];
        await scanTopic(cfg.kafka, topic, 'all', (r) => void recs.push(r), { timeoutMs: 120_000 });
        await pc.prepare(recs.map((r) => r.value)).catch((e) => err(`note: schema registry unreachable, Avro values get no tenant/event id: ${(e as Error).message}`));
        let inserted = 0;
        let existing = 0;
        let notDeadLetters = 0;
        for (const r of recs) {
          let parsed: { tenantId: string; eventId: string } | null = null;
          try {
            const p = pc.parse({ partition: r.partition, offset: r.offset, key: r.key, value: r.value, headers: r.headers });
            if (p.ok) parsed = { tenantId: p.row.tenant_id, eventId: p.row.event_id };
          } catch {
            parsed = null; // schema not prefetched (registry down): metadata without tenant/event id
          }
          const m = metaFromRecord(r, parsed);
          if (!m) {
            notDeadLetters++;
            continue;
          }
          if (await store.insertMeta(m)) inserted++;
          else existing++;
        }
        const res = { topic, scanned: recs.length, inserted, existing, not_dead_letters: notDeadLetters };
        out(json ? JSON.stringify(res, null, 2) : `reindex ${topic}: scanned ${res.scanned}, inserted ${inserted} missing row(s), ${existing} already indexed, ${notDeadLetters} without dlq.* headers`);
        return 0;
      }
      default:
        throw new UsageError(`unknown command "${cmd}"\n${USAGE}`);
    }
  } finally {
    await pool.end().catch(() => undefined);
  }
}

main(process.argv.slice(2)).then(
  (code) => process.exit(code),
  (e) => {
    err(e instanceof UsageError || (e as { code?: string })?.code?.startsWith?.('ERR_PARSE_ARGS') ? (e as Error).message : `dlq-replay: ${(e as Error).stack ?? e}`);
    process.exit(e instanceof UsageError || (e as { code?: string })?.code?.startsWith?.('ERR_PARSE_ARGS') ? 2 : 1);
  },
);
