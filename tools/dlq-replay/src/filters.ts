// Filters over dlq_meta (S3-F6) and the SQL they turn into. Pure: values are ALWAYS bound parameters ($1, $2, ...),
// never concatenated into the SQL text, so a tenant id like "x' or 1=1 --" is just a tenant id.
//
//   --id 12[,13,...]           dlq_meta.id
//   --tenant T                 tenant_id
//   --event-id UUID            event_id
//   --error-class CLASS        error_class (one of packages/retry ERROR_CLASSES)
//   --error-code CODE          error_code (SQLSTATE, parse code, ...)
//   --failed-topic TOPIC       failed_topic (events.raw or events.retry.N)
//   --dlq-topic TOPIC          dlq_topic (default: any; tests use it to stay on their throw-away topics)
//   --source-partition N       source_partition
//   --source-offset-from N     source_offset >= N
//   --source-offset-to N       source_offset <= N
//   --since T / --until T      first_failed_at >= T / < T. T = ISO 8601, or an age like 30m, 2h, 7d (= now - age)
//   --limit N                  at most N rows (oldest first)
//   --include-replayed         also rows already replayed (default: only replayed_at IS NULL)

import { ERROR_CLASSES } from '@eventpulse/retry';
import type { ErrorClass } from '@eventpulse/retry';

export interface Filters {
  ids?: number[];
  tenant?: string;
  eventId?: string;
  errorClass?: ErrorClass;
  errorCode?: string;
  failedTopic?: string;
  dlqTopic?: string;
  sourcePartition?: number;
  sourceOffsetFrom?: number;
  sourceOffsetTo?: number;
  since?: Date;
  until?: Date;
  limit?: number;
  includeReplayed?: boolean;
}

export class UsageError extends Error {
  readonly name = 'UsageError';
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const AGE_RE = /^(\d+)(s|m|h|d)$/;
const UNIT_MS = { s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000 } as const;

function int(name: string, v: string, min = 0): number {
  if (!/^-?\d+$/.test(v) || Number(v) < min || !Number.isSafeInteger(Number(v))) throw new UsageError(`--${name} must be an integer >= ${min} (got "${v}")`);
  return Number(v);
}

/** ISO 8601 time, or an age ("2h" = two hours ago). */
export function parseTime(name: string, v: string, now: Date = new Date()): Date {
  const age = v.match(AGE_RE);
  if (age) return new Date(now.getTime() - Number(age[1]) * UNIT_MS[age[2] as keyof typeof UNIT_MS]);
  if (!/^\d{4}-\d{2}-\d{2}/.test(v) || Number.isNaN(Date.parse(v))) throw new UsageError(`--${name} must be an ISO 8601 time or an age like 30m, 2h, 7d (got "${v}")`);
  return new Date(v);
}

/** Raw option values (strings from the command line) -> validated Filters. */
export function parseFilters(o: Record<string, string | boolean | undefined>, now: Date = new Date()): Filters {
  const f: Filters = {};
  const s = (k: string) => (typeof o[k] === 'string' ? (o[k] as string) : undefined);
  if (s('id') !== undefined) f.ids = s('id')!.split(',').map((x) => int('id', x.trim(), 1));
  if (s('tenant') !== undefined) f.tenant = s('tenant');
  if (s('event-id') !== undefined) {
    if (!UUID_RE.test(s('event-id')!)) throw new UsageError('--event-id must be a UUID');
    f.eventId = s('event-id');
  }
  if (s('error-class') !== undefined) {
    const c = s('error-class')!.toUpperCase();
    if (!(ERROR_CLASSES as readonly string[]).includes(c)) throw new UsageError(`--error-class must be one of ${ERROR_CLASSES.join(', ')}`);
    f.errorClass = c as ErrorClass;
  }
  if (s('error-code') !== undefined) f.errorCode = s('error-code');
  if (s('failed-topic') !== undefined) f.failedTopic = s('failed-topic');
  if (s('dlq-topic') !== undefined) f.dlqTopic = s('dlq-topic');
  if (s('source-partition') !== undefined) f.sourcePartition = int('source-partition', s('source-partition')!);
  if (s('source-offset-from') !== undefined) f.sourceOffsetFrom = int('source-offset-from', s('source-offset-from')!);
  if (s('source-offset-to') !== undefined) f.sourceOffsetTo = int('source-offset-to', s('source-offset-to')!);
  if (f.sourceOffsetFrom !== undefined && f.sourceOffsetTo !== undefined && f.sourceOffsetFrom > f.sourceOffsetTo) {
    throw new UsageError('--source-offset-from must be <= --source-offset-to');
  }
  if (s('since') !== undefined) f.since = parseTime('since', s('since')!, now);
  if (s('until') !== undefined) f.until = parseTime('until', s('until')!, now);
  if (f.since && f.until && f.since >= f.until) throw new UsageError('--since must be before --until');
  if (s('limit') !== undefined) f.limit = int('limit', s('limit')!, 1);
  if (o['include-replayed'] === true) f.includeReplayed = true;
  return f;
}

export interface Sql {
  text: string;
  values: unknown[];
}

/** WHERE clause (always present) + its parameters, numbered from `first`. */
export function whereClause(f: Filters, first = 1): Sql {
  const conds: string[] = [];
  const values: unknown[] = [];
  const add = (cond: (p: string) => string, v: unknown) => {
    values.push(v);
    conds.push(cond(`$${first + values.length - 1}`));
  };
  if (f.ids) add((p) => `id = ANY(${p}::bigint[])`, f.ids);
  if (f.tenant !== undefined) add((p) => `tenant_id = ${p}`, f.tenant);
  if (f.eventId !== undefined) add((p) => `event_id = ${p}::uuid`, f.eventId);
  if (f.errorClass !== undefined) add((p) => `error_class = ${p}`, f.errorClass);
  if (f.errorCode !== undefined) add((p) => `error_code = ${p}`, f.errorCode);
  if (f.failedTopic !== undefined) add((p) => `failed_topic = ${p}`, f.failedTopic);
  if (f.dlqTopic !== undefined) add((p) => `dlq_topic = ${p}`, f.dlqTopic);
  if (f.sourcePartition !== undefined) add((p) => `source_partition = ${p}`, f.sourcePartition);
  if (f.sourceOffsetFrom !== undefined) add((p) => `source_offset >= ${p}`, f.sourceOffsetFrom);
  if (f.sourceOffsetTo !== undefined) add((p) => `source_offset <= ${p}`, f.sourceOffsetTo);
  if (f.since !== undefined) add((p) => `first_failed_at >= ${p}`, f.since.toISOString());
  if (f.until !== undefined) add((p) => `first_failed_at < ${p}`, f.until.toISOString());
  if (!f.includeReplayed) conds.push('replayed_at IS NULL');
  return { text: conds.length ? `WHERE ${conds.join(' AND ')}` : '', values };
}

export const META_COLUMNS =
  'id, source_topic, source_partition, source_offset, failed_topic, dlq_topic, dlq_partition, dlq_offset, tenant_id, event_id, ' +
  'error_class, error_code, error_message, attempts, first_failed_at, last_failed_at, replayed_at, replay_count, last_replay_id';

/** The rows a command works on, oldest first. */
export function selectSql(f: Filters): Sql {
  const w = whereClause(f);
  const values = [...w.values];
  let text = `SELECT ${META_COLUMNS} FROM dlq_meta ${w.text} ORDER BY first_failed_at, id`.replace(/\s+/g, ' ').trim();
  if (f.limit !== undefined) {
    values.push(f.limit);
    text += ` LIMIT $${values.length}`;
  }
  return { text, values };
}

/** JSON-able form of the filters, stored in dlq_replays.filters. */
export function filtersJson(f: Filters): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(f)) if (v !== undefined) out[k] = v instanceof Date ? v.toISOString() : v;
  return out;
}
