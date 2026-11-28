// What happens to one selected dead letter, and the headers of the re-injected record (S3-F6). Pure.
//
// Loop guard: an event's replay count is how many times it has been re-injected from the DLQ so far:
//   (replay.count header on its dead letter: rounds before this dead letter existed)
// + (dlq_meta.replay_count of this row: times THIS dead letter was replayed already, > 0 with --include-replayed)
// The next replay gets count + 1. If that exceeds DLQ_MAX_REPLAYS the row is skipped: a record that keeps coming
// back is not re-injected forever.

import { DLQ_HEADERS, REPLAY_HEADERS, RETRY_HEADERS, headerValue } from '@eventpulse/retry';

export type Decision =
  | { kind: 'replay'; count: number; forced: boolean }
  | { kind: 'skip'; reason: 'loop_guard' | 'would_fail'; detail: string }
  | { kind: 'fail'; reason: 'missing_in_dlq'; detail: string };

export type CheckResult = { ok: true } | { ok: false; errorClass: string; code: string; message: string };

/** Times this event was re-injected before (see above). A broken header counts as 0. */
export function priorReplays(rowReplayCount: number, dlqHeaders: Record<string, unknown> | undefined): number {
  const h = Number(headerValue(dlqHeaders, REPLAY_HEADERS.count));
  return (Number.isInteger(h) && h > 0 ? h : 0) + Math.max(0, rowReplayCount);
}

export interface DecideInput {
  /** false = the record is not in the DLQ topic any more (retention) or never was. */
  found: boolean;
  rowReplayCount: number;
  dlqHeaders?: Record<string, unknown>;
  /** Pre-check result; undefined = --no-check. */
  check?: CheckResult;
  maxReplays: number;
  includeFailing: boolean;
}

export function decide(i: DecideInput): Decision {
  if (!i.found) return { kind: 'fail', reason: 'missing_in_dlq', detail: 'record not found at its DLQ offset (retention?)' };
  const next = priorReplays(i.rowReplayCount, i.dlqHeaders) + 1;
  if (next > i.maxReplays) return { kind: 'skip', reason: 'loop_guard', detail: `replay ${next} would exceed DLQ_MAX_REPLAYS=${i.maxReplays}` };
  if (i.check && !i.check.ok) {
    if (!i.includeFailing) return { kind: 'skip', reason: 'would_fail', detail: `${i.check.errorClass} ${i.check.code}: ${i.check.message}` };
    return { kind: 'replay', count: next, forced: true };
  }
  return { kind: 'replay', count: next, forced: false };
}

const OURS = [...Object.values(DLQ_HEADERS), ...Object.values(REPLAY_HEADERS), ...Object.values(RETRY_HEADERS)];
const OUR_HEADERS = new Set<string>(OURS);

export interface ReplayMark {
  replayId: string;
  count: number;
  dlqPartition: number;
  dlqOffset: string;
}

/**
 * The headers of the re-injected record: the ORIGINAL ones (what the API wrote: received_at, request id, content
 * type, ...) without our dlq.* / retry.* / old replay.* bookkeeping, plus the new replay.* set.
 */
export function replayHeaders(dlqHeaders: Record<string, unknown> | undefined, m: ReplayMark): Record<string, string | Buffer> {
  const out: Record<string, string | Buffer> = {};
  for (const [k, v] of Object.entries(dlqHeaders ?? {})) {
    if (OUR_HEADERS.has(k)) continue;
    const one = Array.isArray(v) ? v[0] : v;
    if (Buffer.isBuffer(one) || typeof one === 'string') out[k] = one;
    else if (one !== undefined && one !== null) out[k] = String(one);
  }
  out[REPLAY_HEADERS.id] = m.replayId;
  out[REPLAY_HEADERS.count] = String(m.count);
  out[REPLAY_HEADERS.dlqPartition] = String(m.dlqPartition);
  out[REPLAY_HEADERS.dlqOffset] = m.dlqOffset;
  return out;
}
