// Pre-check (S3-F6): run the SAME parse/deserialize step the sink runs (services/consumer-sink parseMessage, Avro via
// packages/schema with the sink's reader schema) on a dead letter's bytes. A record that would fail permanently
// again is skipped by default, so a replay does not just refill the DLQ.
//
// It cannot predict PERMANENT_DB failures (those need the INSERT); those come back to the DLQ with replay.count.

import { classifyParseCode } from '@eventpulse/retry';
import { EventSerde, RegistryClient, latestSchema, looksLikeWire } from '@eventpulse/schema';
import { parseMessage } from '../../../services/consumer-sink/src/sink/parse.ts';
import type { EventRow, RawMessage } from '../../../services/consumer-sink/src/sink/parse.ts';
import type { CheckResult } from './decide.ts';

export interface PreCheckConfig {
  registryUrl: string;
  registryTimeoutMs: number;
  subject: string;
  acceptJson: boolean;
}

export class PreCheck {
  private readonly serde: EventSerde;
  private readonly cfg: PreCheckConfig;

  constructor(cfg: PreCheckConfig) {
    this.cfg = cfg;
    this.serde = new EventSerde(new RegistryClient({ url: cfg.registryUrl, timeoutMs: cfg.registryTimeoutMs }), {
      subject: cfg.subject,
      schema: latestSchema(cfg.subject).parsed,
    });
  }

  /** Fetch the writer schemas of these values. Throws RegistryUnavailableError: then nothing can be checked. */
  prepare(values: (Buffer | null)[]): Promise<void> {
    return this.serde.prefetch(values);
  }

  parse(m: RawMessage): { ok: true; row: EventRow } | { ok: false; errorClass: string; code: string; message: string } {
    const r = parseMessage(m, { serde: this.serde, acceptJson: this.cfg.acceptJson });
    if (r.ok) return r;
    return { ok: false, errorClass: classifyParseCode(r.error.code), code: r.error.code, message: r.error.message };
  }

  check(m: RawMessage): CheckResult {
    const r = this.parse(m);
    return r.ok ? { ok: true } : r;
  }

  /** For `show`: the value as an object (Avro or JSON), or null when it cannot be decoded. */
  decode(value: Buffer | null): { format: 'avro' | 'json'; value: unknown } | null {
    if (!value || value.length === 0) return null;
    try {
      if (looksLikeWire(value)) return { format: 'avro', value: this.serde.decode(value) };
      return { format: 'json', value: JSON.parse(value.toString('utf8')) };
    } catch {
      return null;
    }
  }
}
