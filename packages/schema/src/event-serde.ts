// Avro serde for events.raw (S3-F7).
//
// Producer: init() looks up the id of its compiled-in writer schema (schemas/events/vN.avsc) in the registry.
//           It NEVER registers: schemas are registered by the deploy step (`make schemas`, tools/schema-check).
//           encode() is then synchronous and needs no registry: a registry outage after startup changes nothing.
// Consumer: decode() reads the writer schema id from the record, fetches that schema once (prefetch(), cached
//           per id), and resolves it against the consumer's own reader schema, so a v1 record is read by a v2
//           consumer (defaults filled in) and a v2 record by a v1 consumer (new fields skipped).
//
// Field mapping (Avro <-> envelope): occurred_at / received_at are epoch milliseconds in Avro and ISO strings in
// the envelope (sub-millisecond digits are not kept: Stage 3 step 8's precision rule); payload is canonical JSON
// text (payload_json, plain JSON.stringify: the tenant's strings travel EXACTLY as sent; canonical form, which also
// NFC-normalizes strings, is only for hashing and is computed by the sink); event_id is lower case.

import avsc from 'avsc';
import type { Type } from 'avsc';
import { AvroDecodeError, AvroEncodeError, IncompatibleWriterError, SerdeNotReadyError, UnknownSchemaIdError } from './errors.js';
import type { RegistryClient } from './registry.js';
import { decodeWire, encodeWire, schemaIdOf } from './wire.js';

export const CONTENT_TYPE_AVRO = 'application/vnd.eventpulse.event+avro';
export const CONTENT_TYPE_JSON = 'application/vnd.eventpulse.event+json';
export const HEADER_CONTENT_TYPE = 'content-type';

export interface EventInput {
  event_id: string;
  tenant_id: string;
  type: string;
  occurred_at: string;
  received_at: string | Date;
  schema_version: number;
  payload: Record<string, unknown>;
  source?: string | null;
}

export interface DecodedEvent {
  event_id: string;
  tenant_id: string;
  type: string;
  occurred_at: string;
  received_at: string;
  schema_version: number;
  payload: Record<string, unknown>;
  source: string | null;
  /** Registry id of the writer schema. */
  schema_id: number;
}

export const parseSchema = (schema: string | object): Type => avsc.Type.forSchema(typeof schema === 'string' ? JSON.parse(schema) : schema);

function toMillis(v: string | Date, field: string): number {
  const t = v instanceof Date ? v.getTime() : Date.parse(v);
  if (Number.isNaN(t)) throw new AvroEncodeError(`${field} is not a date-time`);
  return t;
}

/** Envelope -> Avro record (pure). */
export function toRecord(e: EventInput): Record<string, unknown> {
  return {
    event_id: e.event_id.toLowerCase(),
    tenant_id: e.tenant_id,
    type: e.type,
    occurred_at: toMillis(e.occurred_at, 'occurred_at'),
    received_at: toMillis(e.received_at, 'received_at'),
    schema_version: e.schema_version,
    payload_json: JSON.stringify(e.payload),
    source: e.source ?? null,
  };
}

/** Avro record -> envelope (pure). Throws AvroDecodeError on values the envelope cannot hold. */
export function fromRecord(r: any, schemaId: number): DecodedEvent {
  let payload: unknown;
  try {
    payload = JSON.parse(r.payload_json);
  } catch (e) {
    throw new AvroDecodeError(`payload_json is not JSON: ${(e as Error).message}`);
  }
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) throw new AvroDecodeError('payload_json is not a JSON object');
  const iso = (ms: unknown, field: string) => {
    const d = new Date(Number(ms));
    if (!Number.isFinite(d.getTime())) throw new AvroDecodeError(`${field} is out of range`);
    return d.toISOString();
  };
  return {
    event_id: r.event_id,
    tenant_id: r.tenant_id,
    type: r.type,
    occurred_at: iso(r.occurred_at, 'occurred_at'),
    received_at: iso(r.received_at, 'received_at'),
    schema_version: r.schema_version,
    payload: payload as Record<string, unknown>,
    source: r.source ?? null,
    schema_id: schemaId,
  };
}

type Loaded = { kind: 'ok'; resolver: any } | { kind: 'unknown' } | { kind: 'incompatible'; message: string };

export interface EventSerdeOptions {
  subject: string;
  /** The schema this process writes with (producer) / reads into (consumer). */
  schema: string | object;
  /** How long a "this id does not exist" answer is remembered (default 60 s). */
  negativeCacheMs?: number;
  now?: () => number;
}

export class EventSerde {
  readonly type: Type;
  readonly schemaText: string;
  private writerSchemaId?: number;
  private readonly loaded = new Map<number, Loaded & { at: number }>();
  private readonly now: () => number;

  constructor(private readonly registry: RegistryClient | undefined, private readonly opts: EventSerdeOptions) {
    const parsed = typeof opts.schema === 'string' ? JSON.parse(opts.schema) : opts.schema;
    this.schemaText = JSON.stringify(parsed);
    this.type = avsc.Type.forSchema(parsed); // already parsed: a primitive schema like "string" must not be parsed twice
    this.now = opts.now ?? Date.now;
  }

  /** Producer: find our writer schema's id (lookup only, never register). Throws Registry* / SchemaNotRegistered. */
  async init(): Promise<number> {
    if (!this.registry) throw new SerdeNotReadyError('no registry configured');
    const { id } = await this.registry.lookup(this.opts.subject, this.schemaText);
    this.writerSchemaId = id;
    this.loaded.set(id, { kind: 'ok', resolver: this.type.createResolver(this.type), at: this.now() });
    return id;
  }

  /** Test / tooling seam: use a known id without a registry round trip. */
  useWriterId(id: number) {
    this.writerSchemaId = id;
    this.loaded.set(id, { kind: 'ok', resolver: this.type.createResolver(this.type), at: this.now() });
  }

  get writerId(): number | undefined {
    return this.writerSchemaId;
  }

  get ready(): boolean {
    return this.writerSchemaId !== undefined;
  }

  /** Synchronous: no registry call. Throws SerdeNotReadyError before init(), AvroEncodeError on a bad value. */
  encode(e: EventInput): Buffer {
    if (this.writerSchemaId === undefined) throw new SerdeNotReadyError('schema id not known yet (registry unreachable at startup?)');
    const rec = toRecord(e);
    const problems: string[] = [];
    if (!this.type.isValid(rec, { errorHook: (path, v) => problems.push(`${path.join('.') || '(root)'}=${JSON.stringify(v)?.slice(0, 60)}`) })) {
      throw new AvroEncodeError(`event does not fit the writer schema: ${problems.join(', ')}`);
    }
    return encodeWire(this.writerSchemaId, this.type.toBuffer(rec));
  }

  /**
   * Make sure the writer schemas of these records are known. Fetches each unknown id once. Throws
   * RegistryUnavailableError when the registry cannot be asked (the caller must retry later, NEVER dead-letter).
   * Ids the registry does not know are remembered (negative cache) and fail at decode() as UnknownSchemaIdError.
   */
  async prefetch(buffers: (Buffer | null | undefined)[]): Promise<void> {
    const ids = new Set<number>();
    for (const b of buffers) {
      const id = schemaIdOf(b);
      if (id !== undefined && !this.isFresh(id)) ids.add(id);
    }
    for (const id of ids) {
      if (!this.registry) {
        this.loaded.set(id, { kind: 'unknown', at: this.now() });
        continue;
      }
      try {
        const writer = parseSchema(await this.registry.getSchemaById(id));
        try {
          this.loaded.set(id, { kind: 'ok', resolver: this.type.createResolver(writer), at: this.now() });
        } catch (e) {
          this.loaded.set(id, { kind: 'incompatible', message: (e as Error).message, at: this.now() });
        }
      } catch (e) {
        if (e instanceof UnknownSchemaIdError) this.loaded.set(id, { kind: 'unknown', at: this.now() });
        else throw e; // RegistryUnavailableError and friends: let the caller pause and retry
      }
    }
  }

  private isFresh(id: number): boolean {
    const l = this.loaded.get(id);
    if (!l) return false;
    return l.kind !== 'unknown' || this.now() - l.at < (this.opts.negativeCacheMs ?? 60_000);
  }

  /** Synchronous after prefetch(). Throws NotAvroError | UnknownSchemaIdError | IncompatibleWriterError | AvroDecodeError. */
  decode(b: Buffer): DecodedEvent {
    const { schemaId, body } = decodeWire(b);
    const l = this.loaded.get(schemaId);
    if (!l) throw new SerdeNotReadyError(`writer schema ${schemaId} not prefetched`);
    if (l.kind === 'unknown') throw new UnknownSchemaIdError(schemaId);
    if (l.kind === 'incompatible') throw new IncompatibleWriterError(schemaId, l.message);
    let rec: unknown;
    try {
      rec = this.type.fromBuffer(body, l.resolver);
    } catch (e) {
      throw new AvroDecodeError(`record does not decode with writer schema ${schemaId}: ${(e as Error).message}`);
    }
    return fromRecord(rec, schemaId);
  }

  /** Writer schema ids currently known (for /debug/stats). */
  knownIds(): number[] {
    return [...this.loaded.entries()].filter(([, l]) => l.kind === 'ok').map(([id]) => id).sort((a, b) => a - b);
  }
}
