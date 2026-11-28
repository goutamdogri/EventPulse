// Avro serde for any registered subject whose values are plain records (S3-F4: config.changed-value).
// Same rules as EventSerde (events.raw): the producer only LOOKS UP its writer schema id (never registers), encode()
// is synchronous afterwards; the consumer fetches each writer schema id once and resolves it against its own
// reader schema. Values go in and out as plain Avro records (no field mapping): the caller owns that.

import avsc from 'avsc';
import type { Type } from 'avsc';
import { AvroDecodeError, AvroEncodeError, IncompatibleWriterError, SerdeNotReadyError, UnknownSchemaIdError } from './errors.js';
import type { RegistryClient } from './registry.js';
import { decodeWire, encodeWire, schemaIdOf } from './wire.js';

type Resolver = ReturnType<Type['createResolver']>;
type Loaded = { kind: 'ok'; resolver: Resolver } | { kind: 'unknown' } | { kind: 'incompatible'; message: string };

export interface RecordSerdeOptions {
  subject: string;
  /** The schema this process writes with (producer) / reads into (consumer). */
  schema: string | object;
}

export class RecordSerde<T = Record<string, unknown>> {
  readonly type: Type;
  readonly schemaText: string;
  private writerSchemaId?: number;
  private readonly loaded = new Map<number, Loaded>();

  constructor(private readonly registry: RegistryClient | undefined, readonly opts: RecordSerdeOptions) {
    const parsed = typeof opts.schema === 'string' ? JSON.parse(opts.schema) : opts.schema;
    this.schemaText = JSON.stringify(parsed);
    this.type = avsc.Type.forSchema(parsed);
  }

  /** Producer: the id of our writer schema under the subject (lookup only). Throws Registry* / SchemaNotRegistered. */
  async init(): Promise<number> {
    if (!this.registry) throw new SerdeNotReadyError('no registry configured');
    const { id } = await this.registry.lookup(this.opts.subject, this.schemaText);
    this.useWriterId(id);
    return id;
  }

  /** Test / tooling seam: use a known id without a registry round trip. */
  useWriterId(id: number) {
    this.writerSchemaId = id;
    this.loaded.set(id, { kind: 'ok', resolver: this.type.createResolver(this.type) });
  }

  get writerId(): number | undefined {
    return this.writerSchemaId;
  }

  get ready(): boolean {
    return this.writerSchemaId !== undefined;
  }

  /** Synchronous: no registry call. Throws SerdeNotReadyError before init(), AvroEncodeError on a bad value. */
  encode(value: T): Buffer {
    if (this.writerSchemaId === undefined) throw new SerdeNotReadyError(`schema id of ${this.opts.subject} not known yet`);
    const problems: string[] = [];
    if (!this.type.isValid(value, { errorHook: (path, v) => problems.push(`${path.join('.') || '(root)'}=${JSON.stringify(v)?.slice(0, 60)}`) })) {
      throw new AvroEncodeError(`value does not fit the ${this.opts.subject} writer schema: ${problems.join(', ')}`);
    }
    return encodeWire(this.writerSchemaId, this.type.toBuffer(value));
  }

  /** Fetch the writer schema of every unknown id among these values. Throws RegistryUnavailableError. */
  async prefetch(buffers: (Buffer | null | undefined)[]): Promise<void> {
    for (const id of new Set(buffers.map(schemaIdOf).filter((x): x is number => x !== undefined && !this.loaded.has(x)))) {
      if (!this.registry) {
        this.loaded.set(id, { kind: 'unknown' });
        continue;
      }
      try {
        const writer = avsc.Type.forSchema(JSON.parse(await this.registry.getSchemaById(id)));
        try {
          this.loaded.set(id, { kind: 'ok', resolver: this.type.createResolver(writer) });
        } catch (e) {
          this.loaded.set(id, { kind: 'incompatible', message: (e as Error).message });
        }
      } catch (e) {
        if (e instanceof UnknownSchemaIdError) this.loaded.set(id, { kind: 'unknown' });
        else throw e;
      }
    }
  }

  /** Synchronous after prefetch(). */
  decode(b: Buffer): T {
    const { schemaId, body } = decodeWire(b);
    const l = this.loaded.get(schemaId);
    if (!l) throw new SerdeNotReadyError(`writer schema ${schemaId} not prefetched`);
    if (l.kind === 'unknown') throw new UnknownSchemaIdError(schemaId);
    if (l.kind === 'incompatible') throw new IncompatibleWriterError(schemaId, l.message);
    try {
      return this.type.fromBuffer(body, l.resolver) as T;
    } catch (e) {
      throw new AvroDecodeError(`record does not decode with writer schema ${schemaId}: ${(e as Error).message}`);
    }
  }

  /** prefetch + decode of one value. */
  async decodeAsync(b: Buffer): Promise<T> {
    await this.prefetch([b]);
    return this.decode(b);
  }
}
