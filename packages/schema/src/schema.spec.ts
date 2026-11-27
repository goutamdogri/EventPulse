import fc from 'fast-check';
import { resolve } from 'node:path';
import {
  AvroDecodeError,
  AvroEncodeError,
  IncompatibleWriterError,
  NotAvroError,
  RegistryHttpError,
  RegistryUnavailableError,
  SchemaNotRegisteredError,
  SerdeNotReadyError,
  UnknownSchemaIdError,
} from './errors.js';
import { EventSerde, fromRecord, toRecord } from './event-serde.js';
import { SUBJECT_EVENTS_RAW, loadSubjects, readSchemaFile, schemasDir, subjectSpec } from './files.js';
import { RegistryClient } from './registry.js';
import { decodeWire, encodeWire, looksLikeWire, schemaIdOf } from './wire.js';

const DIR = schemasDir({ SCHEMAS_DIR: resolve(import.meta.dirname, '../../../schemas') });
const V1 = readSchemaFile(DIR, 'events/v1.avsc').schema;
const V2 = readSchemaFile(DIR, 'events/v2.avsc').schema;

const event = (o: Record<string, unknown> = {}) => ({
  event_id: '6F1C2B1E-8A43-4C0E-9D2F-0A1B2C3D4E5F',
  tenant_id: 'tenant_a',
  type: 'order.created',
  occurred_at: '2026-11-12T10:00:00.123Z',
  received_at: '2026-11-12T10:00:01.000Z',
  schema_version: 1,
  payload: { b: [1, { y: 2, x: 1 }], a: 'é' },
  ...o,
});

/** In-memory registry: id -> schema text, subject -> ids. */
function fakeRegistry(schemas: Record<number, string>, subjects: Record<string, Record<string, number>> = {}) {
  const calls: string[] = [];
  const f = async (url: string, init: RequestInit) => {
    const u = new URL(url);
    calls.push(`${init.method} ${u.pathname}`);
    const reply = (status: number, body: unknown) => new Response(JSON.stringify(body), { status });
    const m = u.pathname.match(/^\/schemas\/ids\/(\d+)$/);
    if (m) return schemas[Number(m[1])] ? reply(200, { schema: schemas[Number(m[1])] }) : reply(404, { error_code: 40403, message: 'Schema not found' });
    const l = u.pathname.match(/^\/subjects\/([^/]+)$/);
    if (l && init.method === 'POST') {
      const id = subjects[decodeURIComponent(l[1])]?.[JSON.parse(String(init.body)).schema];
      return id ? reply(200, { id, version: 1 }) : reply(404, { error_code: 40403, message: 'Schema not found' });
    }
    return reply(500, { message: 'unexpected' });
  };
  return { client: new RegistryClient({ url: 'http://registry', fetch: f as any }), calls };
}

describe('wire format', () => {
  it('magic byte 0, big-endian id, then the body', () => {
    const w = encodeWire(258, Buffer.from([7, 8]));
    expect([...w]).toEqual([0, 0, 0, 1, 2, 7, 8]);
    expect(decodeWire(w)).toEqual({ schemaId: 258, body: Buffer.from([7, 8]) });
    expect(schemaIdOf(w)).toBe(258);
    expect(looksLikeWire(w)).toBe(true);
  });

  it('rejects short buffers, a wrong magic byte (legacy JSON starts with "{"), negative ids', () => {
    expect(() => decodeWire(Buffer.from([0, 0, 0]))).toThrow(NotAvroError);
    expect(() => decodeWire(Buffer.from('{"event_id":1}'))).toThrow(/magic byte is 0x7b/);
    expect(() => decodeWire(Buffer.from([0, 0xff, 0xff, 0xff, 0xff, 1]))).toThrow(/negative/);
    expect(looksLikeWire(Buffer.from('{}'))).toBe(false);
    expect(schemaIdOf(Buffer.from('{"a":1}'))).toBeUndefined();
  });
});

describe('schema files', () => {
  it('subjects.json: events.raw-value, FULL_TRANSITIVE, v1 then v2', () => {
    const s = subjectSpec(SUBJECT_EVENTS_RAW, DIR);
    expect(s.compatibility).toBe('FULL_TRANSITIVE');
    expect(s.versions.map((v) => v.file)).toEqual(['events/v1.avsc', 'events/v2.avsc']);
    expect(loadSubjects(DIR).length).toBeGreaterThanOrEqual(1);
  });
});

describe('record mapping', () => {
  it('envelope -> record: lower-case id, epoch millis, payload JSON as sent, source null', () => {
    expect(toRecord(event())).toEqual({
      event_id: '6f1c2b1e-8a43-4c0e-9d2f-0a1b2c3d4e5f',
      tenant_id: 'tenant_a',
      type: 'order.created',
      occurred_at: Date.parse('2026-11-12T10:00:00.123Z'),
      received_at: Date.parse('2026-11-12T10:00:01.000Z'),
      schema_version: 1,
      payload_json: '{"b":[1,{"y":2,"x":1}],"a":"é"}',
      source: null,
    });
  });

  it('record -> envelope rejects a payload that is not a JSON object', () => {
    const r = toRecord(event());
    expect(() => fromRecord({ ...r, payload_json: '[1]' }, 1)).toThrow(AvroDecodeError);
    expect(() => fromRecord({ ...r, payload_json: '{oops' }, 1)).toThrow(AvroDecodeError);
  });
});

describe('EventSerde', () => {
  it('round trip with one schema; encode needs no registry once the id is known', () => {
    const s = new EventSerde(undefined, { subject: SUBJECT_EVENTS_RAW, schema: V2 });
    s.useWriterId(7);
    const d = s.decode(s.encode(event({ source: 'sdk-js' })));
    expect(d).toEqual({
      event_id: '6f1c2b1e-8a43-4c0e-9d2f-0a1b2c3d4e5f',
      tenant_id: 'tenant_a',
      type: 'order.created',
      occurred_at: '2026-11-12T10:00:00.123Z',
      received_at: '2026-11-12T10:00:01.000Z',
      schema_version: 1,
      payload: { b: [1, { y: 2, x: 1 }], a: 'é' },
      source: 'sdk-js',
      schema_id: 7,
    });
  });

  it('[C10.v1-writer-v2-reader] old producer, new consumer: the default for the new field is filled in', async () => {
    const writer = new EventSerde(undefined, { subject: SUBJECT_EVENTS_RAW, schema: V1 });
    writer.useWriterId(1);
    const { client } = fakeRegistry({ 1: V1 });
    const reader = new EventSerde(client, { subject: SUBJECT_EVENTS_RAW, schema: V2 });
    const b = writer.encode(event());
    await reader.prefetch([b]);
    expect(reader.decode(b)).toMatchObject({ source: null, schema_id: 1, type: 'order.created' });
  });

  it('[C10.v2-writer-v1-reader] new producer, old consumer: the new field is skipped', async () => {
    const writer = new EventSerde(undefined, { subject: SUBJECT_EVENTS_RAW, schema: V2 });
    writer.useWriterId(2);
    const { client } = fakeRegistry({ 2: V2 });
    const reader = new EventSerde(client, { subject: SUBJECT_EVENTS_RAW, schema: V1 });
    const b = writer.encode(event({ source: 'sdk-js' }));
    await reader.prefetch([b]);
    const d = reader.decode(b);
    expect(d.source).toBeNull(); // a v1 reader has no such field
    expect(d.payload).toEqual({ a: 'é', b: [1, { x: 1, y: 2 }] });
  });

  it('prefetch asks the registry once per id; decode before prefetch is an error', async () => {
    const writer = new EventSerde(undefined, { subject: SUBJECT_EVENTS_RAW, schema: V2 });
    writer.useWriterId(2);
    const { client, calls } = fakeRegistry({ 2: V2 });
    const reader = new EventSerde(client, { subject: SUBJECT_EVENTS_RAW, schema: V2 });
    const b = writer.encode(event());
    expect(() => reader.decode(b)).toThrow(SerdeNotReadyError);
    await reader.prefetch([b, b, b, Buffer.from('{"legacy":true}'), null]);
    await reader.prefetch([b]);
    expect(calls).toEqual(['GET /schemas/ids/2']);
  });

  it('an id the registry does not know: UnknownSchemaIdError at decode (permanent), remembered for a while', async () => {
    let t = 0;
    const { client, calls } = fakeRegistry({});
    const reader = new EventSerde(client, { subject: SUBJECT_EVENTS_RAW, schema: V2, negativeCacheMs: 1000, now: () => t });
    const b = encodeWire(99, Buffer.from([1, 2, 3]));
    await reader.prefetch([b]);
    expect(() => reader.decode(b)).toThrow(UnknownSchemaIdError);
    await reader.prefetch([b]);
    expect(calls).toHaveLength(1);
    t = 2000;
    await reader.prefetch([b]);
    expect(calls).toHaveLength(2); // asked again after the negative cache expired
  });

  it('registry down: prefetch throws RegistryUnavailableError (the caller retries, never dead-letters)', async () => {
    const down = new RegistryClient({ url: 'http://registry', fetch: (async () => { throw new Error('ECONNREFUSED'); }) as any });
    const reader = new EventSerde(down, { subject: SUBJECT_EVENTS_RAW, schema: V2 });
    await expect(reader.prefetch([encodeWire(5, Buffer.from([0]))])).rejects.toBeInstanceOf(RegistryUnavailableError);
  });

  it('a writer schema the reader cannot resolve: IncompatibleWriterError', async () => {
    const other = JSON.stringify({ type: 'record', name: 'Event', namespace: 'com.eventpulse', fields: [{ name: 'event_id', type: 'int' }] });
    const { client } = fakeRegistry({ 3: other });
    const reader = new EventSerde(client, { subject: SUBJECT_EVENTS_RAW, schema: V2 });
    const b = encodeWire(3, Buffer.from([2]));
    await reader.prefetch([b]);
    expect(() => reader.decode(b)).toThrow(IncompatibleWriterError);
  });

  it('truncated or corrupt bytes: AvroDecodeError', () => {
    const s = new EventSerde(undefined, { subject: SUBJECT_EVENTS_RAW, schema: V2 });
    s.useWriterId(1);
    const good = s.encode(event());
    expect(() => s.decode(good.subarray(0, good.length - 10))).toThrow(AvroDecodeError);
    expect(() => s.decode(Buffer.concat([good.subarray(0, 5), Buffer.from([0xff, 0xff, 0xff])]))).toThrow(AvroDecodeError);
  });

  it('encode: before init is an error; a value that does not fit is AvroEncodeError with the field', () => {
    const s = new EventSerde(undefined, { subject: SUBJECT_EVENTS_RAW, schema: V2 });
    expect(() => s.encode(event())).toThrow(SerdeNotReadyError);
    s.useWriterId(1);
    expect(() => s.encode(event({ schema_version: 1.5 }))).toThrow(/schema_version=1.5/);
    expect(() => s.encode(event({ occurred_at: 'nope' }))).toThrow(AvroEncodeError);
  });

  it('init looks the schema up (never registers); not registered -> SchemaNotRegisteredError', async () => {
    const { client, calls } = fakeRegistry({}, { [SUBJECT_EVENTS_RAW]: { [V2]: 12 } });
    const s = new EventSerde(client, { subject: SUBJECT_EVENTS_RAW, schema: V2 });
    expect(await s.init()).toBe(12);
    expect(calls).toEqual([`POST /subjects/${SUBJECT_EVENTS_RAW}`]);
    const missing = new EventSerde(client, { subject: SUBJECT_EVENTS_RAW, schema: V1 });
    await expect(missing.init()).rejects.toBeInstanceOf(SchemaNotRegisteredError);
  });

  it('payload strings travel exactly as sent (no Unicode normalization in transport)', () => {
    const s = new EventSerde(undefined, { subject: SUBJECT_EVENTS_RAW, schema: V2 });
    s.useWriterId(1);
    const decomposed = 'cafe\u0301';
    expect(s.decode(s.encode(event({ payload: { [decomposed]: decomposed } }))).payload).toEqual({ [decomposed]: decomposed });
  });

  it('property: any JSON object payload round-trips', () => {
    const s = new EventSerde(undefined, { subject: SUBJECT_EVENTS_RAW, schema: V2 });
    s.useWriterId(1);
    fc.assert(
      fc.property(fc.dictionary(fc.string(), fc.jsonValue()), (payload) => {
        const p = JSON.parse(JSON.stringify(payload)); // what an HTTP body can carry (-0 -> 0 etc.)
        expect(s.decode(s.encode(event({ payload: p }))).payload).toEqual(p);
      }),
      { numRuns: 500 },
    );
  });
});

describe('RegistryClient error mapping', () => {
  const client = (status: number, body: unknown) =>
    new RegistryClient({ url: 'http://r', fetch: (async () => new Response(JSON.stringify(body), { status })) as any });

  it('5xx and network errors are RegistryUnavailableError; other 4xx are RegistryHttpError', async () => {
    await expect(client(503, {}).getSchemaById(1)).rejects.toBeInstanceOf(RegistryUnavailableError);
    await expect(client(422, { error_code: 42201, message: 'Invalid schema' }).register('s', '{}')).rejects.toMatchObject({ status: 422, errorCode: 42201 });
    await expect(client(409, { error_code: 409, message: 'incompatible' }).register('s', '{}')).rejects.toBeInstanceOf(RegistryHttpError);
  });

  it('timeouts are RegistryUnavailableError', async () => {
    const slow = new RegistryClient({
      url: 'http://r',
      timeoutMs: 20,
      fetch: ((_u: string, init: RequestInit) =>
        new Promise((_r, rej) => init.signal!.addEventListener('abort', () => rej(new Error('aborted'))))) as any,
    });
    await expect(slow.getSchemaById(1)).rejects.toBeInstanceOf(RegistryUnavailableError);
  });

  it('observer sees every request with its outcome', async () => {
    const seen: string[] = [];
    const c = new RegistryClient({ url: 'http://r', fetch: (async () => new Response('{"schema":"\\"int\\""}', { status: 200 })) as any, observer: { request: (op, ok) => seen.push(`${op}:${ok}`) } });
    await c.getSchemaById(1);
    expect(seen).toEqual(['get_by_id:true']);
  });
});
