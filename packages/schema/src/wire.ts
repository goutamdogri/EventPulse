// Confluent wire format: [magic byte 0x00][schema id: 4 bytes, big endian][Avro binary body].
import { NotAvroError } from './errors.js';

export const MAGIC_BYTE = 0;
export const HEADER_SIZE = 5;

export function encodeWire(schemaId: number, body: Buffer): Buffer {
  if (!Number.isInteger(schemaId) || schemaId < 0 || schemaId > 0x7fffffff) throw new RangeError(`bad schema id ${schemaId}`);
  const out = Buffer.allocUnsafe(HEADER_SIZE + body.length);
  out.writeUInt8(MAGIC_BYTE, 0);
  out.writeInt32BE(schemaId, 1);
  body.copy(out, HEADER_SIZE);
  return out;
}

/** True if the buffer starts like a wire-format record (cheap check used to route Avro vs legacy JSON). */
export const looksLikeWire = (b: Buffer | null | undefined) => !!b && b.length >= HEADER_SIZE && b[0] === MAGIC_BYTE;

export function decodeWire(b: Buffer): { schemaId: number; body: Buffer } {
  if (b.length < HEADER_SIZE) throw new NotAvroError(`record is ${b.length} bytes, shorter than the ${HEADER_SIZE}-byte wire header`);
  if (b[0] !== MAGIC_BYTE) throw new NotAvroError(`magic byte is 0x${b[0].toString(16).padStart(2, '0')}, expected 0x00`);
  const schemaId = b.readInt32BE(1);
  if (schemaId < 0) throw new NotAvroError(`negative schema id ${schemaId}`);
  return { schemaId, body: b.subarray(HEADER_SIZE) };
}

/** Schema id of a wire record, or undefined if it is not one. */
export function schemaIdOf(b: Buffer | null | undefined): number | undefined {
  if (!looksLikeWire(b)) return undefined;
  const id = b!.readInt32BE(1);
  return id >= 0 ? id : undefined;
}
