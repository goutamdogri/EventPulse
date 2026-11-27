export {
  AvroDecodeError,
  AvroEncodeError,
  IncompatibleWriterError,
  NotAvroError,
  RegistryHttpError,
  RegistryUnavailableError,
  SchemaError,
  SchemaNotRegisteredError,
  SerdeNotReadyError,
  UnknownSchemaIdError,
} from './errors.js';
export { HEADER_SIZE, MAGIC_BYTE, decodeWire, encodeWire, looksLikeWire, schemaIdOf } from './wire.js';
export { COMPATIBILITY_LEVELS, RegistryClient } from './registry.js';
export type { CompatibilityLevel, RegistryObserver, RegistryOp, RegistryOptions } from './registry.js';
export { SUBJECT_EVENTS_RAW, latestSchema, loadSubjects, readSchemaFile, schemaText, schemasDir, subjectSpec } from './files.js';
export type { SubjectSpec } from './files.js';
export {
  CONTENT_TYPE_AVRO,
  CONTENT_TYPE_JSON,
  EventSerde,
  HEADER_CONTENT_TYPE,
  fromRecord,
  parseSchema,
  toRecord,
} from './event-serde.js';
export type { DecodedEvent, EventInput, EventSerdeOptions } from './event-serde.js';
