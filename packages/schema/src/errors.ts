// Typed schema errors (S3-F7). The sink's classifier (Stage 3 step 5: packages/retry) maps them:
//   NotAvroError, AvroDecodeError, IncompatibleWriterError, UnknownSchemaIdError -> PERMANENT (the record is bad)
//   RegistryUnavailableError                                                     -> TRANSIENT, blocking: a dead
//                                                registry must NEVER dead-letter records it could have decoded

export class SchemaError extends Error {}

/** Not the Confluent wire format (no magic byte 0 + 4-byte id), or too short. */
export class NotAvroError extends SchemaError {
  readonly name = 'NotAvroError';
}

/** The registry answered: this schema id does not exist. */
export class UnknownSchemaIdError extends SchemaError {
  readonly name = 'UnknownSchemaIdError';
  constructor(readonly schemaId: number) {
    super(`schema id ${schemaId} is not registered`);
  }
}

/** The writer schema of a record cannot be read with this consumer's reader schema. */
export class IncompatibleWriterError extends SchemaError {
  readonly name = 'IncompatibleWriterError';
  constructor(readonly schemaId: number, message: string) {
    super(`writer schema ${schemaId} cannot be read with the reader schema: ${message}`);
  }
}

/** The bytes do not decode with the writer schema (truncated, corrupt), or the decoded values are invalid. */
export class AvroDecodeError extends SchemaError {
  readonly name = 'AvroDecodeError';
}

/** A value does not fit the writer schema (the producer side). */
export class AvroEncodeError extends SchemaError {
  readonly name = 'AvroEncodeError';
}

/** The registry could not be reached or failed (timeout, connection error, 5xx). Retry later. */
export class RegistryUnavailableError extends SchemaError {
  readonly name = 'RegistryUnavailableError';
  constructor(message: string, readonly cause?: unknown) {
    super(message);
  }
}

/** The registry answered with a 4xx we do not map to something more specific. */
export class RegistryHttpError extends SchemaError {
  readonly name = 'RegistryHttpError';
  constructor(readonly status: number, readonly errorCode: number | undefined, message: string) {
    super(message);
  }
}

/** lookup: this subject does not have this schema (it was never registered: run `make schemas`). */
export class SchemaNotRegisteredError extends SchemaError {
  readonly name = 'SchemaNotRegisteredError';
}

/** The serde was used before init() found the writer schema id. */
export class SerdeNotReadyError extends SchemaError {
  readonly name = 'SerdeNotReadyError';
}
