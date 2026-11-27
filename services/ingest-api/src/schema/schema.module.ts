import { Global, Module } from '@nestjs/common';
import { SchemaService } from './schema.service.js';

/** S3-F7: the Avro writer for events.raw (one per process). */
@Global()
@Module({ providers: [SchemaService], exports: [SchemaService] })
export class SchemaModule {}
