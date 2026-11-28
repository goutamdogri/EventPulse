import {
  IsInt,
  IsISO8601,
  IsObject,
  IsString,
  IsUUID,
  Length,
  Matches,
  Min,
} from 'class-validator';
import { OCCURRED_AT_RE } from './event-time.js';

export class CreateEventDto {
  @IsUUID()
  event_id!: string;

  @IsString()
  @Length(1, 100)
  type!: string;

  /** RFC 3339 with a zone; normalized to UTC milliseconds by the service (event-time.ts). */
  @IsISO8601({ strict: true })
  @Matches(OCCURRED_AT_RE, { message: 'occurred_at must be an RFC 3339 date-time with a time zone (Z or ±hh:mm)' })
  occurred_at!: string;

  @IsInt()
  @Min(1)
  schema_version!: number;

  @IsObject()
  payload!: Record<string, unknown>;
}
