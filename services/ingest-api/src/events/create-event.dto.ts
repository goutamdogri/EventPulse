import {
  IsInt,
  IsISO8601,
  IsObject,
  IsString,
  IsUUID,
  Length,
  Min,
} from 'class-validator';

export class CreateEventDto {
  @IsUUID()
  event_id!: string;

  @IsString()
  @Length(1, 100)
  type!: string;

  @IsISO8601({ strict: true })
  occurred_at!: string;

  @IsInt()
  @Min(1)
  schema_version!: number;

  @IsObject()
  payload!: Record<string, unknown>;
}
