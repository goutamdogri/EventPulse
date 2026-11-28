// The tenant config document (S3-F4). Known keys and types only: an unknown key is a 400 (forbidNonWhitelisted),
// so a typo cannot silently become a config nobody reads. Every key is optional; a PUT replaces the WHOLE document.
// Body size is capped by BODY_LIMIT (16 kb).
import { Type } from 'class-transformer';
import {
  ArrayMaxSize,
  IsArray,
  IsBoolean,
  IsDefined,
  IsInt,
  IsObject,
  IsOptional,
  IsString,
  Matches,
  Max,
  MaxLength,
  Min,
  ValidateNested,
  ValidatorConstraint,
  Validate,
} from 'class-validator';
import type { ValidatorConstraintInterface } from 'class-validator';

@ValidatorConstraint({ name: 'booleanMap' })
export class BooleanMap implements ValidatorConstraintInterface {
  validate(v: unknown) {
    if (v === null || typeof v !== 'object' || Array.isArray(v)) return false;
    const entries = Object.entries(v as Record<string, unknown>);
    return entries.length <= 50 && entries.every(([k, b]) => /^[a-z][a-z0-9_]{0,63}$/.test(k) && typeof b === 'boolean');
  }
  defaultMessage() {
    return 'features must be an object of at most 50 snake_case keys with boolean values';
  }
}

export class TenantConfigDto {
  /** Ingest rate limit for the tenant (Stage 4 enforces it). */
  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(1_000_000)
  rate_limit_per_s?: number;

  /** How long the tenant's events are kept. */
  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(3650)
  retention_days?: number;

  /** Event types the tenant may send (empty or absent = any). */
  @IsOptional()
  @IsArray()
  @ArrayMaxSize(200)
  @IsString({ each: true })
  @MaxLength(64, { each: true })
  @Matches(/^[A-Za-z0-9_.:-]+$/, { each: true })
  allowed_event_types?: string[];

  /** Payload fields to redact downstream. */
  @IsOptional()
  @IsArray()
  @ArrayMaxSize(100)
  @IsString({ each: true })
  @MaxLength(128, { each: true })
  pii_fields?: string[];

  @IsOptional()
  @IsObject()
  @Validate(BooleanMap)
  features?: Record<string, boolean>;

  /** Example of a non-string field type the snapshot must carry exactly. */
  @IsOptional()
  @IsBoolean()
  paused?: boolean;
}

export class PutConfigDto {
  @IsDefined()
  @IsObject()
  @ValidateNested()
  @Type(() => TenantConfigDto)
  config: TenantConfigDto;
}

export const TENANT_ID_RE = /^[A-Za-z0-9_.-]{1,64}$/;
