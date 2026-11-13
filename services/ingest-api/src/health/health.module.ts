import {
  Controller,
  Get,
  Inject,
  Module,
  ServiceUnavailableException,
} from '@nestjs/common';
import { Pool } from 'pg';
import { PG_POOL } from '../db/db.module.js';

@Controller()
export class HealthController {
  constructor(@Inject(PG_POOL) private readonly pool: Pool) {}

  @Get('healthz') // liveness: shallow, never touches dependencies
  healthz() {
    return { ok: true };
  }

  @Get('readyz') // readiness: can we reach the DB?
  async readyz() {
    try {
      await this.pool.query('SELECT 1');
      return { ok: true, db: 'up' };
    } catch {
      throw new ServiceUnavailableException({ ok: false, db: 'down' });
    }
  }
}

@Module({ controllers: [HealthController] })
export class HealthModule {}
