import {
  Global,
  Inject,
  Injectable,
  Module,
  OnApplicationShutdown,
} from '@nestjs/common';
import { Pool } from 'pg';
import { AppConfig, CONFIG } from '../config.js';

export const PG_POOL = Symbol('PG_POOL');

@Injectable()
class DbShutdown implements OnApplicationShutdown {
  constructor(@Inject(PG_POOL) private readonly pool: Pool) {}

  async onApplicationShutdown() {
    await this.pool.end();
  }
}

@Global()
@Module({
  providers: [
    {
      provide: PG_POOL,
      inject: [CONFIG],

      useFactory: (c: AppConfig) => {
        const pool = new Pool({
          connectionString: c.databaseUrl,
          max: c.pgPoolMax,
          connectionTimeoutMillis: c.pgConnTimeoutMs || undefined,
          statement_timeout: c.pgStatementTimeoutMs || undefined,
          idleTimeoutMillis: 30_000,
          application_name: 'ingest-api', // shows up in pg_stat_activity
        });
        // Without this, an error on an idle client crashes the process.
        pool.on('error', (e) =>
          console.error('[pg idle client error]', e.message),
        );
        return pool;
      },
    },

    DbShutdown,
  ],
  exports: [PG_POOL],
})
export class DbModule {}
