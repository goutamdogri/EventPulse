// Seed `tenants` from the API keys ingest-api knows (S3-F4): API_KEYS=key:tenant,... from the environment, or
// from services/ingest-api/.env. Idempotent (ON CONFLICT DO NOTHING). Run: scripts/seed-tenants.sh
import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import pg from 'pg';

function apiKeys(): string {
  if (process.env.API_KEYS) return process.env.API_KEYS;
  const f = resolve(import.meta.dirname, '../../ingest-api/.env');
  if (!existsSync(f)) return '';
  const line = readFileSync(f, 'utf8').split('\n').find((l) => l.startsWith('API_KEYS='));
  return line?.slice('API_KEYS='.length).trim().replace(/^["']|["']$/g, '') ?? '';
}

const tenants = [...new Set(apiKeys().split(',').filter(Boolean).map((p) => p.split(':')[1]?.trim()).filter((t): t is string => !!t))];
if (tenants.length === 0) {
  console.error('no tenants: set API_KEYS=key:tenant,... or services/ingest-api/.env');
  process.exit(2);
}
if (!process.env.DATABASE_URL) {
  console.error('DATABASE_URL is required');
  process.exit(2);
}
const client = new pg.Client({ connectionString: process.env.DATABASE_URL });
await client.connect();
try {
  const r = await client.query('INSERT INTO tenants (tenant_id, name) SELECT t, t FROM unnest($1::text[]) t ON CONFLICT DO NOTHING RETURNING tenant_id', [tenants]);
  console.log(`tenants: ${tenants.length} known, ${r.rowCount} inserted (${r.rows.map((x) => x.tenant_id).join(', ') || 'none new'})`);
} finally {
  await client.end();
}
