// A small Confluent Schema Registry REST client (Stage 3, S3-F7): only what EventPulse uses, with bounded
// timeouts, typed errors and an observer for metrics. Caching lives in the serde (per id), not here, so tools
// (schema-check) always see the registry's current answer.
//
// Error mapping:
//   network error, timeout, 5xx              -> RegistryUnavailableError (transient)
//   404 + 40403 on GET /schemas/ids/{id}     -> UnknownSchemaIdError (permanent for that record)
//   404 (40401 / 40403) on lookup            -> SchemaNotRegisteredError
//   other 4xx                                -> RegistryHttpError

import {
  RegistryHttpError,
  RegistryUnavailableError,
  SchemaNotRegisteredError,
  UnknownSchemaIdError,
} from './errors.js';

export const COMPATIBILITY_LEVELS = ['BACKWARD', 'BACKWARD_TRANSITIVE', 'FORWARD', 'FORWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE', 'NONE'] as const;
export type CompatibilityLevel = (typeof COMPATIBILITY_LEVELS)[number];

export type RegistryOp = 'get_by_id' | 'lookup' | 'register' | 'compatibility' | 'config' | 'versions' | 'delete';

export interface RegistryObserver {
  request?(op: RegistryOp, ok: boolean, seconds: number): void;
}

export interface RegistryOptions {
  url: string;
  timeoutMs?: number;
  fetch?: typeof fetch;
  observer?: RegistryObserver;
}

const CT = 'application/vnd.schemaregistry.v1+json';

export class RegistryClient {
  readonly url: string;
  private readonly timeoutMs: number;
  private readonly f: typeof fetch;

  constructor(private readonly opts: RegistryOptions) {
    this.url = opts.url.replace(/\/+$/, '');
    this.timeoutMs = opts.timeoutMs ?? 2_000;
    this.f = opts.fetch ?? fetch;
  }

  private async call(op: RegistryOp, method: string, path: string, body?: unknown): Promise<{ status: number; json: any }> {
    const t0 = performance.now();
    let ok = false;
    try {
      let res: Response;
      try {
        res = await this.f(`${this.url}${path}`, {
          method,
          headers: { accept: CT, ...(body !== undefined ? { 'content-type': CT } : {}) },
          body: body !== undefined ? JSON.stringify(body) : undefined,
          signal: AbortSignal.timeout(this.timeoutMs),
        });
      } catch (e) {
        throw new RegistryUnavailableError(`schema registry ${this.url} unreachable (${op}): ${(e as Error)?.message ?? e}`, e);
      }
      const json = await res.json().catch(() => null);
      if (res.status >= 500) throw new RegistryUnavailableError(`schema registry ${op} failed: HTTP ${res.status} ${json?.message ?? ''}`.trim());
      ok = res.status < 400;
      return { status: res.status, json };
    } finally {
      this.opts.observer?.request?.(op, ok, (performance.now() - t0) / 1000);
    }
  }

  private fail(op: RegistryOp, r: { status: number; json: any }): never {
    throw new RegistryHttpError(r.status, r.json?.error_code, `schema registry ${op}: HTTP ${r.status} ${r.json?.message ?? ''}`.trim());
  }

  /** The schema text registered under a global id. */
  async getSchemaById(id: number): Promise<string> {
    const r = await this.call('get_by_id', 'GET', `/schemas/ids/${id}`);
    if (r.status === 404) throw new UnknownSchemaIdError(id);
    if (r.status >= 400) this.fail('get_by_id', r);
    return r.json.schema as string;
  }

  /** Id and version of a schema already registered under a subject (never registers). */
  async lookup(subject: string, schema: string): Promise<{ id: number; version: number }> {
    const r = await this.call('lookup', 'POST', `/subjects/${encodeURIComponent(subject)}`, { schema });
    if (r.status === 404) throw new SchemaNotRegisteredError(`schema not registered under subject ${subject} (${r.json?.message ?? 'not found'}): run \`make schemas\``);
    if (r.status >= 400) this.fail('lookup', r);
    return { id: r.json.id, version: r.json.version };
  }

  /** Register (idempotent: an existing identical schema returns its id). 409 = incompatible under the level. */
  async register(subject: string, schema: string): Promise<{ id: number }> {
    const r = await this.call('register', 'POST', `/subjects/${encodeURIComponent(subject)}/versions`, { schema });
    if (r.status >= 400) this.fail('register', r);
    return { id: r.json.id };
  }

  /** Would this schema be accepted under the subject's level? (verbose: the registry's reasons) */
  async testCompatibility(subject: string, schema: string, version: 'latest' | number = 'latest'): Promise<{ compatible: boolean; messages: string[] }> {
    const r = await this.call('compatibility', 'POST', `/compatibility/subjects/${encodeURIComponent(subject)}/versions/${version}?verbose=true`, { schema });
    if (r.status === 404) return { compatible: true, messages: ['subject has no versions yet'] };
    if (r.status >= 400) this.fail('compatibility', r);
    return { compatible: r.json.is_compatible === true, messages: r.json.messages ?? [] };
  }

  async setCompatibility(subject: string, level: CompatibilityLevel): Promise<void> {
    const r = await this.call('config', 'PUT', `/config/${encodeURIComponent(subject)}`, { compatibility: level });
    if (r.status >= 400) this.fail('config', r);
  }

  /** The subject's own level, or undefined when it inherits the global one. */
  async getCompatibility(subject: string): Promise<CompatibilityLevel | undefined> {
    const r = await this.call('config', 'GET', `/config/${encodeURIComponent(subject)}`);
    if (r.status === 404) return undefined;
    if (r.status >= 400) this.fail('config', r);
    return r.json.compatibilityLevel;
  }

  async versions(subject: string): Promise<number[]> {
    const r = await this.call('versions', 'GET', `/subjects/${encodeURIComponent(subject)}/versions`);
    if (r.status === 404) return [];
    if (r.status >= 400) this.fail('versions', r);
    return r.json;
  }

  /** Soft delete, then (permanent=true) hard delete. Used only for throw-away subjects (schema-check, tests). */
  async deleteSubject(subject: string, permanent = true): Promise<void> {
    const s = encodeURIComponent(subject);
    const soft = await this.call('delete', 'DELETE', `/subjects/${s}`);
    if (soft.status >= 400 && soft.status !== 404) this.fail('delete', soft);
    if (permanent) {
      const hard = await this.call('delete', 'DELETE', `/subjects/${s}?permanent=true`);
      if (hard.status >= 400 && hard.status !== 404) this.fail('delete', hard);
    }
    await this.call('config', 'DELETE', `/config/${s}`).catch(() => undefined);
  }
}
