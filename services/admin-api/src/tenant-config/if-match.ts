// If-Match on PUT /v1/admin/tenants/:id/config (optimistic concurrency, S3-F4). The ETag is the version: "3".
//
//   absent          no check (last writer wins; the admin chose not to protect the change)
//   *               the config must exist (version >= 1)
//   "3" / W/"3"     current version must be 3 (a list "2", "3" matches any)
// A mismatch is 412 Precondition Failed: two admins editing the same version cannot silently overwrite each other.
// Anything else is a 400.

export type IfMatch = { kind: 'none' } | { kind: 'any' } | { kind: 'versions'; versions: number[] };

export class BadIfMatchError extends Error {
  readonly name = 'BadIfMatchError';
}

export function parseIfMatch(h: string | undefined): IfMatch {
  if (h === undefined || h.trim() === '') return { kind: 'none' };
  if (h.trim() === '*') return { kind: 'any' };
  const versions = h.split(',').map((part) => {
    const m = part.trim().match(/^(?:W\/)?"(\d{1,15})"$/);
    if (!m) throw new BadIfMatchError(`If-Match must be "*" or quoted versions like "3" (got ${JSON.stringify(h).slice(0, 80)})`);
    return Number(m[1]);
  });
  return { kind: 'versions', versions };
}

/** current = the stored version, 0 when the tenant has no config yet. */
export function ifMatchOk(m: IfMatch, current: number): boolean {
  if (m.kind === 'none') return true;
  if (m.kind === 'any') return current >= 1;
  return m.versions.includes(current);
}

export const etagOf = (version: number) => `"${version}"`;
