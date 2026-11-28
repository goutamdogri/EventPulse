// Parser of docs/delivery-guarantees.md (S3-F9). Pure.
//
// A claim is a level-3 heading "### G<n> <title>" followed by the fixed fields, one list item each:
//   - Claim: ...
//   - Conditions: ...
//   - Mechanism: ...
//   - Proved by: [test: G2.same-chunk-twice] [test: G2.crash-matrix]
//   - Not guaranteed: ...
// A claim ends at the next heading of level 1 to 3.

export const FIELDS = ['Claim', 'Conditions', 'Mechanism', 'Proved by', 'Not guaranteed'] as const;
export type Field = (typeof FIELDS)[number];

export interface ParsedClaim {
  id: string;
  title: string;
  line: number;
  fields: Partial<Record<Field, string>>;
  /** Test ids in "Proved by". */
  refs: string[];
}

export interface ParsedDoc {
  claims: ParsedClaim[];
  /** Every [test: id] anywhere in the document, with its line. */
  allRefs: { id: string; line: number }[];
}

const CLAIM_RE = /^###\s+(G\d+)\b\s*(.*)$/;
const HEADING_RE = /^#{1,3}\s/;
const FIELD_RE = /^-\s+([A-Za-z ]+?):\s*(.*)$/;
export const REF_RE = /\[test:\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*\]/g;

export function parseDoc(md: string): ParsedDoc {
  const claims: ParsedClaim[] = [];
  const allRefs: { id: string; line: number }[] = [];
  let cur: ParsedClaim | undefined;
  let curField: Field | undefined;
  md.split('\n').forEach((raw, i) => {
    const line = raw.trimEnd();
    for (const m of line.matchAll(REF_RE)) allRefs.push({ id: m[1], line: i + 1 });
    const c = line.match(CLAIM_RE);
    if (c) {
      cur = { id: c[1], title: c[2].trim(), line: i + 1, fields: {}, refs: [] };
      curField = undefined;
      claims.push(cur);
      return;
    }
    if (HEADING_RE.test(line)) {
      cur = undefined;
      curField = undefined;
      return;
    }
    if (!cur) return;
    const f = line.match(FIELD_RE);
    if (f && (FIELDS as readonly string[]).includes(f[1])) {
      curField = f[1] as Field;
      cur.fields[curField] = f[2].trim();
    } else if (curField && /^\s+\S/.test(raw)) {
      cur.fields[curField] = `${cur.fields[curField]} ${line.trim()}`.trim(); // continuation line
    } else if (line.trim() !== '') {
      curField = undefined;
    }
  });
  for (const c of claims) c.refs = [...(c.fields['Proved by'] ?? '').matchAll(REF_RE)].map((m) => m[1]);
  return { claims, allRefs };
}
