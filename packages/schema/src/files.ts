// The repo's schema files (schemas/): subjects.json and the .avsc files it lists.
import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { COMPATIBILITY_LEVELS } from './registry.js';
import type { CompatibilityLevel } from './registry.js';

export const SUBJECT_EVENTS_RAW = 'events.raw-value';

/** schemas/ at the repo root (packages/schema/dist -> ../../../schemas), or SCHEMAS_DIR. */
export function schemasDir(env: Record<string, string | undefined> = process.env): string {
  const d = env.SCHEMAS_DIR || resolve(import.meta.dirname, '../../../schemas');
  if (!existsSync(resolve(d, 'subjects.json'))) throw new Error(`no subjects.json in ${d} (set SCHEMAS_DIR)`);
  return d;
}

export interface SubjectSpec {
  subject: string;
  compatibility: CompatibilityLevel;
  /** Versions in registration order: file path (relative to schemas/) and canonical schema text. */
  versions: { file: string; schema: string; parsed: object }[];
}

/** The schema text sent to the registry: the parsed JSON re-serialized without whitespace (stable). */
export const schemaText = (parsed: object) => JSON.stringify(parsed);

export function readSchemaFile(dir: string, file: string): { schema: string; parsed: object } {
  const parsed = JSON.parse(readFileSync(resolve(dir, file), 'utf8'));
  return { schema: schemaText(parsed), parsed };
}

export function loadSubjects(dir = schemasDir()): SubjectSpec[] {
  const cfg = JSON.parse(readFileSync(resolve(dir, 'subjects.json'), 'utf8'));
  return Object.entries(cfg.subjects as Record<string, { files: string[]; compatibility: string }>).map(([subject, s]) => {
    if (!COMPATIBILITY_LEVELS.includes(s.compatibility as CompatibilityLevel)) throw new Error(`${subject}: bad compatibility ${s.compatibility}`);
    if (!Array.isArray(s.files) || s.files.length === 0) throw new Error(`${subject}: no files`);
    return { subject, compatibility: s.compatibility as CompatibilityLevel, versions: s.files.map((file) => ({ file, ...readSchemaFile(dir, file) })) };
  });
}

export function subjectSpec(subject: string, dir = schemasDir()): SubjectSpec {
  const s = loadSubjects(dir).find((x) => x.subject === subject);
  if (!s) throw new Error(`subject ${subject} is not in ${dir}/subjects.json`);
  return s;
}

/** The newest version of a subject: what producers write and consumers read with. */
export const latestSchema = (subject: string, dir?: string) => subjectSpec(subject, dir).versions.at(-1)!;
