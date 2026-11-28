// S3-F7 compatibility rules, as checks against a real Schema Registry (it is the authority on Avro compatibility,
// so this tool never re-implements it).
//
//   checkHistory  registers the WHOLE history of a subject, in order, under its level, into a THROW-AWAY subject
//                 (prefix + subject), optionally tests one more candidate, then deletes the throw-away subject.
//                 Every version must be accepted: with a *_TRANSITIVE level that checks each one against ALL
//                 earlier ones. Works on an empty CI registry and on the dev registry alike (nothing real touched).
//   checkAgainst  is the newest local version compatible with what a running registry has under the REAL subject?
//   register      idempotent deploy step: set the level, then register each version in order (an existing identical
//                 schema returns its id; an incompatible one fails and stops).
//   checkFixtures every fixtures/incompatible/* must be rejected and every fixtures/compatible/* accepted on top of
//                 the real history, under the subject's level (also reports what BACKWARD alone would do).

import { readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { RegistryHttpError, readSchemaFile } from '@eventpulse/schema';
import type { CompatibilityLevel, RegistryClient, SubjectSpec } from '@eventpulse/schema';

export interface StepResult {
  file: string;
  ok: boolean;
  id?: number;
  messages: string[];
}

export interface SubjectResult {
  subject: string;
  level: CompatibilityLevel;
  ok: boolean;
  steps: StepResult[];
  candidate?: StepResult;
}

const short = (msgs: string[]) => msgs.map((m) => m.replace(/\s+/g, ' ').slice(0, 300));

async function registerOne(client: RegistryClient, subject: string, file: string, schema: string): Promise<StepResult> {
  const t = await client.testCompatibility(subject, schema);
  if (!t.compatible) return { file, ok: false, messages: short(t.messages) };
  try {
    const { id } = await client.register(subject, schema);
    return { file, ok: true, id, messages: [] };
  } catch (e) {
    if (e instanceof RegistryHttpError && (e.status === 409 || e.status === 422)) return { file, ok: false, messages: [e.message] };
    throw e;
  }
}

export async function checkHistory(
  client: RegistryClient,
  spec: SubjectSpec,
  opts: { prefix: string; candidate?: { file: string; schema: string }; level?: CompatibilityLevel },
): Promise<SubjectResult> {
  const level = opts.level ?? spec.compatibility;
  const temp = `${opts.prefix}-${spec.subject}`;
  const steps: StepResult[] = [];
  let candidate: StepResult | undefined;
  try {
    await client.setCompatibility(temp, level);
    for (const v of spec.versions) {
      const r = await registerOne(client, temp, v.file, v.schema);
      steps.push(r);
      if (!r.ok) break;
    }
    if (opts.candidate && steps.every((s) => s.ok)) {
      const t = await client.testCompatibility(temp, opts.candidate.schema);
      candidate = { file: opts.candidate.file, ok: t.compatible, messages: short(t.messages) };
    }
  } finally {
    await client.deleteSubject(temp, true).catch(() => undefined);
  }
  return { subject: spec.subject, level, ok: steps.every((s) => s.ok), steps, ...(candidate ? { candidate } : {}) };
}

export async function checkAgainst(client: RegistryClient, spec: SubjectSpec): Promise<SubjectResult> {
  const latest = spec.versions.at(-1)!;
  const registered = await client.versions(spec.subject);
  const t = await client.testCompatibility(spec.subject, latest.schema);
  return {
    subject: spec.subject,
    level: (await client.getCompatibility(spec.subject)) ?? spec.compatibility,
    ok: t.compatible,
    steps: [{ file: latest.file, ok: t.compatible, messages: [`registered versions: ${registered.join(', ') || 'none'}`, ...short(t.messages)] }],
  };
}

export async function register(client: RegistryClient, spec: SubjectSpec): Promise<SubjectResult> {
  await client.setCompatibility(spec.subject, spec.compatibility);
  const steps: StepResult[] = [];
  for (const v of spec.versions) {
    const r = await registerOne(client, spec.subject, v.file, v.schema);
    steps.push(r);
    if (!r.ok) break;
  }
  return { subject: spec.subject, level: spec.compatibility, ok: steps.every((s) => s.ok), steps };
}

export interface FixtureResult {
  file: string;
  expected: 'compatible' | 'incompatible';
  /** Verdict under the subject's own level (decides pass/fail). */
  verdict: boolean;
  /** What BACKWARD alone would say (for the policy write-up). */
  backward: boolean;
  pass: boolean;
  messages: string[];
}

export async function checkFixtures(client: RegistryClient, spec: SubjectSpec, schemasDir: string, prefix: string): Promise<FixtureResult[]> {
  const out: FixtureResult[] = [];
  if (!spec.fixtures) return out; // a subject's fixtures are written against ITS schema: none listed, none run
  for (const expected of ['incompatible', 'compatible'] as const) {
    const dir = resolve(schemasDir, spec.fixtures, expected);
    let files: string[] = [];
    try {
      files = readdirSync(dir).filter((f) => f.endsWith('.avsc')).sort();
    } catch {
      continue;
    }
    for (const f of files) {
      const file = `${spec.fixtures}/${expected}/${f}`;
      const candidate = { file, schema: readSchemaFile(schemasDir, file).schema };
      const own = await checkHistory(client, spec, { prefix: `${prefix}-own`, candidate });
      const back = await checkHistory(client, spec, { prefix: `${prefix}-bwd`, candidate, level: 'BACKWARD' });
      const verdict = own.candidate?.ok ?? false;
      out.push({ file, expected, verdict, backward: back.candidate?.ok ?? false, pass: verdict === (expected === 'compatible'), messages: own.candidate?.messages ?? [] });
    }
  }
  return out;
}
