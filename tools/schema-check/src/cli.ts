// schema-check (S3-F7): compatibility in CI, registration at deploy. See check.ts for the rules.
//
//   schema-check check                 every subject's whole history under its level (throw-away subjects)
//   schema-check check --against       newest local version vs the REAL subject on the registry
//   schema-check check --candidate F   history + one candidate schema file (relative to schemas/)
//   schema-check fixtures              incompatible fixtures rejected, compatible ones accepted
//   schema-check register              idempotent registration of every subject (deploy step: make schemas)
//
// Env: SCHEMA_REGISTRY_URL (default http://localhost:8081), SCHEMAS_DIR (default <repo>/schemas).
// Exit: 0 ok, 1 incompatible (or a fixture verdict is wrong), 2 registry / infrastructure error, 64 usage.

import { randomBytes } from 'node:crypto';
import { RegistryClient, loadSubjects, readSchemaFile, schemasDir } from '@eventpulse/schema';
import { checkAgainst, checkFixtures, checkHistory, register } from './check.ts';
import type { SubjectResult } from './check.ts';

const url = process.env.SCHEMA_REGISTRY_URL || 'http://localhost:8081';
const client = new RegistryClient({ url, timeoutMs: 10_000 });
const prefix = `schema-check-${randomBytes(4).toString('hex')}`;

function print(r: SubjectResult) {
  console.log(`${r.ok ? 'OK  ' : 'FAIL'} ${r.subject} (${r.level})`);
  for (const s of r.steps) {
    console.log(`  ${s.ok ? 'ok  ' : 'FAIL'} ${s.file}${s.id !== undefined ? `  id ${s.id}` : ''}`);
    for (const m of s.messages) console.log(`         ${m}`);
  }
  if (r.candidate) {
    console.log(`  ${r.candidate.ok ? 'ok  ' : 'FAIL'} candidate ${r.candidate.file}`);
    for (const m of r.candidate.messages) console.log(`         ${m}`);
  }
}

async function main(argv: string[]): Promise<number> {
  const [cmd, ...rest] = argv;
  const dir = schemasDir();
  const specs = loadSubjects(dir);
  const flag = (f: string) => rest.includes(f);
  const value = (f: string) => (rest.indexOf(f) >= 0 ? rest[rest.indexOf(f) + 1] : undefined);
  console.log(`schema registry ${url}, schemas ${dir}`);

  if (cmd === 'check') {
    const candidateFile = value('--candidate');
    let ok = true;
    for (const spec of specs) {
      const r = flag('--against')
        ? await checkAgainst(client, spec)
        : await checkHistory(client, spec, { prefix, ...(candidateFile ? { candidate: { file: candidateFile, schema: readSchemaFile(dir, candidateFile).schema } } : {}) });
      print(r);
      ok &&= r.ok && (r.candidate?.ok ?? true);
    }
    return ok ? 0 : 1;
  }
  if (cmd === 'register') {
    let ok = true;
    for (const spec of specs) {
      const r = await register(client, spec);
      print(r);
      ok &&= r.ok;
    }
    return ok ? 0 : 1;
  }
  if (cmd === 'fixtures') {
    let ok = true;
    for (const spec of specs) {
      console.log(`${spec.subject} (${spec.compatibility}; BACKWARD shown for comparison)`);
      for (const f of await checkFixtures(client, spec, dir, prefix)) {
        const word = (v: boolean) => (v ? 'accepted' : 'rejected');
        console.log(`  ${f.pass ? 'ok  ' : 'FAIL'} ${f.file}: expected ${f.expected}, ${spec.compatibility} ${word(f.verdict)}, BACKWARD ${word(f.backward)}`);
        for (const m of f.messages.slice(0, 2)) console.log(`         ${m}`);
        ok &&= f.pass;
      }
    }
    return ok ? 0 : 1;
  }
  console.error('usage: schema-check check [--against | --candidate <file>] | fixtures | register');
  return 64;
}

main(process.argv.slice(2)).then(
  (code) => process.exit(code),
  (e: unknown) => {
    console.error(`schema-check error: ${(e as Error)?.message ?? e}`);
    process.exit(2); // registry unreachable or another infrastructure error, not a compatibility verdict
  },
);
