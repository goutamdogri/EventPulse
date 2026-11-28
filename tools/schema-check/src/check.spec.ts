// Unit tests of the check logic with a fake registry (the real registry runs in tests/correctness/test/schema.e2e-spec.ts).
import { RegistryHttpError, RegistryUnavailableError } from '@eventpulse/schema';
import type { RegistryClient, SubjectSpec } from '@eventpulse/schema';
import { checkHistory, register } from './check.ts';

const spec = (n: number): SubjectSpec => ({
  subject: 'events.raw-value',
  compatibility: 'FULL_TRANSITIVE',
  versions: Array.from({ length: n }, (_, i) => ({ file: `events/v${i + 1}.avsc`, schema: `s${i + 1}`, parsed: {} })),
});

/** A registry that rejects the schemas listed in `incompatible`. */
function fake(o: { incompatible?: string[]; failOn?: string } = {}) {
  const calls: string[] = [];
  let id = 100;
  const client = {
    setCompatibility: async (s: string, l: string) => void calls.push(`config ${s} ${l}`),
    testCompatibility: async (s: string, schema: string) => {
      calls.push(`test ${s} ${schema}`);
      if (o.failOn === schema) throw new RegistryUnavailableError('registry down');
      return o.incompatible?.includes(schema) ? { compatible: false, messages: [`${schema} is incompatible`] } : { compatible: true, messages: [] };
    },
    register: async (s: string, schema: string) => (calls.push(`register ${s} ${schema}`), { id: ++id }),
    deleteSubject: async (s: string) => void calls.push(`delete ${s}`),
  } as unknown as RegistryClient;
  return { client, calls };
}

describe('checkHistory', () => {
  it('registers every version in order under the level, into a throw-away subject, then deletes it', async () => {
    const f = fake();
    const r = await checkHistory(f.client, spec(2), { prefix: 'tmp' });
    expect(r).toMatchObject({ ok: true, level: 'FULL_TRANSITIVE' });
    expect(f.calls).toEqual([
      'config tmp-events.raw-value FULL_TRANSITIVE',
      'test tmp-events.raw-value s1',
      'register tmp-events.raw-value s1',
      'test tmp-events.raw-value s2',
      'register tmp-events.raw-value s2',
      'delete tmp-events.raw-value',
    ]);
  });

  it('stops at the first rejected version and reports the registry reasons', async () => {
    const f = fake({ incompatible: ['s2'] });
    const r = await checkHistory(f.client, spec(3), { prefix: 'tmp' });
    expect(r.ok).toBe(false);
    expect(r.steps.map((s) => `${s.file}:${s.ok}`)).toEqual(['events/v1.avsc:true', 'events/v2.avsc:false']);
    expect(r.steps[1].messages).toEqual(['s2 is incompatible']);
    expect(f.calls).not.toContain('test tmp-events.raw-value s3');
  });

  it('a candidate is tested on top of the history (and only if the history is fine)', async () => {
    const ok = await checkHistory(fake().client, spec(2), { prefix: 'tmp', candidate: { file: 'fixtures/x.avsc', schema: 'cand' } });
    expect(ok.candidate).toMatchObject({ file: 'fixtures/x.avsc', ok: true });
    const bad = await checkHistory(fake({ incompatible: ['cand'] }).client, spec(2), { prefix: 'tmp', candidate: { file: 'fixtures/x.avsc', schema: 'cand' } });
    expect(bad.candidate).toMatchObject({ ok: false });
    const broken = await checkHistory(fake({ incompatible: ['s1'] }).client, spec(2), { prefix: 'tmp', candidate: { file: 'fixtures/x.avsc', schema: 'cand' } });
    expect(broken.candidate).toBeUndefined();
  });

  it('a level override (BACKWARD for comparison) is applied to the throw-away subject', async () => {
    const f = fake();
    await checkHistory(f.client, spec(1), { prefix: 'tmp', level: 'BACKWARD' });
    expect(f.calls[0]).toBe('config tmp-events.raw-value BACKWARD');
  });

  it('the throw-away subject is deleted even when the registry fails mid-way; the error propagates (exit 2)', async () => {
    const f = fake({ failOn: 's2' });
    await expect(checkHistory(f.client, spec(2), { prefix: 'tmp' })).rejects.toBeInstanceOf(RegistryUnavailableError);
    expect(f.calls.at(-1)).toBe('delete tmp-events.raw-value');
  });
});

describe('register (deploy)', () => {
  it('sets the REAL subject level, registers in order, returns the ids', async () => {
    const f = fake();
    const r = await register(f.client, spec(2));
    expect(r.ok).toBe(true);
    expect(r.steps.map((s) => s.id)).toEqual([101, 102]);
    expect(f.calls[0]).toBe('config events.raw-value FULL_TRANSITIVE');
    expect(f.calls.filter((c) => c.startsWith('delete'))).toEqual([]); // never deletes real subjects
  });

  it('a registry 409 (incompatible) on register is a failed step, not a crash', async () => {
    const f = fake();
    (f.client as any).register = async () => {
      throw new RegistryHttpError(409, 409, 'Schema being registered is incompatible');
    };
    const r = await register(f.client, spec(1));
    expect(r.ok).toBe(false);
    expect(r.steps[0].messages[0]).toMatch(/incompatible/);
  });
});
