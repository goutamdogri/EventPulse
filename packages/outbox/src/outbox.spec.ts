import { randomUUID } from 'node:crypto';
import { ENQUEUE_SQL, OUTBOX_CHANNEL, OutsideTransactionError, enqueue } from './enqueue.js';
import { VersionedView, applyIfNewer } from './apply.js';
import { ConfigChangedSerde, fromConfigRecord, toConfigRecord } from './config-events.js';
import { CLAIM_SQL, LOCK_SQL, toMessages } from './relay.js';

/** A fake pg client: answers txid_current_if_assigned() with `xid`, records every query. */
function fakeTx(xid: string | null) {
  const calls: { text: string; values?: unknown[] }[] = [];
  return {
    calls,
    async query(text: string, values?: unknown[]) {
      calls.push({ text, values });
      if (text.includes('txid_current_if_assigned')) return { rows: [{ xid }] };
      if (text === ENQUEUE_SQL) return { rows: [{ id: '42' }] };
      return { rows: [] };
    },
  };
}

const msg = { aggregateId: 't1', topic: 'config.changed', key: 't1', payload: Buffer.from('x'), headers: { event_id: 'e', version: '3' } };

describe('enqueue', () => {
  it('refuses to run outside a transaction (autocommit / nothing written yet): throws, writes nothing', async () => {
    const tx = fakeTx(null);
    await expect(enqueue(tx, msg)).rejects.toBeInstanceOf(OutsideTransactionError);
    expect(tx.calls.map((c) => c.text)).toEqual(['SELECT txid_current_if_assigned() AS xid']);
  });

  it('inside a writing transaction: one INSERT with the caller\'s client, then NOTIFY (delivered at commit)', async () => {
    const tx = fakeTx('1234');
    expect(await enqueue(tx, msg)).toBe('42');
    expect(tx.calls[1]).toEqual({ text: ENQUEUE_SQL, values: ['t1', 'config.changed', 't1', msg.payload, JSON.stringify(msg.headers)] });
    expect(tx.calls[2]).toEqual({ text: 'SELECT pg_notify($1, $2)', values: [OUTBOX_CHANNEL, 'config.changed'] });
  });

  it('notify: false skips the NOTIFY', async () => {
    const tx = fakeTx('1');
    await enqueue(tx, msg, { notify: false });
    expect(tx.calls).toHaveLength(2);
  });
});

describe('applyIfNewer', () => {
  const ev = (version: number, eventId = randomUUID()) => ({ eventId, version, value: { v: version } });

  it('applies only a newer version; equal and older are stale', () => {
    const cur = { version: 5, value: { v: 5 }, eventId: 'a' };
    expect(applyIfNewer(cur, ev(6)).outcome).toBe('applied');
    expect(applyIfNewer(cur, ev(6)).state!.version).toBe(6);
    expect(applyIfNewer(cur, ev(5))).toEqual({ state: cur, outcome: 'stale' });
    expect(applyIfNewer(cur, ev(4))).toEqual({ state: cur, outcome: 'stale' });
  });

  it('first event applies whatever its version', () => {
    expect(applyIfNewer(undefined, ev(9)).outcome).toBe('applied');
  });

  it('a republished copy (same event_id) is a duplicate, never re-applied', () => {
    const e = ev(7);
    const cur = applyIfNewer(undefined, e).state;
    expect(applyIfNewer(cur, e).outcome).toBe('duplicate');
    expect(applyIfNewer({ version: 9, value: {} }, e, new Set([e.eventId])).outcome).toBe('duplicate');
  });

  it('VersionedView: duplicates and reordering converge on the latest version', () => {
    const view = new VersionedView();
    const events = Array.from({ length: 50 }, (_, i) => ev(i + 1));
    const delivered = [...events, ...events.slice(10, 20)].sort(() => Math.random() - 0.5); // shuffled + 10 duplicates
    for (const e of delivered) view.apply('t1', e);
    expect(view.get('t1')!.version).toBe(50);
    expect(view.counts.duplicate).toBe(10);
    expect(view.counts.applied + view.counts.stale + view.counts.duplicate).toBe(60);
  });
});

describe('config.changed', () => {
  const e = { eventId: randomUUID(), tenantId: 'acme', version: 3, changedAt: new Date('2026-11-28T10:00:00.123Z'), config: { rate_limit_per_s: 50, features: { a: true } } };

  it('record mapping round trip', () => {
    expect(fromConfigRecord(toConfigRecord(e))).toEqual(e);
  });

  it('Avro round trip with the local schema (no registry: known writer id)', async () => {
    const s = new ConfigChangedSerde(undefined);
    s.serde.useWriterId(77);
    const b = s.encode(e);
    expect(b[0]).toBe(0);
    expect(b.readUInt32BE(1)).toBe(77);
    expect(await s.decode(b)).toEqual(e);
  });

  it('encode rejects a value that does not fit the schema', () => {
    const s = new ConfigChangedSerde(undefined);
    s.serde.useWriterId(1);
    expect(() => s.encode({ ...e, version: 1.5 })).toThrow(/does not fit/);
  });
});

describe('relay', () => {
  const row = (id: string, topic: string, key: string) => ({ id, topic, msg_key: key, payload: Buffer.from(id), headers: { event_id: `e${id}` }, created_at: new Date(), attempts: 1 });

  it('toMessages: grouped by topic, id order (numeric), outbox_id header added, original headers kept', () => {
    const m = toMessages([row('10', 'a', 'k'), row('9', 'a', 'k'), row('11', 'b', 'k')]);
    expect([...m.keys()]).toEqual(['a', 'b']);
    expect(m.get('a')!.map((x) => x.headers.outbox_id)).toEqual(['9', '10']);
    expect(m.get('a')![0]).toMatchObject({ key: 'k', headers: { event_id: 'e9', outbox_id: '9' } });
  });

  it('claim and lock skip rows locked by another relay, oldest first, bounded', () => {
    expect(CLAIM_SQL).toMatch(/published_at IS NULL[\s\S]*ORDER BY id LIMIT \$1 FOR UPDATE SKIP LOCKED/);
    expect(CLAIM_SQL).toMatch(/attempts = attempts \+ 1/);
    expect(LOCK_SQL).toMatch(/published_at IS NULL ORDER BY id FOR UPDATE SKIP LOCKED/);
  });
});
