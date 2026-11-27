import { CircuitBreaker } from './breaker.js';

describe('CircuitBreaker', () => {
  let t = 0;
  const mk = (failures = 3, cooldown = 1000) => new CircuitBreaker(failures, cooldown, () => t);
  beforeEach(() => (t = 0));

  it('stays closed below the failure threshold, success resets the count', () => {
    const b = mk();
    b.onFailure();
    b.onFailure();
    expect(b.state()).toBe('closed');
    b.onSuccess();
    b.onFailure();
    b.onFailure();
    expect(b.state()).toBe('closed');
  });

  it('opens after N consecutive failures and rejects during the cooldown', () => {
    const b = mk();
    for (let i = 0; i < 3; i++) b.onFailure();
    expect(b.state()).toBe('open');
    expect(b.tryAcquire()).toBe(false);
    t = 999;
    expect(b.tryAcquire()).toBe(false);
  });

  it('after the cooldown lets exactly one probe through; probe success closes it', () => {
    const b = mk();
    for (let i = 0; i < 3; i++) b.onFailure();
    t = 1000;
    expect(b.state()).toBe('half-open');
    expect(b.tryAcquire()).toBe(true); // the probe
    expect(b.tryAcquire()).toBe(false); // everyone else still rejected
    b.onSuccess();
    expect(b.state()).toBe('closed');
    expect(b.tryAcquire()).toBe(true);
  });

  it('probe failure opens it again for a new cooldown', () => {
    const b = mk();
    for (let i = 0; i < 3; i++) b.onFailure();
    t = 1000;
    expect(b.tryAcquire()).toBe(true);
    b.onFailure();
    expect(b.state()).toBe('open');
    t = 1999;
    expect(b.tryAcquire()).toBe(false);
    t = 2000;
    expect(b.tryAcquire()).toBe(true);
  });

  it('failures = 0 disables it', () => {
    const b = mk(0);
    for (let i = 0; i < 100; i++) b.onFailure();
    expect(b.state()).toBe('closed');
    expect(b.tryAcquire()).toBe(true);
  });

  it('forceOpen opens at once and restarts the cooldown; no-op when disabled', () => {
    let t = 0;
    const b = new CircuitBreaker(3, 1000, () => t);
    b.forceOpen();
    expect(b.state()).toBe('open');
    t = 900;
    b.forceOpen(); // still down: cooldown restarts
    t = 1500;
    expect(b.state()).toBe('open');
    t = 1900;
    expect(b.state()).toBe('half-open');
    const off = new CircuitBreaker(0, 1000);
    off.forceOpen();
    expect(off.state()).toBe('closed');
  });
});
