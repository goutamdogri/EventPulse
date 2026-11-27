// IdempotencyStore on Redis (ioredis). Each operation is one Lua script, run with EVALSHA (ioredis loads it and
// falls back to EVAL on NOSCRIPT), so it is atomic on the Redis server.
import type { Redis } from 'ioredis';
import type { AcquireResult, IdempotencyStore } from './store.js';

export const LUA_ACQUIRE = `
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2]) then
  return {1}
end
return {0, redis.call('GET', KEYS[1])}`;

export const LUA_REPLACE = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
  return 1
end
return 0`;

export const LUA_REMOVE = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0`;

type Scripted = Redis & {
  idemAcquire(key: string, value: string, ttlS: number): Promise<[number, (string | null)?]>;
  idemReplace(key: string, expected: string, value: string, ttlS: number): Promise<number>;
  idemRemove(key: string, expected: string): Promise<number>;
};

export class RedisStore implements IdempotencyStore {
  private readonly r: Scripted;

  constructor(redis: Redis) {
    redis.defineCommand('idemAcquire', { numberOfKeys: 1, lua: LUA_ACQUIRE });
    redis.defineCommand('idemReplace', { numberOfKeys: 1, lua: LUA_REPLACE });
    redis.defineCommand('idemRemove', { numberOfKeys: 1, lua: LUA_REMOVE });
    this.r = redis as Scripted;
  }

  async acquire(key: string, value: string, ttlS: number): Promise<AcquireResult> {
    const [ok, current] = await this.r.idemAcquire(key, value, ttlS);
    return ok === 1 ? { acquired: true } : { acquired: false, current: current ?? null };
  }

  async replace(key: string, expected: string, value: string, ttlS: number): Promise<boolean> {
    return (await this.r.idemReplace(key, expected, value, ttlS)) === 1;
  }

  async remove(key: string, expected: string): Promise<boolean> {
    return (await this.r.idemRemove(key, expected)) === 1;
  }
}
