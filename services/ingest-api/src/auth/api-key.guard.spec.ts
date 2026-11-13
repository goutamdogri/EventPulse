import { UnauthorizedException } from '@nestjs/common';
import { ApiKeyGuard } from './api-key.guard.js';

const ctx = (headers: any, req: any = { headers }) =>
  ({ switchToHttp: () => ({ getRequest: () => req }) }) as any;

const guard = new ApiKeyGuard({ apiKeys: new Map([['k1', 't1']]) } as any);

describe('ApiKeyGuard', () => {
  it('sets tenantId for a valid key', () => {
    const req: any = { headers: { authorization: 'Bearer k1' } };
    expect(guard.canActivate(ctx(null, req))).toBe(true);
    expect(req.tenantId).toBe('t1');
  });

  it('rejects missing header', () =>
    expect(() => guard.canActivate(ctx({}))).toThrow(UnauthorizedException));

  it('rejects wrong key', () =>
    expect(() =>
      guard.canActivate(ctx({ authorization: 'Bearer zz' })),
    ).toThrow(UnauthorizedException));
});
