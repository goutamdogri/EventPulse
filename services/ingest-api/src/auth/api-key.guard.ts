import {
  CanActivate,
  ExecutionContext,
  Inject,
  Injectable,
  UnauthorizedException,
  createParamDecorator,
} from '@nestjs/common';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';

@Injectable()
export class ApiKeyGuard implements CanActivate {
  constructor(@Inject(CONFIG) private readonly cfg: AppConfig) {}

  canActivate(ctx: ExecutionContext): boolean {
    const req = ctx.switchToHttp().getRequest();
    const header: string | undefined = req.headers['authorization'];
    const m = header?.match(/^Bearer (.+)$/);
    const tenant = m ? this.cfg.apiKeys.get(m[1]) : undefined;
    if (!tenant) throw new UnauthorizedException('Missing or invalid API key');
    req.tenantId = tenant; // tenant comes from the key, never from the body
    return true;
  }
}

export const TenantId = createParamDecorator(
  (_d: unknown, ctx: ExecutionContext) =>
    ctx.switchToHttp().getRequest().tenantId as string,
);
