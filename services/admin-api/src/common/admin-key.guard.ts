import { CanActivate, ExecutionContext, Inject, Injectable, UnauthorizedException } from '@nestjs/common';
import { timingSafeEqual } from 'node:crypto';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';

/** Authorization: Bearer <ADMIN_API_KEY> (constant-time compare). Tenant API keys are NOT accepted here. */
@Injectable()
export class AdminKeyGuard implements CanActivate {
  private readonly key: Buffer;
  constructor(@Inject(CONFIG) cfg: AppConfig) {
    this.key = Buffer.from(cfg.adminApiKey);
  }

  canActivate(ctx: ExecutionContext): boolean {
    const m = (ctx.switchToHttp().getRequest().headers['authorization'] as string | undefined)?.match(/^Bearer (.+)$/);
    const given = Buffer.from(m?.[1] ?? '');
    if (given.length !== this.key.length || !timingSafeEqual(given, this.key)) throw new UnauthorizedException('Missing or invalid admin API key');
    return true;
  }
}
