import { BadRequestException, Body, Controller, Get, Headers, HttpCode, Param, Put, Res, UseGuards } from '@nestjs/common';
import type { Response } from 'express';
import { AdminKeyGuard } from '../common/admin-key.guard.js';
import { AdminMetrics } from '../observability/metrics.js';
import { BadIfMatchError, etagOf, parseIfMatch } from './if-match.js';
import { PutConfigDto, TENANT_ID_RE } from './tenant-config.dto.js';
import { TenantConfigService } from './tenant-config.service.js';

function tenantParam(id: string): string {
  if (!TENANT_ID_RE.test(id)) throw new BadRequestException('tenantId must be 1-64 of [A-Za-z0-9_.-]');
  return id;
}

@Controller('v1/admin/tenants/:tenantId/config')
@UseGuards(AdminKeyGuard)
export class TenantConfigController {
  constructor(private readonly svc: TenantConfigService, private readonly metrics: AdminMetrics) {}

  @Get()
  async get(@Param('tenantId') tenantId: string, @Res({ passthrough: true }) res: Response) {
    const v = await this.svc.get(tenantParam(tenantId));
    res.setHeader('ETag', etagOf(v.version));
    return v;
  }

  /** Replaces the whole config document. Validation errors are counted by the exception filter. */
  @Put()
  @HttpCode(200)
  async put(@Param('tenantId') tenantId: string, @Body() body: PutConfigDto, @Headers('if-match') ifMatchHeader: string | undefined, @Res({ passthrough: true }) res: Response) {
    let ifMatch;
    try {
      ifMatch = parseIfMatch(ifMatchHeader);
    } catch (e) {
      if (e instanceof BadIfMatchError) throw new BadRequestException(e.message);
      throw e;
    }
    const end = this.metrics.putSeconds.startTimer();
    const plain = JSON.parse(JSON.stringify(body.config)) as Record<string, unknown>; // class instance -> stored document
    const r = await this.svc.put(tenantParam(tenantId), plain, ifMatch);
    end();
    this.metrics.puts.labels('ok').inc();
    res.setHeader('ETag', etagOf(r.version));
    return r;
  }
}
