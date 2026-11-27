// Debug-only runtime control of the fault injector (Stage 3). Lets a test start the sink, warm it up, and only
// then arm a crash point or an injected error. Every route answers 404 unless DEBUG_FAULTS=true.
//
//   GET    /debug/faults                       what is armed, hit counters, batch number
//   POST   /debug/faults  {faults?, errors?}   replace what is armed (same syntax as FAULTS / FAULT_ERRORS)
//   DELETE /debug/faults                       disarm everything

import { BadRequestException, Body, Controller, Delete, Get, HttpCode, Inject, NotFoundException, Post } from '@nestjs/common';
import { FaultConfigError, FaultInjector } from '@eventpulse/fault-injector';

export const FAULT_INJECTOR = Symbol('FAULT_INJECTOR');

@Controller('debug/faults')
export class FaultsController {
  constructor(@Inject(FAULT_INJECTOR) private readonly faults: FaultInjector) {}

  private guard() {
    if (!this.faults.enabled) throw new NotFoundException();
  }

  @Get()
  get() {
    this.guard();
    return this.faults.describe();
  }

  @Post()
  @HttpCode(200)
  arm(@Body() body: { faults?: unknown; errors?: unknown }) {
    this.guard();
    const str = (v: unknown, name: string) => {
      if (v === undefined || v === null) return undefined;
      if (typeof v !== 'string') throw new BadRequestException(`${name} must be a string`);
      return v;
    };
    try {
      this.faults.arm({ faults: str(body?.faults, 'faults'), errors: str(body?.errors, 'errors') });
    } catch (e) {
      if (e instanceof FaultConfigError) throw new BadRequestException(e.message);
      throw e;
    }
    return this.faults.describe();
  }

  @Delete()
  @HttpCode(200)
  disarm() {
    this.guard();
    this.faults.disarm();
    return this.faults.describe();
  }
}
