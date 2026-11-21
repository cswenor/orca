export class StructuredMailboxRedriveState {
  private readonly inFlight = new Set<string>()
  private readonly rerun = new Map<string, Set<string>>()
  private readonly idleWaiters = new Map<string, PromiseWithResolvers<void>>()

  isInFlight(mailbox: string): boolean {
    return this.inFlight.has(mailbox)
  }

  begin(mailbox: string, reservedTypes?: ReadonlySet<string>): boolean {
    if (!this.inFlight.has(mailbox)) {
      this.inFlight.add(mailbox)
      return true
    }
    const reserved = this.rerun.get(mailbox) ?? new Set<string>()
    reservedTypes?.forEach((type) => reserved.add(type))
    this.rerun.set(mailbox, reserved)
    return false
  }

  whenIdle(mailbox: string): Promise<void> | undefined {
    if (!this.inFlight.has(mailbox)) {
      return undefined
    }
    let waiter = this.idleWaiters.get(mailbox)
    if (!waiter) {
      waiter = Promise.withResolvers<void>()
      this.idleWaiters.set(mailbox, waiter)
    }
    return waiter.promise
  }

  finish(mailbox: string): Set<string> | undefined {
    this.inFlight.delete(mailbox)
    const rerun = this.rerun.get(mailbox)
    if (rerun) {
      this.rerun.delete(mailbox)
    } else {
      this.idleWaiters.get(mailbox)?.resolve()
      this.idleWaiters.delete(mailbox)
    }
    return rerun
  }
}
