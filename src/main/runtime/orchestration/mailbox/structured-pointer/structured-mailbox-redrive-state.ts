export class StructuredMailboxRedriveState {
  private readonly inFlight = new Map<string, number>()
  private readonly rerun = new Map<string, Set<string>>()
  private readonly idleWaiters = new Map<string, Map<number, PromiseWithResolvers<void>>>()

  isInFlight(mailbox: string): boolean {
    return this.inFlight.has(mailbox)
  }

  begin(mailbox: string, reservedTypes?: ReadonlySet<string>): boolean {
    if (!this.inFlight.has(mailbox)) {
      this.inFlight.set(mailbox, 0)
      return true
    }
    const reserved = this.rerun.get(mailbox) ?? new Set<string>()
    reservedTypes?.forEach((type) => reserved.add(type))
    this.rerun.set(mailbox, reserved)
    return false
  }

  whenIdle(mailbox: string, throughSequence = Infinity): Promise<void> | undefined {
    const offering = this.inFlight.get(mailbox)
    if (offering === undefined) {
      return undefined
    }
    if (offering > throughSequence) {
      return undefined
    }
    let waiters = this.idleWaiters.get(mailbox)
    if (!waiters) {
      waiters = new Map()
      this.idleWaiters.set(mailbox, waiters)
    }
    let waiter = waiters.get(throughSequence)
    if (!waiter) {
      waiter = Promise.withResolvers<void>()
      waiters.set(throughSequence, waiter)
    }
    return waiter.promise
  }

  beforeOffering(mailbox: string, sequence: number): void {
    this.inFlight.set(mailbox, sequence)
    const waiters = this.idleWaiters.get(mailbox)
    if (!waiters) {
      return
    }
    for (const [through, waiter] of waiters) {
      if (through < sequence) {
        waiters.delete(through)
        waiter.resolve()
      }
    }
    if (waiters.size === 0) {
      this.idleWaiters.delete(mailbox)
    }
  }

  endOffer(mailbox: string): void {
    this.inFlight.set(mailbox, 0)
  }

  finish(mailbox: string): Set<string> | undefined {
    this.inFlight.delete(mailbox)
    const rerun = this.rerun.get(mailbox)
    if (rerun) {
      this.rerun.delete(mailbox)
    } else {
      this.idleWaiters.get(mailbox)?.forEach((waiter) => waiter.resolve())
      this.idleWaiters.delete(mailbox)
    }
    return rerun
  }
}
