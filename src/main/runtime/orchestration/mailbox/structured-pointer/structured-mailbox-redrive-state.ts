export class StructuredMailboxRedriveState {
  private readonly inFlight = new Set<string>()
  private readonly rerun = new Map<string, Set<string>>()
  private readonly idleWaiters = new Map<string, Map<number, PromiseWithResolvers<void>>>()

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

  whenIdle(mailbox: string, watermark?: () => number | undefined): Promise<void> | undefined {
    if (!this.inFlight.has(mailbox)) {
      return undefined
    }
    const throughSequence = watermark?.() ?? Infinity
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

  join(mailboxes: Iterable<string>): Promise<void> | undefined {
    const pending = new Set<Promise<void>>()
    for (const mailbox of mailboxes) {
      const redrive = this.whenIdle(mailbox)
      if (redrive) {
        pending.add(redrive)
      }
    }
    return pending.size ? Promise.all(pending).then(() => undefined) : undefined
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
