import type { OrchestrationDb } from '../../db/orchestration-db'
import type { StructuredMailResend } from '../structured-mail/structured-mail-custody'

export class StructuredMailboxRedriveState {
  private readonly inFlight = new Map<string, number | (() => number)>()
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
    if (throughSequence !== Infinity && workSequence(offering) > throughSequence) {
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

  beforeWork(mailbox: string, oldest: number | (() => number)): void {
    this.inFlight.set(mailbox, oldest)
    const waiters = this.idleWaiters.get(mailbox)
    if (!waiters) {
      return
    }
    const sequence = workSequence(oldest)
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

function workSequence(work: number | (() => number)): number {
  return typeof work === 'number' ? work : work()
}

/** Ignore stable cards and retries already attempted in this pass, not the rest of the queue. */
export function oldestStructuredWork(
  db: OrchestrationDb,
  mailbox: string,
  outstanding: ReadonlySet<string>,
  retries: readonly StructuredMailResend[],
  from: number
): number {
  const settled = new Set(outstanding)
  for (let i = from; i < retries.length; i++) {
    settled.delete(retries[i].row.message_id)
  }
  return db.oldestStructuredMailboxWork(mailbox, [...settled])
}
