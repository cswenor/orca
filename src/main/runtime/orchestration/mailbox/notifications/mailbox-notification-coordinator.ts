import type { OrchestrationDb } from '../../db/orchestration-db'
import type {
  OrchestrationMailboxOwner,
  OrchestrationMailboxTerminal,
  RoutedOrchestrationMailbox
} from '../routing/mailbox-owner'
import type { OrchestrationMailboxPointerDelivery } from '../terminal-pointer/mailbox-pointer-delivery'
import type { OrchestrationMessageWaiter } from '../terminal-pointer/mailbox-pointer-eligibility'
import {
  hasUnfilteredOrchestrationWaiter,
  selectOrchestrationPointerBatch
} from '../terminal-pointer/mailbox-pointer-eligibility'
import type { OrchestrationStructuredMailboxPointerDelivery } from '../structured-pointer/structured-mailbox-pointer-delivery'

type NotificationCoordinatorDependencies<TWaiter extends OrchestrationMessageWaiter> = {
  mailboxOwner: OrchestrationMailboxOwner
  pointerDelivery: OrchestrationMailboxPointerDelivery<TWaiter>
  /** Sibling lane for workers that ARE a structured session; it has no PTY to type into. */
  structuredPointerDelivery?: OrchestrationStructuredMailboxPointerDelivery<TWaiter>
  getDb: () => OrchestrationDb | null
  getTerminalForHandle: (handle: string) => OrchestrationMailboxTerminal | null
  getPaneKeyForHandle: (handle: string) => string | undefined
  getMessageWaiters: (mailboxHandle: string) => ReadonlySet<TWaiter> | undefined
  hasTerminalHandle: (handle: string) => boolean
  deliverForHandle: (handle: string, reservedTypes?: ReadonlySet<string>) => void
  notifyMessageArrived: (handle: string, messageType: string) => void
  /** Waiters resolved together share one wake, so they share the batch the first one takes. */
  resolveMessageWaiters: (waiters: readonly TWaiter[]) => void
}

export class OrchestrationMailboxNotificationCoordinator<
  TWaiter extends OrchestrationMessageWaiter
> {
  constructor(private readonly deps: NotificationCoordinatorDependencies<TWaiter>) {}

  deliverForHandle(
    handle: string,
    reservedTypes?: ReadonlySet<string>,
    through?: number
  ): Promise<void> | undefined {
    const { pointerDelivery, structuredPointerDelivery } = this.deps
    const terminal = structuredPointerDelivery?.deliverForHandle(handle, reservedTypes)
      ? undefined
      : pointerDelivery.deliverForHandle(handle, reservedTypes, through)
    if (through === undefined) {
      return undefined
    }
    return this.waitForRecovery(terminal?.mailbox ?? handle, through, terminal)
  }

  withdrawStaleChatMail(
    mailboxes: Iterable<string> | undefined,
    waitForRedrives: boolean
  ): Promise<void> | undefined {
    const { structuredPointerDelivery, getDb } = this.deps
    if (!structuredPointerDelivery || !waitForRedrives) {
      return structuredPointerDelivery?.withdrawStale(mailboxes)
    }
    const targets = new Set(mailboxes ?? getDb()?.getStructuredMailSendMailboxes())
    structuredPointerDelivery.withdrawStale(targets)
    return this.waitForRoutingRedrives(Promise.resolve(targets))
  }

  waitForRoutingRedrives(
    completion: Promise<ReadonlySet<string>>,
    through = this.deps.getDb()?.getMessageSequenceWatermark?.() ?? 0
  ): Promise<void> {
    return this.deps.mailboxOwner.waitForRoutingRedrives(completion, {
      whenIdle: (handle) => this.waitForRecovery(handle, through)
    })
  }

  private waitForRecovery(
    handle: string,
    through: number,
    pending = this.deps.pointerDelivery.pendingRecovery(handle, through),
    previousRouting?: Promise<ReadonlySet<string>>
  ): Promise<void> | undefined {
    const { routing, flight } = pending ?? {}
    const routed =
      routing && routing !== previousRouting
        ? this.waitForRoutingRedrives(routing, through)
        : undefined
    const structured = this.deps.structuredPointerDelivery?.whenIdle(handle, through)
    const settling = structured ? Promise.all([flight, structured]).then(() => true) : flight
    // The ordinary settlement stack can queue a redrive to a different owner after resolving
    // its flight. Observe it after that microtask has had a chance to admit its delivery.
    const successor = settling?.then(async (settled) => {
      if (!settled) {
        return
      }
      await Promise.resolve()
      return this.waitForRecovery(
        handle,
        through,
        this.deps.pointerDelivery.pendingRecovery(handle, through),
        routing
      )
    })
    return routed && successor
      ? Promise.all([routed, successor]).then(() => undefined)
      : (routed ?? successor)
  }

  deliverForTerminal(terminal: OrchestrationMailboxTerminal): void {
    this.notifyForwarded(this.deps.mailboxOwner.routeForeignDirectMessages(terminal))
    for (const mailboxHandle of this.deps.mailboxOwner.resolveAll(terminal)) {
      this.deps.pointerDelivery.deliver(terminal, { mailboxHandle })
    }
  }

  /** Whether delivery has work for this terminal: a pointer reservation to recover, or unread, undelivered mail no waiter or held batch claims. */
  hasDeliverableMail(terminal: OrchestrationMailboxTerminal): boolean {
    const db = this.deps.getDb()
    return (
      !!db &&
      this.deps.mailboxOwner.resolveAll(terminal).some((mailboxHandle) => {
        const waiters = this.deps.getMessageWaiters(mailboxHandle)
        return (
          // Mirrors deliver(): an unfiltered waiter preempts recovery too.
          !hasUnfilteredOrchestrationWaiter(waiters) &&
          (db.getPendingMailboxPointerMessages(mailboxHandle).length > 0 ||
            selectOrchestrationPointerBatch({
              db,
              mailboxHandle,
              waiters,
              reservedTypes: undefined,
              excludeRunOwnedDirect: mailboxHandle === terminal.handle
            }).messages.length > 0)
        )
      })
    )
  }

  notifyMessageArrived(
    handle: string,
    messageType?: string,
    waitForRecovery = false
  ): Promise<void> | undefined {
    const { mailboxHandle, forwarded, directTypes } = this.resolveArrivedMailboxes(handle)
    this.notifyForwarded(forwarded)
    if (!mailboxHandle) {
      return
    }
    const waiters = this.deps.getMessageWaiters(mailboxHandle)
    const arrivedTypes = directTypes ?? (messageType ? [messageType] : undefined)
    const consumers = waiters
      ? [...waiters].filter(
          (waiter) =>
            !arrivedTypes ||
            !waiter.typeFilter ||
            arrivedTypes.some((type) => waiter.typeFilter?.includes(type))
        )
      : []
    if (consumers.length === 0) {
      const reservedTypes = new Set(
        waiters ? [...waiters].flatMap((waiter) => waiter.typeFilter ?? []) : []
      )
      // Let a check awakened in this drain mark its rows read before the push re-reads them.
      if (waitForRecovery) {
        const through = this.deps.getDb()?.getMessageSequenceWatermark?.() ?? 0
        return Promise.resolve().then(() =>
          this.deliverForHandle(mailboxHandle, reservedTypes, through)
        )
      }
      queueMicrotask(() => this.deps.deliverForHandle(mailboxHandle, reservedTypes))
      return
    }
    this.deps.resolveMessageWaiters(consumers)
    return undefined
  }

  wakeRoutedMessageWaiters(mailboxHandle: string, types: readonly string[]): void {
    const waiters = [...(this.deps.getMessageWaiters(mailboxHandle) ?? [])]
    if (waiters.length === 0 || types.length === 0) {
      return
    }
    // Snapshot ownership before the routed rows wake and remove their waiters.
    queueMicrotask(() => {
      const liveWaiters = this.deps.getMessageWaiters(mailboxHandle)
      this.deps.resolveMessageWaiters(
        waiters.filter(
          (waiter) =>
            liveWaiters?.has(waiter) &&
            (!waiter.typeFilter || types.some((type) => waiter.typeFilter?.includes(type)))
        )
      )
    })
  }

  retirePty(ptyId: string): void {
    this.deps.pointerDelivery.retirePty(ptyId)
  }

  private resolveArrivedMailboxes(handle: string): {
    mailboxHandle: string | null
    forwarded: RoutedOrchestrationMailbox[]
    directTypes?: string[]
  } {
    if (handle.startsWith('run:') || handle.startsWith('dispatch:')) {
      return { mailboxHandle: handle, forwarded: [] }
    }
    if (!this.deps.getDb()) {
      return { mailboxHandle: handle, forwarded: [] }
    }
    if (!this.deps.hasTerminalHandle(handle)) {
      return this.resolveDetachedMailbox(handle)
    }
    const terminal = this.deps.getTerminalForHandle(handle)
    if (!terminal) {
      return this.resolveDetachedMailbox(handle)
    }
    return {
      mailboxHandle: this.deps.mailboxOwner.resolve(terminal, handle, {
        requireRequestedMail: true
      }),
      forwarded: this.deps.mailboxOwner.routeForeignDirectMessages(terminal)
    }
  }

  private resolveDetachedMailbox(handle: string): {
    mailboxHandle: string | null
    forwarded: RoutedOrchestrationMailbox[]
    directTypes?: string[]
  } {
    const routed = this.deps.mailboxOwner.routeDetachedDirectMessages(
      handle,
      this.deps.getPaneKeyForHandle(handle)
    )
    const directTypes = routed.hasMore
      ? []
      : (this.deps.getDb()?.getUnreadDirectMessageTypes(handle) ?? [])
    return {
      mailboxHandle: directTypes.length > 0 ? handle : null,
      forwarded: routed.mailboxes,
      directTypes
    }
  }

  private notifyForwarded(forwarded: readonly RoutedOrchestrationMailbox[]): void {
    for (const routed of forwarded) {
      for (const messageType of routed.types) {
        this.deps.notifyMessageArrived(routed.mailboxHandle, messageType)
      }
    }
  }
}
