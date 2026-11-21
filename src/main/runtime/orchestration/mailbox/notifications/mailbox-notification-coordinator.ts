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
    waitForRedrives = false
  ): Promise<void> | undefined {
    const { pointerDelivery, structuredPointerDelivery } = this.deps
    const terminal = structuredPointerDelivery?.deliverForHandle(handle, reservedTypes)
      ? undefined
      : pointerDelivery.deliverForHandle(handle, reservedTypes, waitForRedrives)
    if (!waitForRedrives) {
      return undefined
    }
    const direct = this.waitForTerminalRecovery(terminal)
    const structured = structuredPointerDelivery?.whenIdle(handle)
    // A structured pass can give mail back to a PTY. Its redrive starts before the
    // structured completion resolves, but the PTY flight won't exist when we start.
    const handback = structured?.then(() =>
      this.waitForTerminalRecovery(pointerDelivery.pendingRecovery(handle))
    )
    return direct && handback
      ? Promise.all([direct, handback]).then(() => undefined)
      : (direct ?? handback)
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

  waitForRoutingRedrives(completion: Promise<ReadonlySet<string>>): Promise<void> {
    return this.deps.mailboxOwner.waitForRoutingRedrives(completion, {
      whenIdle: (handle) => {
        const { pointerDelivery, structuredPointerDelivery } = this.deps
        const structured = structuredPointerDelivery?.whenIdle(handle)
        const terminal = () => this.waitForTerminalRecovery(pointerDelivery.pendingRecovery(handle))
        return structured ? structured.then(terminal) : terminal()
      }
    })
  }

  private waitForTerminalRecovery(
    pending?: ReturnType<typeof this.deps.pointerDelivery.pendingRecovery>
  ): Promise<void> | undefined {
    const { routing, flight } = pending ?? {}
    if (!routing) {
      return flight
    }
    const routed = this.waitForRoutingRedrives(routing)
    return flight ? Promise.all([routed, flight]).then(() => undefined) : routed
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
        return Promise.resolve().then(() =>
          this.deliverForHandle(mailboxHandle, reservedTypes, true)
        )
      }
      queueMicrotask(() => this.deps.deliverForHandle(mailboxHandle, reservedTypes))
      return
    }
    this.deps.resolveMessageWaiters(consumers)
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
