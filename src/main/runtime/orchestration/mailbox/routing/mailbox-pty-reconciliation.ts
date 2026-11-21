type Reconciliation = {
  observers: Map<number, PromiseWithResolvers<ReadonlySet<string>>>
  notified: Set<string>
  nextSequence: number
  paneKey?: string
}

/** Follow ordinary page boundaries without joining mail beyond a startup observer's cutoff. */
export class MailboxPtyReconciliation {
  private readonly pending = new Map<string, Reconciliation>()

  constructor(
    private readonly routePage: (address: string, notified: Set<string>, paneKey?: string) => void
  ) {}

  completion(ptyId: string, through: number): Promise<ReadonlySet<string>> | undefined {
    const state = this.pending.get(ptyId)
    if (!state || state.nextSequence > through) {
      return undefined
    }
    let observer = state.observers.get(through)
    if (!observer) {
      observer = Promise.withResolvers<ReadonlySet<string>>()
      state.observers.set(through, observer)
    }
    return observer.promise
  }

  record(ptyId: string, mailbox: string): void {
    this.pending.get(ptyId)?.notified.add(mailbox)
  }

  schedule(ptyId: string, nextSequence = 0, paneKey?: string): void {
    const pending = this.pending.get(ptyId)
    if (pending) {
      pending.nextSequence = Math.min(pending.nextSequence, nextSequence)
      return
    }
    const state: Reconciliation = {
      observers: new Map(),
      notified: new Set(),
      nextSequence,
      paneKey
    }
    this.pending.set(ptyId, state)
    const advance = () => {
      state.nextSequence = Infinity
      try {
        this.routePage(ptyId, state.notified, state.paneKey)
      } catch (error) {
        console.warn(`[orchestration] failed to continue routing terminal ${ptyId}`, error)
      }
      const hasMore = state.nextSequence !== Infinity
      for (const [through, observer] of state.observers) {
        if (state.nextSequence > through) {
          state.observers.delete(through)
          observer.resolve(hasMore ? new Set(state.notified) : state.notified)
        }
      }
      if (hasMore) {
        setImmediate(advance)
      } else {
        this.pending.delete(ptyId)
      }
    }
    setImmediate(advance)
  }
}
