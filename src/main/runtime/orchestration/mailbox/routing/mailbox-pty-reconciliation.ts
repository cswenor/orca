type Reconciliation = {
  completion: Promise<ReadonlySet<string>>
  notified: Set<string>
  more: boolean
}

/** Keep the same setImmediate page boundaries while allowing startup to observe the entire pass. */
export class MailboxPtyReconciliation {
  private readonly pending = new Map<string, Reconciliation>()

  constructor(private readonly routePage: (ptyId: string, notified: Set<string>) => void) {}

  completion(ptyId: string): Promise<ReadonlySet<string>> | undefined {
    return this.pending.get(ptyId)?.completion
  }

  record(ptyId: string, mailbox: string): void {
    this.pending.get(ptyId)?.notified.add(mailbox)
  }

  schedule(ptyId: string): void {
    const pending = this.pending.get(ptyId)
    if (pending) {
      pending.more = true
      return
    }
    const { promise, resolve } = Promise.withResolvers<ReadonlySet<string>>()
    const state: Reconciliation = { completion: promise, notified: new Set(), more: false }
    this.pending.set(ptyId, state)
    const advance = () => {
      state.more = false
      try {
        this.routePage(ptyId, state.notified)
      } catch (error) {
        console.warn(`[orchestration] failed to continue routing terminal ${ptyId}`, error)
      }
      if (state.more) {
        setImmediate(advance)
        return
      }
      this.pending.delete(ptyId)
      resolve(state.notified)
    }
    setImmediate(advance)
  }
}
