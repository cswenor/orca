const MAIL_POINTER_REPOINT_DELAY_MS = 2_000

export class MailPointerRepointScheduler {
  private readonly timersByHandle = new Map<string, ReturnType<typeof setTimeout>>()
  private readonly completions = new Map<
    string,
    { through: number; completion: PromiseWithResolvers<void> }
  >()

  constructor(
    private readonly repoint: (handle: string, through?: number) => void | Promise<void>
  ) {}

  schedule(handle: string): void {
    if (this.timersByHandle.has(handle)) {
      return
    }
    const timer = setTimeout(() => {
      if (this.timersByHandle.get(handle) !== timer) {
        return
      }
      this.timersByHandle.delete(handle)
      const pending = this.completions.get(handle)
      this.completions.delete(handle)
      const redrive = this.repoint(handle, pending?.through)
      if (pending) {
        const { completion } = pending
        if (redrive) {
          void redrive.then(completion.resolve, completion.resolve)
        } else {
          completion.resolve()
        }
      }
    }, MAIL_POINTER_REPOINT_DELAY_MS)
    timer.unref?.()
    this.timersByHandle.set(handle, timer)
  }

  completion(handle: string, through: number): Promise<void> {
    let pending = this.completions.get(handle)
    if (!pending) {
      pending = { through, completion: Promise.withResolvers<void>() }
      this.completions.set(handle, pending)
    }
    return pending.completion.promise
  }

  /** Handles still awaiting a repoint; a quiet scheduler holds none. */
  get pendingCount(): number {
    return this.timersByHandle.size
  }

  clear(): void {
    for (const timer of this.timersByHandle.values()) {
      clearTimeout(timer)
    }
    this.timersByHandle.clear()
    for (const pending of this.completions.values()) {
      pending.completion.resolve()
    }
    this.completions.clear()
  }
}
